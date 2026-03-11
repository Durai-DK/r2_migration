from js import console, fetch, Object, Date, JSON
from create_summary_and_transfer import *
from handling_failure import *
import uuid


def get_now():
    return datetime.fromtimestamp(time.time(), tz=timezone.utc)


def get_date_paths():
    now = get_now()
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

#--------------------------------------------------------------------

async def save_process_errors(env, pri_id, errors):

    if not errors or not pri_id:
        return

    year, month, day = get_date_paths()

    error_key = f"crm_error_response/{year}/{month}/{day}/{pri_id}.json"

    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    error_data = {"pri_id": pri_id, "timestamp": now, "errors": errors}
    metadata = {"pri_id": str(pri_id), "timestamp": now}

    await store_json_data(env, error_data, error_key, metadata, bucket_binding="POS_BUCKET_PROD")


async def crm_customer_id(env, invoice):
    try:
        if not isinstance(invoice, dict):
             console.warn("CRM: Invoice is not a dict")
             return "", {"stage": "crm_pre_check", "message": "Invoice is not a dict"}

        api_url = env.API_CRM_BASE_URL
        auth_token = env.API_CRM_AUTH_TOKEN

        if not api_url or not auth_token:
            console.warn("[CRM] Missing credentials")
            return "", None

        data = {
            "name": str(invoice.get("customer_fname__c") or invoice.get("customer_lname__c") or ""),
            "mobile": str(invoice.get("Customer_Number__c") or invoice.get("customer_mobile__c") or
                          invoice.get("customer_phone_no2") or ""),
            "gender": str(invoice.get("customer_gender__c") or ""),
            "email": str(invoice.get("customer_email__c") or ""),
            "type": "Customer",
            "pincode": str(invoice.get("customer_pincode") or invoice.get("billed_at_pincode") or
                           invoice.get("delivery_to_pincode") or ""),
            "branch": "",
            "city": str(invoice.get("customer_city__c") or invoice.get("billed_at_city") or
                        invoice.get("delivery_to_city") or ""),
            "state": str(invoice.get("customer_state__c") or invoice.get("billed_at_state") or
                         invoice.get("delivery_from_state") or ""),
            "alt_mobile": str(invoice.get("customer_mobile__c") or invoice.get("customer_phone_no2") or ""),
            "source": "Python",
            "status": True
        }

        body = deep_to_py(data)
        body = json.dumps(body)
        # console.log(f"[CRM] Sending payload: {body}")

        headers = Object.fromEntries(
            {"Authorization": auth_token, "Content-Type": "application/json"}.items()
        )

        options = Object.fromEntries(
            {"method": "POST", "headers": headers, "body": body}.items()
        )

        resp = await fetch(api_url, options)
        resp_json = await resp.json()
        resp_json = deep_to_py(resp_json)

        # console.log(f"[CRM] Response ({resp.status}): {json.dumps(resp_json)}")

        # Logic to parse success/failure
        if resp.status in (200, 201):
            if isinstance(resp_json, dict):
                data_obj = resp_json.get("data")
                if isinstance(data_obj, dict):
                    return str(data_obj.get("cus_id", "")), None
            return "", None

        if resp.status == 400:
            if isinstance(resp_json, dict):
                # Check for Duplicate Entry error (Code 1062)
                # Since we can't extract ID, we return a simpler duplicate error
                if "ER_DUP_ENTRY" in str(resp_json):
                    return "", {"stage": "crm_duplicate", "message": "Duplicate Customer Entry (Email/Mobile likely exists)", "details": "CRM did not return pre-existing cus_id"}

                errors = resp_json.get("errors") or []
                if isinstance(errors, list) and errors:
                    if isinstance(errors[0], dict):
                        return str(errors[0].get("cus_id", "")), None

        return "", {"stage": "crm", "status": resp.status, "response": resp_json}

    except Exception as e:
        console.error(f"CRM Exception: {e}")
        return "", {"stage": "crm_exception", "message": str(e)}


async def process_record(invoice, env, batch_id):
    header = header_record_mapping(invoice)

    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    header["created_At"] = now
    header["updated_At"] = now

    output_items = invoice.get("output")
    if not isinstance(output_items, list) or len(output_items) == 0:
        output_items = [{}]

    # CRM lookup (header-level)
    cus_id, crm_err = await crm_customer_id(env, header)
    customer_id = str(cus_id) if cus_id else ""
    header["customerId"] = customer_id

    if not customer_id and crm_err:
        err = ({"batch_id": batch_id, "type": "CRM_CUSTOMER_ERROR", "error": crm_err})
        console.error(f"[QUEUE] CRM Errors for batch {batch_id}")
        await save_process_errors(env, f"{batch_id}-ERR", err)

    # Record expansion
    records = []

    # Add batch_id to header
    header["batch_id"] = batch_id

    for item in output_items:
        try:
            footer = footer_record_mapping(item)

            sn_raw = str(item.get("item_remarks1") or "")
            serial_nos = [s.strip() for s in sn_raw.split(",") if s.strip()] or [""]

            for sn in serial_nos:
                pri_id = str(uuid.uuid4())

                # Use header for shallow copy
                record = header.copy()
                record.update(footer)

                record["pri_id"] = pri_id
                if sn:
                    record["item_remarks1__c"] = sn

                records.append(record)

        except Exception as item_err:
            console.error(f"[Initial Process] ❌ Error processing item: {item_err}")

    return records

#--------------------------------------------------------------------

async def fetch_age_of_device(env, serial_no):
    if not serial_no:
        return None

    # Securely access credentials and URL from environment
    security_code = getattr(env, "APX_SECURITY_CODE", "")
    user_id = getattr(env, "APX_USERID", "")
    base_url = getattr(env, "APX_API_URL", "")

    if not security_code or not user_id or not base_url:
        console.warn("⚠️ [AGE] APX configuration missing")
        return None

    try:
        headers = {"securitycode": security_code, "userid": user_id}

        params = f"?CompanyCode=pmpl&ImeiSerialNo={serial_no}"

        js_headers = Object.fromEntries([[k, str(v)] for k, v in headers.items()])

        options = Object.fromEntries([[k, v] for k, v in {
            "method": "GET",
            "headers": js_headers
        }.items()])

        resp = await fetch(base_url + params, options)
        if not resp.ok:
            console.error(f"[AGE] ❌ Fetch failed status={resp.status}")
            return None

        response_js = await resp.json()
        response = deep_to_py(response_js)

        if response.get("StatusCode") != 0:
            console.warn(f"[AGE] ⚠️ Age API error: {response.get('StatusMessage')}")
            return None

        # console.log(f"[AGE] Response ({resp.status}): {json.dumps(response)}")

        data = response.get("Data") or []
        inward_raw = data[0].get("INWARD_DATE") if data else None

        if inward_raw:
            inward_date = datetime.strptime(str(inward_raw), "%Y%m%d").date()
            return (date.today() - inward_date).days
        return None

    except Exception as e:
        console.error(f"[AGE] ❌ Age API Exception for SN {serial_no}: {type(e).__name__}: {e}")
        return None

IMEI_REBUILD_LIMIT = 3
imei_rebuild_semaphore = asyncio.Semaphore(IMEI_REBUILD_LIMIT)

async def safe_rebuild_imei(env, imei):
    async with imei_rebuild_semaphore:
        return await rebuild_imei_summary(env, imei)

#--------------------------------------------------------------------

async def r2_bucket_push(env, records):
    if not records:
        return

    mobile = str(records[0].get("customer_mobile__c") or
                 records[0].get("Customer_Number__c") or
                 records[0].get("customer_phone_no2") or "")

    if not mobile:
        if not records[0].get("customer_mobile__c"):
            raise FatalProcessingError("Missing mobile")

    # 🔁 STEP 1: Transfer old verified records
    if mobile:
        await transfer_verified_records(env, mobile, records)

    upload_tasks = []
    year, month, day = get_date_paths()

    all_records = []
    age_cache = {}
    all_sn = set()
    for record in records:
        pri_id = record.get("pri_id", "")
        email = str(record.get("customer_email__c", ""))
        customer_id = record.get("customerId")
        invoice_id = record.get("bill_transaction_no__c", "")
        serial_no = str(record.get("item_remarks1__c") or "")
        batch_id = str(record.get("batch_id") or "")

        if serial_no:
            if serial_no not in age_cache:
                try:
                    age_cache[serial_no] = await fetch_age_of_device(env, serial_no)
                except Exception as e:
                    console.error(f"[AGE] ❌ {serial_no}: {e}")
                    age_cache[serial_no] = None

            record["ageOfDevice"] = age_cache[serial_no]
        else:
            record["ageOfDevice"] = None

        # Process ---> emp_id & emp_name
        emp_id, emp_name = "", ""
        remarks = (record.get("item_remarks2__c") or "").strip()

        if remarks:
            parts = [p.strip() for p in remarks.split("-")]
            emp_id = parts[0]
            emp_name = parts[1] if len(parts) > 1 else ""

        record["emp_id"] = emp_id
        record["emp_name"] = emp_name

        # collect all records for mobile summary
        all_records.append(record)

        metadata = {
            "invoice-id": str(invoice_id or ""),
            "mobile": str(mobile or ""),
            "serial-no": str(serial_no or ""),
            "pri-id": str(pri_id or ""),
            "batch_id": str(batch_id or ""),
        }

        # History (IMEI_BUCKET_PROD)
        history_key = f"history/{year}/{month}/{day}/{pri_id}.json"
        upload_tasks.append(r2_store(env, record, history_key, metadata, "IMEI_BUCKET_PROD"))

        # Store ID file (POS_BUCKET_PROD)
        mob_pri_id_key = f"mobile/{mobile}/id/{pri_id}.json"
        upload_tasks.append(r2_store(env, record, mob_pri_id_key, metadata, "POS_BUCKET_PROD"))

        if serial_no:
            all_sn.add(serial_no)
            # mobile/9876543210/imei/serial_no (POS_BUCKET_PROD)
            mob_imei_key = f"mobile/{mobile}/imei/{serial_no}.json"
            upload_tasks.append(r2_store(env, record, mob_imei_key, metadata, "POS_BUCKET_PROD"))

            # imei/serial_no/id/pri_id.json (IMEI_BUCKET_PROD)
            imei_key = f"imei/{serial_no}/id/{pri_id}.json"
            upload_tasks.append(r2_store(env, record, imei_key, metadata, "IMEI_BUCKET_PROD"))

        if customer_id:
            # (IMEI_BUCKET_PROD)
            cus_id_key = f"customer_id/{customer_id}/pri_id/{pri_id}.json"
            upload_tasks.append(r2_store(env, record, cus_id_key, metadata, "IMEI_BUCKET_PROD"))

        if email:
            # (IMEI_BUCKET_PROD)
            email_key = f"email/{email}/pri_id/{pri_id}.json"
            upload_tasks.append(r2_store(env, record, email_key, metadata, "IMEI_BUCKET_PROD"))

    # 🚦 STEP 4: Chunked uploads
    chunk_size = 30
    for i in range(0, len(upload_tasks), chunk_size):
        results = await asyncio.gather(*upload_tasks[i: i + chunk_size], return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                console.error(f"[UPLOAD] ❌ Error in chunk {i}: {res}")

    # 🔄 STEP 5: Rebuild summary
    if mobile and all_records:
        await update_mobile_summary(env, mobile, all_records)
        console.log(f"[SUMMARY] ✅ Mobile summary updated | mobile={mobile} | new_records={len(all_records)}")

    if all_sn:
        results = await asyncio.gather(*[safe_rebuild_imei(env, imei) for imei in all_sn], return_exceptions=True)

        for imei, res in zip(all_sn, results):
            if isinstance(res, Exception):
                console.error(f"[REBUILD] ❌ IMEI {imei}: {res}")

        console.log(f"[REBUILD] ✅ IMEI summaries rebuilt | count={len(all_sn)} | serials={all_sn}")

#--------------------------------------------------------------------