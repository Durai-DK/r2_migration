from js import console, fetch, Object, Date, JSON
from datetime import date
from helper import *
import asyncio, uuid

def get_now():
    return datetime.fromtimestamp(time.time(), tz=timezone.utc)


def get_date_paths():
    now = get_now()
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")


def is_verified(record):
    if not record:
        return False
    # More robust mobile check
    mobile = record.get("customer_mobile__c") or record.get("Customer_Number__c") or record.get("customer_phone_no2")

    # Required fields for a "verified" record
    required_fields = ["item_remarks1__c", "store_code__c", "Item_Code__c", "bill_transaction_no__c"]

    if not mobile or str(mobile).strip() == "":
        return False

    for key in required_fields:
        val = record.get(key)
        if val is None or str(val).strip() == "":
            return False
    return True


async def get_verified_pri_ids_from_summary(env, mobile):
    summary_key = f"mobile/{mobile}/mobile_summary.json"
    summary = await read_json_data(env, summary_key)

    if not summary:
        return []

    verified_pri_ids = []
    for rec in summary.get("records", []):
        if is_verified(rec):
            pri_id = rec.get("pri_id")
            if pri_id:
                verified_pri_ids.append(pri_id)

    return verified_pri_ids


async def safe_transfer_old_record(env, mobile, pri_id):
    year, month, day = get_date_paths()

    src_key = f"mobile/{mobile}/id/{pri_id}.json"
    tgt = f"mobile/{mobile}/transferred/{year}/{month}/{day}/{pri_id}.json"

    old_data = await read_json_data(env, src_key)
    if not old_data:
        return  # already transferred or missing

    # Check if already transferred
    exists = await read_json_data(env, tgt)
    if exists:
        return

    await store_json_data(env, old_data, tgt)
    await env.POS_BUCKET.delete(src_key)

    console.log(f"[TRANSFER] 🔁 Transferred PRI_ID={pri_id}")


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


async def rebuild_mobile_summary(env, mobile):
    prefix = f"mobile/{mobile}/id/"

    try:
        # Async List from POS_BUCKET
        listed = await env.POS_BUCKET.list(prefix=prefix)
        keys = [obj.key for obj in listed.objects]

        # Read all files concurrently using helper (POS_BUCKET)
        file_contents = await asyncio.gather(*[
            read_json_data(env, k, bucket_binding="POS_BUCKET") for k in keys]
        )

        final_records = [d for d in file_contents if d]

        unique_invoices = {}
        for record in file_contents:
            if not record:
                continue

            invoice_no = record.get("bill_transaction_no__c")
            if not invoice_no:
                continue  # skip records without invoice

            if invoice_no not in unique_invoices:
                unique_invoices[invoice_no] = record

        unique_invoices = list(unique_invoices.values())

        spends = sum(safe_float(i.get("Bill_Grant_Total__c", 0)) for i in unique_invoices)

        summary_data = {
            "mobile": mobile,
            "total_records": len(final_records),
            "invoice_count": len(unique_invoices),
            "invoice_spends": spends,
            "records": final_records,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

        summary_meta = {
            "mobile": str(mobile),
            "records-count": str(len(final_records)),
            "invoice-count": str(len(unique_invoices)),
            "total-spends": f"{spends:.2f}",
        }

        summary_key = f"mobile/{mobile}/mobile_summary.json"
        # Store summary in POS_BUCKET
        await store_json_data(env, summary_data, summary_key, summary_meta, bucket_binding="POS_BUCKET")

    except Exception as e:
        console.error(f"Failed summary rebuild: {e}")


async def r2_bucket_push(env, records):
    if not records:
        return

    mobile = str(records[0].get("customer_mobile__c") or
                 records[0].get("Customer_Number__c") or
                 records[0].get("customer_phone_no2") or "")


    # 🔁 STEP 1: Transfer old verified records
    if mobile:
        try:
            # Existing verified PRI IDs from summary
            existing_verified_pri_ids = await get_verified_pri_ids_from_summary(env, mobile)

            # PRI IDs from current payload (verified only)
            current_verified_pri_ids = {
                str(r.get("pri_id")) for r in records if is_verified(r) and r.get("pri_id")
            }

            # Transfer only stale PRI IDs
            transfer_tasks = [
                safe_transfer_old_record(env, mobile, old_pri_id)
                for old_pri_id in existing_verified_pri_ids
                if str(old_pri_id) not in current_verified_pri_ids
            ]

            if transfer_tasks:
                await asyncio.gather(*transfer_tasks)

        except Exception as e:
            console.warn(
                f"[TRANSFER] ⚠️ Verified record transfer failed (mobile={mobile}) → {str(e)}"
            )

    # ⚡ STEP 2: Fetch Age of Device
    serials = {
        str(r.get("item_remarks1__c")) for r in records if r.get("item_remarks1__c")
    }

    age_cache = {}
    if serials:
        age_results = await asyncio.gather(
            *[fetch_age_of_device(env, sn) for sn in serials], return_exceptions=True
        )
        age_cache = {
            sn: (None if isinstance(res, Exception) else res)
            for sn, res in zip(serials, age_results)
        }

    # 🚀 STEP 3: Build upload tasks
    upload_tasks = []
    year, month, day = get_date_paths()
    for record in records:
        pri_id = record.get("pri_id", "")
        email = str(record.get("customer_email__c", ""))
        customer_id = record.get("customerId")
        invoice_id = record.get("bill_transaction_no__c", "")
        serial_no = str(record.get("item_remarks1__c") or "")
        batch_id = str(record.get("item_remarks1__c"))

        record["ageOfDevice"] = age_cache.get(serial_no)

        # Process ---> emp_id & emp_name
        emp_id, emp_name = "", ""
        remarks = (record.get("item_remarks2__c") or "").strip()
        if remarks:
            parts = [p.strip() for p in remarks.split("-")]
            emp_id = parts[0]
            emp_name = parts[1] if len(parts) > 1 else ""

        record["emp_id"] = emp_id
        record["emp_name"] = emp_name

        metadata = {
            "customer-id": str(customer_id or ""),
            "invoice-id": str(invoice_id or ""),
            "mobile": str(mobile or ""),
            "serial-no": str(serial_no or ""),
            "pri-id": str(pri_id or ""),
            "batch_id": str(batch_id or ""),
            "email": str(email or ""),
        }

        # History (IMEI_BUCKET)
        history_key = f"history/{year}/{month}/{day}/{pri_id}.json"
        upload_tasks.append(store_json_data(env, record, history_key, metadata, bucket_binding="IMEI_BUCKET"))

        # Store ID file (POS_BUCKET)
        mob_pri_id_key = f"mobile/{mobile}/id/{pri_id}.json"
        upload_tasks.append(store_json_data(env, record, mob_pri_id_key, metadata))

        if serial_no:
            # mobile/9876543210/imei/serial_no (POS_BUCKET)
            mob_imei_key = f"mobile/{mobile}/imei/{serial_no}.json"
            upload_tasks.append(store_json_data(env, record, mob_imei_key, metadata))

            # imei/serial_no (IMEI_BUCKET)
            imei_key = f"imei/{serial_no}.json"
            upload_tasks.append(store_json_data(env, record, imei_key, metadata, bucket_binding="IMEI_BUCKET"))

        if customer_id:
            # (IMEI_BUCKET)
            cus_id_key = f"customer_id/{customer_id}/pri_id/{pri_id}.json"
            upload_tasks.append(store_json_data(env, record, cus_id_key, metadata, bucket_binding="IMEI_BUCKET"))

        if email:
            # (IMEI_BUCKET)
            email_key = f"email/{email}/pri_id/{pri_id}.json"
            upload_tasks.append(store_json_data(env, record, email_key, metadata, bucket_binding="IMEI_BUCKET"))

    # 🚦 STEP 4: Chunked uploads
    chunk_size = 75
    for i in range(0, len(upload_tasks), chunk_size):
        await asyncio.gather(*upload_tasks[i: i + chunk_size])

    # 🔄 STEP 5: Rebuild summary
    if mobile:
        await rebuild_mobile_summary(env, mobile)


#--------------------------------------------------------------------
async def save_process_errors(env, pri_id, errors):
    if not errors or not pri_id:
        return

    year, month, day = get_date_paths()
    error_key = f"crm_error_response/{year}/{month}/{day}/{pri_id}.json"

    now = datetime.now(timezone.utc).isoformat()

    error_data = {"pri_id": pri_id, "timestamp": now, "errors": errors}
    metadata = {"pri_id": str(pri_id), "timestamp": datetime.now(timezone.utc).isoformat()}

    await store_json_data(env, error_data, error_key, metadata, bucket_binding="POS_BUCKET")


async def crm_customer_id(env, invoice):
    try:

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
            return str(resp_json.get("data", {}).get("cus_id", "")), None

        if resp.status == 400:
            errors = resp_json.get("errors") or []
            if errors:
                return str(errors[0].get("cus_id", "")), None

        return "", {"stage": "crm", "status": resp.status, "response": resp_json}

    except Exception as e:
        console.error(f"CRM Exception: {e}")
        return "", {"stage": "crm_exception", "message": str(e)}


async def process_record(invoice, env, batch_id):
    header = header_record_mapping(invoice)

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