import json, uuid, asyncio
from js import console, fetch, Object, Date, JSON
from datetime import datetime, timezone, timedelta, date
from handling_failure import TemporaryError, retry_operation
from create_summary_and_transfer import transfer_verified_records, update_mobile_summary
from helper import get_date_paths, store_json_data, header_record_mapping, deep_to_py, footer_record_mapping, clean_email


AGE_API_CONCURRENCY = 1

AGE_TIMEOUT_SECONDS = 4


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
             console.warn("[MAIN] CRM Invoice is not a dict")
             return "", {"stage": "crm_pre_check", "message": "Invoice is not a dict"}

        api_url = env.API_CRM_BASE_URL
        auth_token = env.API_CRM_AUTH_TOKEN

        if not api_url or not auth_token:
            console.warn("[MAIN] CRM Missing credentials")
            return "", None

        name = str(invoice.get("customer_fname__c") or invoice.get("customer_lname__c") or "").strip()

        mobile = str(invoice.get("Customer_Number__c") or
                     invoice.get("customer_mobile__c") or
                     invoice.get("customer_phone_no2") or
                     "").strip().replace(" ", "")

        city = str(invoice.get("customer_city__c") or
                   invoice.get("billed_at_city") or
                   invoice.get("delivery_to_city") or "").strip()

        state = str(invoice.get("customer_state__c") or
                    invoice.get("billed_at_state") or
                    invoice.get("delivery_from_state") or "").strip()

        alt_mobile = str(invoice.get("customer_mobile__c") or
                         invoice.get("customer_phone_no2") or
                         "").strip().replace(" ", "")

        # Validate email
        email = clean_email(str(invoice.get("customer_email__c") or ""))

        # Validate pincode
        pincode = str(invoice.get("customer_pincode") or
                      invoice.get("billed_at_pincode") or
                      invoice.get("delivery_to_pincode") or
                      "").strip().replace(" ", "")

        if not (pincode.isdigit() and len(pincode) == 6):
            pincode = ""

        data = {
            "name": name,
            "mobile": mobile,
            "gender": str(invoice.get("customer_gender__c") or "").strip(),
            "email": email,
            "type": "Customer",
            "pincode": pincode,
            "branch": "",
            "city": city,
            "state": state,
            "alt_mobile": alt_mobile,
            "source": "Python",
            "status": True
        }

        body = json.dumps(deep_to_py(data))

        headers = Object.fromEntries(
            {"Authorization": auth_token, "Content-Type": "application/json"}.items()
        )

        options = Object.fromEntries(
            {"method": "POST", "headers": headers, "body": body}.items()
        )

        # ----------- API Call -----------
        try:
            resp = await asyncio.wait_for(fetch(api_url, options), timeout=20)
        except asyncio.TimeoutError:
            console.error("[MAIN] ❌ CRM Request Timeout")
            return "", {"stage": "crm_timeout", "message": "API Request timeout"}

        status = resp.status
        content_type = resp.headers.get("content-type", "").lower()

        # -------------------------------
        if "application/json" in content_type:
            try:
                resp_json = deep_to_py(await resp.json())
            except Exception:
                text_response = await resp.text()
                console.error(f"[MAIN] ❌ CRM JSON Parse Failed: {text_response}")
                return "", {
                    "stage": "crm_parse_error",
                    "status": status,
                    "message": text_response
                }
        else:
            text_response = await resp.text()
            console.error(f"[MAIN] ❌ Non-JSON Response: {text_response}")
            return "", {
                "stage": "crm_invalid_response",
                "status": status,
                "message": text_response
            }

        # -------------------------------
        if resp.status in (200, 201):
            if isinstance(resp_json, dict):
                data_obj = resp_json.get("data")
                if isinstance(data_obj, dict):
                    return str(data_obj.get("cus_id", "")), None
            return "", {
                "stage": "crm_success_but_no_id", "response": resp_json
            }

        # -------------------------------
        if resp.status == 400:
            if isinstance(resp_json, dict):
                if "ER_DUP_ENTRY" in str(resp_json):
                    return "", {"stage": "CRM Api Failed",
                                "message": "CRM Api error produced",
                                "sent_email": email,
                                "sent_mobile": mobile,
                                "crm_status": status,
                                "crm_response": resp_json
                                }

                errors = resp_json.get("errors") or []
                if isinstance(errors, list) and errors:
                    if isinstance(errors[0], dict):
                        return str(errors[0].get("cus_id", "")), None

        # -------------------------------
        if status == 503:
            return "", {
                "stage": "crm_service_unavailable", "message": "CRM service temporarily unavailable"
            }

        # -------------------------------
        return "", {
            "stage": "crm_error",
            "sent_email": email,
            "sent_mobile": mobile,
            "crm_status": status,
            "crm_response": resp_json
        }

    except Exception as e:
        console.error(f"[MAIN] ❌ CRM Exception >>> {str(e)}")
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
        console.error(f"[MAIN] ❌ CRM Errors >>> batch: {batch_id}")
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
                    record["item_quantity__c"] = 1
                    record["item_remarks1__c"] = sn

                records.append(record)

        except Exception as item_err:
            console.error(f"[MAIN] ❌ Error processing item: {item_err}")

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
        console.warn("⚠️ [MAIN] AGE APX configuration missing")
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
            console.error(f"[MAIN] ❌ AGE Failed >>> status: {resp.status}")
            return None

        response_js = await resp.json()
        response = deep_to_py(response_js)

        if response.get("StatusCode") != 0:
            console.warn(f"[MAIN] ⚠️ Age API error: {response.get('StatusMessage')}")
            return None

        data = response.get("Data") or []
        inward_raw = data[0].get("INWARD_DATE") if data else None

        if inward_raw:
            inward_date = datetime.strptime(str(inward_raw), "%Y%m%d").date()
            return (date.today() - inward_date).days
        return None

    except Exception as e:
        console.error(f"[MAIN] ❌ AGE Failed >>> SN: {serial_no} | error({type(e).__name__}): {e}")
        return None


async def fetch_age_for_serials(env, serials: set):
    if not serials:
        return {}

    semaphore = asyncio.Semaphore(AGE_API_CONCURRENCY)
    results = {}

    async def safe_fetch(sn):
        async with semaphore:
            try:
                age = await asyncio.wait_for(fetch_age_of_device(env, sn), timeout=AGE_TIMEOUT_SECONDS)
                results[sn] = age
            except asyncio.TimeoutError:
                console.warn(f"[MAIN] ❌ AGE API Timeout | sn={sn}")
                results[sn] = None

            except Exception as e:
                console.error(f"[MAIN] ❌ AGE API Error >>> sn: {sn} | error: {str(e)}")
                results[sn] = None

    await asyncio.gather(*[safe_fetch(sn) for sn in serials], return_exceptions=False)

    return results

#--------------------------------------------------------------------

async def r2_bucket_push(env, records):
    if not records:
        return

    mobile = str(records[0].get("customer_mobile__c") or
                 records[0].get("Customer_Number__c") or
                 records[0].get("customer_phone_no2") or "")

    # 🔁 STEP 1: Transfer old verified records
    if mobile:
        await transfer_verified_records(env, mobile, records)

    all_sn = {
        str(r.get("item_remarks1__c") or "").strip() for r in records if str(r.get("item_remarks1__c") or "").strip()
    }

    age_cache = {}
    if all_sn:
        try:
            age_cache = await fetch_age_for_serials(env, all_sn)
        except Exception as e:
            console.error(f"[MAIN] ❌ AGE Bulk fetch failure >>> {str(e)}")
            age_cache = {}

    batch_id = records[0].get("batch_id")

    upload_tasks = []
    year, month, day = get_date_paths()

    all_records = []
    all_sn = set()
    for record in records:
        pri_id = record.get("pri_id", "")
        email = str(record.get("customer_email__c", ""))
        customer_id = record.get("customerId")
        invoice_id = record.get("bill_transaction_no__c", "")
        serial_no = str(record.get("item_remarks1__c") or "")

        if serial_no:
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
        upload_tasks.append(store_json_data(env, record, history_key, metadata, "IMEI_BUCKET_PROD"))

        # Store ID file (POS_BUCKET_PROD)
        mob_pri_id_key = f"mobile/{mobile}/id/{pri_id}.json"
        upload_tasks.append(store_json_data(env, record, mob_pri_id_key, metadata, "POS_BUCKET_PROD"))

        if serial_no:
            all_sn.add(serial_no)
            # mobile/9876543210/imei/serial_no (POS_BUCKET_PROD)
            mob_imei_key = f"mobile/{mobile}/imei/{serial_no}.json"
            upload_tasks.append(store_json_data(env, record, mob_imei_key, metadata, "POS_BUCKET_PROD"))

            # imei/serial_no/id/pri_id.json (IMEI_BUCKET_PROD)
            imei_key = f"imei/{serial_no}/id/{pri_id}.json"
            upload_tasks.append(store_json_data(env, record, imei_key, metadata, "IMEI_BUCKET_PROD"))

        if customer_id:
            # (IMEI_BUCKET_PROD)
            cus_id_key = f"customer_id/{customer_id}/pri_id/{pri_id}.json"
            upload_tasks.append(store_json_data(env, record, cus_id_key, metadata, "IMEI_BUCKET_PROD"))

        if email:
            # (IMEI_BUCKET_PROD)
            email_key = f"email/{email}/pri_id/{pri_id}.json"
            upload_tasks.append(store_json_data(env, record, email_key, metadata, "IMEI_BUCKET_PROD"))

    # 🚦 STEP 4: Chunked uploads
    if not upload_tasks:
        console.log(f"[MAIN] ℹ️ No tasks to upload for batch_id={batch_id}")

    chunk_size = min(20, len(upload_tasks))
    upload_failed = False
    for i in range(0, len(upload_tasks), chunk_size):
        results = await asyncio.gather(*upload_tasks[i: i + chunk_size], return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                upload_failed = True
                console.error(f"[MAIN] ❌ Error in Upload chunk {i}: {res}")

    # 🚨 Abort if any upload failed
    if upload_failed:
        console.error(f"[MAIN] ❌ Partial R2 upload failure >>> batch_id: {batch_id}")
        raise TemporaryError("R2 Upload Failure")

    # 🔄 STEP 5: Rebuild summary
    if mobile:
        try:
            await update_mobile_summary(env, mobile, all_records)
            console.log(f"[MAIN] ✅ Mobile Summary Updated | batch_id={batch_id} | mobile={mobile}")

        except Exception as e:
            console.error(f"[MAIN] ❌ Mobile Summary Failed >>> batch_id: {batch_id} | error: {str(e)}")
            raise TemporaryError(f"Mobile Summary Update Failed: {str(e)}")

    if all_sn:
        cleaned_serials = [sn for sn in all_sn if sn]

        for sn in cleaned_serials:
            payload = {
                "batch_id": batch_id,
                "serial_number": sn,
                "replay_count": 0,
                "source": "summary-worker"
            }

            await retry_operation(
                lambda: env.POS_SUMMARY_QUEUE.send(payload),
                base_delay=1
            )

        console.log(
            f"[MAIN] 🚀 IMEI Summary queued | batch_id={batch_id} | count={len(cleaned_serials)}"
        )

#--------------------------------------------------------------------