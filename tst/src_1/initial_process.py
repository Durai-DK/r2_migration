from js import console, fetch, Object, Date
from key_mapping import *
import json

async def store_json_data(env, data, key, metadata=None, bucket_binding="POS_BUCKET"):
    if not key:
        raise ValueError("R2 object key cannot be empty")

    body = json.dumps(data, indent=2, ensure_ascii=False)

    try:
        bucket = getattr(env, bucket_binding, None)

        if not bucket:
            raise ValueError(f"Bucket binding {bucket_binding} not found in env")

        # Convert metadata to JS Object
        metadata_js = Object.fromEntries([[k, str(v)] for k, v in (metadata or {}).items()])
        http_meta_js = Object.fromEntries([["contentType", "application/json"]])

        # Added httpMetadata for dashboard preview (Object Preview available)
        await bucket.put(key, body, customMetadata=metadata_js, httpMetadata=http_meta_js)
        # console.log(f" ✅ Stored R2 ({bucket_binding}): {key}")
    except Exception as e:
        console.error(f"❌ Failed to store {key} in {bucket_binding}: {e}")
        raise


async def read_json_data(env, key, bucket_binding="POS_BUCKET"):
    try:
        bucket = getattr(env, bucket_binding, None)
        if not bucket:
            return None

        obj = await bucket.get(key)
        if not obj:
            return None

        text = await obj.text()
        return json.loads(text)
    except Exception as e:
        # Key not found logic usually returns null from get(), so exception means true error
        console.warn(f"⚠️ Failed to read {key} from {bucket_binding}: {e}")
        return None


def validate_pri_range(start_id, end_id, output_items):
    expected = end_id - start_id + 1
    
    total_sn_count = 0
    if isinstance(output_items, list):
        for item in output_items:
            sn_raw = str(item.get("item_remarks1") or "")
            sn_list = [s.strip() for s in sn_raw.split(",") if s.strip()]
            total_sn_count += max(len(sn_list), 1)
    else:
        total_sn_count = 1

    if expected != total_sn_count:
        raise ValueError(f"[Initial Process] PRI range mismatch: expected={expected}, actual={total_sn_count}")


def force_py(obj):
    if hasattr(obj, "to_py"):
        return force_py(obj.to_py())
    if isinstance(obj, dict):
        return {k: force_py(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [force_py(i) for i in obj]
    return obj


async def crm_customer_id(env, invoice):
    try:
        api_url = env.API_CRM_BASE_URL
        auth_token = env.API_CRM_AUTH_TOKEN

        if not api_url or not auth_token:
            console.warn("[CRM] Missing credentials")
            return "", None

        data = {
            "name": str(invoice.get("customer_fname__c") or invoice.get("customer_lname__c")),
            "mobile": str(invoice.get("Customer_Number__c") or invoice.get("customer_mobile__c") or invoice.get("customer_phone_no2")),
            "gender": str(invoice.get("customer_gender__c")),
            "email": str(invoice.get("customer_email__c")) ,
            "type": "Customer",
            "pincode": str(invoice.get("customer_pincode") or invoice.get("billed_at_pincode") or invoice.get("delivery_to_pincode")),
            "branch": "",
            "city": str(invoice.get("customer_city__c") or invoice.get("billed_at_city") or invoice.get("delivery_to_city")),
            "state": str(invoice.get("customer_state__c") or invoice.get("billed_at_state") or invoice.get("delivery_from_state")),
            "alt_mobile": str(invoice.get("customer_mobile__c") or invoice.get("customer_phone_no2")),
            "source": "Python",
            "status": True
        }

        body = force_py(data)
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
        resp_json = force_py(resp_json)

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


async def save_process_errors(env, pri_id, errors):
    if not errors or not pri_id:
        return

    year, month, day = get_date_paths()
    error_key = f"crm_error_response/{year}/{month}/{day}/{pri_id}.json"
    now = datetime.now(timezone.utc).isoformat()
    error_data = {"pri_id": pri_id, "timestamp": now, "errors": errors}
    metadata = {"pri_id": str(pri_id), "timestamp": datetime.now(timezone.utc).isoformat()}
    await store_json_data(env, error_data, error_key, metadata, bucket_binding="POS_BUCKET")


async def process_record(invoice, env, start_id, end_id):
    header = header_record_mapping(invoice)

    output_items = invoice.get("output")
    if not isinstance(output_items, list) or len(output_items) == 0:
        output_items = [{}]

    # Validate PRI range (count only)
    expected_records = 0
    for item in output_items:
        sn_raw = str(item.get("item_remarks1") or "")
        serials = [s.strip() for s in sn_raw.split(",") if s.strip()]
        expected_records += max(1, len(serials))

    validate_pri_range(start_id, end_id, output_items)

    # CRM lookup (header-level)
    cus_id, crm_err = await crm_customer_id(env, header)
    customer_id = str(cus_id) if cus_id else ""
    header["customerId"] = customer_id

    if not customer_id and crm_err:
        err = ({"pri_id": start_id, "type": "CRM_CUSTOMER_ERROR", "error": crm_err})
        console.error(f"[QUEUE] Errors for {start_id}")
        await save_process_errors(env, start_id, err)

    # Record expansion
    records = []
    current_pri_id = start_id
    for item in output_items:
        try:
            footer = footer_record_mapping(item)
            
            sn_raw = str(item.get("item_remarks1") or "")
            serial_nos = [s.strip() for s in sn_raw.split(",") if s.strip()] or [""]

            for sn in serial_nos:
                pri_id = current_pri_id
                
                # Use header for shallow copy
                record = header.copy()
                record.update(footer)
                
                record["pri_id"] = pri_id
                if sn:
                    record["item_remarks1__c"] = sn

                records.append(record)
                # console.log(f"[Initial Process] ✅ Stored PRI_ID={pri_id}")
                current_pri_id += 1

        except Exception as item_err:
            console.error(f"[Initial Process] ❌ Error processing item: {item_err}")

    # Final safety check
    # -------------------------------
    if current_pri_id - 1 != end_id:
        console.warn(
            f"[PROCESS] ⚠️ PRI consumption mismatch: "
            f"used={current_pri_id - start_id}, "
            f"expected={end_id - start_id + 1}"
        )

    return records
