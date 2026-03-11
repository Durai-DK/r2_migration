from key_mapping import header_record_mapping, footer_record_mapping
from logs.log_settings import error_log, success_log
import requests, os, json, concurrent.futures
from datetime import datetime, date, timezone
from fastapi.responses import JSONResponse
from typing import Any, Dict, Optional
from core.creds import r2_client

pos_bucket = "pos-transaction-test"
pos_imei_bucket = "pos-transaction-imei-test"
mobile_prefix = "mobile"
imei_prefix = "imei_numbers"

age_security_code = os.getenv("AGE_SECURITY_CODE", "")
age_user_id = os.getenv("AGE_USER_ID", "")
age_url = os.getenv("AGE_URL", "")

API_CRM_BASE_URL = os.getenv("API_CRM_BASE_URL", "")
API_CRM_AUTH_TOKEN = os.getenv("API_CRM_AUTH_TOKEN", "")

now = datetime.now(timezone.utc)
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")

def safe_float(val: Any) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def error_response(code: int, message: str, error: Any):
    return JSONResponse(
        status_code=code,
        content={"status": "failed", "message": message, "error": error},
    )

def safe_json_dumps(data: Any) -> str:
    if isinstance(data, (dict, list, str, int, float)):
        return json.dumps(data, indent=2, ensure_ascii=False)
    raise TypeError(f"Unsupported JSON type: {type(data)}")

def store_json_data(bucket: str, data: Any, key: str, metadata: Optional[Dict[str, str]] = None):
    if not key:
        raise ValueError("R2 object key cannot be empty")

    body = safe_json_dumps(data)
    r2 = r2_client()

    try:
        r2.put_object(Bucket=bucket,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=metadata or {})

        success_log.info(f"✅ Stored R2 Bucket | Key: {key} ")

    except Exception as e:
        error_log.exception(f"❌ Failed to store R2 data {key}: {e}")
        raise

def read_json_data(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    r2 = r2_client()
    try:
        response = r2.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except r2.exceptions.NoSuchKey:
        return None
    except Exception as e:
        error_log.warning(f"⚠️ Failed to read R2 data {key}: {e}")
        return None

def delete_r2_object(bucket: str, key: str) -> bool:
    r2 = r2_client()
    try:
        r2.delete_object(Bucket=bucket, Key=key)
        return True
    except r2.exceptions.NoSuchKey:
        return False
    except Exception as e:
        error_log.warning(f"⚠️ Failed to delete R2 data {key}: {e}")
        return False

def fetch_age_of_device(serial_no: str):
    if not serial_no:
        return None

    age_headers = {"securitycode": age_security_code, "userid": age_user_id}
    age_param = f"?CompanyCode=pmpl&ImeiSerialNo={serial_no}"

    try:
        response = requests.get(age_url, headers=age_headers, params=age_param).json()

        # ---- Validate API Status ----
        if response.get("StatusCode") != 0:
            error_log.warning(f"⚠️ Age API error: {response.get('StatusMessage')}")
            return None

        # ---- Validate Date ----
        data = response.get("Data") or []
        inward_raw = data[0].get("INWARD_DATE") if data else None

        if inward_raw:
            inward_date = datetime.strptime(str(inward_raw), "%Y%m%d").date()
            return (date.today() - inward_date).days
        return None

    except Exception as e:
        error_log.exception(f"❌ Age-of-device API failed : {str(e)}")
        return None

def crm_customer_id(invoice):
    try:
        data = {
                "name": str(invoice.get("customer_fname"))
                        or str(invoice.get("customer_lname")),

            "mobile": str(invoice.get("customer_mobile"))
                      or str(invoice.get("customer_phone_no1"))
                      or str(invoice.get("customer_phone_no2")),

            "gender": str(invoice.get("customer_gender")),
            "email": str(invoice.get("customer_email")),
            "type": "Customer",

            "pincode": str(invoice.get("customer_pincode"))
                       or str(invoice.get("billed_at_pincode"))
                       or str(invoice.get("delivery_to_pincode")),

            "branch": [str(invoice.get("store_code"))],

            "city": str(invoice.get("customer_city"))
                    or str(invoice.get("billed_at_city"))
                    or str(invoice.get("delivery_to_city")),

            "state": str(invoice.get("customer_state"))
                     or str(invoice.get("billed_at_state"))
                     or str(invoice.get("delivery_from_state")),

            "alt_mobile": str(invoice.get("customer_phone_no1"))
                          or str(invoice.get("customer_phone_no2")),

            "source": "Python",
            "status": True
        }

        headers = {
            "Authorization": API_CRM_AUTH_TOKEN,
            "Content-Type": "application/json"
        }

        resp = requests.post(API_CRM_BASE_URL, json=data, headers=headers).json()

        success_log.info(resp)

        if resp.get("status") in (200, 201):
            return resp.get("data", {}).get("customer_id", ""), None

        if resp.get("status") == 400:
            return resp.get("errors", [{}])[0].get("customerId", ""), None

        return "", {"stage": "crm", "response": resp}

    except Exception as e:
        error_log.exception("❌ CRM exception")
        return "", {"stage": "crm_exception", "message": str(e)}

def is_verified(record):
    required_fields = ["customer_mobile__c", "item_remarks1__c", "store_code__c", "Item_Code__c", "bill_transaction_no__c"]
    for key in required_fields:
        val = record.get(key)
        if val is None or str(val).strip() == "":
            return False
    return True

def get_verified_pri_ids_from_summary(mobile):
    summary_key = f"mobile/{mobile}/mobile_summary.json"
    summary = read_json_data(pos_bucket, summary_key)

    if not summary:
        return []

    verified_pri_ids = []
    for rec in summary.get("records", []):
        if is_verified(rec):
            pri_id = rec.get("pri_id")
            if pri_id:
                verified_pri_ids.append(pri_id)

    return verified_pri_ids

def transfer_old_record(mobile, pri_id):
    src = f"mobile/{mobile}/id/{pri_id}.json"
    dst = f"mobile/{mobile}/transfered/{pri_id}.json"

    old_data = read_json_data(pos_bucket, src)
    if not old_data:
        return

    store_json_data(pos_bucket, old_data, dst)
    delete_r2_object(pos_bucket, src)

    success_log.log(f"🔁 Transferred old record: {pri_id}")

def process_record(invoice, pri_id):
    # ------    CRM & Records Build   -------------------------------------------------
    customer_id, crm_error = crm_customer_id(invoice)

    error_bucket = []
    if not customer_id and crm_error:
        error_bucket.append({"pri_id": pri_id, "type": "CRM_CUSTOMER_ERROR", "error": crm_error, })

    # ------    Process emp_id & emp_name   -------------------------------------------
    emp_id, emp_name = "", ""
    remarks = (invoice.get("item_remarks2") or "").strip()
    if remarks:
        parts = [p.strip() for p in remarks.split("-")]
        emp_id = parts[0]
        emp_name = parts[1] if len(parts) > 1 else ""

    # ------    Final Record    -------------------------------------------------------
    header = header_record_mapping(invoice)
    footer = footer_record_mapping(invoice)
    record = {
        "pri_id": pri_id,
        "emp_id": emp_id,
        "emp_name": emp_name,
        "customerId": customer_id or "",
        **header,
        **footer
    }
    return record, error_bucket

def generate_upload_tasks(record, pri_id):
    upload_tasks = []
    base_record = record.copy()
    error_bucket = []

    mobile = base_record.get("customer_mobile__c", "")
    email = str(base_record.get("customer_email__c", ""))
    customer_id = str(base_record.get("customerId", ""))
    invoice_id = str(base_record.get("bill_transaction_no__c", ""))
    serial_raw = base_record.get("item_serial_no__c", "")
    serial_nos = [s.strip() for s in serial_raw.split(",") if s.strip()]

    age_cache = {}
    def get_age(sl_no):
        if sl_no not in age_cache:
            age_cache[sl_no] = fetch_age_of_device(sl_no)
        return age_cache[sl_no]

    if len(serial_nos) == 1:
        base_record["ageOfDevice"] = get_age(serial_nos[0])

    base_metadata = {
        "customer-id": str(customer_id),
        "invoice-id": invoice_id,
        "mobile": str(mobile),
        "serial-no": str(serial_raw),
        "pri_id": str(pri_id),
        "email": str(email),
    }

    # Store ID file (POS_BUCKET)
    mobile_pri_id_key = f"{mobile_prefix}/{mobile}/id/{pri_id}.json"
    upload_tasks.append(store_json_data(pos_bucket, base_record, mobile_pri_id_key, base_metadata))

    # History (IMEI_BUCKET)
    history_key = f"history/{year}/{month}/{day}/{pri_id}.json"
    upload_tasks.append(store_json_data(pos_imei_bucket, base_record, history_key, base_metadata))

    if customer_id:
        # POS_BUCKET
        cus_id_key = f"{mobile_prefix}/{mobile}/cus_id/{customer_id}.json"
        upload_tasks.append(store_json_data(pos_bucket, base_record, cus_id_key, base_metadata))

    if email:
        # POS_BUCKET
        email_key = f"{mobile_prefix}/{mobile}/email/{email}.json"
        upload_tasks.append(store_json_data(pos_bucket, base_record, email_key, base_metadata))

    for sn in serial_nos:
        sn_record = base_record.copy()
        sn_record["item_serial_no__c"] = sn
        sn_record["ageOfDevice"] = get_age(sn)

        imei_metadata = base_metadata.copy()
        imei_metadata["serial-no"] = sn

        # 1. mobile/mobile/imei/sn (POS_BUCKET)
        key_1 = f"{mobile_prefix}/{mobile}/imei/{sn}.json"
        upload_tasks.append(store_json_data(pos_bucket, sn_record, key_1, imei_metadata))

        # 2. imei/sn (IMEI_BUCKET)
        key_2 = f"{imei_prefix}/{sn}.json"
        upload_tasks.append(store_json_data(pos_imei_bucket, sn_record, key_2, imei_metadata))

    return upload_tasks, error_bucket

def rebuild_mobile_summary(mobile: str):
    r2 = r2_client()
    prefix = f"{mobile_prefix}/{mobile}/id/"
    
    final_records = []
    try:
        # Paginator to handle > 1000 objects if needed (though user said max 500)
        paginator = r2.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=pos_bucket, Prefix=prefix)
        
        for page in pages:
            if 'Contents' not in page:
                continue

            keys = [obj['Key'] for obj in page['Contents'] if obj['Key'].endswith('.json')]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_key = {executor.submit(read_json_data, pos_bucket, key): key for key in keys}
                for future in concurrent.futures.as_completed(future_to_key):
                    data = future.result()
                    if data:
                        final_records.append(data)

    except Exception as e:
        error_log.exception(f"❌ Failed to rebuild summary for {mobile}: {e}")
        return

    unique_invoices = {}
    for record in final_records:
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
        "records": final_records
    }
    
    summary_meta = {
        "mobile": mobile, 
        "records-count": str(len(final_records)),
        "invoice-count": str(len(unique_invoices)),
        "total-spends": str(spends),
    }

    summary_key = f"{mobile_prefix}/{mobile}/mobile_summary.json"
    store_json_data(pos_bucket, summary_data, summary_key, summary_meta)

def save_process_errors(pri_id, errors):
    if not errors or not pri_id:
        return

    error_key = f"crm_error_response/{pri_id}.json"

    error_data = {
        "pri_id": pri_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "errors": errors
    }

    metadata = {"pri_id": pri_id, "timestamp": datetime.now(timezone.utc).isoformat()}
    store_json_data(pos_bucket, error_data, error_key, metadata)

def push_to_bucket(invoice: Dict[str, Any]):
    pri_id = str(invoice.get("pri_id") or "")
    tasks, errors = generate_upload_tasks(invoice, pri_id)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(func, *args) for func, args in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                error_log.exception(f"Task failed in push_to_bucket: {e}")
                errors.append({"stage": "upload", "error": str(e)})

    mobile = invoice.get("customer_mobile__c") or ""
    if mobile:
        rebuild_mobile_summary(mobile)

    save_process_errors(pri_id, errors)
