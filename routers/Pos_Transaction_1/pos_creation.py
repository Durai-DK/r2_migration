import requests, os, json, re
from core.creds import r2_client
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from fastapi.responses import JSONResponse
from logs.log_settings import error_log, success_log
from key_mapping import header_record_mapping, footer_record_mapping, verify_record_mapping
import concurrent.futures
import time

INVALID_MOBILE_PLACEHOLDER = "invalid_mobile"
NON_DIGIT = re.compile(r"\D")

API_CRM_BASE_URL = os.getenv("API_CRM_BASE_URL", "")
API_CRM_AUTH_TOKEN = os.getenv("API_CRM_AUTH_TOKEN", "")

age_param = {"CompanyCode": "pmpl"}

age_header = {"securitycode": "5382-1294-8763-3537", "userid": "poorvika"}

age_url = "https://apx.poorvikamobiles.net/api/apxapi/GetImeiSerialNoCurrentStatus"

pos_bucket = "pos-transaction-test"
pos_imei_bucket = "pos-transaction-imei-test"
mobile_prefix = "mobile"
imei_prefix = "imei_numbers"

def mobile_validation(mobile: str) -> Optional[str]:
    if not mobile:
        return None

    cleaned = NON_DIGIT.sub('', mobile)

    if cleaned.startswith("044"):
        return cleaned

    if len(cleaned) > 10 and cleaned.startswith("91"):
        cleaned = cleaned[2:]

    if cleaned.startswith("0"):
        cleaned = cleaned[1:]

    if len(cleaned) != 10:
        return None

    return cleaned

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

def verify_and_archive_record(bucket: str, key: str, new_record: Dict[str, Any], mobile: str, imei: str, pre_fetched_summary_record: Optional[Dict[str, Any]] = None):
    """
    Checks for existing record (via pre-fetched summary or lookup) and archives it.
    """
    
    current_invoice_no = new_record.get("bill_transaction_no__c")
    if not current_invoice_no:
        return

    record_to_archive = pre_fetched_summary_record
    
    # 1. Check Mobile Summary (Primary Source) - If not provided
    if not record_to_archive:
        summary_key = f"{mobile_prefix}/{mobile}/mobile_summary.json"
        summary_data = read_json_data(bucket, summary_key)
        
        if summary_data:
            for rec in summary_data.get("records", []):
                if rec.get("bill_transaction_no__c") == current_invoice_no:
                    record_to_archive = rec
                    break
    
    # 2. Fallback: Check ID path
    if not record_to_archive:
        id_key = f"{mobile_prefix}/{mobile}/id/{current_invoice_no}.json"
        record_to_archive = read_json_data(bucket, id_key)

    if record_to_archive:
        # Move to archive using PRI_ID as filename
        archive_key = f"{mobile_prefix}/{mobile}/transferd_data/{current_invoice_no}.json"
        
        try:
            store_json_data(bucket, record_to_archive, archive_key, {"archived": "true", "trigger": "update", "pri_id": current_invoice_no})
            success_log.info(f"📦 Archived existing record to {archive_key}")
        except Exception as e:
            error_log.exception(f"❌ Failed to archive record {current_invoice_no} to {archive_key}: {e}")


def fetch_age_of_device(serial_no: str):
    if not serial_no:
        return None

    age_param["ImeiSerialNo"] = serial_no

    try:
        response = requests.get(age_url, headers=age_header, params=age_param).json()
    except Exception as e:
        error_log.exception(f"❌ Age-of-device API failed : {str(e)}")
        return None

    # ---- Validate API Status ----
    if response.get("StatusCode") != 0:
        error_log.warning(f"⚠️ Age API error: {response.get('StatusMessage')}")
        return None

    # ---- Validate Data list ----
    data = response.get("Data") or []
    inward_raw = data[0].get("INWARD_DATE") if data else None
    if not inward_raw:
        return None

    try:
        inward_date = datetime.strptime(str(inward_raw), "%Y%m%d").date()
        return (date.today() - inward_date).days
    except Exception as e:
        error_log.exception(f"❌ Failed parsing INWARD_DATE : {str(e)}")
        return None

def crm_customer_id(request_body):
    try:
        current_data = request_body

        data = {
            "name": str(current_data.get("customer_fname"))
                    or str(current_data.get("customer_lname")),

            "mobile": str(current_data.get("customer_mobile"))
                      or str(current_data.get("customer_phone_no1"))
                      or str(current_data.get("customer_phone_no2")),

            "gender": str(current_data.get("customer_gender")),
            "email": str(current_data.get("customer_email")),
            "type": "Customer",

            "pincode": str(current_data.get("customer_pincode"))
                       or str(current_data.get("billed_at_pincode"))
                       or str(current_data.get("delivery_to_pincode")),

            "branch": [str(current_data.get("store_code"))],

            "city": str(current_data.get("customer_city"))
                    or str(current_data.get("billed_at_city"))
                    or str(current_data.get("delivery_to_city")),

            "state": str(current_data.get("customer_state"))
                     or str(current_data.get("billed_at_state"))
                     or str(current_data.get("delivery_from_state")),

            "alt_mobile": str(current_data.get("customer_phone_no1"))
                          or str(current_data.get("customer_phone_no2")),

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

def build_final_records(invoice, pri_id):
    header = header_record_mapping(invoice)
    output_items = invoice.get("output", [])

    customer_id, crm_error = crm_customer_id(invoice)

    error_bucket = []
    if not customer_id and crm_error:
        error_bucket.append({
            "pri_id": pri_id, "type": "CRM_CUSTOMER_ERROR", "error": crm_error,
        })

    records = []
    for item in output_items:
        footer = footer_record_mapping(item)

        # Extract emp_id & emp_name
        remarks = (item.get("item_remarks2") or "").strip()
        emp_id, emp_name = "", ""

        if remarks:
            parts = [p.strip() for p in remarks.split("-")]
            emp_id = parts[0]
            emp_name = parts[1] if len(parts) > 1 else ""

        # Extract serial_no
        serial_raw = (item.get("item_remarks1") or "").strip()
        serial_nos = [s.strip() for s in serial_raw.split(",")] if serial_raw else [""]

        for serial_no in serial_nos:
            age = fetch_age_of_device(serial_no)

            records.append({
                "pri_id": pri_id,
                "emp_id": emp_id,
                "emp_name": emp_name,
                "item_remarks1__c": serial_no or "",
                "ageOfDevice": age or "",
                "customerId": customer_id or "",
                **header,
                **footer
            })

    return records, error_bucket

def generate_upload_tasks(invoice: Dict[str, Any], pri_id: str, old_summary_record: Optional[Dict[str, Any]] = None):
    records, error_bucket = build_final_records(invoice, pri_id)

    if not records:
        return []

    tasks = []
    base_record = records[0]

    mobile = base_record.get("customer_mobile__c", "")
    customer_id = base_record.get("customerId", "")
    all_serial_no = base_record.get("item_remarks1__c", "")
    invoice_id = base_record.get("bill_transaction_no__c", "")

    base_metadata = {
            "customer-id": str(customer_id),
            "invoice-id": invoice_id,
            "serial-no": str(all_serial_no),
            "mobile": str(mobile),
            "pri_id": str(pri_id),
        }

    mobile_pri_id = f"{mobile_prefix}/{mobile}/id/{pri_id}.json"
    # Convert direct execution to task
    tasks.append((store_json_data, (pos_bucket, base_record, mobile_pri_id, base_metadata)))

    for record in records:
        serial_no = record.get("item_remarks1__c", "")
        if not serial_no:
            continue

        metadata = {
            "customer-id": str(customer_id),
            "invoice-id": invoice_id,
            "serial-no": str(serial_no),
            "mobile": str(mobile),
            "pri_id": str(pri_id),
        }

        mobile_imei = f"{mobile_prefix}/{mobile}/imei/{serial_no}.json"
        
        # New: Store by Customer ID and Email
        if customer_id:
            cus_id_key = f"{mobile_prefix}/{mobile}/cus_id/{customer_id}.json"
            tasks.append((store_json_data, (pos_bucket, record, cus_id_key, metadata)))
            
        email = record.get("customer_email__c", "")
        if email:
             email_key = f"{mobile_prefix}/{mobile}/email/{email}.json"
             tasks.append((store_json_data, (pos_bucket, record, email_key, metadata)))
        
        # Add task for IMEI record (verify & archive + upload)
        # Note: verify_and_upload_imei_record handles both updating the mobile-path IMEI file 
        # AND updating the global IMEI file (imei_prefix).
        # We pass imei_prefix as argument.
        tasks.append((verify_and_upload_imei_record, (pos_bucket, mobile_imei, record, mobile, serial_no, metadata, imei_prefix, old_summary_record)))

    if error_bucket:
        err_key = f"crm_error_response/{pri_id}.json"
        tasks.append((store_json_data, (pos_bucket, error_bucket, err_key, {"pri_id": str(pri_id)})))

    return tasks

def verify_and_upload_imei_record(bucket, mobile_imei_key, record, mobile, serial_no, metadata, imei_pfx, old_summary_record=None):
    # Verify and Archive
    verify_and_archive_record(bucket, mobile_imei_key, record, mobile, serial_no, pre_fetched_summary_record=old_summary_record)
    
    # Store New Data at mobile path
    store_json_data(bucket, record, mobile_imei_key, metadata)
    
    # Store New Data at global IMEI path
    imei_key = f"{imei_pfx}/{serial_no}.json"
    store_json_data(bucket, record, imei_key, metadata)

def make_json_data(row: Dict[str, Any]) -> Dict[str, Any]:
    return header_record_mapping(row)

def rebuild_mobile_summary(bucket: str, mobile: str):
    """
    Scans {mobile_prefix}/{mobile}/id/ and rebuilds the summary file.
    """
    r2 = r2_client()
    prefix = f"{mobile_prefix}/{mobile}/id/"
    
    final_records = []
    
    try:
        # Paginator to handle > 1000 objects if needed (though user said max 500)
        paginator = r2.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if 'Contents' not in page:
                continue
            
            # For each file, read it 
            # Optimization: Can use threads here too if needed, but linear for 500 files is OK?
            # 500 sequential reads might be slow (e.g., 50ms * 500 = 25s). 
            # Let's use a small thread pool for reading.
            
            keys = [obj['Key'] for obj in page['Contents'] if obj['Key'].endswith('.json')]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_key = {executor.submit(read_json_data, bucket, key): key for key in keys}
                for future in concurrent.futures.as_completed(future_to_key):
                    data = future.result()
                    if data:
                        final_records.append(data)

    except Exception as e:
        error_log.exception(f"❌ Failed to rebuild summary for {mobile}: {e}")
        return

    # Calculate totals
    total_spends = sum(safe_float(i.get("Bill_Grant_Total__c", 0)) for i in final_records)
    total_records_count = len(final_records)

    summary = {
                "mobile": mobile,
                "total_records": total_records_count,
                "total_spends": total_spends,
                "records": final_records,
    }
    
    summary_meta = {
        "mobile": mobile, 
        "records-count": str(total_records_count), 
        "total-spends": str(total_spends),
    }

    summary_key = f"{mobile_prefix}/{mobile}/mobile_summary.json"
    store_json_data(bucket, summary, summary_key, summary_meta)


def process_mobile_data(raw_mobile_key: str, rows: List[Dict[str, Any]], idx: int, total_count: int, executor: concurrent.futures.ThreadPoolExecutor):
    start_time = time.time()
    
    # Validate the mobile key itself for the summary file path
    val_mobile = mobile_validation(raw_mobile_key)
    mobile = val_mobile if val_mobile else INVALID_MOBILE_PLACEHOLDER
    
    if not rows:
        return

    # 1. Pre-fetch Summary for Archiving Checks
    summary_key = f"{mobile_prefix}/{mobile}/mobile_summary.json"
    existing_summary = read_json_data(pos_bucket, summary_key)
    old_records_map = {}
    if existing_summary:
        for rec in existing_summary.get("records", []):
            inv = rec.get("bill_transaction_no__c")
            if inv:
                old_records_map[inv] = rec
    
    # 2. Generate all upload tasks
    all_tasks = []
    
    for row in rows:
        p_id = row.get("pri_id") or row.get("id") or row.get("bill_transcation_no") or "unknown"
        p_id = str(p_id)
        
        # Pass the old record if it exists for this invoice
        old_rec = old_records_map.get(p_id)
        
        all_tasks.extend(generate_upload_tasks(row, p_id, old_summary_record=old_rec))
    
    # 3. Execute Uploads
    if executor:
        futures = [executor.submit(func, *args) for func, args in all_tasks]
        concurrent.futures.wait(futures)
    else:
        for func, args in all_tasks:
            func(*args)
            
    # 4. Rebuild Summary from ID files
    rebuild_mobile_summary(pos_bucket, mobile)

    elapsed = time.time() - start_time
    # success_log.info(f"Processed mobile {mobile} in {elapsed:.2f}s")


# Updated push_to_bucket to use parallel execution for IMEIs
def push_to_bucket(invoice: Dict[str, Any], pri_id):
    # This function handles a single invoice but uses parallelism for its items (IMEIs)
    tasks = generate_upload_tasks(invoice, pri_id)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(func, *args) for func, args in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                error_log.exception(f"Task failed in push_to_bucket: {e}")

    # ---- CRITICAL: Rebuild Summary for this Mobile ----
    # Extract mobile to rebuild its summary
    # We can try to get it from the header or loops, but tasks[0] usually has metadata
    # Or just re-parse invoice
    # Let's extract safely from invoice input
    try:
        raw_mobile = str(invoice.get("customer_mobile") or 
                         invoice.get("customer_phone_no1") or 
                         invoice.get("customer_phone_no2") or "")
        
        mobile = mobile_validation(raw_mobile)
        
        if mobile:
            rebuild_mobile_summary(pos_bucket, mobile)
            success_log.info(f"🔄 Rebuilt summary for {mobile}")
        else:
            error_log.warning(f"⚠️ Could not rebuild summary, invalid mobile in invoice: {raw_mobile}")

    except Exception as e:
        error_log.exception(f"❌ Failed to trigger summary rebuild in push_to_bucket: {e}")
