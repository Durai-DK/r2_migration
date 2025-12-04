import json
from typing import Dict, Any
from decimal import Decimal
from core.creds import r2_client
from datetime import datetime, date
from logs.log_settings import error_log, success_log


def process_invoice(row: Dict[str, Any], mob_bucket: str, imei_bucket: str, mob_prefix: str, imei_prefix: str):
    data = make_json_data(row)
    mobile = (data.get("customer_mobile__c") or "").strip()
    invoice_id = (data.get("bill_transaction_no__c") or "").strip()

    serial_no = (data.get("item_remarks1__c") or "").strip()
    customer_id = (data.get("customerId") or "").strip()

    pri_id = str(data.get("pri_id"))
    pri_key = f"{mob_prefix}/{mobile}/id/{pri_id}.json"

    metadata = {
                "customer-id": customer_id,
                "invoice-id": invoice_id,
                "serial-no": serial_no,
                "mobile": mobile,
                "pri_id": pri_id,
            }

    # Store invoice
    store_json_data(mob_bucket, data, pri_key, metadata)

    serial_numbers = [s.strip() for s in serial_no.split(",") if s.strip()]
    for sn in serial_numbers:
        # Store Mobile/Imei
        imei_mob_path = f"{mob_prefix}/{mobile}/imei/{sn}.json"
        store_json_data(mob_bucket, data, imei_mob_path, metadata)

        # imei_summary(mob_bucket, mob_prefix, mobile, data, sn)

        # Store Imei
        imei_path = f"{imei_prefix}/{sn}.json"
        store_json_data(imei_bucket, data, imei_path, metadata)

    # mobile_summary(mob_bucket, mob_prefix, mobile, data)

def make_json_data(record: Dict[str, Any]):
    clean = {}
    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            clean[k] = v.isoformat()
        elif isinstance(v, Decimal):
            clean[k] = float(v)
        elif isinstance(v, bytes):
            clean[k] = v.decode(errors="ignore")
        else:
            clean[k] = v
    return clean

def store_json_data(bucket, data, key, metadata):
    if metadata is None:
        metadata = {}

    if not key:
        raise ValueError("R2 object key cannot be empty.")

    try:
        if isinstance(data, (dict, list, int, float, str)):
            body = json.dumps(data, indent=2, ensure_ascii=False)
        else:
            raise TypeError(f"Unsupported data type for R2 upload: {type(data)}")
    except (TypeError, ValueError) as e:
        error_log.error(f"❌ JSON serialization failed for key {key}: {e}")
        raise

    try:
        r2 = r2_client()
        r2.put_object(Bucket=bucket,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=metadata)

        success_log.info(f"✅ Stored {key} ({len(body)} bytes)")

    except Exception as e:
        error_log.exception(f"❌ Unexpected error storing {key}: {e}")
        raise

# def check_existing_json(bucket, key):
#     r2 = r2_client()
#     try:
#         response = r2.get_object(Bucket=bucket, Key=key)
#         return json.loads(response['Body'].read().decode('utf-8'))
#     except r2.exceptions.NoSuchKey:
#         return None
#     except Exception as e:
#         logger.error(f"⚠️ Error checking existing summary → {e}")
#         return None

# def imei_summary(bucket, mob_prefix, mobile, data, current_imei):
#     customer_id = (data.get("customerId") or "").strip()
#
#     key = f"{mob_prefix}/{mobile}/imei_summary.json"
#     existing_summary = check_existing_json(bucket, key)
#
#     if not existing_summary:
#         imei_data = {"imei_count": 1, "imei_data": [data]}
#         metadata = {"mobile": mobile, "customer-id": customer_id}
#         store_json_data(bucket, imei_data, key, metadata)
#         return
#
#     current_list = existing_summary.get("imei_data", [])
#
#     existing_ids = set()
#     for item in current_list:
#         remarks = (item.get("item_remarks1__c") or "").strip()
#         parts = [x.strip() for x in remarks.split(",") if x.strip()]
#         existing_ids.update(parts)
#
#     if current_imei not in existing_ids:
#         current_list.append(data)
#
#     updated_summary = {"imei_count": len(current_list),
#                        "imei_data": current_list}
#
#     metadata = {"mobile": mobile, "customer-id": customer_id}
#
#     store_json_data(bucket, updated_summary, key, metadata)
#
# def mobile_summary(bucket, mob_prefix, mobile, data):
#     customer_id = (data.get("customerId") or "").strip()
#     invoice_id = (data.get("bill_transaction_no__c") or "").strip()
#     invoice_value = (data.get("bill_grand_total__c") or 0)
#
#     key = f"{mob_prefix}/{mobile}/imei_numbers/mobile_summary.json"
#     existing_summary = check_existing_json(bucket, key)
#
#     if not existing_summary:
#         mobile_data = {"invoice_count": 1, "total_value": invoice_value, "invoices": [data]}
#         metadata = {"mobile": mobile, "customer-id": customer_id}
#         store_json_data(bucket, mobile_data, key, metadata)
#         return
#
#     current_list = existing_summary.get("invoices", [])
#     existing_id = {i.get("bill_transaction_no__c") for i in current_list}
#     if invoice_id not in existing_id:
#         current_list.append(data)
#
#     updated_summary = {
#         "invoice_count": len(current_list),
#         "total_value": sum(float(i.get("bill_grand_total__c", 0)) for i in current_list),
#         "invoices": current_list
#     }
#
#     metadata = {"mobile": mobile, "customer-id": customer_id}
#
#     store_json_data(bucket, updated_summary, key, metadata)

# def upload_summary_json(bucket, mobile, data, key):
#     existing_summary = check_existing_json(bucket, key)
#
#     # -------- spends ----------------
#     customer_id = (data.get("customerId") or "").strip()
#     customer_name = (data.get("Customer_Name__c") or "").strip()
#     status_type = (data.get("item_status__c") or "").strip()
#     invoice_date = (data.get("Bill_Date__c") or "").strip()
#     invoice_id = (data.get("bill_transaction_no__c") or "").strip()
#     invoice_value = float(data.get("bill_grand_total__c") or 0)
#     tender_type = (data.get("bill_tender_type__c") or "").strip()
#     item_code = (data.get("Item_Code__c") or "").strip()
#     item_quantity = (data.get("item_quantity__c") or 0)
#     item_name = (data.get("Item_Name__c") or "").strip()
#     serial_no = (data.get("item_remarks1__c") or "").strip()
#     item_rate = float(data.get("Item_Rate__c") or 0)
#     item_tax = float(data.get("item_tax__c") or 0)
#     total = float(item_rate) + float(item_tax)
#
#     invoice_obj = {
#                     "customer_id": customer_id,
#                     "customer_name": customer_name,
#                     "status_type": status_type,
#                     "invoice_date": invoice_date,
#                     "invoice_id": invoice_id,
#                     "invoice_value": invoice_value,
#                     "tender_type": tender_type,
#                     "item_code": item_code,
#                     "item_quantity": item_quantity,
#                     "item_name": item_name,
#                     "serial_no": serial_no,
#                     "item_rate": item_rate,
#                     "item_tax": item_tax,
#                     "total": total,
#                 }
#
#     if not existing_summary:
#         summary_data = {
#                         "invoice_count": 1,
#                         "total_value": invoice_value,
#                         "invoices": [data]
#                     }
#
#         metadata = {"mobile": mobile, "customer-id": customer_id}
#         store_json_data(bucket, summary_data, key, metadata)
#         return
#
#     # UPDATE EXISTING SUMMARY
#     invoices = existing_summary.get("invoices", [])
#     existing_ids = {i.get("bill_transaction_no__c") for i in invoices}
#
#     # Append only if invoice_id is new
#     if invoice_id not in existing_ids:
#         invoices.append(data)
#
#     updated_summary = {
#         "invoice_count": len(invoices),
#         "total_value": sum(float(i.get("invoice_value", 0)) for i in invoices),
#         "invoices": invoices
#     }
#
#     metadata = {"mobile": mobile, "customer-id": customer_id, "customer-name": customer_name}
#
#     store_json_data(bucket, updated_summary, key, metadata)
