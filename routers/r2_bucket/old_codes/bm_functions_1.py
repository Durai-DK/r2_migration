import json, os
from decimal import Decimal
from typing import Dict, Any
from datetime import datetime, date
from core.creds import logger, r2_client

R2_BUCKET = os.getenv("BUCKET_NAME")

def make_json_data(record: dict[str, Any]):
    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            record[k] = v.isoformat()
        elif isinstance(v, Decimal):
            record[k] = float(v)
        elif isinstance(v, bytes):
            record[k] = v.decode(errors="ignore")
    return record

def store_json_data(data, key, metadata):
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
        logger.error(f"❌ JSON serialization failed for key {key}: {e}")
        raise

    try:
        r2 = r2_client()
        r2.put_object(Bucket=R2_BUCKET,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=metadata)

        logger.info(f"✅ Stored {key} ({len(body)} bytes)")

    except Exception as e:
        logger.exception(f"❌ Unexpected error storing {key}: {e}")
        raise

def check_existing_json(key):
    r2 = r2_client()
    try:
        response = r2.get_object(Bucket=R2_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except r2.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.error(f"⚠️ Error checking existing summary → {e}")
        return None

def upload_summary_json(mobile, data, key):
    existing_summary = check_existing_json(key)

    # -------- spends ----------------
    customer_id = (data.get("customerId") or "").strip()
    customer_name = (data.get("Customer_Name__c") or "").strip()
    status_type = (data.get("item_status__c") or "").strip()
    invoice_date = (data.get("Bill_Date__c") or "").strip()
    invoice_id = (data.get("bill_transaction_no__c") or "").strip()
    invoice_value = float(data.get("bill_grand_total__c") or 0)
    tender_type = (data.get("bill_tender_type__c") or "").strip()
    item_code = (data.get("Item_Code__c") or "").strip()
    item_quantity = (data.get("item_quantity__c") or 0)
    item_name = (data.get("Item_Name__c") or "").strip()
    serial_no = (data.get("item_remarks1__c") or "").strip()
    item_rate = float(data.get("Item_Rate__c") or 0)
    item_tax = float(data.get("item_tax__c") or 0)
    total = float(item_rate) + float(item_tax)

    invoice_obj = {
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "status_type": status_type,
                    "invoice_date": invoice_date,
                    "invoice_id": invoice_id,
                    "invoice_value": invoice_value,
                    "tender_type": tender_type,
                    "item_code": item_code,
                    "item_quantity": item_quantity,
                    "item_name": item_name,
                    "serial_no": serial_no,
                    "item_rate": item_rate,
                    "item_tax": item_tax,
                    "total": total,
                }

    if not existing_summary:
        summary_data = {
                        "invoice_count": 1,
                        "total_value": invoice_value,
                        "invoices": [invoice_obj]
                    }

        metadata = {"mobile": mobile, "customer-id": customer_id}
        store_json_data(summary_data, key, metadata)
        return

    # UPDATE EXISTING SUMMARY
    invoices = existing_summary.get("invoices", [])
    existing_ids = {i.get("invoice_id") for i in invoices}

    # Append only if invoice_id is new
    if invoice_id not in existing_ids:
        invoices.append(invoice_obj)

    updated_summary = {
        "invoice_count": len(invoices),
        "total_value": sum(float(i.get("invoice_value", 0)) for i in invoices),
        "invoices": invoices
    }

    metadata = {"mobile": mobile, "customer-id": customer_id, "customer-name": customer_name}

    store_json_data(updated_summary, key, metadata)

def write_global_summary(total_invoices, total_purchase_value, key="Mobile_Summary.json"):
    existing = check_existing_json(key)

    if existing:
        existing_invoices = existing.get("TOTAL_INVOICES", 0)
        existing_value = existing.get("TOTAL_PURCHASE_VALUE", 0.0)
    else:
        existing_invoices = 0
        existing_value = 0.0

    final_summary = {
        "TOTAL_INVOICES": existing_invoices + total_invoices,
        "TOTAL_PURCHASE_VALUE": round(existing_value + total_purchase_value, 2)
    }

    store_json_data(final_summary, key, metadata=None)


def process_invoice(row: Dict[str, Any], prefix: str):
    data = make_json_data(row)

    mobile = (data.get("customer_mobile__c") or "").strip()
    invoice_id = (data.get("bill_transaction_no__c") or "").strip()
    serial_no = (data.get("item_remarks1__c") or "").strip()
    customer_id = (data.get("customerId") or "").strip()
    invoice_value = float(data.get("bill_grand_total__c") or 0)

    invoice_id_fix = invoice_id.replace("/", "_")
    invoice_path = f"{prefix}/{mobile}/invoices/{invoice_id_fix}.json"
    mobile_summary_path = f"{prefix}/{mobile}/summary.json"

    invoice_metadata = {
        "customer-id": customer_id,
        "invoice-id": invoice_id,
        "serial-no": serial_no,
        "mobile": mobile,
    }

    # Store invoice
    store_json_data(data, invoice_path, invoice_metadata)

    # IMEI storing
    serial_numbers = [s.strip() for s in serial_no.split(",") if s.strip()]
    for sn in serial_numbers:
        imei_path = f"{prefix}/{mobile}/imei_numbers/{sn}.json"
        store_json_data(data, imei_path, invoice_metadata)

    # Summary update
    upload_summary_json(mobile, data, mobile_summary_path)

    return {
        "invoice_value": invoice_value,
        "count": 1
    }



def imei_json_data(imei, data, key):
    serial_no = (data.get("item_remarks1__c") or "").strip()
    mobile = (data.get("customer_mobile__c") or "").strip()
    item_code = (data.get("Item_Code__c") or 0)
    branch_name = (data.get("TN-A-T.NAGAR") or "").strip()
    customer_name = (data.get("delivery_to_Name") or "").strip()
    product = (data.get("item_product_name") or "").strip()
    brand = (data.get("item_brand_name") or "").strip()
    model = (data.get("Item_Name__c") or "").strip()
    product_value = (data.get("item_net_amount__c") or "").strip()
    bill_no = (data.get("bill_transaction_no__c") or "").strip()     # invoice id
    bill_date = (data.get("Bill_Date__c") or "").strip()