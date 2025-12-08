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

# dt = datetime.now()
# now = dt.strftime("%Y-%m-%d (%H:%M:%S)")
#
# d_bucket = "dev-soc-media"
# d_data = {"pri_id": 1, "updated_at": now}
# count_key = f"count/old_pri_id.json"
# d_metadata = {"pri_id": str(1), "updated_at": now}
#
# store_json_data(d_bucket, d_data, count_key, d_metadata)
