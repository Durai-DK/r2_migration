import time, json, re, csv
from decimal import Decimal
from typing import Dict, Any, List, Tuple
from datetime import datetime, date
from collections import defaultdict
from core.creds import r2_client, fetch_batch_mobiles
from concurrent.futures import ThreadPoolExecutor, as_completed
from logs.log_settings import error_log, success_log

BATCH_SIZE = 2500
CSV_FILE = r"/Users/DK/Desktop/Data_Backup/stored_data/Transaction_01.csv"
NON_DIGIT = re.compile(r"\D")

mob_bucket = "pos-transaction"
mob_prefix = "mobile"
imei_bucket = "pos-transaction-imei"
imei_prefix = "imei"

# Tuning: DB is batched/safe. R2 can handle high concurrency.
MOBILE_WORKERS = 20  # mobile-level parallelism (for fetching/coordinating)
UPLOAD_WORKERS = 70  # parallel uploads
INVALID_MOBILE_PLACEHOLDER = "0000000000"


def read_mobiles_from_csv(start: int, end: int) -> list[str]:
    mobiles = []
    with open(CSV_FILE, newline="") as f:
        for idx, row in enumerate(csv.DictReader(f)):
            if start <= idx < end:
                mobile = str(row.get("customer_mobile__c", "")).strip()
                if mobile:
                    mobiles.append(mobile)
    return mobiles


def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0


def make_json_data(record: Dict[str, Any]) -> Dict[str, Any]:
    clean = {}
    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            clean[k] = v.isoformat()
        elif isinstance(v, Decimal):
            clean[k] = float(v)
        elif isinstance(v, bytes):
            clean[k] = v.decode("utf-8", errors="ignore")
        else:
            clean[k] = v
    return clean


def store_json_data(payload: Tuple[str, Any, str, Dict]):
    """
    Worker function to upload a single file.
    Payload: (bucket, data, key, metadata)
    """
    bucket, data, key, metadata = payload

    if metadata is None:
        metadata = {}

    if not key:
        return

    try:
        if isinstance(data, (dict, list, int, float, str)):
            body = json.dumps(data, indent=2, ensure_ascii=False)
        else:
            return

        r2 = r2_client()
        r2.put_object(Bucket=bucket,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=metadata)

    except Exception as e:
        error_log.error(f"❌ Error storing {key}: {e}")


def mobile_validation(mobile: str):
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


def generate_upload_tasks(row: Dict[str, Any]) -> List[Tuple]:
    """
    Returns a list of upload tasks (bucket, data, key, meta) for a single invoice row.
    """
    tasks = []
    data = make_json_data(row)

    raw_mobile = str(data.get("customer_mobile__c") or "")
    mobile = mobile_validation(raw_mobile) if raw_mobile else None

    # If invalid, fallback to placeholder so we don't lose data
    if not mobile:
        mobile = INVALID_MOBILE_PLACEHOLDER

    pri_id = str(data.get("pri_id"))
    invoice_id = str(data.get("bill_transaction_no__c", ""))
    serial_no = str(data.get("item_remarks1__c", ""))
    customer_id = str(data.get("customerId", ""))

    metadata = {
        "mobile": mobile,
        "pri_id": pri_id,
        "invoice-id": invoice_id,
        "customer-id": customer_id,
        "serial-no": serial_no,
    }

    pri_key = f"{mob_prefix}/{mobile}/id/{pri_id}.json"
    tasks.append((mob_bucket, data, pri_key, metadata))

    serial_numbers = [s.strip() for s in serial_no.split(",") if s.strip()]
    for sn in serial_numbers:
        # Create a shallow copy so we don't affect other tasks
        current_data = data.copy()
        current_data["item_remarks1__c"] = sn

        imei_mob_path = f"{mob_prefix}/{mobile}/imei/{sn}.json"
        tasks.append((mob_bucket, current_data, imei_mob_path, metadata))

        imei_path = f"{imei_prefix}/{sn}.json"
        tasks.append((imei_bucket, current_data, imei_path, metadata))

    return tasks


def process_mobile_data(raw_mobile_key: str, rows: List[Dict[str, Any]], idx: int, total_count: int,
                        executor: ThreadPoolExecutor):
    start_time = time.time()

    # Validate the mobile key itself for the summary file path
    val_mobile = mobile_validation(raw_mobile_key)
    mobile = val_mobile if val_mobile else INVALID_MOBILE_PLACEHOLDER

    if not rows:
        return

    records = [make_json_data(r) for r in rows]
    total_spends = sum(safe_float(i.get("bill_grand_total__c", 0)) for i in records)

    # 1. Generate all upload tasks for this mobile
    all_tasks = []
    for row in rows:
        all_tasks.extend(generate_upload_tasks(row))

    # 2. Add summary task
    summary = {
        "mobile": mobile,
        "total_records": len(records),
        "total_spends": total_spends,
        "records": records,
    }
    summary_key = f"{mob_prefix}/{mobile}/mobile_summary.json"
    summary_meta = {
        "mobile": mobile, "records-count": str(len(records)), "total-spends": str(total_spends),
    }
    all_tasks.append((mob_bucket, summary, summary_key, summary_meta))

    # 3. Submit all tasks to the shared executor
    futures = [executor.submit(store_json_data, task) for task in all_tasks]

    # Wait for this mobile's uploads to finish to log accurate time
    for f in as_completed(futures):
        pass

    success_log.info(
        f"✅ [{idx}/{total_count}] '{raw_mobile_key}' -> '{mobile}' | records = {len(records)} | items_uploaded={len(all_tasks)} | "
        f"time = {round(time.time() - start_time, 2)}s"
    )


def push_r2_bucket(start_range: int, end_range: int):
    overall_start = time.time()

    if end_range <= start_range:
        error_log.error(400, "Invalid range")
        return

    mobiles = read_mobiles_from_csv(start_range, end_range)
    total_mobiles = len(mobiles)
    success_log.info(
        f"🚀 Processing mobiles {start_range} → {end_range} | count={total_mobiles}"
    )

    # Shared executor for ALL uploads to maximize throughput
    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as upload_executor:
        # for batch_no, i in enumerate(range(0, len(mobiles), BATCH_SIZE), start=1):
        for batch_no, i in enumerate(range(0, len(mobiles), BATCH_SIZE), start=1):
            batch_start_time = time.time()

            chunk = mobiles[i: i + BATCH_SIZE]
            chunk_start_idx = start_range + i

            try:
                fetch_start = time.time()
                all_rows = fetch_batch_mobiles(chunk)
                fetch_time = time.time() - fetch_start
                success_log.info(f"Fetched batch {i // BATCH_SIZE} in {round(fetch_time, 2)}s")
            except Exception as e:
                error_log.error(f"Batch fetch error: {e}")
                continue

            grouped = defaultdict(list)
            for row in all_rows:
                m = row.get("customer_mobile__c")
                if m:
                    grouped[m].append(row)

            # Use a separate executor for "Mobile Logic" which submits to "Upload Executor".
            with ThreadPoolExecutor(max_workers=MOBILE_WORKERS) as logic_executor:
                futures = []
                for j, mobile in enumerate(chunk):
                    rows = grouped.get(mobile, [])
                    if not rows:
                        continue

                    # Pass the upload_executor to the worker
                    futures.append(
                        logic_executor.submit(process_mobile_data, mobile, rows, chunk_start_idx + j, end_range,
                                              upload_executor))

                # Wait for this batch's logic (and implicitly uploads because process_mobile_data waits)
                for f in as_completed(futures):
                    pass
            batch_time = time.time() - batch_start_time
            success_log.info(
                f"✅ BATCH DONE | Batch={batch_no} | Mobiles={len(chunk)} | Time={round(batch_time, 2)}s"
            )

    success_log.info(
        f"🏁 DONE | Range {start_range} → {end_range} | Total time={round(time.time() - overall_start, 2)}s"
    )


if __name__ == "__main__":
    push_r2_bucket(0, 100)
