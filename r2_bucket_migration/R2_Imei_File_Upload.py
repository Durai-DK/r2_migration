from concurrent.futures import ThreadPoolExecutor, as_completed
from r2_bucket_migration.helper import *
import time, pandas as pd

l_pri_id = 0
BATCH_SIZE = 50000

WORKERS = 100

imei_bucket = "pos-transaction-imei"
imei_prefix = "imei"


def save_imei_to_json(start: str, end: str, batch_imei: set):
    if not batch_imei:
        return

    today = datetime.now().strftime("%Y_%m_%d_")
    filename = f"unique_imei_{start}_{end}_{today}.json"

    # Convert set → sorted list of strings (stable + readable)
    imei_list = sorted(str(imei) for imei in batch_imei)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(imei_list, f, ensure_ascii=False, indent=2)

    success_log.info(f"📝 Batch JSON created: {filename} | IMEIs: {len(imei_list)}")


def generate_upload_tasks(row: Dict[str, Any]) -> List[Tuple]:
    data = make_json_data(row)

    serial_no = str(data.get("item_remarks1__c", "")).strip()
    if not serial_no:
        return []

    serial_numbers = [s.strip() for s in serial_no.split(",") if s.strip()]
    if not serial_numbers:
        return []

    mobile = str(data.get("customer_mobile__c") or "")
    pri_id = str(data.get("pri_id"))
    batch_id = str(data.get("batch_id"))
    invoice_id = str(data.get("bill_transaction_no__c", ""))

    tasks = []

    for sn in serial_numbers:
        current_data = data.copy()
        current_data["item_remarks1__c"] = sn

        metadata = {
            "mobile": mobile,
            "pri_id": pri_id,
            "batch_id": batch_id,
            "invoice_id": invoice_id,
            "serial_no": sn
        }

        imei_path = f"{imei_prefix}/{sn}/id/{pri_id}.json"
        tasks.append((imei_bucket, current_data, imei_path, metadata))

    return tasks


def push_r2_bucket(num_records: int):
    overall_start = time.time()
    last_pri_id = l_pri_id
    records_processed = 0

    success_log.info(f"🚀 Starting R2 imei Migration | Target: {num_records} records | Batch Size: {BATCH_SIZE}\n")

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            while records_processed < num_records:
                batch_start_time = time.time()
                limit = min(BATCH_SIZE, num_records - records_processed)

                # -----------------  Mysql DB  ---------------------------------

                rows = fetch_mysql(last_pri_id, limit)
                if not rows:
                    success_log.info("🏁 No more records found in database.")
                    break

                last_pri_id = rows[-1]['pri_id']
                fetching_time = time.time() - batch_start_time
                success_log.info(f"🚀 Records '{len(rows)}' fetched successfully in {fetching_time:2f}s | Last Pri_id : {last_pri_id}.\n")

                # --------------------------------------------------------------

                batch_count = len(rows)

                batch_unique_imei = set()
                batch_start_pri_id = rows[0]["pri_id"]
                batch_end_pri_id = rows[-1]["pri_id"]

                # Group records by serial number locally for this batch
                grouped_rows = {}
                skipped_count = 0

                for r in rows:
                    imei_no = str(r.get("item_remarks1__c") or "").strip()

                    if not imei_no:
                        skipped_count += 1
                        continue

                    imei_list = [s.strip() for s in imei_no.split(",") if s.strip()]

                    for imei in imei_list:
                        grouped_rows.setdefault(imei, []).append(r)
                        batch_unique_imei.add(imei)

                save_imei_to_json(
                    start=str(batch_start_pri_id),
                    end=str(batch_end_pri_id),
                    batch_imei=batch_unique_imei
                )
                # --------------------------------------------------------------

                success_log.info(f"📊 Grouped '{len(grouped_rows)}' unique IMEIs | Skipped records: '{skipped_count}'")

                record_futures = []
                for imei_no, imei_rows in grouped_rows.items():
                    for row in imei_rows:
                        tasks = generate_upload_tasks(row)

                        for bucket, data, path, meta in tasks:
                            if meta["serial_no"] == imei_no:  # ✅ prevent duplicates
                                record_futures.append(
                                    executor.submit(store_json_data, bucket, data, path, meta)
                                )

                total_upload_tasks = len(record_futures)
                for idx, f in enumerate(as_completed(record_futures), start=1):
                    try:
                        f.result()
                        percent = round((idx / total_upload_tasks) * 100, 2)

                        if idx % 1000 == 0 or idx == total_upload_tasks:
                            print(f"---> 💛 R2 upload completed | {percent}% ({idx}/{total_upload_tasks})")

                        if idx % 10000 == 0 or idx == total_upload_tasks:
                            success_log.info(f"⬆️ R2 upload completed ---> {percent}% ({idx}/{total_upload_tasks})")

                    except Exception as e:
                        error_log.error(f"❌ Individual record upload failed: {e}")

                records_processed += batch_count
                batch_time = round((time.time() - batch_start_time) / 60, 2)
                success_log.info(f"✅ Batch processed | Records: {batch_count} | Last pri_id: {last_pri_id} | Time: {batch_time}min\n")

    finally:
        total_minutes = round((time.time() - overall_start) / 3600, 2)
        success_log.info(f"🏁 DONE | Total records: {records_processed} | Total time: {total_minutes}hrs")


if __name__ == "__main__":
    push_r2_bucket(50000)
