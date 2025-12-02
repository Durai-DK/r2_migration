import pandas as pd
import os, json, time
from decimal import Decimal
from datetime import datetime, date
from core.creds import logger, MysqlCatalog
from fastapi import APIRouter, HTTPException, Query
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(prefix="/api/extract", tags=["Extract MySQL → JSON"])

MAX_WORKERS = 5
BASE_STORE_PATH = "/Users/DK/Desktop/Project/r2_migration/stored_data/Transaction_Mysql_Dump"

def convert_value(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.decode(errors="ignore")
    return v

def save_json_file(output_dir, rec):
    pri_id = rec.get("pri_id")
    if not pri_id:
        return False

    path = f"{output_dir}/{pri_id}.json"

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"Failed to write {path}: {e}")
        return False


@router.post("/range/json/")
def extract_mysql_to_json(start_range: int, end_range: int):
    if end_range <= start_range:
        raise HTTPException(400, "end_range must be greater than start_range")

    base_folder = f"{BASE_STORE_PATH}_{start_range}_{end_range}"
    os.makedirs(base_folder, exist_ok=True)

    mysql = MysqlCatalog()

    # Step 1: Fetch
    start = time.time()
    rows = mysql.get_range("Transaction", start_range, end_range)
    mysql_duration = round(time.time() - start, 2)

    if not rows:
        raise HTTPException(404, "No data found in range")

    # Step 2: Pandas DataFrame
    df = pd.DataFrame(rows)
    for col in df.columns:
        df[col] = df[col].apply(convert_value)

    #Step 3: Parallel JSON Writing
    save_start = time.time()
    success = 0

    record_list = df.to_dict(orient="records")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(save_json_file, base_folder, rec) for rec in record_list]

        for fut in as_completed(futures):
            if fut.result():
                success += 1

    save_duration = round(time.time() - save_start, 2)
    total_duration = round(time.time() - start, 2)

    return {
        "message": "JSON extraction completed",
        "total_saved": success,
        "output_dir": base_folder,
        "mysql_duration_sec": mysql_duration,
        "file_write_duration_sec": save_duration,
        "total_duration_sec": total_duration
    }

# # # Save a single row to JSON
# def save_json(record: Dict[str, Any], output_dir: str) -> Dict[str, Any]:
#     pri_id = record.get("pri_id")
#     if not pri_id:
#         return {"status": "error", "error": "Missing pri_id"}
#
#     try:
#         file_path = os.path.join(output_dir, f"{pri_id}.json")
#         with open(file_path, "w", encoding="utf-8") as f:
#             json.dump(record, f, indent=2, ensure_ascii=False)
#
#         return {"status": "ok", "id": pri_id}
#
#     except Exception as e:
#         logger.error(f"❌ Error saving JSON for {pri_id}: {e}")
#         return {"status": "error", "id": pri_id, "error": str(e)}
#
# # Main API: Download MySQL rows → Save JSON locally
# @router.post("/id/")
# def download_and_store_locally(start_range, end_range, output_dir = Query(description="Directory to store JSON files")):
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql = MysqlCatalog()
#     db_name = "Transaction"
#
#     os.makedirs(output_dir, exist_ok=True)
#
#     # Step 1: Fetch MySQL data
#     try:
#         mysql_start = time.time()
#         rows = mysql.get_range(db_name, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#
#         if not rows:
#             raise HTTPException(status_code=404, detail="No data found for the given range")
#
#     except Exception as e:
#         logger.error(f"❌ MySQL fetch error: {e}")
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")
#
#     logger.info(f"Fetched {len(rows)} records from MySQL in {mysql_duration}s")
#
#     # Save concurrently
#     df_start = time.time()
#     df = pd.DataFrame(rows)
#
#     # Convert all values
#     for col in df.columns:
#         df[col] = df[col].apply(convert_value)
#
#     df_duration = round(time.time() - df_start, 2)
#     logger.info(f"Converted to Pandas DataFrame in {df_duration}s")
#
#     # Convert DF to list of dicts
#     records: List[Dict[str, Any]] = df.to_dict(orient="records")
#
#     # Save files using ThreadPoolExecutor
#     save_start = time.time()
#     results = []
#
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = [executor.submit(save_json, rec, output_dir) for rec in records]
#         for future in as_completed(futures):
#             results.append(future.result())
#
#     save_duration = round(time.time() - save_start, 2)
#
#     # Summary
#     success = sum(1 for r in results if r["status"] == "ok")
#     failed = len(results) - success
#
#     total_time = round(time.time() - total_start, 2)
#
#     logger.info(
#         f"Completed storing JSON files | Total: {len(records)}, Success: {success}, Failed: {failed}, "
#         f"MySQL: {mysql_duration}s, Pandas: {df_duration}s, Save: {save_duration}s, Total: {total_time}s"
#     )
#
#     return {
#                 "message": "MySQL → Pandas → JSON store completed",
#                 "requested_range": {"start": start_range, "end": end_range},
#                 "total_records": len(records),
#                 "success": success,
#                 "failed": failed,
#                 "mysql_time_sec": mysql_duration,
#                 "pandas_time_sec": df_duration,
#                 "save_time_sec": save_duration,
#                 "total_time_sec": total_time,
#                 "output_directory": os.path.abspath(output_dir),
#     }