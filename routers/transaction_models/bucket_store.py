import time, json, os, math
from typing import Any, List
from datetime import datetime, date
from fastapi import APIRouter,HTTPException, Query
from core.creds import logger, r2_client, MysqlCatalog
from concurrent.futures import ThreadPoolExecutor, as_completed


router = APIRouter(prefix="/api/bucket", tags=["Store Data to Bucket "])


# --- CONFIG ---
BATCH_SIZE = 1000
MAX_PARALLEL_UPLOADS = 5
R2_BUCKET_NAME = os.getenv("BUCKET_NAME")
R2_ENDPOINT = os.getenv("ENDPOINT")
R2_ACCESS_KEY = os.getenv("ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("SECRET_ACCESS_KEY")

def store_data_json_to_r2(data, key, metadata):
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

    safe_metadata = {}
    if metadata and isinstance(metadata, dict):
        for k, v in metadata.items():
            safe_key = str(k).lower().replace("_", "-")
            safe_metadata[safe_key] = str(v)

    elif metadata:
        logger.warning(f"⚠️ Metadata provided is not a dict: {metadata}, ignoring metadata.")

    r2 = r2_client()
    try:
        r2.put_object(Bucket=R2_BUCKET_NAME,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=safe_metadata)

        logger.info(f"✅ Stored {key} ({len(body)} bytes)")


    except Exception as e:
        logger.exception(f"❌ Unexpected error storing {key}: {e}")
        raise

def make_json_data(record: dict[str, Any]):
    from decimal import Decimal

    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            record[k] = v.isoformat()
        elif isinstance(v, Decimal):
            record[k] = float(v)
        elif isinstance(v, bytes):
            record[k] = v.decode(errors="ignore")
    return record


@router.post("/id/")
async def create_bucket(start_range, end_range):
    if end_range <= start_range:
        logger.error(f"❌ end_range must be greater than start_range")
        raise HTTPException(status_code=400, detail="end_range must be greater than start_range")

    total_start = time.time()
    mysql_creds = MysqlCatalog()
    mysql_db_name = "Transaction"

    try:
        mysql_start = time.time()
        rows = mysql_creds.get_range(mysql_db_name, start_range, end_range)
        mysql_duration = round(time.time() - mysql_start, 2)
        if not rows:
            logger.error(f"❌ No data found in the given range.")
            raise HTTPException(status_code=404, detail="No data found in the given range.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")

    stored_count, failed_count = 0, 0
    error_logs: List[Any] = []

    def process_record(record: dict[str, Any]):
        pri_id = record.get("pri_id")
        if not pri_id:
            return {"status": "error", "error": "Missing pri_id"}

        try:
            record_safe = make_json_data(record)
            key = f"id/{pri_id}.json"

            metadata = {"pri_id": pri_id,
                        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}

            store_data_json_to_r2([record_safe], key, metadata)
            return {"status": "ok"}

        except Exception as e:
            return {"status": "error", "error": str(e)}

    total_rows = len(rows)
    num_batches = math.ceil(total_rows / BATCH_SIZE)

    for batch_index in range(num_batches):
        batch_start_idx = batch_index * BATCH_SIZE
        batch_end_idx = min(batch_start_idx + BATCH_SIZE, total_rows)
        batch_data = rows[batch_start_idx:batch_end_idx]

        print(f"🟡 Processing Batch {batch_index + 1}/{num_batches} → Rows {batch_start_idx}–{batch_end_idx}")

        batch_start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(process_record, batch_data))

        batch_duration = round(time.time() - batch_start_time, 2)

        # Collect stats
        ok = sum(1 for r in results if r["status"] == "ok")
        err = len(results) - ok
        stored_count += ok
        failed_count += err
        error_logs.extend(r for r in results if r["status"] == "error")

        print(f"✅ Batch {batch_index + 1} Completed in {batch_duration}s ({ok} ok / {err} failed)")

    total_elapsed = round(time.time() - total_start, 2)

    # --- Step 3: Summary ---
    return {
        "status": "success",
        "mysql_duration_sec": mysql_duration,
        "rows_fetched": total_rows,
        "rows_stored": stored_count,
        "failed_count": failed_count,
        "elapsed_total_sec": total_elapsed,
        "batches": num_batches,
        "r2_key_pattern": "id/<pri_id>.json",
        "errors": error_logs[:5],
    }


@router.get("/list/")
def get_bucket_list(bucket_name: str = Query("dev-soc-media"), bucket_path: str = Query("soc-media")):
    r2 = r2_client()
    try:
        paginator = r2.get_paginator("list_objects_v2")
        files = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=f"{bucket_path}/"):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])

        return {"total_files": len(files), "files": files}

    except Exception as e:
        logger.error(f"❌ Failed to list objects in bucket {bucket_name}: {e}")
        return {"error": str(e)}
