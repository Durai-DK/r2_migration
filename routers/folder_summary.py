import json, time
from core.creds import r2_client
from logs.log_settings import error_log, success_log
from concurrent.futures import ThreadPoolExecutor, as_completed

r2 = r2_client()
workers_count = 70

pos_bucket = "pos-transaction"

def list_json_files(bucket: str, prefix: str):
    keys = []
    token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)
        keys.extend(obj["Key"] for obj in resp.get("Contents", []))

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    return keys

def read_json(args):
    bucket, key = args
    try:
        res = r2.get_object(Bucket=bucket, Key=key)
        return json.loads(res["Body"].read().decode("utf-8"))
    except r2.exceptions.NoSuchKey:
        error_log.error(f"❌ File not found: {key}")
        return None
    except Exception as e:
        error_log.error(f"❌ Error reading {key}: {e}")
        return None

def parallel_read_json(bucket, keys, workers):
    if not keys:
        return []

    records = []
    tasks = [(bucket, key) for key in keys]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(read_json, task): task[1] for task in tasks
        }
        for future in as_completed(future_map):
            key = future_map[future]
            try:
                content = future.result()
                if content:
                    records.append(content)
                else:
                    error_log.error(f"⚠️ Empty / invalid JSON: {key}")

            except Exception as e:
                error_log.exception(f"❌ Failed to process key {key}: {e}")

    return records

def fetch_mobile_summary(bucket: str, mobile: str):
    start = time.time()

    mobile_prefix = f"mobile/{mobile}/id/"

    files = list_json_files(bucket, mobile_prefix)
    if not files:
        success_log.info(f"ℹ️ No records found for mobile {mobile}")
        return

    workers = min(workers_count, len(files))
    records = parallel_read_json(bucket, files, workers)

    result = {
        "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
        "status": "success",
        "message": "data retrieved",
        "total_records": len(records),
        # "records": records
    }

    print(result)

fetch_mobile_summary("dev-soc-media", "9500604848")
