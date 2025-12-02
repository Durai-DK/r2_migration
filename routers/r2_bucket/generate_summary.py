import json, time
from core.creds import logger, r2_client
from concurrent.futures import ProcessPoolExecutor


def read_json(bucket, key: str):
    r2 = r2_client()

    try:
        res = r2.get_object(Bucket=bucket, Key=key)
        return json.loads(res["Body"].read().decode("utf-8"))
    except r2.exceptions.NoSuchKey:
        logger.error(f"❌ File not found: {key}")
        return None
    except Exception as e:
        logger.error(f"❌ Error reading {key}: {e}")
        return None

def list_json_files(bucket: str, prefix: str):
    r2 = r2_client()
    keys = []
    token = None

    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)
        contents = resp.get("Contents", [])

        for item in contents:
            keys.append(item["Key"])

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")

    return keys

def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0

def read_json_worker(args):
    bucket, key = args
    try:
        return key, read_json(bucket, key)   # returns (key, content)
    except Exception as e:
        return key, None

def parallel_read_json(bucket, keys, workers=4):
    imei_list = []

    tasks = [(bucket, key) for key in keys]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for key, content in executor.map(read_json_worker, tasks):
            if content:
                print("key", key)
                imei_list.append(content)
            else:
                logger.error(f"⚠️ Failed to parse IMEI file: {key}")

    return imei_list

def read_and_merge_json(bucket, key, new_data: dict):
    existing = read_json(bucket, key)

    if not existing:
        # Nothing to merge
        return new_data

    merged = {}

    # ---- merge base keys ----
    for k in set(existing.keys()).union(new_data.keys()):
        if k not in existing:
            merged[k] = new_data[k]
        elif k not in new_data:
            merged[k] = existing[k]
        else:
            # If value is a list → append
            if isinstance(existing[k], list) and isinstance(new_data[k], list):
                merged[k] = existing[k] + new_data[k]

            # If numeric → sum
            elif isinstance(existing[k], (int, float)) and isinstance(new_data[k], (int, float)):
                merged[k] = existing[k] + new_data[k]

            # Otherwise → override
            else:
                merged[k] = new_data[k]

    return merged

def imei_summary_fun(bucket, mobile):
    #  ----- Imei Summary -----
    imei_prefix = f"mobile/{mobile}/imei/"
    imei_summary_key = f"mobile/{mobile}/imei_summary.json"

    imei_files = list_json_files(bucket, imei_prefix)

    # MULTIPROCESS IMEI READING
    workers_count = min(8, len(imei_files))
    records = parallel_read_json(bucket, imei_files, workers=workers_count)

    imei_summary = {"total_records": len(records), "records": records}

    metadata = {"mobile": mobile, "records-count": str(len(records))}

    store_json_data(bucket, imei_summary, imei_summary_key, metadata)

def id_summary_fun(bucket, mobile):
    #  ----- Invoice Summary -----
    id_prefix = f"mobile/{mobile}/id/"
    id_summary_key = f"mobile/{mobile}/id_summary.json"

    id_files = list_json_files(bucket, id_prefix)

    # MULTIPROCESS INVOICE READING
    workers_count = min(8, len(id_files))
    records = parallel_read_json(bucket, id_files, workers=workers_count)

    total_spends = sum(safe_float(i.get("bill_grand_total__c", 0)) for i in records)

    id_summary = {
        "total_records": len(records), "total_spends": total_spends, "records": records
    }

    metadata = {
        "mobile": mobile, "records-count": str(len(records)), "total-spends": str(total_spends)}

    store_json_data(bucket, id_summary, id_summary_key, metadata)

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
        logger.error(f"❌ JSON serialization failed for key {key}: {e}")
        raise

    try:
        r2 = r2_client()
        r2.put_object(Bucket=bucket,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=metadata)

        logger.info(f"✅ Stored {key} ({len(body)} bytes)")

    except Exception as e:
        logger.exception(f"❌ Unexpected error storing {key}: {e}")
        raise

def generate_mobile_summary(bucket: str, mobile: str):
    start = time.time()

    imei_summary_fun(bucket, mobile)
    id_summary_fun(bucket, mobile)

    return {
            "status": "success",
            "message": "Mobile summary updated successfully.",
            "retrieved_in": f"{round((time.time() - start), 2)}s",
    }

def generate_global_summary(bucket: str):
    start = time.time()

    summary_key = "mobile_summary.json"

    # ---------- Load old summary if exists ----------
    try:
        old_summary = read_json(bucket, summary_key)  # returns dict or None
    except Exception:
        old_summary = None

    old_mobile_set = set()
    if old_summary and "unique_mobiles" in old_summary:
        old_mobile_set = set(old_summary["unique_mobiles"])

    # ---------- Read all files under mobile/ ----------
    mobile_files = list_json_files(bucket, "mobile/")
    new_mobile_set = set()

    for path in mobile_files:
        parts = path.split("/")
        if len(parts) > 1 and parts[1].isdigit():
            new_mobile_set.add(parts[1])

    # ---------- Merge old + new mobile numbers ----------
    final_mobile_set = old_mobile_set.union(new_mobile_set)

    # ---------- Build summary ----------
    clean = sorted(list(final_mobile_set))      # sorted for cleaner structure

    global_summary = {"mobile_count": len(final_mobile_set), "unique_mobiles": clean}

    metadata = {"mobile_count": str(len(final_mobile_set))}
    store_json_data(bucket, global_summary, summary_key, metadata)

    return {
        "status": "success",
        "message": "Global mobile count updated successfully.",
        "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
        "added_new": len(final_mobile_set) - len(old_mobile_set)
    }

if __name__ == "__main__":
    result = generate_mobile_summary("dev-soc-media", "9500604848")
    print(result)

    # result = generate_global_summary("dev-soc-media")
    # print(result)