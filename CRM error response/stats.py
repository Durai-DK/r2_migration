import boto3, json, time
from decouple import config
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor, as_completed


def r2_client():

    try:
        return boto3.client("s3",
                            endpoint_url=config("ENDPOINT"),
                            aws_access_key_id=config("ACCESS_KEY_ID"),
                            aws_secret_access_key=config("SECRET_ACCESS_KEY"),
                            config=Config(signature_version="s3v4"),
                            region_name="auto"
                            )
    except Exception as e:
        print(f"\n❌ Failed to initialize R2 client: {e}")


def download_single_json(r2, bucket, key):
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
        data = json.loads(content)

        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            return []

    except Exception as e:
        print(f"⚠️ Failed {key}: {e}")
        return []


def download_and_merge_json(bucket: str, prefix: str, date, max_workers):
    start = time.time()
    r2 = r2_client()

    keys = []
    token = None

    # 1️⃣ List JSON keys
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)

        for item in resp.get("Contents", []):
            key = item["Key"]
            if key.endswith(".json"):
                keys.append(key)

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")

    print(f"📂 Total JSON files found: {len(keys)}")

    # 2️⃣ Parallel Download
    merged_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_single_json, r2, bucket, key)
            for key in keys
        ]

        for future in as_completed(futures):
            merged_data.extend(future.result())

    print(f"✅ Total merged records: {len(merged_data)}")
    print(f"⏱ Completed in {round(time.time() - start, 2)}s")

    with open(f"merged_output_{date}.json", "w") as f:
        json.dump(merged_data, f, indent=2)


BUCKET = "pos-transaction"
PREFIX = "crm_error_response/2026/03/09/"
today = "10-03-2026"

download_and_merge_json(bucket=BUCKET, prefix=PREFIX, date=today, max_workers=20)
