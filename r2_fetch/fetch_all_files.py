import boto3, time, json
from decouple import config
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor, as_completed


DATE = "04-03-2026 "

BUCKET = "pos-transaction"
PREFIX = "mobile/9840643109/id/"

MAX_WORKERS = 10


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

# -------------------------------------------------------------

def fetch_object(r2, bucket, key):
    """Download and parse JSON"""
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        data = json.loads(content)

        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]

    except Exception as e:
        print(f"❌ Error reading {key}: {e}")

    return []

# -------------------------------------------------------------

def list_keys(r2, bucket, prefix):
    """Collect all JSON keys"""
    keys = []
    token = None

    while True:
        params = {"Bucket": bucket, "Prefix": prefix}

        if token:
            params["ContinuationToken"] = token

        resp = r2.list_objects_v2(**params)

        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)

        if not resp.get("IsTruncated"):
            break

        token = resp["NextContinuationToken"]

    return keys


# -------------------------------------------------------------

def download_all(bucket, prefix, output_file=f"all_records_{DATE}.json"):

    start = time.time()
    r2 = r2_client()

    print("\n🚀 Collecting file keys...")
    keys = list_keys(r2, bucket, prefix)

    print(f"📂 Total JSON files: {len(keys)}")
    print(f"⚡ Starting parallel download ({MAX_WORKERS} workers)\n")

    total_records = 0
    first_record = True

    with open(output_file, "w", encoding="utf-8") as f:

        f.write("[\n")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            futures = [executor.submit(fetch_object, r2, bucket, key) for key in keys]

            for future in as_completed(futures):

                records = future.result()

                for record in records:

                    if not first_record:
                        f.write(",\n")

                    json.dump(record, f, ensure_ascii=False)

                    first_record = False
                    total_records += 1

                    if total_records % 500 == 0:
                        print(f"🚀 {total_records} records written...")

        f.write("\n]")

    print("\n--------------------------------")
    print(f"📊 Total Records: {total_records}")
    print(f"💾 Output File : {output_file}")
    print(f"⏱ Time Taken  : {round(time.time()-start,2)} sec")
    print("--------------------------------\n")

# -------------------------------------------------------------

download_all(bucket=BUCKET, prefix=PREFIX)

# -------------------------------------------------------------