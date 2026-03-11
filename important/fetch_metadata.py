import time
from core.creds import r2_client

mob_bucket = "pos-transaction-test"
imei_bucket = "pos-transaction-imei-test"
mob_prefix = "mobile"
imei_prefix = "imei"

def get_json_meta(bucket: str, key: str):

    start = time.time()
    r2 = r2_client()

    try:
        head = r2.head_object(Bucket=bucket, Key=key)

        end = time.time()
        print(f"metadata ---> {head}\n")

        return {
            "status": "success",
            "retrieved_in": f"{round((end - start) * 1000, 2)}ms",
            "metadata": head.get("Metadata", {}),
            "size": head.get("ContentLength", 0),
            "last_modified": str(head.get("LastModified")),
            "etag": head.get("ETag")
        }

    except r2.exceptions.NoSuchKey:
        return {
            "status": "failed",
            "message": f"Key '{key}' not found"
        }

if __name__ == "__main__":
    data = get_json_meta(mob_bucket, "mobile/7708910002/mobile_summary.json")
    print(data)
