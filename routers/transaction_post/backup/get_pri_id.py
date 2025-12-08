import json, datetime
from core.creds import r2_client
from fastapi.responses import JSONResponse
from botocore.exceptions import ClientError
from logs.log_settings import success_log, error_log

r2 = r2_client()

max_retries = 3

def error_response(code, message, error):
        error_log.error(f"{message}: {error}")
        error_payload = {"status": "failed", "message": message, "error": error}
        return JSONResponse(status_code=code, content=error_payload)


def get_count(bucket: str, key: str):
    obj = r2.get_object(Bucket=bucket, Key=key)
    etag = obj["ETag"]
    data = json.loads(obj["Body"].read().decode("utf-8"))

    count = int(data.get("pri_id", 0))
    success_log.info(f"Fetched count={count}, ETag={etag}")
    return count, etag


def fetch_and_update_count(bucket: str, key: str) -> int:
    for attempt in range(max_retries):
        try:
            old_count, etag = get_count(bucket, key)
            new_count = old_count + 1

            now = datetime.datetime.now().strftime("%Y-%m-%d ( %H:%M:%S )")

            body = {"pri_id": new_count, "updated_at": now}

            params = {
                "Bucket": bucket,
                "Key": key,
                "Body": json.dumps(body).encode("utf-8"),
                "ContentType": "application/json",
                "Metadata": {"pri_id": str(new_count), "updated_at": now}
            }

            if etag:
                params["IfMatch"] = etag

            r2.put_object(**params)
            success_log.info(f"[{attempt+1}] Count updated → {new_count}")
            return new_count


        except ClientError as e:
            if e.response["Error"]["Code"] in ("PreconditionFailed", "412"):
                error_log.warning(f"⚠ Retry {attempt + 1}")
                continue
            raise

    raise RuntimeError("High concurrency conflict while generating PRI_ID")