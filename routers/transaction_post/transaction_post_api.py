import json, datetime
from core.creds import r2_client
from fastapi.responses import JSONResponse
from botocore.exceptions import ClientError
from logs.log_settings import success_log, error_log

class R2Manager:

    def __init__(self, max_retries=5):
        self.r2 = r2_client
        self.max_retries = max_retries

    # ---------- Timestamp helper ----------
    @staticmethod
    def ts():
        """2025-12-02 (20:11:35)"""
        now = datetime.datetime.now()
        return now.strftime("%Y-%m-%d (%H:%M:%S)")

    # ---------- error handler ----------
    @staticmethod
    def error_response(code, message, error):
        error_log.error(f"{message}: {error}")
        error_payload = {"status": "failed", "message": message, "error": error}
        return JSONResponse(status_code=code, content=error_payload)

    def get_count(self, bucket: str, key: str):
        try:
            obj = self.r2().get_object(Bucket=bucket, Key=key)
            etag = obj["ETag"]

            data = json.loads(obj["Body"].read().decode("utf-8"))
            count = int(data.get("pri_id", 0))

            success_log.info(f"Fetched count {count} (ETag={etag})")
            return count, etag

        except ClientError:
            success_log.info(f"No counter found. Starting from 0.")
            return 0, None

        except Exception as e:
            return self.error_response(500, "Get Count Failed", str(e))

    def increment_count(self, bucket: str, key: str, old_count: int, etag: str):
        for attempt in range(self.max_retries):
            try:
                new_count = old_count + 1

                body = {
                    "pri_id": new_count, "updated_at": self.ts()
                }

                params = {
                            "Bucket": bucket,
                            "Key": key,
                            "Body": json.dumps(body).encode("utf-8"),
                            "ContentType": "application/json",
                            "Metadata": {
                                "pri_id": str(new_count),
                                "updated_at": self.ts()
                            }
                }

                # ETag ensures atomic write
                if etag:
                    params["IfMatch"] = etag

                self.r2().put_object(**params)

                success_log.info(f"[{attempt+1}] Count updated → {new_count}")
                return new_count

            except ClientError as e:
                if e.response["Error"]["Code"] in ("PreconditionFailed", "412"):
                    error_log.warning(f"⚠ Write conflict. Retrying... attempt {attempt+1}")
                    old_count, etag = self.get_count(bucket, key)
                    continue
                return self.error_response(500, "Count Update Failed", str(e))

            except Exception as e:
                return self.error_response(500, "Unexpected Error", str(e))

        return self.error_response(409, "Write Conflict", "Too many retries — high concurrency conflict")

    def write_json(self, bucket: str, key: str, data: dict, metadata: dict = None):
        try:
            body = json.dumps(data, indent=2, ensure_ascii=False)
        except Exception as e:
            return self.error_response(500, "JSON Serialization Failed", str(e))

        try:
            self.r2().put_object(
                            Bucket=bucket,
                            Key=key,
                            Body=body.encode("utf-8"),
                            ContentType="application/json",
                            Metadata=metadata or {}
            )

            success_log.info(f"Stored object: {key} ({len(body)} bytes)")
            return True

        except Exception as e:
            return self.error_response(500, "JSON Write Failed", str(e))
