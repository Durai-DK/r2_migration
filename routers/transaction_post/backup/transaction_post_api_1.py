# import json, datetime, time
# from core.creds import r2_client
# from botocore.exceptions import ClientError
# from logs.log_settings import success_log, error_log
# from routers.r2_bucket.get_functions import error_response
#
# max_retries = 5
#
# pos_bucket = "pos-transaction"
# path_key = "count/old_pri_id.json"
# dt = datetime.datetime.fromisoformat("2025-12-02T20:11:35")
# now = dt.strftime("%Y-%m-%d ( %H:%M:%S )")
#
# def store_json_data_etag(bucket, key, data, metadata, if_match=None):
#     if metadata is None:
#         metadata = {}
#
#     if not key:
#         raise ValueError("R2 object key cannot be empty.")
#
#     try:
#         body = json.dumps(data, indent=2, ensure_ascii=False)
#     except Exception as e:
#         error_log.error(f"❌ JSON serialization failed for key {key}: {e}")
#         return error_response(500, "JSON Serialization Failed", str(e))
#
#     try:
#         r2 = r2_client()
#         params = dict(Bucket=bucket,
#                       Key=key,
#                       Body=body.encode("utf-8"),
#                       ContentType="application/json",
#                       Metadata=metadata
#         )
#         if if_match:
#             params["IfMatch"] = if_match
#
#         r2.put_object(**params)
#
#         success_log.info(f"✅ Stored {key} ({len(body)} bytes) with ETag: {if_match}")
#         return True
#
#     except ClientError as e:
#         error_log.error(f"❌ Client error storing {key}: {e}")
#         return error_response(500, "R2 put_object failed", str(e))
#
#     except Exception as e:
#         error_log.error(f"❌ Unexpected error storing {key}: {e}")
#         return error_response(500, "Unexpected Error", str(e))
#
# def get_and_update_count(bucket, key):
#     r2 = r2_client()
#
#     for attempt in range(max_retries):
#         try:
#             # Step 1 — Read old count + ETag
#             try:
#                 obj = r2.get_object(Bucket=bucket, Key=key)
#                 etag = obj["ETag"]
#                 old_content = json.loads(obj["Body"].read().decode("utf-8"))
#                 old_count = old_content.get("pri_id", 0)
#             except ClientError:
#                 etag = None
#                 old_count = 0  # first time case
#
#             success_log.info(f"old count is : {old_count}")
#             new_count = old_count + 1
#
#             result = {"pri_id": new_count, "updated_at": now}
#             meta_data = {"pri_id": str(new_count), "updated_at": now}
#
#             store_json_data_etag(pos_bucket, path_key, result, meta_data, etag)
#
#             success_log.info(f"Updated {key} → New count: {new_count}")
#
#         except ClientError as e:
#             # Handle race condition
#             if e.response["Error"]["Code"] in ("PreconditionFailed", "412"):
#                 error_log.warning(f"⚠ Write conflict for {key}. Retrying... attempt {attempt + 1}")
#                 time.sleep(0.05)
#                 continue
#             return error_response(500, "R2 Error", str(e))
#
#         except Exception as e:
#             error_log.error(f"❌ Unexpected error updating count for {key}: {e}")
#             return error_response(500, "Unexpected Error", str(e))
#
#     error_log.error(f"❌ Too many retries for {key} — high write conflict")
#     return error_response(409, "Write Conflict", "Too many retries, parallel writes detected")
#
# def get_count_etg(bucket, key):
#     r2 = r2_client()
#
#     try:
#         obj = r2.get_object(Bucket=bucket, Key=key)
#         etag = obj["ETag"]
#
#         data = json.loads(obj["Body"].read().decode("utf-8"))
#         count = int(data.get("pri_id", 0))
#
#         success_log.info(f"Fetched count: {count} (ETag={etag})")
#         return count, etag
#
#     except ClientError:
#         # First time
#         success_log.info("No count object found. Starting from 0.")
#         return 0, None
#
# def update_count(bucket, key, old_count, etag):
#     r2 = r2_client()
#
#     for attempt in range(max_retries):
#         try:
#             new_count = old_count + 1
#
#             body = {
#                 "pri_id": new_count,
#                 "updated_at": ts()
#             }
#
#             params = {
#                 "Bucket": bucket,
#                 "Key": key,
#                 "Body": json.dumps(body).encode("utf-8"),
#                 "ContentType": "application/json",
#                 "Metadata": {
#                     "pri_id": str(new_count),
#                     "updated_at": ts()
#                 }
#             }
#
#             if etag:
#                 params["IfMatch"] = etag
#
#             r2.put_object(**params)
#
#             success_log.info(f"Updated count → {new_count}")
#             return new_count
#
#         except ClientError as e:
#             if e.response["Error"]["Code"] in ("PreconditionFailed", "412"):
#                 # Another request updated it → fetch again
#                 error_log.warning(
#                     f"Write conflict, retrying... attempt {attempt+1}"
#                 )
#                 old_count, etag = get_count_etg(bucket, key)
#                 continue
#
#             error_log.error(str(e))
#             raise
#
#     raise Exception("Too many retries")
#
# def write_final_payload(bucket, key, payload, metadata):
#     r2 = r2_client()
#     r2.put_object(
#         Bucket=bucket,
#         Key=key,
#         Body=json.dumps(payload).encode("utf-8"),
#         ContentType="application/json",
#         Metadata=metadata
#     )
#     success_log.info(f"Stored final record: {key}")
#
# @app.post("/api/create/")
# def create_record(...):
#
#     # 1. Get count safely
#     old_count, etag = get_count(count_bucket, "counter.json")
#
#     # 2. Add this count to your response payload
#     response_payload = {
#         "pri_id": old_count + 1,
#         "data": request_body,
#         "timestamp": ts()
#     }
#
#     # 3. Write main record to another bucket
#     write_final_payload(main_bucket, record_key, response_payload, metadata={})
#
#     # 4. Update count bucket in background or parallel
#     update_count(count_bucket, "counter.json", old_count, etag)
#
#     return {
#         "status": "success",
#         "pri_id": old_count + 1  # VERY IMPORTANT
#     }
#
# def create_old_pri_id():
#     count_key = f"count/old_pri_id.json"
#
#     body = {
#         "pri_id": 0, "updated_at": now
#     }
#
#     metadata = {
#         "pri_id": str(0), "updated_at": now
#     }
#
#     params = {
#         "Bucket": pos_bucket,
#         "Key": count_key,
#         "Body": json.dumps(body).encode("utf-8"),
#         "ContentType": "application/json",
#         "Metadata": metadata
#     }
#
#     r2 = r2_client()
#     r2.put_object(**params)
#     print("count file created")
