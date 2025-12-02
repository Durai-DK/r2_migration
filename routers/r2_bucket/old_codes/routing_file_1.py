# from fastapi import Query
# from fastapi import APIRouter
# from routers.r2_bucket.get_functions import *
# from core.creds import logger, MysqlCatalog
# from concurrent.futures import ProcessPoolExecutor
# from routers.r2_bucket.create_functions import process_invoice
#
#
# # router = APIRouter(prefix="/api/bucket", tags=["Store Data to Bucket "])
#
# mob_bucket = "dev-soc-media"
# imei_bucket = "dev-soc-media-1"
#
# mob_prefix = "mobile"
# imei_prefix = "imei_numbers"
#
# mysql_db_name = "Transaction"
#
# WORKERS = 10
#
# @router.post("/create-files/")
# def create_bucket(start_range: int, end_range: int):
#
#     if end_range <= start_range:
#         logger.error(f"❌ end_date must be greater than start_date.")
#         return error_response(code=400, message="invalid date range", error="end_date must be greater than start_date.")
#
#     overall_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#
#     # ------------- Fetch Mysql Data -------------
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(mysql_db_name, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#
#         if not rows:
#             return error_response(code=404, message="data not found", error="No data found in the given range.")
#
#         print(f"✅ MySQL data fetched: range = {start_range}-{end_range} records = {len(rows)}, time = {mysql_duration}s")
#
#     except Exception as e:
#         return error_response(code=500, message="MySQL retrival error", error=str(e))
#
#     # ------------- Multiprocessing -------------
#     with ProcessPoolExecutor(max_workers=WORKERS) as executor:
#         futures = [executor.submit(process_invoice,row, mob_bucket, imei_bucket, mob_prefix, imei_prefix) for row in rows]
#
#         for future in futures:
#             try:
#                 future.result()
#             except Exception as worker_err:
#                 logger.error(f"❌ Worker error: {worker_err}")
#
#
#     total_time = round(time.time() - overall_start, 2)
#
#     logger.info(f"✅ Migration completed: processed = {len(rows)}, range = {start_range}-{end_range}, time = {total_time}s")
#
#     data = {
#             "status": "success",
#             "message": "Data migrated successfully to R2",
#             "detail": f"Processed {len(rows)} records (range {start_range}-{end_range}) in {total_time}s",
#         }
#     return JSONResponse(status_code=200, content=data)
#
# @router.get("/fetch-files/")
# def fetch_files(
#                     field_type: str = Query(..., enum=["imei", "mobile"]),
#                     imei_no: str | None = None,
#                     mobile_no: str | None = None,
#                     category: str = Query(None, enum=["specific_file", "summary"]),
#                     specific_field: str | None = Query(None, enum=["id", "imei"]),
#                     specific_id: str | None = None,
#
#                     summary_field: str | None = Query(None, enum=["id", "imei", "mobile", "id_list", "imei_list"]),
#                     invoice_no: str | None = None,
#
#                     per_page: int = 10,
#                     page_no: int = 1,
#                     start_date: str | None = Query(None, description="Date format: YYYY-MM-DD"),
#                     end_date: str | None = Query(None, description="Date format: YYYY-MM-DD"),
# ):
#     # ----- Validation -----
#     if field_type == "imei":
#         if not imei_no:
#             return error_response(400, "imei_no is missing", "imei_no is required to fetching IMEI data")
#
#     elif field_type == "mobile":
#         if not mobile_no:
#             return error_response(400, "mobile_no is missing", "mobile_no is required to fetch mobile data.")
#
#         if category == "specific_file":
#             if not specific_field:
#                 return error_response(400, "specific_field is missing", "specific_field is required when category == 'specific_file'.")
#
#             if specific_field == "id" and not specific_id:
#                 return error_response(400, "specific_id is missing", "specific_id is required to fetching ID file.")
#
#             if specific_field == "imei" and not imei_no:
#                 return error_response(400, "imei_no is missing", "imei_no is required to fetching IMEI file.")
#
#         if category == "summary":
#             if not summary_field:
#                 return error_response(400, "summary_field is missing", "summary_field is required when category == 'summary'.")
#
#             if summary_field in ["id_list", "imei_list"] and not invoice_no:
#                 return error_response(400, "invoice_no is missing", "invoice_no is required for ID/IMEI list fetch.")
#
#         if start_date and end_date and end_date < start_date:
#             return error_response(400,"Invalid date range", "'end_date' cannot be earlier than 'start_date'.")
#
#     # ----- EXECUTION -----
#     if field_type == "imei" and imei_no:
#         key = f"{imei_prefix}/{imei_no}.json"
#         return fetch_specific_file(imei_bucket, key)
#
#     elif field_type == "mobile":
#
#         if category == "specific_file":
#
#             if specific_field == "id" and specific_id:
#                 key = f"{mob_prefix}/{mobile_no}/id/{specific_id}.json"
#                 return fetch_specific_file(mob_bucket, key)
#
#             if specific_field == "imei" and imei_no:
#                 key = f"{mob_prefix}/{mobile_no}/imei/{imei_no}.json"
#                 return fetch_specific_file(mob_bucket, key)
#
#         if category == "summary":
#
#             if summary_field == "id":
#                 key = f"{mob_prefix}/{mobile_no}/id_summary.json"
#                 return summary_retrieve(mob_bucket, key, page_no, per_page, start_date, end_date)
#
#             if summary_field == "imei":
#                 key = f"{mob_prefix}/{mobile_no}/imei_summary.json"
#                 return summary_retrieve(mob_bucket, key, page_no, per_page, start_date, end_date)
#
#             if summary_field == "mobile":
#                 key = f"{mob_prefix}/{mobile_no}/mobile_summary.json"
#                 return summary_retrieve(mob_bucket, key, page_no, per_page, start_date, end_date)
#
#             if summary_field == "id_list":
#                 key = f"{mob_prefix}/{mobile_no}/id_summary.json"
#                 return records_retrieve(mob_bucket, key, invoice_no)
#
#             if summary_field == "imei_list":
#                 key = f"{mob_prefix}/{mobile_no}/imei_summary.json"
#                 return records_retrieve(mob_bucket, key, invoice_no)
#
#     return error_response(400,
#                           "Invalid request combination",
#                           [
#                               "Verify 'field_type' is either 'mobile' or 'imei'",
#                               "Ensure required parameters match the selected category",
#                               "Check 'specific_field' or 'summary_field' values",
#                               "Review required dependencies: imei_no, mobile_no, invoice_no, etc.",
#                           ]
#     )
#
# @router.get("/count-files/")
# def count_files_api(
#         bucket_type: str = Query(..., enum=["imei", "mobile"]),
#         mobile_no: str | None = Query(None, description="Mobile number"),
#         file_type: str | None = Query(None, enum=["id", "imei", "both", "overall"])
# ):
#
#     if bucket_type == "mobile":
#
#         if not mobile_no:
#             return error_response(code=400,
#                                   message="mobile number is missing",
#                                   error="mobile_no is required when bucket_type='mobile'")
#         if not file_type:
#             return error_response(code=400,
#                                   message="file_type is missing",
#                                   error="file_type is required when bucket_type='mobile'.")
#
#     # ----- Execution -----
#     if bucket_type == "imei":
#         path_key = f"{imei_prefix}/"
#         return count_files(imei_bucket, path_key)
#
#     elif bucket_type == "mobile":
#         if mobile_no and file_type == "id":
#             path_key = f"{mob_prefix}/{mobile_no}/id/"
#             return count_files(mob_bucket, path_key)
#
#         if mobile_no and file_type == "imei":
#             path_key = f"{mob_prefix}/{mobile_no}/imei/"
#             return count_files(mob_bucket, path_key)
#
#         if mobile_no and file_type == "both":
#             path_key = f"{mob_prefix}/{mobile_no}/"
#             return count_files(mob_bucket, path_key)
#
#         if mobile_no and file_type == "overall":
#             path_key = f"{mob_prefix}/"
#             return count_mobile_numbers(mob_bucket, path_key)
#
#     return error_response(
#         code=400,
#         message="Invalid request combination",
#         error=[
#             "Verify 'bucket_type' is either 'imei' or 'mobile'",
#             "Ensure required parameters match the selected category.",
#             "For 'mobile' both 'mobile_no' and 'file_type' are required."
#         ]
#     )
#
# @router.get("/get-folder/")
# def fetch_folder(prefix: str, page: int = 1, per_page: int = 10):
#
#     result = fetch_folder(mob_bucket, prefix, page, per_page)
#
#     json_bytes = json.dumps(result, ensure_ascii=False).encode("utf-8")
#     gzipped = gzip.compress(json_bytes, compresslevel=6)
#
#     return Response(
#         content=gzipped,
#         media_type="application/json",
#         headers={
#             "Content-Encoding": "gzip",
#             "Vary": "Accept-Encoding"
#         }
#     )
#
#
# # @router.get("/get-folder-token-based/")
# # def get_folder_token_based(prefix: str, result_per_page: int = 250, token: str = None):
# #     def fetch_file(r2, bucket, obj):
# #         key_path = obj["Key"]
# #         data = r2.get_object(Bucket=bucket, Key=key_path)
# #         body = data["Body"].read().decode("utf-8")
# #         return {
# #             "key": key_path,
# #             "content": body,
# #             "last_modified": obj["LastModified"].isoformat()
# #         }
# #
# #     def fetch_folder(bucket, prefix, result_per_page, token=None):
# #         r2 = r2_client()
# #         start = time.time()
# #
# #         try:
# #             params = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": result_per_page}
# #             if token:
# #                 params["ContinuationToken"] = token
# #
# #             response = r2.list_objects_v2(**params)
# #
# #             sorted_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
# #
# #             file_objects = [obj for obj in sorted_files if not obj["Key"].endswith("/")]
# #
# #             files = []
# #             with ThreadPoolExecutor(max_workers=min(10, len(file_objects))) as executor:
# #                 future_map = {executor.submit(fetch_file, r2, bucket, obj): obj for obj in file_objects}
# #                 for future in as_completed(future_map):
# #                     try:
# #                         files.append(future.result())
# #                     except Exception as e:
# #                         obj = future_map[future]
# #                         print(f"❌ Error fetching {obj['Key']}: {e}")
# #
# #             return {
# #                 "success": True,
# #                 "count": len(files),
# #                 "next_token": response.get("NextContinuationToken"),
# #                 "has_more": response.get("IsTruncated", False),
# #                 "retrieved_in_ms": round((time.time() - start) * 1000, 2),
# #                 "files": files,
# #             }
# #
# #         except r2.exceptions.NoSuchKey:
# #             return {"success": False, "error": "File not found", "prefix": prefix}
# #
# #         except Exception as e:
# #             return {"success": False, "error": str(e)}
# #
# #     data = fetch_folder(mob_bucket, prefix, result_per_page, token)
# #     json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
# #     gzipped = gzip.compress(json_bytes, compresslevel=6)
# #
# #     return Response(
# #         content=gzipped,
# #         media_type="application/json",
# #         headers={
# #             "Content-Encoding": "gzip",
# #             "Vary": "Accept-Encoding"
# #         }
# #     )
#
#
# # @router.get("/get-specific-file/")
# # def get_specific_file(
# #         bucket_type: str = Query(..., enum=["mobile", "imei"]),
# #         mobile: str | None = None,
# #         file_type: str | None = Query(None, enum=["invoices", "imei_numbers", "summary"]),
# #         invoice_no: str | None = None,
# #         pri_id: str | None = None,
# #         imei_no: str | None = None,
# #         summary_fields: str | None = Query(None, enum=["invoice", "imei", "imei_list"])
# # ):
# #
# #     # --- Case 1: bucket_type = mobile ---
# #     if bucket_type == "mobile":
# #         if not mobile:
# #             return error_response(400, "mobile number is missing", "mobile_no is required when bucket_type='mobile'.")
# #
# #         if not file_type:
# #             return error_response(400, "file_type is missing", "file_type is required when bucket_type='mobile'.")
# #
# #         if file_type == "invoices":
# #             if not invoice_no or not pri_id:
# #                 return error_response(400, "invoice id or pri_id is missing", "invoice_no and pri_id are required when file_type='invoices'.")
# #
# #         if file_type == "imei_numbers" and not imei_no:
# #             return error_response(400, "imei number is missing", "imei_no is required when file_type='imei_numbers'.")
# #
# #         if file_type == "summary":
# #
# #             if not summary_fields:
# #                 return error_response(400, "summary_fields is missing", "summary_fields is required when file_type='summary'.")
# #
# #             if summary_fields == "imei_list" and not invoice_no:
# #                 return error_response(400, "invoice id is missing", "invoice_no is required when summary_fields='imei_list'.")
# #
# #     # --- Case 2: bucket_type = imei ---
# #     if bucket_type == "imei":
# #         if not imei_no:
# #             return error_response(400, "imei number is missing", "imei_no is required when bucket_type='imei'.")
# #
# #     # ------------------------------------------------------------------------------------------------------------------
# #
# #     if bucket_type == "mobile" and file_type == "summary" and summary_fields == "imei_list":
# #         key = f"mobile/{mobile}/imei_summary.json"
# #         return get_imei_list(mob_bucket, key, invoice_no)
# #
# #     # ----- KEY GENERATION -----
# #     path_key = None
# #     if bucket_type == "mobile":
# #         bucket = mob_bucket
# #         if file_type == "invoices":
# #             invoice_no_fix = invoice_no.replace("/", "_")
# #             path_key = f"mobile/{mobile}/invoices/{invoice_no_fix}_{pri_id}.json"
# #
# #         elif file_type == "imei_numbers":
# #             path_key = f"mobile/{mobile}/imei_numbers/{imei_no}.json"
# #
# #         elif file_type == "summary":
# #
# #             if summary_fields == "invoice":
# #                 path_key = f"mobile/{mobile}/invoice_summary.json"
# #
# #             elif summary_fields == "imei":
# #                 path_key = f"mobile/{mobile}/imei_summary.json"
# #
# #             else:
# #                 return error_response(400, "Invalid summary_fields", "Allowed: invoice, imei, imei_list")
# #
# #     else:  # imei bucket
# #         bucket = imei_bucket
# #         path_key = f"imei_numbers/{imei_no}.json"
# #
# #     data = fetch_single_file(bucket, path_key)
# #
# #     if data.get("status") == "failed":
# #         return JSONResponse(status_code=400, content=data)
# #
# #     json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
# #     gzipped = gzip.compress(json_bytes, compresslevel=6)
# #
# #     return Response(status_code=200,
# #                     content=gzipped,
# #                     media_type="application/json",
# #                     headers={"Content-Encoding": "gzip",
# #                              "Vary": "Accept-Encoding"}
# #                     )
