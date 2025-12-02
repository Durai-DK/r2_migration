# import json, time, math, gzip
# from datetime import datetime
# from core.creds import r2_client
# from fastapi.responses import JSONResponse, Response
# from concurrent.futures import ThreadPoolExecutor, as_completed
#
# def error_response(code, message, error):
#
#     error_payload = {"status": "failed", "message": message, "error": error}
#     return JSONResponse(status_code=code, content=error_payload)
#
# def summary_retrieve(bucket, key, page=1, per_page=10, start_date=None, end_date=None):
#
#     start = time.time()
#     r2 = r2_client()
#
#     try:
#         obj = r2.get_object(Bucket=bucket, Key=key)
#         body = obj["Body"].read()
#         content = body.decode("utf-8", errors="ignore")
#
#         data = json.loads(content)
#         items = data.get("records", [])
#
#         def parse_date(inv):
#             try:
#                 return datetime.strptime(inv.get("Bill_Date__c", ""), "%Y-%m-%d")
#             except:
#                 return None
#
#         from_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
#         to_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
#
#         if from_dt or to_dt:
#             filtered = []
#             for inv in items:
#                 d = parse_date(inv)
#                 if not d:
#                     continue
#
#                 if from_dt and d < from_dt:
#                     continue
#                 if to_dt and d > to_dt:
#                     continue
#
#                 filtered.append(inv)
#             items = filtered
#
#         items.sort(key=lambda x: x.get("pri_id", 0), reverse=True)
#
#         # ---- Pagination ----
#         total = len(items)
#         start_i = (page - 1) * per_page
#         end_i = start_i + per_page
#         paginated = items[start_i:end_i]
#
#         result = {
#                     "key": key,
#                     "retrieved_in_ms": round((time.time() - start) * 1000, 2),
#                     "status": "success",
#                     "message": "Mobile summary retrieved successfully",
#                     "page_no": page,
#                     "per_page": per_page,
#                     "total_pages": (total + per_page - 1) // per_page,
#                     "total_records": total,
#                     "records": paginated
#         }
#
#         json_bytes = json.dumps(result, ensure_ascii=False).encode("utf-8")
#         data = gzip.compress(json_bytes, compresslevel=6)
#
#         return Response(
#                         status_code=200,
#                         content=data,
#                         media_type="application/json",
#                         headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"}
#                  )
#     except r2.exceptions.NoSuchKey:
#         return error_response(404, "Failed to process mobile summary", f"This key '{key}' not found")
#     except Exception as e:
#         return error_response(400, "Failed to process mobile summary", str(e))
#
# def fetch_specific_file(bucket: str, key: str):
#     start = time.time()
#     r2 = r2_client()
#
#     try:
#         obj = r2.get_object(Bucket=bucket, Key=key)
#         body = obj["Body"].read()
#
#         try:
#             content = body.decode("utf-8")
#         except:
#             content = body
#
#         end = time.time()
#         retrieve_ms = round((end - start) * 1000, 2)
#
#         result = {
#             "retrieved_in_ms": retrieve_ms,
#             "status": "success",
#             "message": "Specific record retrieved successfully",
#             "records": content
#         }
#
#         return JSONResponse(status_code=200, content=result)
#
#     except r2.exceptions.NoSuchKey:
#         return error_response(404, "Failed to retrieve Specific record", f"This key '{key}' not found")
#
#     except Exception as e:
#         return error_response(400, "Failed to retrieve specific records", str(e))
#
# def records_retrieve(bucket, key, invoice_no):
#     start = time.time()
#     r2 = r2_client()
#
#     try:
#         obj = r2.get_object(Bucket=bucket, Key=key)
#         body = obj["Body"].read()
#         content = body.decode("utf-8", errors="ignore")
#         data = json.loads(content)
#
#         files = data.get("records", [])
#
#         matched = []
#
#         for item in files:
#             if invoice_no == item.get("bill_transaction_no__c", "").strip():
#                 matched.append(item)
#
#         matched.sort(key=lambda x: x.get("pri_id", ""), reverse=True)
#
#         results = {
#                     "retrieved_in_ms": round((time.time() - start) * 1000, 2),
#                     "status": "success",
#                     "message": "Records retrieved successfully",
#                     "total_records": len(matched),
#                     "records": matched
#         }
#         return JSONResponse(status_code=200, content=results)
#
#     except r2.exceptions.NoSuchKey:
#         return error_response(404, "Failed to retrieve records", f"This key '{key}' not found")
#     except Exception as e:
#         return error_response(400, "Failed to retrieve records", str(e))
#
# def count_files(bucket: str, key:str):
#     start = time.time()
#     r2 = r2_client()
#
#     count = 0
#     continuation_token = None
#
#     try:
#         while True:
#             kwargs = {"Bucket": bucket, "Prefix": key}
#
#             if continuation_token:
#                 kwargs["ContinuationToken"] = continuation_token
#
#             response = r2.list_objects_v2(**kwargs)
#
#             contents = response.get("Contents", [])
#             count += len(contents)
#
#             if response.get("IsTruncated"):
#                 continuation_token = response.get("NextContinuationToken")
#             else:
#                 break
#
#         # ---- SUCCESS RESPONSE ----
#         results = {
#             "retrieved_in_ms": round((time.time() - start) * 1000, 2),
#             "status": "success",
#             "message": "File count retrieval successful",
#             "total_records": count
#         }
#         return JSONResponse(status_code=200, content=results)
#
#     except r2.exceptions.NoSuchKey:
#         return error_response(404, "Failed to count files", f"Key not found: '{key}'")
#
#     except Exception as e:
#         return error_response(400, "Failed to count files", str(e))
#
# def count_mobile_numbers(bucket: str, mob_prefix: str):
#     start = time.time()
#     r2 = r2_client()
#
#     mobile_set = set()
#     continuation_token = None
#     try:
#         while True:
#             kwargs = {"Bucket": bucket, "Prefix": mob_prefix}
#             if continuation_token:
#                 kwargs["ContinuationToken"] = continuation_token
#
#             response = r2.list_objects_v2(**kwargs)
#             contents = response.get("Contents", [])
#
#             for obj in contents:
#                 key = obj["Key"]  # e.g., "mobile/9876543210/id/abc.json"
#                 parts = key.split("/")
#
#                 # Expected format: ["mobile", "9876543210", "id", ...]
#                 if len(parts) > 1 and parts[1].isdigit():
#                     mobile_set.add(parts[1])  # add mobile number
#
#             if response.get("IsTruncated"):
#                 continuation_token = response.get("NextContinuationToken")
#             else:
#                 break
#
#         results = {
#             "retrieved_in_ms": round((time.time() - start) * 1000, 2),
#             "status": "success",
#             "message": "Mobile count retrieved successfully",
#             "total_records": len(mobile_set),
#             "unique_records": list(mobile_set)
#         }
#         return JSONResponse(status_code=200, content=results)
#
#     except Exception as e:
#         return error_response(400, "Failed to count mobile numbers", str(e))
#
# def fetch_file(r2, bucket, obj):
#     body = r2.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
#     data = json.loads(body)
#     invoice_date = (data.get("Bill_Date__c") or "").strip()
#
#     return {
#         "key": obj["Key"],
#         "invoice_date": invoice_date,
#         "data": data
#     }
#
# def safe_parse_date(date_str: str):
#     try:
#         return datetime.strptime(date_str, "%Y-%m-%d")
#     except:
#         return datetime.min
#
# def invoice_based_serial_no(bucket, key, invoice_id, page=1, per_page=10):
#     start = time.time()
#     r2 = r2_client()
#     try:
#         response = r2.list_objects_v2(Bucket=bucket, Prefix=key)
#
#         objects = response.get("Contents", [])
#         file_objects = [obj for obj in objects if not obj["Key"].endswith("/")]
#
#         files = []
#         file_count = len(file_objects)
#
#         if file_count == 0:
#             return error_response(
#                         code=404,
#                         message="Key not found",
#                         error="The folder associated with the provided key does not exist."
#                     )
#
#         max_workers = max(1, min(4, file_count))
#         with ThreadPoolExecutor(max_workers=max_workers) as exe:
#             future_map = {exe.submit(fetch_file, r2, bucket, obj): obj for obj in file_objects}
#
#             for future in as_completed(future_map):
#                 try:
#                     files.append(future.result())
#                 except Exception as e:
#                     obj = future_map[future]
#                     print(f"❌ Error fetching {obj['Key']}: {e}")
#
#     except Exception as e:
#         return {
#             "status": "failed",
#             "message": "Failed to process mobile summary",
#             "error": str(e)
#         }
#
# def fetch_folder(bucket, prefix, page, per_page):
#         r2 = r2_client()
#         start = time.time()
#
#         try:
#             response = r2.list_objects_v2(Bucket=bucket, Prefix=prefix)
#             objects = response.get("Contents", [])
#             file_objects = [obj for obj in objects if not obj["Key"].endswith("/")]
#
#             files = []
#             file_count = len(file_objects)
#
#             if file_count == 0:
#                 return {
#                     "success": True,
#                     "total_count": 0,
#                     "page": page,
#                     "per_page": per_page,
#                     "page_count": 0,
#                     "retrieved_in_ms": round((time.time() - start) * 1000, 2),
#                     "files": [],
#                 }
#
#             max_workers = max(1, min(10, file_count))
#
#             with ThreadPoolExecutor(max_workers=max_workers) as exe:
#                 future_map = {exe.submit(fetch_file, r2, bucket, obj): obj for obj in file_objects}
#
#                 for future in as_completed(future_map):
#                     try:
#                         files.append(future.result())
#                     except Exception as e:
#                         obj = future_map[future]
#                         print(f"❌ Error fetching {obj['Key']}: {e}")
#
#             def parse_date(x):
#                 try:
#                     return datetime.strptime(x["invoice_date"], "%Y-%m-%d")
#                 except:
#                     return datetime.min
#
#             files.sort(key=parse_date, reverse=True)
#
#             total = len(files)
#             start_i = (page - 1) * per_page
#             end_i = start_i + per_page
#             paginated = files[start_i:end_i]
#
#             return {
#                 "success": True,
#                 "total_count": total,
#                 "page": page,
#                 "per_page": per_page,
#                 "page_count": math.ceil(total / per_page),
#                 "retrieved_in_ms": round((time.time() - start) * 1000, 2),
#                 "files": paginated,
#             }
#
#         except Exception as e:
#             print("❌ Unexpected error:", e)
#             return {"success": False, "error": str(e)}
#
# def extract_invoice_date(data):
#     invoices = data.get("invoices", [])
#     if not invoices:
#         return ""
#
#     best = datetime.min
#
#     for inv in invoices:
#         raw = inv.get("Bill_Date__c")
#         if not raw:
#             continue
#         try:
#             d = datetime.strptime(raw, "%Y-%m-%d")
#             if d > best:
#                 best = d
#         except:
#             continue
#
#     if best == datetime.min:
#         return ""
#
#     return best.strftime("%Y-%m-%d")
#
