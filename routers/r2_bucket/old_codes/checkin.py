from fastapi import APIRouter, HTTPException, Query,Body
from datetime import datetime
from typing import Any, List
import time, math, json, asyncio, aioboto3
from ...mysql_creds import *
from botocore.client import Config
import time, json, boto3, os
import os
from ...core.r2_client import get_r2_client
from concurrent.futures import ThreadPoolExecutor, as_completed
MAX_WORKERS = 16
import concurrent.futures
import math

router = APIRouter(prefix="", tags=["bucket store"])

# --- CONFIG ---
BATCH_SIZE = 1000
MAX_PARALLEL_UPLOADS = 5
R2_BUCKET_NAME = os.getenv("BUCKET_NAME")
R2_ENDPOINT = os.getenv("ENDPOINT")
R2_ACCESS_KEY = os.getenv("ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("SECRET_ACCESS_KEY")

def store_json_to_r2(data, key, metadata):
    """Upload JSON (dict or list) with custom metadata."""
    try:
        # --- Ensure data is serializable ---
        if isinstance(data, (dict, list, int, float, str)):
            body = json.dumps(data, indent=2, ensure_ascii=False)
        else:
            raise TypeError(f"Unsupported data type for R2 upload: {type(data)}")

        # --- Prepare metadata ---
        safe_metadata = {}
        if isinstance(metadata, dict):
            for k, v in metadata.items():
                safe_metadata[str(k).lower().replace("_", "-")] = str(v)
        else:
            print(f"⚠️ Metadata is not dict: {metadata}, skipping metadata.")

        # --- Upload to R2 ---
        get_r2_client().put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=body.encode("utf-8"),   # ✅ body is always string now
            ContentType="application/json",
            Metadata=safe_metadata,
        )

        print(f"✅ Stored {key} ({len(body)} bytes)")

    except Exception as e:
        print(f"❌ Error storing {key}: {e}")
        raise

def is_valid_json(file_path):
    try:
        with open(file_path, "r", encoding='utf-8') as f:
            json.load(f)
        return True
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"❌ Invalid JSON in {file_path} → {e}")
        return False


def upload_json_to_r2(key, data, metadata=None):
    if metadata is None:
        metadata = {}

    if isinstance(data, (dict, list, int, float, str)):
        body = json.dumps(data, indent=2, ensure_ascii=False)
    else:
        raise TypeError(f"Unsupported data type for R2 upload: {type(data)}")

    get_r2_client().put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata=metadata
    )


# --- Helper: JSON-safe conversion ---
def make_json_serializable(record: dict[str, Any]) -> dict[str, Any]:
    from datetime import datetime, date
    from decimal import Decimal
    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            record[k] = v.isoformat()
        elif isinstance(v, Decimal):
            record[k] = float(v)
        elif isinstance(v, bytes):
            record[k] = v.decode(errors="ignore")
    return record

@router.get("/bucket/list")
def get_list(
    bucket_name: str = Query("dev-transaction", title="Bucket Name",description="Bucket name (default: dev-transaction)"),
    bucket_path: str = Query("pos_transactions", description="Folder path in R2 (default: pos_transactions)"),
):
    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"

    try:
        paginator = r2_client.get_paginator("list_objects_v2")
        files = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])

        return {
            "total_files": len(files),
            "files": files  # you can remove this if too large
        }

    except Exception as e:
        return {"error": str(e)}

# --- Helper: Upload one batch to R2 ---
# async def upload_batch(batch_index: int, batch_data: List[dict[str, Any]]):
#     key = f"batches/batch_{batch_index:04d}.json"
#     metadata = {
#         "batch_id": str(batch_index),
#         "record_count": str(len(batch_data)),
#         "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#     }
#
#     body = json.dumps(batch_data, ensure_ascii=False).encode("utf-8")
#
#     session = aioboto3.Session()
#     async with session.client(
#         "s3",
#         endpoint_url=R2_ENDPOINT,
#         aws_access_key_id=R2_ACCESS_KEY,
#         aws_secret_access_key=R2_SECRET_KEY,
#     ) as s3:
#         await s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body, Metadata=metadata)


# @router.post("/create-pri-id")
# async def create_pri_id_records(
#     start_range: int = Query(0, description="Start row (e.g., 0)"),
#     end_range: int = Query(100000, description="End row (e.g., 100000)")
# ):
#     print("start *****************")
#     """Fetch records from MySQL and upload them to R2 in parallel batches."""
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     dbname = "Transaction"
#
#     # --- Step 1: Fetch MySQL Data ---
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         print(rows)
#         mysql_duration = round(time.time() - mysql_start, 2)
#         if not rows:
#             raise HTTPException(status_code=404, detail="No data found in the given range.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")
#
#     # --- Step 2: Prepare batches ---
#     for r in rows:
#         make_json_serializable(r)
#
#     total_rows = len(rows)
#     num_batches = math.ceil(total_rows / BATCH_SIZE)
#     print(f"📦 Total Rows: {total_rows} | Total Batches: {num_batches}")
#
#     # --- Step 3: Async batch uploads ---
#     semaphore = asyncio.Semaphore(MAX_PARALLEL_UPLOADS)
#
#     async def upload_with_limit(idx: int, data: List[dict[str, Any]]):
#         async with semaphore:
#             start_time = time.time()
#             print(f"🟡 Batch {idx + 1}/{num_batches} → Rows {idx * BATCH_SIZE}-{min((idx + 1) * BATCH_SIZE, total_rows)}")
#             await upload_batch(idx, data)
#             print(f"✅ Batch {idx + 1} Done in {round(time.time() - start_time, 2)}s")
#
#     tasks = [
#         asyncio.create_task(upload_with_limit(i, rows[i * BATCH_SIZE: (i + 1) * BATCH_SIZE]))
#         for i in range(num_batches)
#     ]
#     await asyncio.gather(*tasks)
#
#     total_time = round(time.time() - total_start, 2)
#
#     # --- Step 4: Return Summary ---
#     return {
#         "status": "success",
#         "rows_fetched": total_rows,
#         "batches_uploaded": num_batches,
#         "mysql_duration_sec": mysql_duration,
#         "elapsed_total_sec": total_time,
#         "r2_key_pattern": "batches/batch_<index>.json"
#     }
# async def upload_batch_file(batch_index: int, batch_data: List[dict[str, Any]]):
#     key = f"batches/batch_{batch_index:04d}.json"
#     metadata = {
#         "batch_id": str(batch_index),
#         "record_count": str(len(batch_data)),
#         "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#     }
#
#     body = json.dumps(batch_data, ensure_ascii=False).encode("utf-8")
#
#     session = aioboto3.Session()
#     async with session.client(
#         "s3",
#         endpoint_url=R2_ENDPOINT,
#         aws_access_key_id=R2_ACCESS_KEY,
#         aws_secret_access_key=R2_SECRET_KEY,
#     ) as s3:
#         await s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body, Metadata=metadata)

# @router.post("/create-pri-id")
# async def create_pri_id_records(
#     start_range: int = Query(0, description="Start row offset"),
#     end_range: int = Query(100000, description="End row offset")
# ):
#     """Fetch records from MySQL and upload each batch as a separate file to R2 (sequentially)."""
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     dbname = "Transaction"
#
#     # Step 1: Fetch Data
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#         if not rows:
#             raise HTTPException(status_code=404, detail="No data found in range.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")
#
#     # Step 2: Convert records to JSON-safe
#     for r in rows:
#         make_json_serializable(r)
#
#     total_rows = len(rows)
#     num_batches = math.ceil(total_rows / BATCH_SIZE)
#     print(f"📦 Total Rows: {total_rows} | Total Batches: {num_batches}")
#
#     # Step 3: Sequential upload (each batch one by one)
#     for i in range(num_batches):
#         batch_start = i * BATCH_SIZE
#         batch_end = min((i + 1) * BATCH_SIZE, total_rows)
#         batch_data = rows[batch_start:batch_end]
#
#         print(f"🟡 Processing Batch {i + 1}/{num_batches} → Rows {batch_start}-{batch_end}")
#         start_time = time.time()
#
#         await upload_batch_file(i, batch_data)
#
#         print(f"✅ Batch {i + 1} Completed in {round(time.time() - start_time, 2)}s")
#
#     total_time = round(time.time() - total_start, 2)
#
#     return {
#         "status": "success",
#         "rows_fetched": total_rows,
#         "batches_uploaded": num_batches,
#         "mysql_duration_sec": mysql_duration,
#         "elapsed_total_sec": total_time,
#         "r2_key_pattern": "batches/batch_<index>.json"
#     }




# @router.post("/bucket/id",description="We extract each record from MySQL, and use the record’s pri_id as the R2 object key. For every row, we serialize the row into JSON (JSON.stringify) and store it as a single JSON file in Cloudflare R2")
# async def create(
#     start_range: int = Query(0, description="Start row (e.g., 0)"),
#     end_range: int = Query(100000, description="End row (e.g., 100000)")
# ):
#     """
#     Fetch records from MySQL and upload each as JSON to R2.
#     Includes batch-based parallelism and detailed time logs.
#     """
#
#     if end_range <= start_range:
#         raise HTTPException(status_code=400, detail="end_range must be greater than start_range")
#
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#     dbname = "Transaction"
#
#     # --- Step 1: Fetch from MySQL ---
#     try:
#         mysql_start = time.time()
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#         mysql_duration = round(time.time() - mysql_start, 2)
#         if not rows:
#             raise HTTPException(status_code=404, detail="No data found in the given range.")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")
#
#     stored_count, failed_count = 0, 0
#     error_logs: List[Any] = []
#
#     # --- Helper: Single record handler ---
#     def process_record(record: dict[str, Any]):
#         pri_id = record.get("pri_id")
#         if not pri_id:
#             return {"status": "error", "error": "Missing pri_id"}
#
#         try:
#             record_safe = make_json_serializable(record)
#             key = f"id/{pri_id}.json"
#
#             # Load existing once; append and write back
#             # existing = load_r2_json(R2_BUCKET_NAME, key)
#             # existing.append(record_safe)
#
#             metadata = {
#                 "pri_id": pri_id,
#                 "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
#             }
#             store_json_to_r2([record_safe], key, metadata)
#             return {"status": "ok"}
#
#         except Exception as e:
#             return {"status": "error", "error": str(e)}
#
#     # --- Step 2: Batch Processing ---
#     total_rows = len(rows)
#     num_batches = math.ceil(total_rows / BATCH_SIZE)
#
#     for batch_index in range(num_batches):
#         batch_start_idx = batch_index * BATCH_SIZE
#         batch_end_idx = min(batch_start_idx + BATCH_SIZE, total_rows)
#         batch_data = rows[batch_start_idx:batch_end_idx]
#
#         print(f"🟡 Processing Batch {batch_index + 1}/{num_batches} → Rows {batch_start_idx}–{batch_end_idx}")
#
#         batch_start_time = time.time()
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             results = list(executor.map(process_record, batch_data))
#
#         batch_duration = round(time.time() - batch_start_time, 2)
#
#         # Collect stats
#         ok = sum(1 for r in results if r["status"] == "ok")
#         err = len(results) - ok
#         stored_count += ok
#         failed_count += err
#         error_logs.extend(r for r in results if r["status"] == "error")
#
#         print(f"✅ Batch {batch_index + 1} Completed in {batch_duration}s ({ok} ok / {err} failed)")
#
#     total_elapsed = round(time.time() - total_start, 2)
#
#     # --- Step 3: Summary ---
#     return {
#         "status": "success",
#         "mysql_duration_sec": mysql_duration,
#         "rows_fetched": total_rows,
#         "rows_stored": stored_count,
#         "failed_count": failed_count,
#         "elapsed_total_sec": total_elapsed,
#         "batches": num_batches,
#         "r2_key_pattern": "id/<pri_id>.json",
#         "errors": error_logs[:5],
#     }

@router.get("/list")
def get_bucket_list(
    bucket_name: str = Query("dev-transaction", title="Bucket Name",description="Bucket name (default: dev-transaction)"),
    bucket_path: str = Query("pos_transactions", description="Folder path in R2 (default: pos_transactions)"),

):
    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"

    try:
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        # print(response)

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return {
            "total_files": len(files),
            "files": files
        }

    except Exception as e:
        return {"error": str(e)}



# @router.delete("/delete-files")
# def delete_files(
#         bucket_name: str = Query(..., title="Bucket Name",description="Bucket name (default:dev-transaction)"),
#         bucket_path: str = Query("pos_transactions", description="Folder path in R2 (default: pos_transactions)"),
#         # bucket_name: str = Query(..., title="Bucket Name"),
#         # bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
#         prefix_only: bool = Query(True, description="Delete all files under prefix (True) or specific file (False)"),
#         file_name: str = Query(None, description="Specific file name (e.g. 'batch_0.json') if prefix_only=False")
# ):
#
#     r2_client = get_r2_client()
#     prefix = f"{bucket_path}/"
#     print("Deleting files")
#     print(f"{prefix}")
#     try:
#         deleted_files = []
#
#         if prefix_only:
#             response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
#             if "Contents" in response:
#                 for obj in response["Contents"]:
#                     print(obj["Key"])
#                     r2_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
#                     deleted_files.append(obj["Key"])
#         else:
#             if not file_name:
#                 return {"error": "file_name is required if prefix_only=False"}
#
#             file_key = prefix + file_name
#             r2_client.delete_object(Bucket=bucket_name, Key=file_key)
#             deleted_files.append(file_key)
#
#         return {
#             "message": f"{len(deleted_files)} file(s) deleted",
#             "deleted_files": deleted_files
#         }
#
#     except Exception as e:
#         return {"error": str(e)}


# multithreading
@router.delete("/delete-files")
def delete_files(
        bucket_name: str = Query(...),
        bucket_path: str = Query("pos_transactions"),
        prefix_only: bool = Query(True),
        file_name: str = Query(None),
        max_workers: int = Query(10, description="Number of threads (default 10)")
):

    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"
    deleted_files = []

    def delete_single(key: str):
        r2_client.delete_object(Bucket=bucket_name, Key=key)
        return key

    try:
        if prefix_only:
            response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if "Contents" in response:
                keys = [obj["Key"] for obj in response["Contents"]]

                # MULTI-THREAD DELETE
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(delete_single, k) for k in keys]
                    for f in as_completed(futures):
                        print(f.result())
                        deleted_files.append(f.result())

        else:
            if not file_name:
                return {"error": "file_name is required if prefix_only=False"}
            file_key = prefix + file_name
            delete_single(file_key)
            deleted_files.append(file_key)

        return {
            "message": f"{len(deleted_files)} file(s) deleted",
            "deleted_files": deleted_files
        }

    except Exception as e:
        return {"error": str(e)}

@router.delete("/delete")
def delete_bucket(
    bucket_name: str = Query("dev-transaction"),
    bucket_path: str = Query("pos_transactions"),
    # action: str = Query("list", description="'list' or 'delete'"),
    max_workers: int = Query(10, description="threads for delete"),
):

    # if action not in ("list", "delete"):
    #     return {"error": "action must be 'list' or 'delete'"}

    r2 = get_r2_client()
    prefix = f"{bucket_path}/"

    paginator = r2.get_paginator("list_objects_v2")
    files = []

    # collect full list
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                print(obj)
                files.append(obj["Key"])

    # if action == "list":
    #     return {
    #         "total_files": len(files),
    #         "files": files
    #     }

    # DELETE MODE
    def delete_one(key:str):
        r2.delete_object(Bucket=bucket_name, Key=key)
        return key

    deleted = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(delete_one, k) for k in files]
        for f in as_completed(futures):
            print(f.result())
            deleted.append(f.result())

    return {
        "deleted_count": len(deleted),
        "deleted_keys": deleted
    }


mobile_summary = {}
TOTAL_INVOICES = 0

@router.post("/create/id")
def create_id(
        start_range: int = Query(0, description="Start row (e.g., 0)"),
        end_range: int = Query(100000, description="End row (e.g., 100000)")
):
    if end_range <= start_range:
        raise HTTPException(status_code=400, detail="end_range must be greater than start_range")

    total_start = time.time()
    mysql_creds = MysqlCatalog()
    dbname = "Transaction"

    try:
        mysql_start = time.time()
        rows = mysql_creds.get_range(dbname, start_range, end_range)
        # print(rows[0])
        mysql_duration = round(time.time() - mysql_start, 2)
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the given range.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")

    try:
        for data in rows:
            pri_id = str(data.get("pri_id") or "").strip()
            mobile = str(data.get("customer_mobile__c") or "").strip()
            invoice_id = str(data.get("bill_transaction_no__c") or "").strip()
            serial_no = str(data.get("item_remarks1__c") or "").strip()

            if not mobile and not invoice_id:
                print(f"No mobile and invoice → SKIPPED ")
                return
            print(pri_id)

            invoice_id_key = invoice_id.replace("/", "_")
            key = f"mobile/{mobile}/{invoice_id_key}.json"
            print(key)
            raw_amount = data.get("bill_grand_total__c")
            amount = float(raw_amount) if raw_amount not in (None, "", "null") else 0.0

            record_safe = make_json_serializable(data)

            metadata = {
                "invoice-id": invoice_id,
                "serial-no": serial_no,
                "amount": str(amount),
                "mobile": mobile
            }
            print(data)
            # Upload invoice file
            upload_json_to_r2(key, record_safe, metadata)
            print(key)

            # TOTAL_INVOICES += 1

            # Create mobile group
            if mobile not in mobile_summary:
                mobile_summary[mobile] = {
                    "invoice_count": 0,
                    "total_value": 0.0,
                    "invoices": []
                }

        # Update summary entry
            ms = mobile_summary[mobile]
            ms["invoice_count"] += 1
            ms["total_value"] += amount
            ms["invoices"].append({
                "pri_id": pri_id,
                "invoice_id": invoice_id,
                "serial_no": serial_no,
                "amount": amount
            })

            print(f"✔ Uploaded → {key}")
            ms = upload_mobile_summaries(data)


    except Exception as e:
        print(f" ERROR  → {e}")

def upload_mobile_summaries(mobile:str):
    """Upload individual mobile summaries under each mobile prefix"""
    print("\n📱 Uploading mobile summaries...")

    for mobile, summary_data in mobile_summary.items():
        try:
            summary_key = f"mobile/{mobile}/summary.json"

            # Prepare metadata
            invoice_ids = [inv.get("invoice_id", "") for inv in summary_data.get("invoices", [])]
            serial_numbers = [inv.get("serial_no", "") for inv in summary_data.get("invoices", [])]

            metadata = {
                "mobile": mobile,
                "invoice-count": str(summary_data.get("invoice_count", 0)),
                "total-value": str(summary_data.get("total_value", 0.0)),
                "invoice-ids": ",".join(filter(None, invoice_ids)),  # Comma-separated
                "serial-numbers": ",".join(filter(None, serial_numbers))  # Comma-separated
            }

            upload_json_to_r2(summary_key, summary_data, metadata)
            print(f"✔ Mobile summary uploaded → {summary_key}")

        except Exception as e:
            print(f"❌ ERROR uploading mobile summary for {mobile} → {e}")

################################################################################
@router.post("/create/phone-inv-imi")
def create(
        transaction_data: dict = Body(...)
):
    try:

        mobile = str(transaction_data.get("customer_mobile__c") or "").strip()
        invoice_id = str(transaction_data.get("bill_transaction_no__c") or "").strip()
        serial_no = str(transaction_data.get("item_remarks1__c") or "").strip()

        invoice_id_key = invoice_id.replace("/", "_")
        key = f"mobile/{mobile}/{invoice_id_key}.json"

        raw_amount = transaction_data.get("bill_grand_total__c")
        amount = float(raw_amount) if raw_amount not in (None, "", "null") else 0.0

        record_safe = make_json_serializable(transaction_data)

        metadata = {
            "invoice_id": invoice_id,
            "serial-no": serial_no,
            "amount": str(amount),
            "mobile": mobile
        }
        print(record_safe)

        upload_json_to_r2(key, record_safe, metadata)


        try:
            r2_client = get_r2_client()
            bucket = "dev-transaction"
            prefix = f"mobile/{mobile}/"

            response = r2_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = []
            total_value = 0.0

            try:
                if "Contents" in response:
                    for obj in response["Contents"]:
                        file_key = obj["Key"]

                        if file_key.endswith("summary.json"):
                            continue

                        files.append(file_key)
                        file_obj = r2_client.get_object(Bucket=bucket, Key=file_key)
                        file_data = json.loads(file_obj["Body"].read().decode("utf-8"))

                        inv_amount = file_data.get("bill_grand_total__c")
                        total_value += float(inv_amount) if inv_amount not in (None, "", "null") else 0.0
                        # print(files)
                        # print(len(files))
                summary_data = {

                    "invoice_count": len(files),
                    "total_value": total_value,
                    "files":files
                }
                summary_key = f"mobile/{mobile}/summary.json"

                summary_meta = {
                    "mobile": mobile,
                    "invoice_count": str(summary_data["invoice_count"]),
                    "total_value": str(summary_data["total_value"]),
                }

                summary_safe = make_json_serializable(summary_data)

                upload_json_to_r2(summary_key, summary_safe, summary_meta)
                ########################################
                # invoice
                invoice_prefix_key = f"mobile/{mobile}/invoice/{invoice_id_key}.json"
                upload_json_to_r2(invoice_prefix_key, record_safe, metadata)

                ######################################
                # Serial nums
                serial_nos_prefix_key = f"mobile/{mobile}/serial_no/{serial_no}.json"
                upload_json_to_r2(serial_nos_prefix_key, record_safe, metadata)

                return {
                    "status": "success",
                    "uploaded_invoice": key,
                    "total_files": len(files),
                    "files": files
                }

            except Exception as e:
                return {"error": str(e)}

        except Exception as e:
            return {"error": str(e)}

    except Exception as e:
        print(f"❌ ERROR uploading summary for → {e}")



