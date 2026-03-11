from fastapi import Query
from fastapi import APIRouter
from core.creds import MysqlCatalog
from logs.log_settings import success_log, error_log
from routers.r2_bucket.get_functions import *
from concurrent.futures import ProcessPoolExecutor
from routers.Pos_Transaction.pos_creation import push_to_bucket


router = APIRouter(prefix="/api/bucket", tags=["Store Data to Bucket "])

mysql_db_name = "Transaction"

mob_bucket = "pos-transaction-test"
imei_bucket = "pos-transaction-imei-test"
mob_prefix = "mobile"
imei_prefix = "imei"

WORKERS = 15
BATCH_SIZE = 25000


@router.get("/fetch-files/")
def fetch_files(
        imei: str | None = Query(None, description="IMEI Number"),
        mobile: str | None = Query(None, description="Mobile Number"),

        s_id: str | None = Query(None, description="Specific pri_id with Mobile"),
        s_imei: str | None = Query(None, description="Specific IMEI with Mobile"),

        category: str | None = Query(None, enum=["mobile_summary", "invoice", "imei"]),
        invoice_no: str | None = Query(None, description="Invoice Number"),

        per_page: int = 20,
        page_no: int = 1,
        start_date: str | None = Query(None, description="Date format: YYYY-MM-DD"),
        end_date: str | None = Query(None, description="Date format: YYYY-MM-DD"),
):
    # ------------------------------------------------------------------------------------------

    # Date validation
    if start_date and end_date and end_date < start_date:
        return error_response(400, "Invalid date range", "'end_date' cannot be earlier than 'start_date'")

    # IMEI-only fetch
    if imei:
        key = f"{imei_prefix}/{imei}.json"
        return fetch_specific_file(imei_bucket, key)

    # ------------------------------------------------------------------------------------------

    # Mobile dependent validations
    if s_id and not mobile:
        return error_response(400, "mobile is missing", "mobile is required with s_id")

    if s_imei and not mobile:
        return error_response(400, "mobile is missing", "mobile is required with s_imei")

    if category in {"invoice", "imei"}:
        if not mobile or not invoice_no:
            return error_response(400, "Missing parameters", "Both mobile and invoice_no are required")

    if category == "mobile_summary" and not mobile:
        return error_response(400, "mobile is missing", "mobile is required for mobile_summary")

    # ------------------------------------------------------------------------------------------
    if mobile:
        # Specific pri_id file
        if s_id:
            key = f"{mob_prefix}/{mobile}/id/{s_id}.json"
            return fetch_specific_file(mob_bucket, key)

        # Specific IMEI file
        if s_imei:
            key = f"{mob_prefix}/{mobile}/imei/{s_imei}.json"
            return fetch_specific_file(mob_bucket, key)

        category_key = f"{mob_prefix}/{mobile}/mobile_summary.json"

        if category == "mobile_summary":
            return summary_retrieve(mob_bucket, category_key, page_no, per_page, start_date, end_date)

        if category == "invoice":
            return invoice_retrieve(mob_bucket, category_key, invoice_no)

        if category == "imei":
            return imei_retrieve(mob_bucket, category_key, invoice_no)

    # ------------------------------------------------------------------------------------------
    return error_response(400,
                          "Invalid request combination",
                          [
                                  "Provide either 'imei' OR 'mobile'",

                                  "s_id requires: mobile + s_id",
                                  "s_imei requires: mobile + s_imei",

                                  "mobile_summary requires: category=mobile_summary + mobile",
                                  "invoice requires: category=invoice + mobile + invoice_no",
                                  "imei requires: category=imei + mobile + invoice_no",
                          ]
    )


@router.post("/create-files/")
def create_bucket(start_range: int, end_range: int):
    if end_range <= start_range:
        error_log.error(f"❌ end_date must be greater than start_date.")
        return error_response(code=400, message="invalid date range", error="end_date must be greater than start_date.")

    overall_start = time.time()
    mysql_creds = MysqlCatalog()
    current_start = start_range

    # ------------- Fetch Mysql Data -------------
    while current_start <= end_range:
        current_end = min(current_start + BATCH_SIZE - 1, end_range)
        batch_fetch_start = time.time()
        try:
            rows = mysql_creds.get_range(mysql_db_name, current_start, current_end)
            fetch_time = round(time.time() - batch_fetch_start, 2)

            if not rows:
                return error_response(code=404, message="data not found", error="No data found in the given range.")

            success_log.info(
                f"📌 MySQL Batch fetched: {current_start}-{current_end} "
                f"Records={len(rows)}, Time={fetch_time}s"
            )
        except Exception as e:
            return error_response(code=500, message="MySQL retrival error", error=str(e))

        # ---------- Batch Multiprocessing ----------
        batch_start = time.time()
        total = len(rows)
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(push_to_bucket,row, mob_bucket, imei_bucket,
                                       mob_prefix, imei_prefix) for row in rows]

            for idx, future in enumerate(futures, start=1):
                try:
                    future.result()
                    if idx % 1000 == 0 or idx == total:
                        percent = round((idx / total) * 100, 2)
                        duration = round(time.time() - batch_start, 2)
                        success_log.info(
                            f"🟡 Batch {current_start}-{current_end}: {percent}% completed in {duration}s"
                        )
                        batch_start = time.time()

                except Exception as worker_err:
                    error_log.error(f"❌ Worker error: {worker_err}")
        current_start = current_end + 1

    # ---------- Total Time ----------
    total_time = round(time.time() - overall_start, 2)
    success_log.info(f"✅ ALL BATCHES COMPLETED: total range={start_range}-{end_range}, time={total_time}s")

    return JSONResponse(status_code=200,
                        content={
                            "status": "success",
                            "message": "Data migrated successfully to R2",
                            "detail": f"Processed batches for range {start_range}-{end_range} in {total_time}s",
                        }
    )


# @router.get("/count-files/")
# def count_files_api(
#         bucket_type: str = Query(..., enum=["imei", "mobile"]),
#         mobile_no: str | None = Query(None, description="Mobile number"),
#         file_type: str | None = Query(None, enum=["both", "id", "imei"])
# ):
#
#     if bucket_type == "mobile":
#
#         if file_type and not mobile_no:
#             return error_response(code=400,
#                                   message="Mobile number is missing",
#                                   error="Mobile number is mandatory when selecting a file type.")
#
#     # ----- Execution -----
#     if bucket_type == "imei":
#         path_key = f"{imei_prefix}/"
#         return count_files(imei_bucket, path_key)
#
#     elif bucket_type == "mobile":
#         if not file_type:
#             # path_key = f"{mob_prefix}/"
#             # return count_mobile_numbers(mob_bucket, path_key)
#             path_key = f"mobile/"
#             # return count_mobile_numbers("pos-transaction", path_key)
#             return count_mobile_numbers("dev-soc-media", path_key)
#
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
#     return error_response(
#         code=400,
#         message="Invalid request combination",
#         error=[
#             "Verify 'bucket_type' is either 'imei' or 'mobile'",
#             "Ensure required parameters match the selected category.",
#             "For 'mobile' both 'mobile_no' and 'file_type' are required."
#         ]
#     )