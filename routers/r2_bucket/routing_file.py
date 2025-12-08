from fastapi import Query
from fastapi import APIRouter
from core.creds import MysqlCatalog
from logs.log_settings import success_log, error_log
from routers.r2_bucket.get_functions import *
from concurrent.futures import ProcessPoolExecutor
from routers.r2_bucket.create_functions import process_invoice


router = APIRouter(prefix="/api/bucket", tags=["Store Data to Bucket "])

mysql_db_name = "Transaction"

mob_bucket = "dev-soc-media"
imei_bucket = "dev-soc-media-1"
mob_prefix = "mobile"
imei_prefix = "imei_numbers"

WORKERS = 15
BATCH_SIZE = 25000

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
            futures = [executor.submit(process_invoice,row, mob_bucket, imei_bucket,
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

@router.get("/fetch-files/")
def fetch_files(
                    field_type: str = Query(..., enum=["imei", "mobile"]),
                    imei_no: str | None = None,
                    mobile_no: str | None = None,
                    category: str = Query(None, enum=["specific_file", "summary"]),
                    specific_field: str | None = Query(None, enum=["id", "imei"]),
                    specific_id: str | None = None,

                    summary_field: str | None = Query(None, enum=["id", "imei", "mobile", "id_list", "imei_list"]),
                    invoice_no: str | None = None,

                    per_page: int = 10,
                    page_no: int = 1,
                    start_date: str | None = Query(None, description="Date format: YYYY-MM-DD"),
                    end_date: str | None = Query(None, description="Date format: YYYY-MM-DD"),
):
    # ----- Validation -----
    if field_type == "imei":
        if not imei_no:
            return error_response(400, "imei_no is missing", "imei_no is required to fetching IMEI data")

    elif field_type == "mobile":
        if not mobile_no:
            return error_response(400, "mobile_no is missing", "mobile_no is required to fetch mobile data.")

        if category == "specific_file":
            if not specific_field:
                return error_response(400, "specific_field is missing", "specific_field is required when category == 'specific_file'.")

            if specific_field == "id" and not specific_id:
                return error_response(400, "specific_id is missing", "specific_id is required to fetching ID file.")

            if specific_field == "imei" and not imei_no:
                return error_response(400, "imei_no is missing", "imei_no is required to fetching IMEI file.")

        if category == "summary":
            if not summary_field:
                return error_response(400, "summary_field is missing", "summary_field is required when category == 'summary'.")

            if summary_field in ["id_list", "imei_list"] and not invoice_no:
                return error_response(400, "invoice_no is missing", "invoice_no is required for ID/IMEI list fetch.")

        if start_date and end_date and end_date < start_date:
            return error_response(400,"Invalid date range", "'end_date' cannot be earlier than 'start_date'.")

    # ----- EXECUTION -----
    if field_type == "imei" and imei_no:
        key = f"{imei_prefix}/{imei_no}.json"
        return fetch_specific_file(imei_bucket, key)

    elif field_type == "mobile":

        if category == "specific_file":

            if specific_field == "id" and specific_id:
                key = f"{mob_prefix}/{mobile_no}/id/{specific_id}.json"
                return fetch_specific_file(mob_bucket, key)

            if specific_field == "imei" and imei_no:
                key = f"{mob_prefix}/{mobile_no}/imei/{imei_no}.json"
                return fetch_specific_file(mob_bucket, key)

        if category == "summary":

            if summary_field == "id":
                key = f"{mob_prefix}/{mobile_no}/id_summary.json"
                return summary_retrieve(mob_bucket, key, page_no, per_page, start_date, end_date)

            if summary_field == "imei":
                key = f"{mob_prefix}/{mobile_no}/imei_summary.json"
                return summary_retrieve(mob_bucket, key, page_no, per_page, start_date, end_date)

            if summary_field == "mobile":
                key = f"{mob_prefix}/{mobile_no}/mobile_summary.json"
                return summary_retrieve(mob_bucket, key, page_no, per_page, start_date, end_date)

            if summary_field == "id_list":
                key = f"{mob_prefix}/{mobile_no}/id_summary.json"
                return records_retrieve(mob_bucket, key, invoice_no)

            if summary_field == "imei_list":
                key = f"{mob_prefix}/{mobile_no}/imei_summary.json"
                return records_retrieve(mob_bucket, key, invoice_no)

    return error_response(400,
                          "Invalid request combination",
                          [
                              "Verify 'field_type' is either 'mobile' or 'imei'",
                              "Ensure required parameters match the selected category",
                              "Check 'specific_field' or 'summary_field' values",
                              "Review required dependencies: imei_no, mobile_no, invoice_no, etc.",
                          ]
    )

@router.get("/count-files/")
def count_files_api(
        bucket_type: str = Query(..., enum=["imei", "mobile"]),
        mobile_no: str | None = Query(None, description="Mobile number"),
        file_type: str | None = Query(None, enum=["both", "id", "imei"])
):

    if bucket_type == "mobile":

        if file_type and not mobile_no:
            return error_response(code=400,
                                  message="Mobile number is missing",
                                  error="Mobile number is mandatory when selecting a file type.")

    # ----- Execution -----
    if bucket_type == "imei":
        path_key = f"{imei_prefix}/"
        return count_files(imei_bucket, path_key)

    elif bucket_type == "mobile":
        if not file_type:
            # path_key = f"{mob_prefix}/"
            # return count_mobile_numbers(mob_bucket, path_key)
            path_key = f"mobile/"
            # return count_mobile_numbers("pos-transaction", path_key)
            return count_mobile_numbers("dev-soc-media", path_key)

        if mobile_no and file_type == "id":
            path_key = f"{mob_prefix}/{mobile_no}/id/"
            return count_files(mob_bucket, path_key)

        if mobile_no and file_type == "imei":
            path_key = f"{mob_prefix}/{mobile_no}/imei/"
            return count_files(mob_bucket, path_key)

        if mobile_no and file_type == "both":
            path_key = f"{mob_prefix}/{mobile_no}/"
            return count_files(mob_bucket, path_key)

    return error_response(
        code=400,
        message="Invalid request combination",
        error=[
            "Verify 'bucket_type' is either 'imei' or 'mobile'",
            "Ensure required parameters match the selected category.",
            "For 'mobile' both 'mobile_no' and 'file_type' are required."
        ]
    )
