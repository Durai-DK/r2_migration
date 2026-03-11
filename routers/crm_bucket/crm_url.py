from fastapi import Query
from fastapi import APIRouter
from routers.crm_bucket.crm_get_form import *

router = APIRouter(prefix="/api/pos-bucket", tags=["POS Bucket Data Retrieval"])

mysql_db_name = "Transaction"

mob_bucket = "pos-transaction-test"
imei_bucket = "pos-transaction-imei-test"
mob_prefix = "mobile"
imei_prefix = "imei"

@router.get("/crm-records/")
def fetch_records(
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
