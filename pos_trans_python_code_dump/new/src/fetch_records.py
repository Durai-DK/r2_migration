from fetch_support import *
from datetime import datetime
from js import Response, JSON, console
from urllib.parse import urlparse, parse_qs

async def handle_get_records(request, env):
    parsed_url = urlparse(request.url)

    params = parse_qs(parsed_url.query)
    def get_param(name):
        value = params.get(name, [None])[0]
        return value.strip() if isinstance(value, str) else value

    # Query params
    imei = get_param("imei")
    mobile = get_param("mobile")
    s_id = get_param("s_id")
    s_imei = get_param("s_imei")
    category = get_param("category")
    invoice_no = get_param("invoice_no")
    start_date = get_param("start_date")
    end_date = get_param("end_date")

    # Pagination
    try:
        per_page = int(get_param("per_page") or 50)
        page_no = int(get_param("page_no") or 1)

        if per_page < 1 or per_page > 500:
            raise ValueError("per_page out of range")
        if page_no < 1:
            raise ValueError("page_no out of range")
    except ValueError:
        return make_response(
            {"status": "failed", "message": "Invalid pagination values"},
            status=400,
            request=request
        )

    # Date validation
    try:
        if start_date:
            start_date = datetime.fromisoformat(start_date)
        if end_date:
            end_date = datetime.fromisoformat(end_date)

        if start_date and end_date and end_date < start_date:
            return make_response(
                {
                    "status": "failed",
                    "message": "Invalid date range",
                    "error": "'end_date' cannot be earlier than 'start_date'",
                },
                status=400,
                request=request
            )
    except ValueError:
        return make_response(
            {
                "status": "failed",
                "message": "Invalid date format",
                "error": "Use ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS",
            },
            status=400,
            request=request
        )


    # Mobile dependent validations
    if s_id and not mobile:
        return make_response({
            "status": "failed", "message": "mobile is missing", "error": "mobile is required with s_id"},
            status=400, request=request
        )

    if s_imei and not mobile:
        return make_response({
            "status": "failed", "message": "mobile is missing", "error": "mobile is required with s_imei"},
            status=400, request=request
        )

    if category in {"invoice", "imei"}:
        if not mobile or not invoice_no:
            return make_response({
                "status": "failed", "message": "Missing parameters", "error": "Both mobile and invoice_no are required"},
                status=400, request=request
            )

    if category in {"mobile_summary", "unique_invoices"} and not mobile:
        return make_response(
            {"status": "failed", "message": "Missing parameters", "error":"mobile is required"},
            status=400,
            request=request
        )

    # IMEI-only flow
    if imei:
        key = f"imei/{imei}/imei_summary.json"

        if category == "imei_history":
            return await imei_history(env.IMEI_BUCKET_PROD, key, request=request)

        return await fetch_single_imei(env.IMEI_BUCKET_PROD, key, request=request)


    if mobile:
        # Specific pri_id file
        if s_id:
            key = f"mobile/{mobile}/id/{s_id}.json"
            return await fetch_specific_file(env.POS_BUCKET_PROD, key, request=request)

        # Specific IMEI file
        if s_imei:
            key = f"mobile/{mobile}/imei/{s_imei}.json"
            return await fetch_specific_file(env.POS_BUCKET_PROD, key, request=request)

        category_key = f"mobile/{mobile}/mobile_summary.json"

        if category == "mobile_summary":
            return await summary_retrieve(env.POS_BUCKET_PROD, category_key, page_no, per_page, start_date, end_date, request=request)

        if category == "unique_invoices":
            return await unique_invoices(env.POS_BUCKET_PROD, category_key, start_date, end_date, request=request)

        if category == "invoice":
            return await invoice_retrieve(env.POS_BUCKET_PROD, category_key, invoice_no, request=request)

        if category == "imei":
            return await imei_retrieve(env.POS_BUCKET_PROD, category_key, invoice_no, request=request)

    return make_response({
            "status": "failed",
            "message": "Invalid request combination",
            "error": [
                "Provide either 'imei' OR 'mobile'",
                "s_id requires: mobile + s_id",
                "s_imei requires: mobile + s_imei",
                "mobile_summary requires: category=mobile_summary + mobile",
                "unique_invoices requires: category=unique_invoices + mobile",
                "invoice requires: category=invoice + mobile + invoice_no",
                "imei history requires: category= imei_history + imei",
                "single imei requires: imei",
            ],
        },
        status=400,
        request=request
    )
