from fetch_support import *
from js import Response, JSON, console
from urllib.parse import urlparse, parse_qs


async def handle_get_records(request, env):
    parsed_url = urlparse(request.url)

    params = parse_qs(parsed_url.query)

    def get_param(name):
        return params.get(name, [None])[0]

    imei = get_param("imei")
    mobile = get_param("mobile")
    s_id = get_param("s_id")
    s_imei = get_param("s_imei")
    category = get_param("category")
    invoice_no = get_param("invoice_no")

    per_page_str = get_param("per_page")
    per_page = int(per_page_str) if per_page_str else 20

    page_no_str = get_param("page_no")
    page_no = int(page_no_str) if page_no_str else 1

    start_date = get_param("start_date")
    end_date = get_param("end_date")

    # Date validation
    if start_date and end_date and end_date < start_date:
        return make_response({
            "status": "failed",
            "message": "Invalid date range",
            "error": "'end_date' cannot be earlier than 'start_date'"},
            status=400
    )

    # IMEI-only fetch
    if imei:
        key = f"imei/{imei}.json"
        return await fetch_specific_file(env.IMEI_BUCKET, key)

    # Mobile dependent validations
    if s_id and not mobile:
        return make_response({"status": "failed", "message": "mobile is missing", "error": "mobile is required with s_id"}, status=400)

    if s_imei and not mobile:
        return make_response({"status": "failed", "message": "mobile is missing", "error": "mobile is required with s_imei"}, status=400)

    if category in {"invoice", "imei"}:
        if not mobile or not invoice_no:
            return make_response({"status": "failed", "message": "Missing parameters", "error": "Both mobile and invoice_no are required"}, status=400)

    if category == "mobile_summary" and not mobile:
        return make_response({"status": "failed", "message": "mobile is missing", "error": "mobile is required for mobile_summary"}, status=400)

    if mobile:
        # Specific pri_id file
        if s_id:
            key = f"mobile/{mobile}/id/{s_id}.json"
            return await fetch_specific_file(env.POS_BUCKET, key)

        # Specific IMEI file
        if s_imei:
            key = f"mobile/{mobile}/imei/{s_imei}.json"
            return await fetch_specific_file(env.POS_BUCKET, key)

        category_key = f"mobile/{mobile}/mobile_summary.json"

        if category == "mobile_summary":
            return await summary_retrieve(env.POS_BUCKET, category_key, page_no, per_page, start_date, end_date)

        if category == "invoice":
            return await invoice_retrieve(env.POS_BUCKET, category_key, invoice_no)

        if category == "imei":
            return await imei_retrieve(env.POS_BUCKET, category_key, invoice_no)

    return make_response({
        "status": "failed",
        "message": "Invalid request combination",
        "error": [
            "Provide either 'imei' OR 'mobile'",
            "s_id requires: mobile + s_id",
            "s_imei requires: mobile + s_imei",
            "mobile_summary requires: category=mobile_summary + mobile",
            "invoice requires: category=invoice + mobile + invoice_no",
            "imei requires: category=imei + mobile + invoice_no",
        ]
    }, status=400)
