import time, json
from datetime import datetime
from js import Response, JSON, console
from urllib.parse import urlparse, parse_qs

def parse_bill_date(item):
    date_str = item.get("Bill_Date__c")

    try:
        return datetime.fromisoformat(date_str) if date_str else datetime.min
    except ValueError:
        return datetime.min


def make_response(body, status=200, headers=None):
    if headers is None:
        headers = {}

    if isinstance(body, (dict, list)):
        body = json.dumps(body)
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

    init = JSON.parse(json.dumps({"status": status, "headers": headers}))
    return Response.new(body, init)


async def fetch_specific_file(bucket, key):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Specific] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({"status": "failed", "message": "Failed to retrieve Specific record", "error": f"This key '{key}' not found"}, status=404)

        content = await obj.text()

        console.warn(f"[Specific] ✅ '{key}' record retrieved.")

        result = {
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "Specific record retrieved successfully",
            "records": content
        }
        return make_response(result)

    except Exception as e:
        console.error(f"[Specific] ❌ Failed to retrieve records | error={str(e)}")
        return make_response({"status": "failed", "message": "Failed to retrieve specific records", "error": str(e)}, status=400)


async def summary_retrieve(bucket, key, page=1, per_page=10, start_date=None, end_date=None):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Mobile] ❌ summary retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[Mobile] Failed to process summary",
                "error": f"This key '{key}' not found"},
                status=404
            )

        content = await obj.text()
        data = json.loads(content)
        items = data.get("records", [])

        def parse_date(inv_date):
            try:
                return datetime.strptime(inv_date.get("Bill_Date__c", ""), "%Y-%m-%d")
            except:
                return None

        from_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        to_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None

        if from_dt or to_dt:
            filtered = []
            for inv in items:
                d = parse_date(inv)
                if not d:
                    continue

                if from_dt and d < from_dt:
                    continue
                if to_dt and d > to_dt:
                    continue

                filtered.append(inv)
            items = filtered

        items.sort(key=parse_bill_date, reverse=True)

        console.info(f"[Mobile] ✅ summary record retrieved.")

        # ---- Pagination ----
        total = len(items)
        start_i = (page - 1) * per_page
        end_i = start_i + per_page
        paginated = items[start_i:end_i]

        result = {
                    "key": key,
                    "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
                    "status": "success",
                    "message": "Mobile summary retrieved successfully",
                    "page_no": page,
                    "per_page": per_page,
                    "total_pages": (total + per_page - 1) // per_page,
                    "total_records": total,
                    "records": paginated
        }
        return make_response(result)

    except Exception as e:
        console.error(f"[Mobile] ❌ Failed to process summary | error={str(e)}")
        return make_response({"status": "failed", "message": "[Mobile] Failed to process summary", "error": str(e)}, status=400)


async def invoice_retrieve(bucket, key, invoice_no):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Invoice] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({"status": "failed", "message": "[Invoice] Failed to process mobile summary", "error": f"This key '{key}' not found"}, status=404)

        content = await obj.text()
        data = json.loads(content)
        files = data.get("records", [])

        filtered = [item for item in files if invoice_no.strip() == (item.get("bill_transaction_no__c") or "").strip()]

        if not filtered:
            console.info(f"[Invoice] ⚠️ Records not found | invoice_no={invoice_no} | key={key}")
            return make_response({"status": "failed", "message": "[Invoice] Not found", "error": f"No records found for invoice_no: {invoice_no}"}, status=404)

        filtered.sort(key=parse_bill_date, reverse=True)

        console.warn(f"[Invoice] ✅ '{invoice_no}' record retrieved.")

        result = {
            "key": key,
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "Invoice retrieved successfully",
            "total_records": len(filtered),
            "records": filtered
        }
        return make_response(result)

    except Exception as e:
        console.error(f"[Invoice] ❌ Exception while retrieving invoice | invoice_no={invoice_no} | key={key} | error={str(e)}")
        return make_response({"status": "failed", "message": "[Invoice] Failed to process mobile summary", "error": str(e)}, status=400)


async def imei_retrieve(bucket, key, invoice_no):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[IMEI] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({"status": "failed", "message": "[IMEI] Failed to retrieve records", "error": f"This key '{key}' not found"}, status=404)

        content = await obj.text()
        data = json.loads(content)
        files = data.get("records", [])

        matched = []
        for item in files:
            if not invoice_no or invoice_no.strip() != (item.get("bill_transaction_no__c") or "").strip():
                continue

            imei_str = (item.get("item_remarks1__c") or "").strip()
            if not imei_str:
                continue

            imei_list = [x.strip() for x in imei_str.split(",") if x.strip()]

            for imei in imei_list:
                new_entry = item.copy()
                new_entry["item_remarks1__c"] = imei
                matched.append(new_entry)

        if not matched:
            console.info(f"[IMEI] ⚠️ No IMEI found for invoice_no = {invoice_no} | key = {key}")
            return make_response({"status": "failed", "message": "[IMEI] Not found", "error": f"No IMEI found for invoice_no: {invoice_no}"}, status=404)

        matched.sort(key=parse_bill_date, reverse=True)

        console.warn(f"[IMEI] ✅ '{invoice_no}' record retrieved.")

        results = {
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "IMEI retrieved successfully",
            "total_records": len(matched),
            "records": matched
        }
        return make_response(results)

    except Exception as e:
        console.error(f"[IMEI] ❌ Failed to retrieve records | error={str(e)}")
        return make_response({"status": "failed", "message": "[IMEI] Failed to retrieve records", "error": str(e)}, status=400)


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
