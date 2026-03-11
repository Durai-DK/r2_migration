from helper import *
from datetime import datetime
from js import Response, JSON, console


def parse_bill_date(item):
    date_str = item.get("Bill_Date__c")

    try:
        return datetime.fromisoformat(date_str) if date_str else datetime.min
    except ValueError:
        return datetime.min


async def fetch_specific_file(bucket, key):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Specific] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({"status": "failed", "message": "Failed to retrieve Specific record", "error": f"This key '{key}' not found"}, status=404)

        content = await obj.text()

        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            console.warn("[Specific] ⚠️ records is not valid JSON, returning raw text")

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
        overall_spends = data.get("invoice_spends", [])
        overall_invoice_count = data.get("invoice_count", [])
        overall_total_records = data.get("total_records", [])

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
                    "overall_spends": overall_spends,
                    "overall_invoice_count": overall_invoice_count,
                    "overall_total_records": overall_total_records,
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
