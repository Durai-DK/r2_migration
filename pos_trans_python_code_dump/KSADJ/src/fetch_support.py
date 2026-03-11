import time, json
from js import Response, JSON, console
from helper import make_response, normalize_date, parse_date, parse_bill_date, get_invoice_year

async def fetch_specific_file(bucket, key, request=None):
    start = time.time()

    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Specific] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "Failed to retrieve Specific record",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()

        try:
            content = json.loads(content)

            # 🔹 Check and rename key
            if isinstance(content, dict):
                txn = content.get("bill_transaction_no__c")
                ref = content.get("bill_refference_no")
                if isinstance(txn, str) and txn.upper().startswith("DN") and ref:
                    content["bill_transaction_no__c"] = ref

        except json.JSONDecodeError:
            console.warn("[Specific] ⚠️ records is not valid JSON, returning raw text")

        console.warn(f"[Specific] ✅ '{key}' record retrieved.")

        result = {
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "Specific record retrieved successfully",
            "records": content
        }
        return make_response(result, request=request)

    except Exception as e:
        console.error(f"[Specific] ❌ Failed to retrieve records | error={str(e)}")
        return make_response({
            "status": "failed",
            "message": "Failed to retrieve specific records",
            "error": str(e)},
            status=500,
            request=request
        )


async def summary_retrieve(bucket, key, page, per_page, start_date=None, end_date=None, request=None):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Mobile] ❌ summary retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[Mobile] Failed to process summary",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()
        data = json.loads(content)
        items = data.get("records", [])
        overall_spends = data.get("invoice_spends", [])
        overall_invoice_count = data.get("invoice_count", [])
        overall_total_records = data.get("total_records", [])

        from_dt = normalize_date(start_date)
        to_dt = normalize_date(end_date)

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
        return make_response(result, request=request)

    except Exception as e:
        console.error(f"[Mobile] ❌ Failed to process summary | error={str(e)}")
        return make_response({
            "status": "failed",
            "message": "[Mobile] Failed to process summary",
            "error": str(e)},
            status=500,
            request=request
        )


async def unique_invoices(bucket, key, start_date=None, end_date=None, request=None):
    start = time.time()
    try:
        # ------------------ Fetch object ------------------
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Unique Invoice] ❌ summary retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[Unique Invoice] Failed to process summary",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()
        data = json.loads(content)

        items = data.get("records", [])
        invoice_spends = data.get("invoice_spends", [])
        invoice_count = data.get("invoice_count", [])

        # ------------------ Date filter ------------------
        from_dt = normalize_date(start_date)
        to_dt = normalize_date(end_date)

        if from_dt or to_dt:
            filtered = []
            for inv in items:
                bill_date = parse_bill_date(inv)
                if not bill_date:
                    continue
                if from_dt and bill_date < from_dt:
                    continue
                if to_dt and bill_date > to_dt:
                    continue
                filtered.append(inv)
            items = filtered

        # ------------------ UNIQUE invoice logic ------------------
        # Key = (invoice_no, year)
        unique_map = {}

        for inv in items:
            invoice_no = inv.get("bill_transaction_no__c")
            year = get_invoice_year(inv)

            if not invoice_no or not year:
                continue

            key_tuple = (invoice_no, year)

            # Keep FIRST occurrence per invoice per year
            if key_tuple not in unique_map:
                unique_map[key_tuple] = inv

        items = list(unique_map.values())

        # ----------------------------------------------------------
        items.sort(key=parse_bill_date, reverse=True)
        total = len(items)

        console.info(f"[Unique Invoice] ✅ summary record retrieved.")

        result = {
                    "key": key,
                    "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
                    "status": "success",
                    "message": "Invoice summary retrieved successfully",
                    "invoice_spends": invoice_spends,
                    "invoice_count": invoice_count,
                    "total_records": total,
                    "records": items
        }
        return make_response(result, request=request)

    except Exception as e:
        console.error(f"[Unique Invoice] ❌ Failed to process summary | error={str(e)}")
        return make_response({
            "status": "failed",
            "message": "[Unique Invoice] Failed to process summary",
            "error": str(e)},
            status=500,
            request=request
        )


async def invoice_retrieve(bucket, key, invoice_no, request=None):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[Invoice] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[Invoice] Failed to process mobile summary",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()
        data = json.loads(content)
        files = data.get("records", [])

        filtered = [item for item in files if invoice_no.strip() == (item.get("bill_transaction_no__c") or "").strip()]

        if not filtered:
            console.info(f"[Invoice] ⚠️ Records not found | invoice_no={invoice_no} | key={key}")
            return make_response({
                "status": "failed",
                "message": "[Invoice] Not found",
                "error": f"No records found for invoice_no: {invoice_no}"},
                status=404,
                request=request
            )

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
        return make_response(result, request=request)

    except Exception as e:
        console.error(f"[Invoice] ❌ Exception while retrieving invoice | invoice_no={invoice_no} | key={key} | error={str(e)}")
        return make_response({
            "status": "failed",
            "message": "[Invoice] Failed to process mobile summary",
            "error": str(e)},
            status=500,
            request=request
        )


async def imei_retrieve(bucket, key, invoice_no, request=None):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[IMEI] ❌ Invoice retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[IMEI] Failed to retrieve records",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()
        data = json.loads(content)
        files = data.get("records", [])

        matched = []
        for item in files:
            if not invoice_no or invoice_no.strip() != (item.get("bill_transaction_no__c") or "").strip():
                continue

            imei_str = (item.get("item_remarks1__c") or "").strip()
            if not imei_str:
                console.info(f"[IMEI] ⚠️ No IMEI found for invoice_no = {invoice_no} | key = {key}")
                continue

            imei_list = [x.strip() for x in imei_str.split(",") if x.strip()]

            for imei in imei_list:
                new_entry = item.copy()
                new_entry["item_remarks1__c"] = imei
                matched.append(new_entry)

        if not matched:
            console.info(f"[IMEI] ⚠️ No IMEI found for invoice_no = {invoice_no} | key = {key}")
            return make_response({
                "status": "failed",
                "message": "[IMEI] Not found",
                "error": f"No IMEI found for invoice_no: {invoice_no}"},
                status=404,
                request=request
            )

        matched.sort(key=parse_bill_date, reverse=True)

        console.warn(f"[IMEI] ✅ '{invoice_no}' record retrieved.")

        results = {
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "IMEI retrieved successfully",
            "total_records": len(matched),
            "records": matched
        }
        return make_response(results, request=request)

    except Exception as e:
        console.error(f"[IMEI] ❌ Failed to retrieve records | error={str(e)}")
        return make_response({
            "status": "failed",
            "message": "[IMEI] Failed to retrieve records",
            "error": str(e)},
            status=500,
            request=request
        )


async def imei_history(bucket, key, request=None):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[IMEI] ❌ history retrieve failed | Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[IMEI] Failed to process history",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()
        data = json.loads(content)
        items = data.get("records", [])

        # 🔽 SORT RECORDS (RECENT FIRST)
        items.sort(key=parse_bill_date, reverse=True)

        console.info("[IMEI] ✅ history record retrieved.")

        total = len(items)

        result = {
            "key": key,
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "IMEI history retrieved successfully",
            "total_records": total,
            "records": items,
        }

        return make_response(result, request=request)

    except Exception as e:
        console.error(f"[IMEI] ❌ Failed to process history | error={str(e)}")
        return make_response(
            {
                "status": "failed",
                "message": "[IMEI] Failed to process history",
                "error": str(e),
            },
            status=500,
            request=request,
        )


async def fetch_single_imei(bucket, key, request=None):
    start = time.time()
    try:
        obj = await bucket.get(key)
        if not obj:
            console.warn(f"[IMEI] ❌ Key not found: {key}")
            return make_response({
                "status": "failed",
                "message": "[IMEI] Failed to process summary",
                "error": f"This key '{key}' not found"},
                status=404,
                request=request
            )

        content = await obj.text()
        data = json.loads(content)
        items = data.get("records", [])

        if not items:
            console.info(f"[IMEI] ⚠️ No records found | key={key}")
            return make_response({
                    "status": "failed",
                    "message": "No IMEI records found",
                    "records": None,},
                    status=404,
                    request=request
            )

        # Get latest record
        single_record = max(items, key=parse_bill_date)

        console.info(f"[IMEI] ✅ Single IMEI record retrieved.")

        result = {
            "key": key,
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "single imei retrieved successfully",
            "records": single_record,
        }

        return make_response(result, request=request)

    except Exception as e:
        console.error(f"[IMEI] ❌ Failed to process summary | error={str(e)}")
        return make_response(
            {
                "status": "failed",
                "message": "[IMEI] Failed to process summary",
                "error": str(e),
            },
            status=500,
            request=request
        )
