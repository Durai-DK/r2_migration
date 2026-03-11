import json, time
from datetime import datetime
from core.creds import r2_client
from fastapi.responses import JSONResponse

def fetch_specific_file(bucket: str, key: str):
    start = time.time()

    r2 = r2_client()
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()

        try:
            content = body.decode("utf-8")
        except:
            content = body

        end = time.time()

        result = {
            "retrieved_in": f"{round((end - start) * 1000, 2)}ms",
            "status": "success",
            "message": "Specific record retrieved successfully",
            "records": content
        }

        return JSONResponse(status_code=200, content=result)

    except r2.exceptions.NoSuchKey:
        return error_response(404, "Failed to retrieve Specific record", f"This key '{key}' not found")

    except Exception as e:
        return error_response(400, "Failed to retrieve specific records", str(e))

def summary_retrieve(bucket, key, page=1, per_page=10, start_date=None, end_date=None):
    start = time.time()

    r2 = r2_client()

    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        content = body.decode("utf-8", errors="ignore")

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

        items.sort(key=lambda x: x.get("pri_id", 0), reverse=True)

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

        return JSONResponse(status_code=200, content=result)

    except r2.exceptions.NoSuchKey:
        return error_response(404, "Failed to process mobile summary", f"This key '{key}' not found")
    except Exception as e:
        return error_response(400, "Failed to process mobile summary", str(e))

def invoice_retrieve(bucket, key, invoice_no):
    start = time.time()
    r2 = r2_client()

    try:
        # ---- Fetch JSON ----
        obj = r2.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8", errors="ignore")

        data = json.loads(content)
        files = data.get("records", [])

        filtered = [
            item for item in files
            if invoice_no.strip() == (item.get("bill_transaction_no__c") or "").strip()
        ]

        if not filtered:
            return error_response(
                404, "Invoice not found", f"No records found for invoice_no: {invoice_no}"
            )

        filtered.sort(key=lambda x: int(x.get("pri_id") or 0), reverse=True)

        result = {
            "key": key,
            "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
            "status": "success",
            "message": "Invoice retrieved successfully",
            "total_records": len(filtered),
            "records": filtered
        }

        return JSONResponse(status_code=200, content=result)

    except r2.exceptions.NoSuchKey:
        return error_response(404, "Failed to process mobile summary", f"This key '{key}' not found")

    except Exception as e:
        return error_response(400, "Failed to process mobile summary", str(e))

def imei_retrieve(bucket, key, invoice_no):
    start = time.time()
    r2 = r2_client()

    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8", errors="ignore")

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
            return error_response(404, "IMEI not found", f"No IMEI found for invoice_no: {invoice_no}")

        matched.sort(key=lambda x: x.get("pri_id", ""), reverse=True)

        results = {
                    "retrieved_in": f"{round((time.time() - start) * 1000, 2)}ms",
                    "status": "success",
                    "message": "IMEI retrieved successfully",
                    "total_records": len(matched),
                    "records": matched
        }
        return JSONResponse(status_code=200, content=results)

    except r2.exceptions.NoSuchKey:
        return error_response(404, "Failed to retrieve records", f"This key '{key}' not found")
    except Exception as e:
        return error_response(400, "Failed to retrieve records", str(e))

def error_response(code, message, error):
    error_payload = {"status": "failed", "message": message, "error": error}
    return JSONResponse(status_code=code, content=error_payload)
