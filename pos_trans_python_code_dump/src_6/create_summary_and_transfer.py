import asyncio, json
from handling_failure import FatalProcessingError
from js import console, fetch, Object, Date, JSON
from datetime import datetime, timedelta, timezone
from helper import read_json_data, get_date_paths, store_json_data, safe_float


MAX_SUMMARY_RETRIES = 3


def identity_key(record):

    mobile = (
            record.get("customer_mobile__c")
            or record.get("Customer_Number__c")
            or record.get("customer_phone_no2")
            or ""
    )

    return (
        str(mobile).strip(),
        str(record.get("bill_transaction_no__c", "")).strip(),
        str(record.get("item_remarks1__c", "")).strip(),  # IMEI / serial
        str(record.get("Item_Code__c", "")).strip(),
        str(record.get("Bill_Date__c", "")).strip(),  # YEAR disambiguation
    )


def record_is_valid(record):
    if not isinstance(record, dict):
        return False

    mobile = (
            record.get("customer_mobile__c") or
            record.get("Customer_Number__c") or
            record.get("customer_phone_no2")
    )

    if not mobile or str(mobile).strip() == "":
        return False

    required_fields = [
        "bill_transaction_no__c",
        "item_remarks1__c",
        "Item_Code__c",
        "Bill_Date__c"
    ]

    for key in required_fields:
        if not str(record.get(key, "")).strip():
            return False

    return True


async def mobile_summary_retrieve(env, mobile):
    key = f"mobile/{mobile}/mobile_summary.json"
    summary = await read_json_data(env, key, bucket_binding="POS_BUCKET_PROD")

    if not summary:
        return {
            "mobile": mobile,
            "total_records": 0,
            "invoice_count": 0,
            "invoice_spends": 0.0,
            "records": [],
        }
    return summary


async def mobile_summary_retrieve_with_etag(env, mobile):
    key = f"mobile/{mobile}/mobile_summary.json"
    bucket = env.POS_BUCKET_PROD

    obj = await bucket.get(key)

    if not obj:
        return {
            "mobile": mobile,
            "total_records": 0,
            "invoice_count": 0,
            "invoice_spends": 0.0,
        }, None  # No etag yet

    text = await obj.text()
    data = json.loads(text)

    return data, obj.etag

# -------------------------------------------------------------------------------------------------------

async def safe_replace_old_record(env, mobile, pri_id):
    year, month, day = get_date_paths()

    src_key = f"mobile/{mobile}/id/{pri_id}.json"
    trans_key = f"mobile/{mobile}/transferred/{year}/{month}/{day}/{pri_id}.json"

    old_data = await read_json_data(env, src_key, bucket_binding="POS_BUCKET_PROD")
    if not old_data:
        return  # already transferred or missing

    await store_json_data(env, old_data, trans_key, metadata=None, bucket_binding="POS_BUCKET_PROD")

    await env.POS_BUCKET_PROD.delete(src_key)

    console.log(f"[TRANSFER] 🔁 Transferred | mobile={mobile} | PRI_ID={pri_id}")


async def sync_verified_records(env, mobile: str, old_records: list, new_records: list):
    """
    OLD verified record is TRANSFERRED (moved + deleted)
    if the SAME business identity appears in NEW records.
    """

    # 1️⃣ Build map of NEW verified identities
    new_verified_keys = {identity_key(r) for r in new_records if record_is_valid(r)}

    # 2️⃣ Scan OLD records and transfer matching ones
    for old in old_records:
        if not record_is_valid(old):
            continue

        if identity_key(old) in new_verified_keys:
            pri_id = old.get("pri_id")
            if pri_id:
                await safe_replace_old_record(env, mobile, pri_id)


async def transfer_verified_records(env, mobile: str, new_records: list):
    try:
        summary = await mobile_summary_retrieve(env, mobile)
        old_records = summary.get("records", [])

        await sync_verified_records(env, mobile, old_records, new_records)

    except Exception as e:
        console.warn(
            f"[TRANSFER] ⚠️ Identity-based transfer failed "
            f"(mobile={mobile}) → {str(e)}"
        )

# -------------------------------------------------------------------------------------------------------

def merged_summary_records(old_records, new_records):
    old_records = old_records or []
    new_records = new_records or []

    # 🔑 Step 1: Verified identities from NEW
    new_keys = {identity_key(r) for r in new_records if record_is_valid(r)}

    # 🧹 Step 2: filter OLD records
    filtered_old = []

    for r in old_records:
        if not record_is_valid(r):
            filtered_old.append(r)  # keep invalid OLD
        elif identity_key(r) not in new_keys:
            filtered_old.append(r)  # keep unmatched OLD
        # else: matched & valid → replaced by NEW

    # ➕ Combine (NEW automatically last → newest)
    return filtered_old + new_records


def invoice_calculation(records):
    invoices = {}

    for r in records:
        inv = r.get("bill_transaction_no__c")
        if not inv:
            continue
        if inv not in invoices:
            invoices[inv] = r

    spends = sum(
        safe_float(i.get("Bill_Grant_Total__c", 0))
        for i in invoices.values()
    )

    return len(invoices), spends


async def update_mobile_summary(env, mobile, new_records):
    bucket = env.POS_BUCKET_PROD
    key = f"mobile/{mobile}/mobile_summary.json"

    for attempt in range(MAX_SUMMARY_RETRIES):
        summary, etag = await mobile_summary_retrieve_with_etag(env, mobile)

        old_records = summary.get("records", [])

        merged_records = merged_summary_records(old_records, new_records)

        invoice_count, spends = invoice_calculation(merged_records)

        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

        updated_summary = {
            "mobile": mobile,
            "records": merged_records,
            "total_records": len(merged_records),
            "invoice_count": invoice_count,
            "invoice_spends": spends,
            "updated_at": now
        }

        body = json.dumps(updated_summary, ensure_ascii=False)

        metadata_js = Object.fromEntries([
            ["mobile", str(mobile)],
            ["records-count", str(len(merged_records))],
            ["invoice-count", str(invoice_count)],
            ["total-spends", f"{spends:.2f}"]
        ])

        http_meta_js = Object.fromEntries([["contentType", "application/json"]])
        try:
            if etag:
                # Conditional write (optimistic lock)
                await bucket.put(
                    key, body, customMetadata=metadata_js, httpMetadata=http_meta_js, onlyIf={"etagMatches": etag}
                )
            else:
                # First write
                await bucket.put(key, body, customMetadata=metadata_js, httpMetadata=http_meta_js)
            # SUCCESS
            return


        except Exception as e:
            # Conflict = someone updated before us
            if "etag" in str(e).lower():
                console.warn(
                    f"[SUMMARY] ⚠️ ETag conflict for mobile={mobile}, retry={attempt + 1}"
                )
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            else:
                raise

    # If we exit loop → too many conflicts
    raise FatalProcessingError(
        f"Mobile summary update failed due to repeated ETag conflicts | mobile={mobile}"
    )


async def rebuild_imei_summary(env, imei):
    prefix = f"imei/{imei}/id/"

    try:
        # Async List from IMEI_BUCKET_PROD
        listed = await env.IMEI_BUCKET_PROD.list(prefix=prefix)
        keys = [obj.key for obj in listed.objects]

        # Read all files concurrently using helper (IMEI_BUCKET_PROD)
        file_contents = await asyncio.gather(*[
            read_json_data(env, k, bucket_binding="IMEI_BUCKET_PROD") for k in keys]
        )

        final_records = [d for d in file_contents if d]

        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

        summary_data = {
            "serial_no": imei,
            "total_records": len(final_records),
            "records": final_records,
            "updated_at": now
        }

        summary_meta = {
            "serial_no": imei,
            "total_records": str(len(final_records)),
        }

        summary_key = f"imei/{imei}/imei_summary.json"
        # Store summary in IMEI_BUCKET_PROD
        await store_json_data(env, summary_data, summary_key, summary_meta, bucket_binding="IMEI_BUCKET_PROD")

    except Exception as e:
        console.error(f"[IMEI SUMMARY] ❌ Failed imei summary rebuild: {e} | imei={imei}")

# -------------------------------------------------------------------------------------------------------