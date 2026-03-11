from helper import *
from js import console, fetch, Object, Date, JSON


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
    summary = await read_json_data(env, key, bucket_binding="POS_BUCKET_LIVE")

    if not summary:
        return {
            "mobile": mobile,
            "total_records": 0,
            "invoice_count": 0,
            "invoice_spends": 0.0,
            "records": [],
        }
    return summary

# -------------------------------------------------------------------------------------------------------

async def safe_replace_old_record(env, mobile, pri_id):
    year, month, day = get_date_paths()

    src_key = f"mobile/{mobile}/id/{pri_id}.json"
    tgt = f"mobile/{mobile}/transferred/{year}/{month}/{day}/{pri_id}.json"

    old_data = await read_json_data(env, src_key)
    if not old_data:
        return  # already transferred or missing

    # Check if already transferred
    exists = await read_json_data(env, tgt)
    if exists:
        return

    await store_json_data(env, old_data, tgt)
    await env.POS_BUCKET.delete(src_key)

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
    summary = await mobile_summary_retrieve(env, mobile)

    old_records = summary.get("records", [])

    merged_records = merged_summary_records(old_records, new_records)

    invoice_count, spends = invoice_calculation(merged_records)

    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    summary_data = {
        "mobile": mobile,
        "records": merged_records,
        "total_records": len(merged_records),
        "invoice_count": invoice_count,
        "invoice_spends": spends,
        "updated_at": now
    }

    summary_meta = {
        "mobile": str(mobile),
        "records-count": str(len(merged_records)),
        "invoice-count": str(invoice_count),
        "total-spends": f"{spends:.2f}",
    }

    key = f"mobile/{mobile}/mobile_summary.json"
    await store_json_data(env, summary_data, key, summary_meta, bucket_binding="POS_BUCKET_LIVE")

# async def rebuild_mobile_summary(env, mobile):
#     prefix = f"mobile/{mobile}/id/"
#
#     try:
#         # Async List from POS_BUCKET_LIVE
#         listed = await env.POS_BUCKET_LIVE.list(prefix=prefix)
#         keys = [obj.key for obj in listed.objects]
#
#         # Read all files concurrently using helper (POS_BUCKET_LIVE)
#         file_contents = await asyncio.gather(*[
#             read_json_data(env, k, bucket_binding="POS_BUCKET_LIVE") for k in keys]
#         )
#
#         final_records = [d for d in file_contents if d]
#
#         unique_invoices = {}
#         for record in file_contents:
#             if not record:
#                 continue
#
#             invoice_no = record.get("bill_transaction_no__c")
#             if not invoice_no:
#                 continue  # skip records without invoice
#
#             if invoice_no not in unique_invoices:
#                 unique_invoices[invoice_no] = record
#
#         unique_invoices = list(unique_invoices.values())
#
#         spends = sum(safe_float(i.get("Bill_Grant_Total__c", 0)) for i in unique_invoices)
#
#         ist = timezone(timedelta(hours=5, minutes=30))
#         now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
#
#         summary_data = {
#             "mobile": mobile,
#             "total_records": len(final_records),
#             "invoice_count": len(unique_invoices),
#             "invoice_spends": spends,
#             "records": final_records,
#             "updated_at": now
#         }
#
#         summary_meta = {
#             "mobile": str(mobile),
#             "records-count": str(len(final_records)),
#             "invoice-count": str(len(unique_invoices)),
#             "total-spends": f"{spends:.2f}",
#         }
#
#         summary_key = f"mobile/{mobile}/mobile_summary.json"
#         # Store summary in POS_BUCKET_LIVE
#         await store_json_data(env, summary_data, summary_key, summary_meta, bucket_binding="POS_BUCKET_LIVE")
#
#     except Exception as e:
#         console.error(f"Failed mobile summary rebuild: {e}")
# -------------------------------------------------------------------------------------------------------

async def rebuild_imei_summary(env, imei):
    prefix = f"imei/{imei}/id/"

    try:
        # Async List from IMEI_BUCKET_LIVE
        listed = await env.IMEI_BUCKET_LIVE.list(prefix=prefix)
        keys = [obj.key for obj in listed.objects]

        # Read all files concurrently using helper (IMEI_BUCKET_LIVE)
        file_contents = await asyncio.gather(*[
            read_json_data(env, k, bucket_binding="IMEI_BUCKET_LIVE") for k in keys]
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
        # Store summary in IMEI_BUCKET_LIVE
        await store_json_data(env, summary_data, summary_key, summary_meta, bucket_binding="IMEI_BUCKET_LIVE")

    except Exception as e:
        console.error(f"Failed imei summary rebuild: {e}")

# -------------------------------------------------------------------------------------------------------