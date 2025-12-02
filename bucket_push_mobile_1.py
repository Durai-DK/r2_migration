import os, json, boto3, time
from dotenv import load_dotenv
from botocore.client import Config

load_dotenv()

ROOT = "/Users/DK/Desktop/Data_Backup/test_one/"

BUCKET = os.getenv("BUCKET_NAME")
ENDPOINT = os.getenv("ENDPOINT")
ACCESS_KEY = os.getenv("ACCESS_KEY_ID")
SECRET_KEY = os.getenv("SECRET_ACCESS_KEY")

prefix = "mobile"
summary_file_name = "mobile_number_summary.json"
LIMIT = 15

# Create R2 client
s3 = boto3.client(
                "s3",
                endpoint_url=os.getenv("ENDPOINT"),
                aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
                config=Config(signature_version="s3v4"),
                region_name="auto"
            )

mobile_summary = {}
TOTAL_INVOICES = 0


def is_valid_json(file_path):
    try:
        with open(file_path, "r", encoding='utf-8') as f:
            json.load(f)
        return True
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"❌ Invalid JSON in {file_path} → {e}")
        return False


def upload_json_to_r2(key, data, metadata=None):
    if metadata is None:
        metadata = {}

    if isinstance(data, (dict, list, int, float, str)):
        body = json.dumps(data, indent=2, ensure_ascii=False)
    else:
        raise TypeError(f"Unsupported data type for R2 upload: {type(data)}")

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata=metadata
    )


def upload_one_invoice(file_path):
    global TOTAL_INVOICES

    try:
        # Validate JSON before processing
        if not is_valid_json(file_path):
            return

        with open(file_path, "r", encoding='utf-8') as f:
            data = json.load(f)

        pri_id = str(data.get("pri_id") or "").strip()
        mobile = str(data.get("customer_mobile__c") or "").strip()
        invoice_id = str(data.get("bill_transaction_no__c") or "").strip()
        serial_no = str(data.get("item_remarks1__c") or "").strip()

        if not mobile and not invoice_id:
            print(f"No mobile and invoice → SKIPPED {file_path}")
            return

        invoice_id_key = invoice_id.replace("/", "_")
        key = f"{prefix}/{mobile}/{invoice_id_key}.json"

        raw_amount = data.get("bill_grand_total__c")
        amount = float(raw_amount) if raw_amount not in (None, "", "null") else 0.0

        metadata = {
            "invoice-id": invoice_id,
            "serial-no": serial_no,
            "amount": str(amount),
            "mobile": mobile
        }

        # Upload invoice file
        upload_json_to_r2(key, data, metadata)

        TOTAL_INVOICES += 1

        # Create mobile group
        if mobile not in mobile_summary:
            mobile_summary[mobile] = {
                "invoice_count": 0,
                "total_value": 0.0,
                "invoices": []
            }

        # Update summary entry
        ms = mobile_summary[mobile]
        ms["invoice_count"] += 1
        ms["total_value"] += amount
        ms["invoices"].append({
            "pri_id": pri_id,
            "invoice_id": invoice_id,
            "serial_no": serial_no,
            "amount": amount
        })

        print(f"✔ Uploaded → {key}")

    except Exception as e:
        print(f"❌ ERROR {file_path} → {e}")


def upload_mobile_summaries():
    """Upload individual mobile summaries under each mobile prefix"""
    print("\n📱 Uploading mobile summaries...")

    for mobile, summary_data in mobile_summary.items():
        try:
            summary_key = f"{prefix}/{mobile}/summary.json"

            # Prepare metadata
            invoice_ids = [inv.get("invoice_id", "") for inv in summary_data.get("invoices", [])]
            serial_numbers = [inv.get("serial_no", "") for inv in summary_data.get("invoices", [])]

            metadata = {
                "mobile": mobile,
                "invoice-count": str(summary_data.get("invoice_count", 0)),
                "total-value": str(summary_data.get("total_value", 0.0)),
                "invoice-ids": ",".join(filter(None, invoice_ids)),  # Comma-separated
                "serial-numbers": ",".join(filter(None, serial_numbers))  # Comma-separated
            }

            upload_json_to_r2(summary_key, summary_data, metadata)
            print(f"✔ Mobile summary uploaded → {summary_key}")

        except Exception as e:
            print(f"❌ ERROR uploading mobile summary for {mobile} → {e}")


def write_summary_files():
    sorted_summary = dict(sorted(mobile_summary.items(), key=lambda x: x[1]["total_value"], reverse=True))
    total_purchase_value = sum(data["total_value"] for data in sorted_summary.values())

    # ---- overall summary ----
    overall_summary_data = {
        "TOTAL_INVOICES": TOTAL_INVOICES,
        "TOTAL_UNIQUE_MOBILES": len(sorted_summary),
        "TOTAL_PURCHASE_VALUE": round(total_purchase_value, 2)
    }

    upload_json_to_r2(summary_file_name, overall_summary_data)

    # ---- upload individual mobile summaries ----
    upload_mobile_summaries()

    # print("✅ All summaries uploaded successfully")


def main():
    # Get all JSON files and validate them first
    start = time.time()
    all_files = [os.path.join(ROOT, f) for f in os.listdir(ROOT) if f.endswith(".json")]

    # Validate JSON files first
    valid_files = []
    print("🔍 Validating JSON files...")
    for file_path in all_files[:LIMIT]:
        if is_valid_json(file_path):
            valid_files.append(file_path)
        else:
            print(f"❌ Skipping invalid JSON: {file_path}")

    print(f"\n📁 TOTAL VALID JSON FILES: {len(valid_files)}")
    print(f"🚀 PROCESSING FIRST {len(valid_files)} VALID FILES...\n")

    for i, fpath in enumerate(valid_files, start=1):
        upload_one_invoice(fpath)
        print(f"[{i}/{len(valid_files)}] done")

    write_summary_files()

    end = time.time()
    total_time_minutes = round((end - start) / 60, 2)
    # Final statistics
    print(f"\n🎯 FINAL STATISTICS:")
    print(f"   • Total Time Taken in Min: {total_time_minutes}")
    print(f"   • Total Invoices Processed: {TOTAL_INVOICES}")
    print(f"   • Unique Mobile Numbers: {len(mobile_summary)}")
    print(f"   • Total Value: {sum(ms['total_value'] for ms in mobile_summary.values()):.2f}")


if __name__ == "__main__":
    main()