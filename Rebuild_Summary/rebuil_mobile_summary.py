import boto3, json
from decouple import config
from datetime import datetime
from botocore.config import Config


BUCKET = "pos-transaction"

mobile_number = "9840643109"


def r2_client():
    try:
        return boto3.client(
            "s3",
            endpoint_url=config("ENDPOINT"),
            aws_access_key_id=config("ACCESS_KEY_ID"),
            aws_secret_access_key=config("SECRET_ACCESS_KEY"),
            config=Config(signature_version="s3v4"),
            region_name="auto"
        )
    except Exception as e:
        print(f"❌ Failed to initialize R2 client: {e}")

s3 = r2_client()


# ------------  List JSON files  ------------
def list_files(prefix):

    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])

    return keys


# ------------  Read JSON  ------------
def read_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except:
        return None


def rebuild_mobile_summary(mobile):
    key = f"mobile/{mobile}/id/"

    try:

        keys = list_files(key)

        if not keys:
            print("No files found")
            return

        final_records = []
        unique_invoices = {}

        for key in keys:

            data = read_json(key)
            if not data:
                continue

            final_records.append(data)

            invoice = data.get("bill_transaction_no__c")

            if invoice and invoice not in unique_invoices:
                unique_invoices[invoice] = data

        unique_invoice_list = list(unique_invoices.values())

        # spends = sum(
        #     float(i.get("Bill_Grant_Total__c", 0)) for i in unique_invoice_list
        # )

        def safe_float(v):
            try:
                return float(v)
            except:
                return 0.0

        spends = sum(
            safe_float(i.get("Bill_Grant_Total__c", 0)) for i in unique_invoice_list
        )

        summary = {
            "mobile": mobile,
            "total_records": len(final_records),
            "invoice_count": len(unique_invoice_list),
            "invoice_spends": spends,
            "records": final_records,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        metadata = {
            "mobile": str(mobile),
            "records-count": str(len(final_records)),
            "invoice-count": str(len(unique_invoice_list)),
            "total-spends": str(f"{spends:.2f}"),
        }

        summary_key = f"mobile/{mobile}/mobile_summary.json"

        s3.put_object(
            Bucket=BUCKET,
            Key=summary_key,
            Body=json.dumps(summary),
            ContentType="application/json",
            Metadata=metadata
        )

        print(f"✅ Mobile summary rebuilt for {mobile}")

    except Exception as e:
        print(f"❌ {mobile} failed: {e}")


if __name__ == "__main__":
    rebuild_mobile_summary(mobile_number)
