import time
from core.creds import r2_client
from concurrent.futures import ThreadPoolExecutor, as_completed


def delete_batch(r2, delete_keys, bucket_name):
    try:

        r2.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})

        count = len(delete_keys)
        print(f"Deleted batch of {count} objects")

        return count
    except Exception as e:
        print(f"❌ Error deleting batch: {e}")
        return 0


def delete_mobile_folder(r2, prefix, bucket_name):
    start = time.time()
    paginator = r2.get_paginator("list_objects_v2")

    total_deleted = 0
    futures = []

    print("🚀 Starting deletion process...")

    with ThreadPoolExecutor(max_workers=40) as executor:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):

            objects = page.get("Contents", [])
            if not objects:
                continue

            delete_keys = [{"Key": obj["Key"]} for obj in objects]
            futures.append(executor.submit(delete_batch, r2, delete_keys, bucket_name))

        for future in as_completed(futures):
            total_deleted += future.result()

    print(f"✅ Completed. Total objects deleted: {total_deleted} | in {round(time.time() - start, 2)}s")


client = r2_client()

# folder = "mobile/"
# folder = "crm_error_response/"
folder = "dump/"
bucket = "pos-transaction-test"

if __name__ == "__main__":
    delete_mobile_folder(client, folder, bucket)
