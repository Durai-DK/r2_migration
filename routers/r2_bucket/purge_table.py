from core.creds import r2_client

prefix = "mobile/"
bucket_name = "dev-soc-media"

# prefix = "imei_numbers/"
# bucket_name = "dev-soc-media-1"

# prefix = "pos_transactions/"
# bucket_name = "dev-transaction"


def delete_mobile_folder():

    r2 = r2_client()

    paginator = r2.get_paginator("list_objects_v2")
    total_deleted = 0

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue

        delete_keys = [{"Key": obj["Key"]} for obj in objects]

        r2.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})

        total_deleted += len(delete_keys)
        print(f"Deleted batch of {len(delete_keys)} objects")

    print(f"✅ Completed. Total objects deleted: {total_deleted}")


delete_mobile_folder()
