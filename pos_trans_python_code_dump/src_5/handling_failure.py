import uuid
from helper import *
from js import console, fetch, Object, Date, JSON


async def failed_queue_store(env, payload, batch_id, e):
    y, m, d = get_date_paths()

    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    key = f"Failed/Queues/{y}/{m}/{d}/{batch_id}.json"

    data = {
            "batch_id": batch_id,
            "time": now,
            "error": str(e),
            "record": payload,
        }

    await env.IMEI_BUCKET_PROD.put(key, json.dumps(data, ensure_ascii=False))


async def journal_put_failure(env, data, key, meta, bucket, error):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    y, m, d = get_date_paths()
    stored_id = str(uuid.uuid4())
    stored_key = f"Failed/Stored//{y}/{m}/{d}/{stored_id}.json"
    payload = {
            "bucket": bucket,
            "key": key,
            "data": data,
            "meta": meta,
            "error": str(error),
            "ts": now
        }
    await env.IMEI_BUCKET_PROD.put(stored_key, json.dumps(payload, ensure_ascii=False))
    console.log(f"[FAILED] ✅ failed data stored | failed key = {key} | stored key={stored_key}")


async def r2_store(env, data, key, metadata, bucket):
    try:
        await store_json_data(env, data, key, metadata, bucket_binding=bucket)
    except Exception as e:
        await journal_put_failure(env, data, key, metadata, bucket, str(e))
