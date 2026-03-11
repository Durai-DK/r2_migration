import uuid
from helper import *
from js import console, fetch, Object, Date, JSON

async def journal_failed_put(env, bucket, key, data, meta, error):
    fid = str(uuid.uuid4())
    y, m, d = get_date_paths()

    fail_key = f"failed_puts/{y}/{m}/{d}/{fid}.json"

    await env.POS_BUCKET_LIVE.put(
        fail_key,
        json.dumps({
            "bucket": bucket,
            "key": key,
            "payload": data,
            "meta": meta,
            "error": str(error),
            "ts": time.time()
        })
    )


async def reliable_store(env, data, key, meta, bucket):
    try:
        await store_json_data(env, data, key, meta, bucket)
    except Exception as e:
        console.error(f"[R2] ❌ Permanent failure | {bucket}:{key}")
        await journal_failed_put(env, bucket, key, data, meta, e)

