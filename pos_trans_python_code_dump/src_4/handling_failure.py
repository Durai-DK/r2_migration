import uuid
from helper import *
from js import console, fetch, Object, Date, JSON

MAX_QUEUE_ATTEMPTS = 5
MAX_R2_RETRIES = 5
BASE_BACKOFF = 0.25
UPLOAD_CHUNK_SIZE = 25
MAX_CONCURRENT_TASKS = 3

async def wal_write(env, event):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    wal_id = str(uuid.uuid4())
    y, m, d = get_date_paths()

    await env.POS_BUCKET_LIVE.put(
        f"wal/{y}/{m}/{d}/{wal_id}.json",
        json.dumps({
            "wal_id": wal_id,
            "state": "RECEIVED",
            "event": event,
            "ts": now
        })
    )
    return wal_id

async def wal_update_state(env, wal_id, state, meta=None):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    await env.POS_BUCKET_LIVE.put(
        f"state/{wal_id}.json",
        json.dumps({
            "state": state,
            "meta": meta,
            "ts": now
        })
    )

async def r2_put_safe(env, bucket_name, key, payload, meta):
    bucket = getattr(env, bucket_name)
    delay = BASE_BACKOFF

    for attempt in range(1, MAX_R2_RETRIES + 1):
        try:
            await bucket.put(
                key,
                json.dumps(payload),
                custom_metadata=meta
            )
            return
        except Exception as e:
            if attempt == MAX_R2_RETRIES:
                raise
            await asyncio.sleep(delay)
            delay *= 2

async def journal_put_failure(env, bucket, key, payload, meta, error):
    y, m, d = get_date_paths()
    fid = str(uuid.uuid4())

    await env.POS_BUCKET_LIVE.put(
        f"failed_puts/{y}/{m}/{d}/{fid}.json",
        json.dumps({
            "bucket": bucket,
            "key": key,
            "payload": payload,
            "meta": meta,
            "error": str(error),
            "ts": time.time()
        })
    )

async def reliable_store(env, bucket, key, payload, meta):
    try:
        await r2_put_safe(env, bucket, key, payload, meta)
    except Exception as e:
        await journal_put_failure(env, bucket, key, payload, meta, e)


