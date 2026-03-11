import uuid, random, json
from datetime import timedelta, datetime, timezone
from js import console, fetch, Object, Date, JSON
from helper import get_date_paths, store_json_data, deep_to_py

MAX_RETRIES = 3
MAX_DLQ_REPLAYS = 3


async def failed_queue_store(env, payload, batch_id, e, status):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    y, m, d = get_date_paths()
    key = f"Failed/Queues/{y}/{m}/{d}/{batch_id}.json"

    status = status or "FAILED"
    metadata = {
        "batch_id": str(batch_id),
        "time": str(now),
        "error": str(e),
    }

    data = {
        "batch_id": batch_id,
        "status": status,
        "time": now,
        "error": str(e),
        "record": payload,
        "replay_count": 0
    }

    body = json.dumps(data, ensure_ascii=False)

    # Convert metadata to JS Object
    metadata_js = Object.fromEntries([[k, str(v)] for k, v in (metadata or {}).items()])
    http_meta_js = Object.fromEntries([["contentType", "application/json"]])

    # Added httpMetadata for dashboard preview (Object Preview available)
    await env.IMEI_BUCKET_PROD.put(key, body, customMetadata=metadata_js, httpMetadata=http_meta_js)


async def journal_put_failure(env, data, key, meta, bucket, error):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    y, m, d = get_date_paths()
    stored_id = str(uuid.uuid4())
    stored_key = f"Failed/Journal/{y}/{m}/{d}/{stored_id}.json"

    payload = {
            "bucket": bucket,
            "key": key,
            "data": data,
            "meta": meta,
            "error": str(error),
            "time": now
        }
    body = json.dumps(payload, ensure_ascii=False)

    metadata = {
        "bucket": str(bucket),
        "key": str(key),
        "time": str(now),
        "error": str(error),
    }

    # Convert metadata to JS Object
    metadata_js = Object.fromEntries([[k, str(v)] for k, v in (metadata or {}).items()])
    http_meta_js = Object.fromEntries([["contentType", "application/json"]])

    await env.IMEI_BUCKET_PROD.put(stored_key, body, customMetadata=metadata_js, httpMetadata=http_meta_js)
    console.log(f"[FAILED] ✅ failed data stored | failed key = {key} | stored key={stored_key}")


async def r2_store(env, data, key, metadata, bucket):
    try:
        await store_json_data(env, data, key, metadata, bucket_binding=bucket)
    except Exception as e:
        await journal_put_failure(env, data, key, metadata, bucket, str(e))
        raise


class FatalProcessingError(Exception):
    pass


async def send_to_dlq(env, message, error, source_queue="POS_QUEUE"):
    original_body = deep_to_py(message.body)

    replay_count = original_body.get("replay_count", 0)

    batch_id = original_body.get("batch_id", "unknown")

    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    dlq_payload = {
        "batch_id": batch_id,
        "original_message": original_body,
        "error": str(error),
        "attempts": message.attempts,
        "replay_count": replay_count,
        "ts": now,
        "status": "DLQ",
        "is_replay": original_body.get("is_replay", False),
        "source_queue": source_queue
    }

    js_payload = JSON.parse(json.dumps(dlq_payload))
    await env.POS_DLQ.send(js_payload)


def compute_backoff(attempts):
    base = 5  # seconds
    max_delay = 300  # 5 minutes max
    delay = min(base * (2 ** (attempts - 1)), max_delay)
    jitter = random.uniform(0, 3)
    return delay + jitter


async def is_already_main_queue_processed(env, batch_id):
    return bool(await env.POS_KV.get(f"main_processed:{batch_id}"))


async def mark_main_queue_processed(env, batch_id):
    await env.POS_KV.put(f"main_processed:{batch_id}", "1", expirationTtl=86400)


async def is_already_summary_queue_processed(env, batch_id):
    return bool(await env.POS_KV.get(f"summary_processed:{batch_id}"))


async def mark_summary_queue_processed(env, batch_id):
    await env.POS_KV.put(f"summary_processed:{batch_id}", "1", expirationTtl=86400)


async def handle_retry_or_dlq(env, message, payload, batch_id, status, error=None, source_queue="POS_QUEUE"):
    if message.attempts >= MAX_RETRIES:
        await failed_queue_store(env, payload or {}, batch_id, error or status, status=f"{status}_MAX_RETRY")
        await send_to_dlq(env, message, error or status, source_queue=source_queue)
        message.ack()
    else:
        delay = compute_backoff(message.attempts)
        message.retry(delaySeconds=delay)


async def archive_poison_message(env, payload):
    y, m, d = get_date_paths()
    key = f"Poison/{y}/{m}/{d}/{payload.get('batch_id','unknown')}.json"

    body = json.dumps(payload, ensure_ascii=False)

    await env.IMEI_BUCKET_PROD.put(key, body, httpMetadata=Object.fromEntries([["contentType", "application/json"]]))

    console.error(f"[DLQ] ☠ Archived poison message")


async def replay_dlq(env):
    messages = await env.POS_DLQ.receive(maxMessages=10)

    for msg in messages:
        payload = deep_to_py(msg.body)
        batch_id = payload.get("batch_id", "unknown")
        replay_count = payload.get("replay_count", 0)

        if replay_count >= MAX_DLQ_REPLAYS:
            console.warn(f"[DLQ] Permanent failure {payload['batch_id']}")
            await archive_poison_message(env, payload)
            msg.ack()
            continue

        original = payload.get("original_message", {})
        original["replay_count"] = replay_count + 1
        original["is_replay"] = True

        console.log(
            f"[DLQ] 🔁 Replaying batch_id={batch_id} | count={original['replay_count']}"
        )

        await env.POS_QUEUE.send(JSON.parse(json.dumps(original)))
        msg.ack()
