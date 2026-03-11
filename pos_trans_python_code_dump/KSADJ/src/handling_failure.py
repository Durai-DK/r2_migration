import random, json, asyncio
from datetime import timedelta, datetime, timezone
from js import console, fetch, Object, Date, JSON
from helper import get_date_paths, store_json_data

MAX_RETRIES = 3
MAX_DLQ_REPLAYS = 3

# --------------------------------------------------------------------------------

class PermanentError(Exception):
    pass

class TemporaryError(Exception):
    pass

async def is_already_main_queue_processed(env, batch_id):
    return bool(await env.POS_KV.get(f"main_processed:{batch_id}"))

async def mark_main_queue_processed(env, batch_id):
    await env.POS_KV.put(f"main_processed:{batch_id}", "1", expirationTtl=86400)

# --------------------------------------------------------------------------------

async def retry_operation(fn, base_delay=2):
    for attempt in range(MAX_RETRIES):
        try:
            return await fn()
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise

            delay = base_delay * (2 ** attempt)
            delay += random.uniform(0, 1)
            await asyncio.sleep(delay)


async def move_to_permanent_failure(env, batch_id, invoice, error, key):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    failure_data = {**(invoice or {})}

    p_fail_meta = {
        "batch_id": str(batch_id), "status": "PERMANENT_FAILED", "error": error, "timestamp": now
    }
    await store_json_data(env, failure_data, key, p_fail_meta, bucket_binding="POS_BUCKET_PROD")


async def dump_retry(env, batch_id, invoice, error):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    invoice = invoice or {}

    invoice["batch_id"] = batch_id
    invoice["retry_count"] = invoice.get("retry_count", 0) + 1
    invoice["dump_at"] = now

    year, month, day = get_date_paths()
    retry_meta = {
        "batch_id": str(batch_id),
        "status": "RETRY",
        "retry_count": invoice["retry_count"],
        "error": error or "Temporary failure",
        "timestamp": now
    }
    retry_key = f"dump/{year}/{month}/{day}/{batch_id}.json"

    await retry_operation(
        lambda: store_json_data(env, invoice, retry_key, retry_meta, bucket_binding="POS_BUCKET_PROD")
    )
    queue_payload = JSON.parse(json.dumps({
        "batch_id": batch_id,
        "retry_count": invoice["retry_count"],
        "invoice": invoice,
        "source": "retry-worker",
        "timestamp": now
    }))

    await retry_operation(lambda: env.POS_QUEUE.send(queue_payload))
    console.log(f"[RETRY] ✅ Invoice Re-Queued | batch_id:{batch_id}")


async def main_queue_retry(env, batch_id, payload):
    if not payload:
        raise PermanentError("Cannot retry empty payload")

    replay_count = payload.get("replay_count", 0) + 1

    if replay_count > MAX_DLQ_REPLAYS:
        raise PermanentError("Max retries exceeded")

    delay = min(60 * (2 ** (replay_count - 1)), 600)
    delay += random.randint(0, 10)

    queue_payload = JSON.parse(json.dumps(
        {**payload, "replay_count": replay_count, "is_replay": True}
    ))

    await env.POS_QUEUE.send(queue_payload, {"delaySeconds": delay})

    console.log(
        f"[RETRY] 🔁 batch_id:{batch_id} | replay:{replay_count} | delay:{delay}s"
    )


async def summary_queue_retry(env, batch_id, payload):
    if not payload:
        raise PermanentError("Cannot retry empty payload")

    replay_count = payload.get("replay_count", 0) + 1

    if replay_count > MAX_DLQ_REPLAYS:
        raise PermanentError("Max retries exceeded")

    delay = min(60 * (2 ** (replay_count - 1)), 600)
    delay += random.randint(0, 10)

    queue_payload = JSON.parse(json.dumps(
        {**payload, "replay_count": replay_count, "is_replay": True}
    ))

    await env.POS_SUMMARY_QUEUE.send(queue_payload, {"delaySeconds": delay})

    console.log(
        f"[RETRY] 🔁 batch_id:{batch_id} | replay:{replay_count} | delay:{delay}s"
    )


async def handle_permanent_failure(env, batch_id, payload, error, queue):
    year, month, day = get_date_paths()
    key = f"queue/{queue}/permanent_failure/{year}/{month}/{day}/{batch_id}.json"

    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    failure_data = {
        **(payload if isinstance(payload, dict) else {}),
        "timestamp": now,
        "error": str(error),
    }

    await move_to_permanent_failure(env, batch_id, failure_data, error, key)

    console.error(f"[{queue}][PERMANENT] ❌ Failed >>> batch_id: {batch_id} | error: {error}")
