from create_support import *
from handling_failure import *
from js import console, fetch, Object, Date, JSON


MAX_RETRIES = 3
MAX_CONCURRENT_TASKS = 5


async def handle_post_records(request, env):
    raw_body = await request.json()
    invoice = safe_to_py(raw_body)

    if not isinstance(invoice, dict):
        return make_response({"error": "Invalid JSON payload"}, status=400, request=request)

    batch_id = str(uuid.uuid4())
    invoice["batch_id"] = batch_id

    year, month, day = get_date_paths()
    dump_meta = {"batch_id": str(batch_id)}
    dump_key = f"dump/{year}/{month}/{day}/{batch_id}.json"

    await store_json_data(env, invoice, dump_key, dump_meta, bucket_binding="POS_BUCKET_PROD")
    console.log(f"[CREATE] 📦 Dumped intake payload → {dump_key}")

    # 🚀 Send batch to queue
    queue_payload = JSON.parse(json.dumps({
        "batch_id": batch_id,
        "invoice": invoice,
        "source": "unified-worker"})
    )

    await env.POS_QUEUE.send(queue_payload)

    console.log(f"[CREATE] 🚀 Batch queued | batch_id={batch_id}")

    return make_response({"status": "success", "message": "Invoice Queued", "batch_id": batch_id}, request=request)


async def on_queue(batch, env):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async def safe_process(message):
        async with semaphore:
            await process_single_message(message, env)

    await asyncio.gather(*(safe_process(m) for m in batch.messages))


async def send_to_dlq(env, message, error):
    # Safe conversion: Ensure we aren't sending a JS Proxy Object to the queue directly
    # 'deep_to_py' recursively converts JS objects to Python dicts/lists
    original_body = deep_to_py(message.body)

    dlq_payload = {
        "batch_id": original_body["batch_id"],
        "original_message": original_body,
        "error": str(error),
        "attempts": message.attempts,
        "ts": time.time()
    }

    # Bridge: Convert Python dict -> JSON String -> JS Object
    # This ensures the Queue.send() method receives exactly what it expects (a JS Object)
    # without any Python-specific types getting lost in translation.
    js_payload = JSON.parse(json.dumps(dlq_payload))
    
    await env.POS_DLQ.send(js_payload)


def compute_backoff(attempts):
    base = 5  # seconds
    max_delay = 300  # 5 minutes max

    delay = min(base * (2 ** (attempts - 1)), max_delay)
    return delay


async def is_already_processed(env, batch_id):
    return await env.POS_KV.get(f"processed:{batch_id}")


async def mark_processed(env, batch_id):
    await env.POS_KV.put(f"processed:{batch_id}", "1", expirationTtl=86400)


async def process_single_message(message, env):
    payload = None
    batch_id = "unknown"

    try:
        payload = deep_to_py(message.body)
        batch_id = payload.get("batch_id", "unknown")

        console.log(f"[QUEUE] 🚀 Start {batch_id}")

        # Idempotency check
        if await is_already_processed(env, batch_id):
            console.warn(f"[IDEMPOTENT] Skip {batch_id}")
            message.ack()
            return

        invoice = payload["invoice"]

        # Business processing
        records = await process_record(invoice, env, batch_id)
        await r2_bucket_push(env, records)

        await mark_processed(env, batch_id)

        message.ack()
        console.log(f"[QUEUE] ✅ Completed {batch_id}")

    except FatalProcessingError as e:
        console.error(f"[POISON] {batch_id}: {str(e)}")
        await failed_queue_store(env, payload, batch_id, e)
        await send_to_dlq(env, message, e)
        message.ack()

    except Exception as e:
        if message.attempts >= MAX_RETRIES:
            await failed_queue_store(env, payload, batch_id, e)
            await send_to_dlq(env, message, e)
            console.error(f"[DLQ] ❌ Sending batch {batch_id} to DLQ after {message.attempts} attempts | Error: {str(e)}")
            message.ack()
        else:
            delay = compute_backoff(message.attempts)
            console.warn(f"[RETRY] Attempt {message.attempts} | Delay {delay}s")
            message.retry(delaySeconds=delay)
