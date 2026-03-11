from create_support import *
from handling_failure import *
from js import console, fetch, Object, Date, JSON


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


#--------------------------------------------------------------------
MAX_CONCURRENT_TASKS = 5


async def on_queue(batch, env):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async def safe_process(message):
        async with semaphore:
            await process_single_message(message, env)

    await asyncio.gather(*(safe_process(m) for m in batch.messages))


async def send_to_dlq(env, message, error):
    await env.POS_DLQ.send({
        "original_message": message.body,
        "error": str(error),
        "attempts": message.attempts,
        "ts": time.time()
    })


async def process_single_message(message, env):
    try:
        payload = deep_to_py(message.body)

        batch_id = payload["batch_id"]
        invoice = payload["invoice"]

        console.log(f"[QUEUE] 🚀 Processing batch | batch_id={batch_id}")

        # Business processing
        records = await process_record(invoice, env, batch_id)
        await r2_bucket_push(env, records)

        message.ack()
        console.log(f"[QUEUE] ✅ Completed {batch_id}")

    except Exception as e:
        if message.attempts > 3:
            payload = deep_to_py(message.body)
            batch_id = payload["batch_id"]

            await failed_queue_store(env, payload, batch_id, e)
            await send_to_dlq(env, message, e)

            message.ack()
        else:
            message.retry()
