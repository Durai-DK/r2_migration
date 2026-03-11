from create_support import *
from js import console, fetch, Object, Date, JSON


#--------------------------------------------------------------------
async def handle_post_records(request, env):
    raw_body = await request.json()

    invoice = safe_to_py(raw_body)

    if not isinstance(invoice, dict):
        return make_response({"error": "Invalid JSON payload"}, status=400)

    batch_id = str(uuid.uuid4())
    invoice["batch_id"] = batch_id

    # 🚀 Send batch to queue
    queue_payload = JSON.parse(json.dumps({
        "batch_id": batch_id,
        "invoice": invoice,
        "source": "unified-worker"})
    )

    await env.POS_QUEUE.send(queue_payload)

    console.log(f"[CREATE] 🚀 Batch queued | batch_id={batch_id}")

    return make_response({"status": "success", "message": "Invoice Queued", "batch_id": batch_id})


#--------------------------------------------------------------------
MAX_CONCURRENT_TASKS = 3


async def on_queue(batch, env):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async def safe_process(message):
        async with semaphore:
            await process_single_message(message, env)

    await asyncio.gather(*(safe_process(m) for m in batch.messages))


async def process_single_message(message, env):
    try:
        payload = deep_to_py(message.body)

        batch_id = payload["batch_id"]
        invoice = payload['invoice']

        console.log(f"[QUEUE] 🚀 Processing batch | batch_id={batch_id}")

        # 1 Dump invoice (Background)
        year, month, day = get_date_paths()
        dump_key = f"dump/{year}/{month}/{day}/{batch_id}.json"
        dump_meta = {"batch_id": str(batch_id)}

        await store_json_data(env, invoice, dump_key, dump_meta, bucket_binding="POS_BUCKET")
        console.log(f"[FETCH] 📦 Dumped intake payload → {dump_key}")

        # 2 Business processing
        records = await process_record(invoice, env, batch_id)
        await r2_bucket_push(env, records)

        message.ack()
        console.log(f"[QUEUE] ✅ Completed {batch_id}")

    except Exception as e:
        console.error(f"[QUEUE] ❌ Failed {message.id}: {e}")

        if message.attempts > 3:
            console.error("[QUEUE] 🛑 Dropping poison message")
            message.ack()
        else:
            message.retry()
#--------------------------------------------------------------------