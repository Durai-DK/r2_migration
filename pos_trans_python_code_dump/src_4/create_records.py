from create_support import *
from handling_failure import *
from js import console, fetch, Object, Date, JSON


#--------------------------------------------------------------------
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

    await store_json_data(env, invoice, dump_key, dump_meta, bucket_binding="POS_BUCKET_LIVE")
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


async def process_single_message(message, env):
    try:
        payload = deep_to_py(message.body)
        wal_id = payload["wal_id"]
        invoice = payload["invoice"]

        await wal_update_state(env, wal_id, "VALIDATED")

        records = await process_record(invoice, env, wal_id)
        await wal_update_state(env, wal_id, "PROCESSED")

        await wal_update_state(env, wal_id, "PERSISTING")
        await r2_bucket_push(env, records)

        await wal_update_state(env, wal_id, "COMPLETED")
        message.ack()

    except Exception as e:
        if message.attempts >= MAX_QUEUE_ATTEMPTS:
            await send_to_dlq(env, message, e)
            await wal_update_state(env, wal_id, "FAILED", str(e))
            message.ack()
        else:
            message.retry()


async def send_to_dlq(env, message, error):
    await env.POS_DLQ.send({
        "original_message": message.body,
        "error": str(error),
        "attempts": message.attempts,
        "ts": time.time()
    })


async def repair_failed_puts(env, limit=50):
    listed = await env.POS_BUCKET_LIVE.list(prefix="failed_puts/")
    for obj in listed.objects[:limit]:
        data = json.loads(await env.POS_BUCKET_LIVE.get(obj.key).text())
        await r2_put_safe(
            env,
            data["bucket"],
            data["key"],
            data["payload"],
            data["meta"]
        )
        await env.POS_BUCKET_LIVE.delete(obj.key)

