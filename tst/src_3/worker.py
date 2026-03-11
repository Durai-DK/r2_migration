from upload_tasks import *
from initial_process import *
from fetch_records import *
from js import Response, JSON, console

def safe_to_py(obj):
    return obj.to_py() if hasattr(obj, "to_py") else obj


def deep_to_py(obj):
    if hasattr(obj, "to_py"):
        obj = obj.to_py()

    if isinstance(obj, dict):
        return {k: deep_to_py(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [deep_to_py(i) for i in obj]

    return obj


def verify_static_token(request, env):
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return False
    
    # Split 'Bearer <token>' safely
    parts = auth_header.split(" ")
    if len(parts) != 2:
        return False
        
    scheme, token = parts
    if scheme.lower() != "bearer":
        return False

    expected_token = getattr(env, "STATIC_JWT_TOKEN", None)
    if not expected_token:
        console.error("[AUTH] ❌ STATIC_JWT_TOKEN not configured")
        return False

    return token == expected_token


async def on_fetch(request, env):
    try:
        path = "/" + "/".join(request.url.split("/")[3:]).split("?")[0]

        # 🔐 1. Static JWT Auth Check
        if not verify_static_token(request, env):
            console.warn(f"[AUTH] ❌ Unauthorized access attempt to {path}")
            return make_response(
                {"error": "Unauthorized", "message": "Invalid or missing token"},
                status=401
            )

        if str(path).rstrip("/") == "/api/pos-bucket/records" and request.method == "POST":
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
            console.log(f"[FETCH] 🚀 Batch queued | batch_id={batch_id}")
    
            return make_response({"status": "success", "message": "Invoice Queued", "batch_id": batch_id})

        elif str(path).rstrip("/") == "/api/pos-bucket/fetch-records" and request.method == "GET":
             return await handle_get_records(request, env)

        else:
            return make_response("Result Not Found", status=404)

    except Exception as e:
        console.error(f"[FETCH] ❌ Fatal error | {str(e)}")
        return make_response({"error": "Internal Server Error", "details": str(e)}, status=500)


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
