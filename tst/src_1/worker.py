from fetch_pri_id import fetch_and_update_pri_id
from js import Response, JSON, console
from initial_process import *
from upload_tasks import *
import json

MAX_CONCURRENT_TASKS = 3


def safe_to_py(obj):
    return obj.to_py() if hasattr(obj, "to_py") else obj


def make_response(body, status=200, headers=None):
    """
    Helper to create a JS Response with the correct status code.
    Ensures compatibility between Python dictionaries and JS objects.
    """
    if headers is None:
        headers = {}

    if isinstance(body, (dict, list)):
        body = json.dumps(body)
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
            
    # We must use JSON.parse to create a 'real' JS object for the init options
    # otherwise the status code is ignored or causes a crash.
    init = JSON.parse(json.dumps({
        "status": status,
        "headers": headers
    }))
    return Response.new(body, init)


def verify_static_token(request, env):
    """
    Verifies the Authorization header against a static token.
    Expects format: Authorization: Bearer <static_token>
    """
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

    # Security: hmac is better if available, but simple equality works too
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

        if path != "/api/pos-bucket/records":
            return make_response("Not Found", status=404)

        if request.method != "POST":
            return make_response("Method Not Allowed", status=405)

        raw_body = await request.json()
        invoice = safe_to_py(raw_body)

        if not isinstance(invoice, dict):
            return make_response({"error": "Invalid JSON payload"}, status=400)

        output_items = invoice.get("output", [])
        if not isinstance(output_items, list):
            output_items = [{}]

        # 🔢 Calculate PRI count
        total_sn_count = 0
        for item in output_items:
            sn_raw = str(item.get("item_remarks1") or "")
            sn_list = [s.strip() for s in sn_raw.split(",") if s.strip()]
            total_sn_count += max(len(sn_list), 1)

        item_count = max(total_sn_count, 1)

        # 🔐 Reserve PRI range
        start_id, end_id = await fetch_and_update_pri_id(env, item_count)
        console.log(f"[FETCH] ✅ Pri_id records fetched | Range: {start_id} → {end_id} | Count: {item_count}")

        # 📦 Dump raw invoice payload (POS_BUCKET)
        year, month, day = get_date_paths()

        dump_key = f"dump/{year}/{month}/{day}/{start_id}.json"
        dump_meta = {
            "pri-id-start": str(start_id),
            "pri-id-end": str(end_id),
            "source": "pos-intake",
        }

        try:
            await store_json_data(env, invoice, dump_key, dump_meta, bucket_binding="POS_BUCKET")
            console.log(f"[FETCH] 📦 Dumped intake payload → {dump_key}")
        except Exception as dump_err:
            console.error(f"[FETCH] ❌ Failed to dump intake payload: {dump_err}")
            raise dump_err

        # 🚀 Send to queue
        try:
            queue_payload = JSON.parse(json.dumps({
                "invoice": invoice,
                "start_id": start_id,
                "end_id": end_id,
                "source": "unified-worker"
            }))

            await env.POS_QUEUE.send(queue_payload)
            console.log(f"[FETCH] 🚀 Invoice Send to queue")
        except Exception as q_err:
            console.error(f"[FETCH] ❌ Failed to send to queue: {q_err}")
            raise q_err

        result = {
            "status": "success",
            "message": "POS record accepted. PRI_ID generated and queued.",
            "start_id": start_id,
            "end_id": end_id,
        }

        return make_response(result)

    except Exception as e:
        console.error(f"[FETCH] ❌ Error in on_fetch: {str(e)}")
        return make_response({"error": str(e)}, status=500)


async def on_queue(batch, env):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async def safe_process(message):
        async with semaphore:
            await process_single_message(message, env)

    tasks = [safe_process(msg) for msg in batch.messages]
    await asyncio.gather(*tasks)


def deep_to_py(obj):
    if hasattr(obj, "to_py"):
        obj = obj.to_py()
    if isinstance(obj, dict):
        return {k: deep_to_py(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_to_py(i) for i in obj]
    return obj


async def process_single_message(message, env):
    try:
        payload = deep_to_py(message.body)

        invoice = payload['invoice']
        start_id = payload["start_id"]
        end_id = payload["end_id"]

        console.log(f"[QUEUE] 🚀 Processing Pri_id range {start_id} → {end_id}")

        # 2️⃣ Business processing
        records = await process_record(invoice, env, start_id, end_id)
        await r2_bucket_push(env, records)

        message.ack()
        console.log(f"[QUEUE] ✅ Done {start_id} → {end_id}")
        
    except Exception as e:
        console.error(f"[QUEUE] ❌ Failed {message.id}: {e}")

        if message.attempts > 3:
            console.error("[QUEUE] 🛑 Dropping poison message")
            message.ack()
        else:
            message.retry()
