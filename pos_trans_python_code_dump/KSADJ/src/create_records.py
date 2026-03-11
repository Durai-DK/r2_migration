from handling_failure import *
import uuid, json, asyncio
from datetime import datetime, timezone
from js import console, fetch, Object, Date, JSON
from create_support import process_record, r2_bucket_push
from create_summary_and_transfer import rebuild_imei_summary
from helper import make_response, safe_to_py, store_json_data, deep_to_py, get_date_paths

TIMEOUT = 60
DLQ_CONCURRENCY = 3
MAIN_CONCURRENCY = 4
SUMMARY_CONCURRENCY = 5


async def handle_post_records(request, env):
    batch_id = str(uuid.uuid4())
    invoice = None
    try:
        try:
            raw_body = await request.json()
        except Exception:
            raise PermanentError("Invalid JSON")

        invoice = safe_to_py(raw_body)
        if not isinstance(invoice, dict):
            raise PermanentError("Invalid JSON payload")

        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

        invoice["batch_id"] = batch_id
        invoice["retry_count"] = 0
        invoice["dump_at"] = now

        year, month, day = get_date_paths()
        dump_meta = {
            "batch_id": str(batch_id), "status": "RECEIVED", "retry_count": 0, "timestamp": now
        }
        dump_key = f"dump/{year}/{month}/{day}/{batch_id}.json"

        await retry_operation(
            lambda: store_json_data(env, invoice, dump_key, dump_meta, bucket_binding="POS_BUCKET_PROD")
        )

        console.log(f"[Dump] ✅ Stored | batch_id:{batch_id}")

        queue_payload = JSON.parse(json.dumps({
            "batch_id": batch_id,
            "retry_count": 0,
            "invoice": invoice,
            "source": "unified-worker",
            "timestamp": now
        }))

        await retry_operation(lambda: env.POS_QUEUE.send(queue_payload))
        console.log(f"[DUMP] ✅ Invoice Queued | batch_id:{batch_id}")
        return make_response({
            "status": "success", "message": "Invoice Queued", "batch_id": batch_id}, status=202, request=request
        )

    except PermanentError as e:
        year, month, day = get_date_paths()
        p_fail_key = f"dump/permanent_failure/{year}/{month}/{day}/{batch_id}.json"
        await move_to_permanent_failure(env, batch_id, invoice or {}, str(e), p_fail_key)

        console.log(f"[DUMP] ❌ Permanent Failure >>> batch_id: {batch_id} | error: {str(e)}")
        return make_response({
            "status": "failed", "message": "Invalid Data", "batch_id": batch_id}, status=400, request=request
        )

    except Exception as e:
        await dump_retry(env, batch_id, invoice, str(e))
        console.log(f"[DUMP] ❌ Temporary Failure >>> batch_id: {batch_id} | error: {str(e)}")
        return make_response({
            "status": "failed", "message": "Temporary Failure", "batch_id": batch_id}, status=503, request=request
        )


# ---------- MAIN QUEUE ----------
async def handle_main_queue(batch, env):
    semaphore = asyncio.Semaphore(MAIN_CONCURRENCY)

    async def safe_process(message):
        async with semaphore:
            await process_main_message(message, env)

    await asyncio.gather(*[safe_process(m) for m in batch.messages], return_exceptions=True)


async def process_main_message(message, env):
    payload = None
    batch_id = "unknown"

    try:
        # ------------ Decode & Validate ------------
        payload = deep_to_py(message.body)
        if not isinstance(payload, dict):
            raise PermanentError("Invalid payload")

        batch_id = payload.get("batch_id", "unknown")

        # Replay guard FIRST
        if payload.get("replay_count", 0) >= MAX_DLQ_REPLAYS:
            raise PermanentError("Max retries exceeded")

        invoice = payload.get("invoice")
        if not invoice:
            raise PermanentError("Missing invoice")

        # ------------ Idempotency check ------------
        if await is_already_main_queue_processed(env, batch_id):
            console.warn(f"[MAIN_QUEUE] ⚠ Idempotent skip | {batch_id}")
            message.ack()
            return

        console.log(f"[MAIN_QUEUE] 🚀 Start | batch_id={batch_id}")

        # ------------ Business processing ------------
        async def main_logic():
            records = await process_record(invoice, env, batch_id)
            await r2_bucket_push(env, records)

        try:
            await asyncio.wait_for(main_logic(), timeout=TIMEOUT)
        except asyncio.TimeoutError:
            raise TemporaryError("Main processing timeout")

        # ------------ Mark success ------------
        try:
            await mark_main_queue_processed(env, batch_id)
        except Exception as e:
            console.warn(f"[MAIN][KV] ❌ Mark processed failed  >>> batch_id: {batch_id} | error: {str(e)}")

        console.log(f"[MAIN_QUEUE] ✅ Completed | batch_id={batch_id}")
        message.ack()

    # ---------------- ERROR HANDLING ----------------
    except PermanentError as e:
        await handle_permanent_failure(env, batch_id, payload, str(e), queue="main")
        message.ack()

    except TemporaryError as e:
        console.error(f"[MAIN_QUEUE] ⏳ Temporary | {batch_id} | {e}")
        try:
            await main_queue_retry(env, batch_id, payload)
            message.ack()
        except Exception as retry_error:
            await handle_permanent_failure(env, batch_id, payload, str(retry_error), queue="main")
            message.ack()

    except Exception as e:
        console.error(f"[MAIN_QUEUE] ❌ Unexpected error >>> batch_id: {batch_id} | error: {str(e)}")
        try:
            await main_queue_retry(env, batch_id, payload)
            message.ack()
        except Exception as retry_error:
            await handle_permanent_failure(env, batch_id, payload, str(retry_error), queue="main")
            raise retry_error


# ---------- SUMMARY QUEUE ----------
async def handle_summary_queue(batch, env):
    semaphore = asyncio.Semaphore(SUMMARY_CONCURRENCY)

    async def safe_process(message):
        async with semaphore:
            await process_summary_message(message, env)

    await asyncio.gather(*[safe_process(m) for m in batch.messages], return_exceptions=True)


async def process_summary_message(message, env):
    payload = None
    batch_id = "unknown"

    try:
        payload = message.body

        if not isinstance(payload, dict):
            raise PermanentError("Invalid summary payload")

        serial_number = payload.get("serial_number")
        replay_count = payload.get("replay_count", 0)

        batch_id = payload.get("batch_id")
        if not batch_id or batch_id == "unknown":
            console.log(f"[SUMMARY] ❌ IMEI Missing Batch Id | batch_id={batch_id} | serial_number={serial_number}")
            raise PermanentError("Missing batch_id in summary payload")

        # Replay guard FIRST
        if payload.get("replay_count", 0) >= MAX_DLQ_REPLAYS:
            raise PermanentError("Max retries exceeded")
        
        # ------------ Extract ------------
        console.log(f"[SUMMARY] 🚀 Start | batch_id={batch_id} | replay={replay_count}")

        # -------- Business processing --------
        try:
            await asyncio.wait_for(rebuild_imei_summary(env, batch_id, serial_number), timeout=TIMEOUT)
        except asyncio.TimeoutError:
            raise TemporaryError("Summary processing timeout")

        console.log(f"[SUMMARY] ✅ Completed | batch_id={batch_id}")
        message.ack()

    # ---------------- ERROR HANDLING ----------------
    except PermanentError as e:
        await handle_permanent_failure(env, batch_id, payload, str(e), queue="summary")
        message.ack()

    except TemporaryError as e:
        console.error(f"[SUMMARY] ⏳ Temporary | {batch_id} | {e}")
        try:
            await summary_queue_retry(env, batch_id, payload)
            message.ack()
        except Exception as retry_error:
            await handle_permanent_failure(env, batch_id, payload, str(retry_error), queue="summary")
            raise retry_error

    except Exception as e:
        console.error(f"[SUMMARY] ❌ Unexpected error >>> batch_id: {batch_id} | error: {str(e)}")
        try:
            await summary_queue_retry(env, batch_id, payload)
            message.ack()
        except Exception as retry_error:
            await handle_permanent_failure(env, batch_id, payload, str(retry_error), queue="summary")
            raise retry_error
