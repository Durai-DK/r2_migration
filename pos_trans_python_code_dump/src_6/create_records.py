from handling_failure import *
import uuid, json, asyncio
from datetime import datetime, timezone
from js import console, fetch, Object, Date, JSON
from create_support import process_record, r2_bucket_push
from helper import make_response, safe_to_py, store_json_data, deep_to_py, get_date_paths
from create_summary_and_transfer import rebuild_imei_summary, update_mobile_summary


MAIN_CONCURRENCY = 4
SUMMARY_CONCURRENCY = 3
DLQ_CONCURRENCY = 3

TIMEOUT = 60


async def handle_post_records(request, env):
    """
    Intake endpoint for POS records.
    Dumps raw payload to R2 and queues for processing.
    """
    try:
        raw_body = await request.json()
    except Exception:
        return make_response({"error": "Invalid JSON"}, status=400, request=request)

    invoice = safe_to_py(raw_body)

    if not isinstance(invoice, dict):
        return make_response({"error": "Invalid JSON payload"}, status=400, request=request)

    batch_id = str(uuid.uuid4())
    invoice["batch_id"] = batch_id

    # 2. Dump to R2 (Persistence)
    year, month, day = get_date_paths()
    dump_meta = {"batch_id": str(batch_id)}
    dump_key = f"dump/{year}/{month}/{day}/{batch_id}.json"

    try:
        await store_json_data(env, invoice, dump_key, dump_meta, bucket_binding="POS_BUCKET_PROD")
        console.log(f"[CREATE] 📦 Dumped intake payload → {dump_key}")
    except Exception as e:
        console.error(f"[CREATE] ❌ Failed to dump payload: {e}")

        invoice["error"] = str(e)
        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        error_meta = {"batch_id": str(batch_id), "timestamp": now, "error": str(e)}
        error_key = f"dump/queue_failed/{year}/{month}/{day}/{batch_id}.json"

        await store_json_data(env, invoice, error_key, error_meta, bucket_binding="POS_BUCKET_PROD")

    # 3. Queue for processing
    try:
        queue_payload = JSON.parse(json.dumps({
            "batch_id": batch_id,
            "invoice": invoice,
            "source": "unified-worker",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
        await env.POS_QUEUE.send(queue_payload)
        console.log(f"[CREATE] 🚀 Main Batch queued | batch_id={batch_id}")
    except Exception as e:
        console.error(f"[CREATE] ❌ Main Batch Queue Failed | batch_id={batch_id}: {e}")
        return make_response({
            "error": "Queue Failure", "message": "Failed to add record to processing queue"},
            status=503, request=request
        )

    return make_response({
        "status": "success", "message": "Invoice Queued", "batch_id": batch_id},
        status=202, request=request
    )


# ---------- MAIN QUEUE ----------
async def handle_main_queue(batch, env):
    semaphore = asyncio.Semaphore(MAIN_CONCURRENCY)

    async def safe_process(message):
        async with semaphore:
            await process_main_message(message, env)

    await asyncio.gather(
        *[safe_process(m) for m in batch.messages],
        return_exceptions=True
    )


async def process_main_message(message, env):
    payload = None
    batch_id = "unknown"

    try:
        # ------------ Decode & Validate ------------
        payload = deep_to_py(message.body)
        if not isinstance(payload, dict):
            raise FatalProcessingError("Invalid message payload")

        batch_id = payload.get("batch_id", "unknown")

        invoice = payload.get("invoice")
        if not invoice:
            raise FatalProcessingError("Missing invoice in payload")

        if payload.get("is_replay") and payload.get("replay_count", 0) >= MAX_DLQ_REPLAYS:
            console.error(f"[MAIN_QUEUE] 🚫 Poison replay blocked | {batch_id}")
            raise FatalProcessingError("Poison message detected")

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

        await asyncio.wait_for(main_logic(), timeout=TIMEOUT)

        # ------------ Mark success ------------
        # Mark success safely
        try:
            await mark_main_queue_processed(env, batch_id)
        except Exception as kv_err:
            console.error(f"[MAIN_QUEUE] KV mark failed | {kv_err}")
            raise kv_err

        console.log(f"[MAIN_QUEUE] ✅ Completed | batch_id={batch_id}")
        message.ack()

    # ------------ FATAL ERRORS ------------
    except FatalProcessingError as e:
        console.error(f"[MAIN_QUEUE][FATAL] {batch_id} | {e}")
        try:
            await failed_queue_store(env, payload, batch_id, e, status="MAIN_FATAL")
            await send_to_dlq(env, message, e, source_queue="POS_QUEUE")
        except Exception as dlq_err:
            console.error(f"[MAIN_QUEUE] DLQ failure | {dlq_err}")
        message.ack()

    # ------------ TIMEOUT HANDLING ------------
    except asyncio.TimeoutError:
        console.error(f"[MAIN_QUEUE][TIMEOUT] {batch_id}")
        await handle_retry_or_dlq(env, message, payload, batch_id, "MAIN_TIMEOUT", source_queue="POS_QUEUE")

    # ------------ TRANSIENT ERRORS ------------
    except Exception as e:
        console.error(f"[MAIN_QUEUE][ERROR] {batch_id} | {e}")
        await handle_retry_or_dlq(env, message, payload, batch_id, "MAIN_ERROR", e, source_queue="POS_QUEUE")


# ---------- SUMMARY QUEUE ----------
async def handle_summary_queue(batch, env):
    semaphore = asyncio.Semaphore(SUMMARY_CONCURRENCY)

    async def safe_process(message):
        async with semaphore:
            await process_summary_message(message, env)

    await asyncio.gather(
        *[safe_process(m) for m in batch.messages],
        return_exceptions=True
    )


async def process_summary_message(message, env):
    payload = None
    batch_id = "unknown"

    try:
        # ------------ Decode & Validate ------------
        payload = deep_to_py(message.body)
        if not isinstance(payload, dict):
            raise FatalProcessingError("Invalid summary payload")

        batch_id = payload.get("batch_id", "unknown")
        records = payload.get("records") or []
        serials = list(set(payload.get("serial_numbers") or []))

        mobile = payload.get("mobile")
        if not mobile:
            raise FatalProcessingError("Missing mobile in summary payload")

        if payload.get("is_replay") and payload.get("replay_count", 0) >= MAX_DLQ_REPLAYS:
            console.error(f"[SUMMARY_QUEUE] 🚫 Poison replay blocked | {batch_id}")
            raise FatalProcessingError("Poison message detected")

        # ------------ Idempotency check ------------
        if await is_already_summary_queue_processed(env, batch_id):
            console.warn(f"[SUMMARY_QUEUE] ⚠ Idempotent skip | {batch_id}")
            message.ack()
            return

        console.log(f"[SUMMARY_QUEUE] 🚀 Start | batch_id={batch_id} | mobile={mobile} | serials={len(serials)}")

        # ------------- Summary Logic -------------
        async def summary_logic():
            # Update Mobile Summary
            await update_mobile_summary(env, mobile, records)

            # Rebuild IMEI summaries
            if serials:
                imei_semaphore = asyncio.Semaphore(3)

                async def safe_rebuild(imei):
                    async with imei_semaphore:
                        try:
                            await rebuild_imei_summary(env, imei)
                        except Exception as imei_error:
                            console.error(f"[SUMMARY_QUEUE][IMEI_ERROR] {batch_id} | imei={imei} | {imei_error}")

                            data = {
                                    "batch_id": batch_id,
                                    "mobile": mobile,
                                    "imei": imei
                                }

                            await failed_queue_store(env, data, batch_id, imei_error, status="IMEI_REBUILD_FAILED")

                await asyncio.gather(*[safe_rebuild(i) for i in serials],return_exceptions=False)

        await asyncio.wait_for(summary_logic(), timeout=TIMEOUT)

        # ------------ Mark success ------------
        # Mark success safely
        try:
            await mark_summary_queue_processed(env, batch_id)
        except Exception as kv_err:
            console.error(f"[SUMMARY_QUEUE] KV mark failed | {kv_err}")
            raise kv_err

        console.log(f"[SUMMARY_QUEUE] ✅ Completed | batch_id={batch_id} | mobile={mobile} | serials={len(serials)}")
        message.ack()

    # ------------ FATAL ERRORS ------------
    except FatalProcessingError as e:
        console.error(f"[SUMMARY_QUEUE][FATAL] {batch_id} | {e}")
        try:
            await failed_queue_store(env, payload, batch_id, str(e), status="SUMMARY_FATAL")
            await send_to_dlq(env, message, str(e), source_queue="POS_SUMMARY_QUEUE")
        except Exception as dlq_err:
            console.error(f"[SUMMARY_QUEUE] DLQ failure | {dlq_err}")
        message.ack()

    # ------------ TIMEOUT HANDLING ------------
    except asyncio.TimeoutError:
        console.error(f"[SUMMARY_QUEUE][TIMEOUT] {batch_id}")
        await handle_retry_or_dlq(env, message, payload, batch_id, "SUMMARY_TIMEOUT", source_queue="POS_SUMMARY_QUEUE")

    # ---------------- TRANSIENT ERRORS ----------------
    except Exception as e:
        console.error(f"[SUMMARY_QUEUE][ERROR] {batch_id} | {e}")
        await handle_retry_or_dlq(env, message, payload, batch_id, "SUMMARY_ERROR", e, source_queue="POS_SUMMARY_QUEUE")


# ---------- DLQ Queue ----------
async def handle_dlq_queue(batch, env):
    semaphore = asyncio.Semaphore(DLQ_CONCURRENCY)

    async def safe_replay(message):
        async with semaphore:
            await process_dlq_message(message, env)

    await asyncio.gather(*[safe_replay(m) for m in batch.messages], return_exceptions=True)


async def process_dlq_message(message, env):
    batch_id = "unknown"
    try:
        payload = deep_to_py(message.body)

        batch_id = payload.get("batch_id", "unknown")
        replay_count = payload.get("replay_count", 0)

        if replay_count >= MAX_DLQ_REPLAYS:
            console.warn(f"[DLQ] ☠ Permanent failure | {batch_id}")
            await archive_poison_message(env, payload)
            message.ack()
            return

        original = payload.get("original_message")
        if not isinstance(original, dict):
            console.error(f"[DLQ] ❌ Invalid original message | {batch_id}")
            await archive_poison_message(env, payload)
            message.ack()
            return

        new_replay = replay_count + 1

        original["replay_count"] = new_replay
        original["is_replay"] = True

        delay = compute_backoff(new_replay)
        source_queue = payload.get("source_queue", "POS_QUEUE")

        console.log(
            f"[DLQ] 🔁 Replay | batch_id={batch_id} | count={new_replay} | delay={delay}s | target={source_queue}"
        )

        queue_binding = getattr(env, source_queue, env.POS_QUEUE)
        await queue_binding.send(JSON.parse(json.dumps(original)), delaySeconds=max(1, int(delay)))
        message.ack()

    except Exception as e:
        console.error(f"[DLQ Queue] ❌ Replay failed | batch_id={batch_id} | error={e}")
        message.retry()
