async def update_daily_stats(env, batch_id, event_type):
    import json
    from datetime import datetime, UTC

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    stats_key = f"stats_live:{today}"
    guard_key = f"stats_guard:{today}:{event_type}:{batch_id}"

    # Try to create guard with TTL first
    try:
        await env.POS_KV.put(guard_key, "1", expirationTtl=86400)
    except Exception:
        return  # If put fails, skip

    # Read current stats
    stats_raw = await env.POS_KV.get(stats_key)

    data = json.loads(stats_raw) if stats_raw else {
        "date": today,
        "invoice_received": 0,
        "dump_stored": 0,
        "dump_queued": 0,
        "main_queue_received": 0,
        "main_queue_completed": 0,
        "main_queue_temporary_failed": 0,
        "main_queue_permanent_failed": 0,
        "summary_queue_received": 0,
        "summary_queue_completed": 0,
        "summary_queue_temp_failed": 0,
        "summary_queue_permanent_failed": 0
    }

    if event_type in data:
        data[event_type] += 1

    await env.POS_KV.put(stats_key, json.dumps(data), expirationTtl=172800)
