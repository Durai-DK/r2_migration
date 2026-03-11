import json
from datetime import datetime, timedelta, UTC
from js import console
from helper import store_json_data


async def archive_yesterday_stats(env):
    # Today (UTC aware)
    now = datetime.now(UTC)

    # Yesterday
    yesterday = now - timedelta(days=1)

    date_str = yesterday.strftime("%Y-%m-%d")

    kv_key = f"stats_live:{date_str}"

    # Fetch from KV
    stats_raw = await env.POS_KV.get(kv_key)

    if not stats_raw:
        console.log(f"[ARCHIVE] No stats found for {date_str}")
        return

    stats_data = json.loads(stats_raw)

    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    r2_key = f"stats_archive/{year}/{month}/{day}.json"

    # Store in R2 permanently
    await store_json_data(
        env,
        stats_data,
        r2_key,
        metadata={"date": date_str},
        bucket_binding="POS_BUCKET_PROD"
    )

    console.log(f"[ARCHIVE] ✅ Archived stats for {date_str}")
