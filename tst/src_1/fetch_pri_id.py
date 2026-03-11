import json
from key_mapping import get_now
from js import console, fetch, Object, Date

async def fetch_and_update_pri_id(env, count):
    key = "count/old_pri_id_1.json"
    max_retries = 3

    for attempt in range(max_retries):
        try:
            bucket = getattr(env, "POS_BUCKET")
            obj = await bucket.get(key)

            etag = obj.etag
            data = json.loads(await obj.text())
            old_id = int(data.get("pri_id"))
            start_pri_id = old_id + 1
            end_pri_id = start_pri_id + count - 1

            body = json.dumps({
                "pri_id": end_pri_id, "updated_at": get_now().isoformat()
            })

            metadata = Object.fromEntries(
                [["pri_id", str(end_pri_id)], ["updated_at", get_now().isoformat()]]
            )

            http_meta = Object.fromEntries(
                [["contentType", "application/json"]]
            )

            await bucket.put(key,
                             body,
                             only_if={"etagMatches": etag},
                             customMetadata=metadata,
                             httpMetadata=http_meta
            )
            return start_pri_id, end_pri_id

        except Exception as e:
            console.error(f"[FETCH] ❌ Error in fetch_and_update_pri_id: {e}")
            if attempt == max_retries - 1:
                raise

    raise RuntimeError("High concurrency conflict while generating PRI_ID")
