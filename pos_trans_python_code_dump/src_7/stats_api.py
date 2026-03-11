import json
from datetime import datetime, UTC
from helper import read_json_data, make_response


async def get_stats_api(env, date_param, request):
    if not date_param:
        # Default = today
        date_param = datetime.now(UTC).strftime("%Y-%m-%d")

    today = datetime.now(UTC).strftime("%Y-%m-%d")

    # 🔴 If today → read from KV
    if date_param == today:
        kv_key = f"stats_live:{date_param}"
        data = await env.POS_KV.get(kv_key)

        if not data:
            return make_response(
                {"date": date_param, "message": "No stats yet"},
                status=200,
                request=request
            )

        return make_response(json.loads(data), status=200, request=request)

    # 🟢 If past date → read from R2
    try:
        year, month, day = date_param.split("-")
        r2_key = f"stats_archive/{year}/{month}/{day}.json"

        data = await read_json_data(
            env,
            r2_key,
            bucket_binding="POS_BUCKET_PROD"
        )

        if not data:
            return make_response(
                {"date": date_param, "message": "No archived stats found"},
                status=404,
                request=request
            )

        return make_response(data, status=200, request=request)

    except Exception:
        return make_response(
            {"error": "Invalid date format. Use YYYY-MM-DD"},
            status=400,
            request=request
        )
