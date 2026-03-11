from fetch_records import *
from create_records import *
from js import Response, JSON, console
from token import generate_token, verify_static_token, verify_request_token


async def on_fetch(request, env):

    try:
        path = "/" + "/".join(request.url.split("/")[3:]).split("?")[0]
        if str(path).rstrip("/") == "/api/pos-bucket/get-token" and request.method == "GET":
            return await generate_token(request, env)

        if str(path).rstrip("/") == "/api/pos-bucket/records" and request.method == "POST":
            if not verify_static_token(request, env):
                console.warn(f"[CREATE] ❌ Unauthorized access attempt to {path}")
                return make_response({"error": "Unauthorized", "message": "Invalid or missing token"}, status=401)
            return await handle_post_records(request, env)

        elif str(path).rstrip("/") == "/api/pos-bucket/fetch-records" and request.method == "GET":
            if not verify_request_token(request, env):
                console.warn(f"[FETCH] ❌ Unauthorized access attempt to {path}")
                return make_response({"error": "Unauthorized", "message": "Invalid or missing token"}, status=401)
            return await handle_get_records(request, env)

        else:
            return make_response("Result Not Found", status=404)

    except Exception as e:
        console.error(f"[FETCH] ❌ Fatal error | {str(e)}")
        return make_response({"error": "Internal Server Error", "details": str(e)}, status=500)
