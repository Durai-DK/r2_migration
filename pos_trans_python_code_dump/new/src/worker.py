from helper import make_response
from js import Response, JSON, console
from urllib.parse import urlparse, parse_qs
from fetch_records import handle_get_records
from create_records import handle_post_records, handle_main_queue, handle_summary_queue
from api_token import generate_token, verify_static_token, verify_request_token
from workers import WorkerEntrypoint


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return await on_fetch(request, self.env)

    async def queue(self, batch):
        return await on_queue(batch, self.env)


async def on_fetch(request, env):
    try:
        parsed = urlparse(request.url)
        path = parsed.path.rstrip("/")

        if request.method == "OPTIONS":
            return make_response(None, status=204, request=request)

        # ---------------   CREATE RECORDS   ---------------
        if path == "/api/pos-bucket/records" and request.method == "POST":
            auth = verify_static_token(request, env)

            # Validate auth response structure
            if not auth or not isinstance(auth, dict):
                console.error(f"[CREATE] ❌ Invalid auth response: {auth}")
                return make_response(
                    {"error": "Internal Server Error", "message": "Authentication system error"},
                    status=500,
                    request=request
                )

            auth_status = auth.get("status")

            if auth_status == "ok":
                return await handle_post_records(request, env)

            if auth_status == "unauthorized":
                console.warn(f"[CREATE] ❌ Unauthorized access attempt to {path}")
                return make_response(
                    {"error": "Unauthorized", "message": "Missing or invalid static token"},
                    status=401,
                    headers={"WWW-Authenticate": "Bearer"},
                    request=request
                )

            if auth_status == "forbidden":
                console.warn(f"[CREATE] ❌ Forbidden access attempt to {path}")
                return make_response(
                    {"error": "Forbidden", "message": "Static token validation failed"},
                    status=403,
                    request=request
                )

            # Fallback for any unknown auth status
            console.warn(f"[CREATE] ❌ Unknown auth status: {auth_status}")
            return make_response(
                {"error": "Forbidden", "message": "Authentication failed"},
                status=403,
                request=request
            )

        # ---------------   FETCH RECORDS   ---------------
        if path == "/api/pos-bucket/fetch-records" and request.method == "GET":
            auth = verify_request_token(request, env)

            # Validate auth response structure
            if not auth or not isinstance(auth, dict):
                console.error(f"[FETCH] ❌ Invalid auth response: {auth}")
                return make_response(
                    {"error": "Internal Server Error", "message": "Authentication system error"},
                    status=500,
                    request=request
                )

            auth_status = auth.get("status")

            if auth_status == "ok":
                return await handle_get_records(request, env)

            if auth_status == "unauthorized":
                console.warn(f"[FETCH] ❌ Unauthorized access attempt to {path}")
                return make_response(
                    {"error": "Unauthorized", "message": "Missing or invalid access token"},
                    status=401,
                    headers={"WWW-Authenticate": "Bearer"},
                    request=request
                )

            if auth_status == "forbidden":
                console.warn(f"[FETCH] ❌ Forbidden access attempt to {path}")
                return make_response(
                    {"error": "Forbidden", "message": "Token claims validation failed"},
                    status=403,
                    request=request
                )

            # Fallback for any unknown auth status
            console.warn(f"[FETCH] ❌ Unknown auth status: {auth_status}")
            return make_response(
                {"error": "Forbidden", "message": "Authentication failed"},
                status=403,
                request=request
            )

        # ---------------   GET TOKEN   ---------------
        if path == "/api/pos-bucket/get-token" and request.method == "GET":
            params = parse_qs(parsed.query)
            token_id = params.get("token_id", [None])[0]
            
            console.log(f"[TOKEN] 🔍 Request for token | token_id={token_id}")

            if not token_id:
                console.warn(f"[TOKEN] ❌ Unauthorized access attempt to {path}")
                return make_response({"error": "missing_token_id", "message": "The required query parameter 'token_id' is missing."}, status=400, request=request)

            if token_id != env.TOKEN_ID:
                console.warn(f"[TOKEN] ❌ Unauthorized access attempt to {path}")
                return make_response({"error": "invalid_token_id", "message": "The provided token_id is invalid."}, status=401, request=request)
            return await generate_token(request, env, token_id)

        # ---------------   NOT FOUND   ---------------
        return make_response("Result Not Found", status=404, request=request)

    except Exception as e:
        console.error(f"[FETCH] ❌ Fatal error | {str(e)}")
        return make_response({"error": "Internal Server Error", "details": str(e)}, status=500, request=request)


async def on_queue(batch, env):
    """
    Mandatory Cloudflare queue handler.
    Routes messages based on queue name.
    """

    queue_name = batch.queue

    if queue_name == "pos-transaction-python-queue":
        await handle_main_queue(batch, env)

    elif queue_name == "pos-transaction-python-summary":
        await handle_summary_queue(batch, env)

    else:
        console.error(f"Unknown queue received: {queue_name}")