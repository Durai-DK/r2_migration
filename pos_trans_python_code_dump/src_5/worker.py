from fetch_records import *
from create_records import *
from js import Response, JSON, console
from api_token import generate_token, verify_static_token, verify_request_token


async def on_fetch(request, env):

    try:
        parsed = urlparse(request.url)
        path = parsed.path.rstrip("/")

        if request.method == "OPTIONS":
            return make_response(None, status=204, request=request)

        # ---------------   CREATE RECORDS   ---------------
        if path == "/api/pos-bucket/records" and request.method == "POST":
            auth = verify_static_token(request, env)

            if auth["status"] == "ok":
                return await handle_post_records(request, env)

            if auth["status"] == "unauthorized":
                console.warn(f"[CREATE] ❌ Unauthorized access attempt to {path}")
                return make_response(
                    {"error": "Unauthorized", "message": "Missing or invalid static token"},
                    status=401,
                    headers={"WWW-Authenticate": "Bearer"},
                    request=request
                )

            if auth["status"] == "forbidden":
                console.warn(f"[CREATE] ❌ Forbidden access attempt to {path}")
                return make_response(
                    {"error": "Forbidden", "message": "Static token validation failed"},
                    status=403,
                    request=request
                )

            # Fallback for any unknown auth status
            console.warn(f"[CREATE] ❌ Unknown auth status: {auth.get('status')}")
            return make_response(
                {"error": "Forbidden", "message": "Authentication failed"},
                status=403,
                request=request
            )

        # ---------------   FETCH RECORDS   ---------------
        if path == "/api/pos-bucket/fetch-records" and request.method == "GET":
            auth = verify_request_token(request, env)

            if auth["status"] == "unauthorized":
                console.warn(f"[FETCH] ❌ Unauthorized access attempt to {path}")
                return make_response(
                    {"error": "Unauthorized", "message": "Missing or invalid access token"},
                    status=401,
                    headers={"WWW-Authenticate": "Bearer"},
                    request=request
                )

            if auth["status"] == "forbidden":
                console.warn(f"[FETCH] ❌ Forbidden access attempt to {path}")
                return make_response(
                    {"error": "Forbidden", "message": "Token claims validation failed"},
                    status=403,
                    request=request
                )

            return await handle_get_records(request, env)

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
