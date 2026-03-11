from helper import *
from js import Response, JSON, console
import time, json, hashlib, uuid, hmac, base64

TOKEN_TTL = 86400000  # 24 hours in milliseconds
ISSUER = "pos-bucket-api"
AUDIENCE = "pos-bucket-client"


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


async def generate_token(request, env, token_id, token_type="access", ttl=TOKEN_TTL):
    console.log("[AUTH] 🔐 Token generation request received")

    secret = env.JWT_SECRET.encode()

    header = {
        "alg": "HS256",
        "typ": "JWT"
    }

    now = int(time.time() * 1000)

    payload = {
                "iss": ISSUER,
                "aud": AUDIENCE,
                "iat": now,
                "exp": now + ttl,
                "jti": str(uuid.uuid4()),  # unique token id
                "type": token_type,
                "token_id": token_id
    }

    header_b64 = base64url(json.dumps(header).encode())
    payload_b64 = base64url(json.dumps(payload).encode())

    signing_input = f"{header_b64}.{payload_b64}".encode()

    signature = hmac.new(secret, signing_input, hashlib.sha256).digest()

    signature_b64 = base64url(signature)

    token = f"{header_b64}.{payload_b64}.{signature_b64}"

    console.log(f"[AUTH] ✅ Access token issued")

    return make_response({
        "status": "success", "token_type": "Bearer",
        "access_token": token, "expires_in": TOKEN_TTL}, status=200, request=request
    )


def base64url_decode(data):
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def verify_request_token(request, env):
    try:
        auth_header = (request.headers.get("Authorization") or request.headers.get("authorization"))

        if not auth_header or not auth_header.startswith("Bearer "):
            console.warn("[AUTH] ❌ Missing Authorization header")
            return {"status": "unauthorized"}

        # Split 'Bearer <token>' safely
        token = auth_header.split(" ")[1]
        parts = token.split(".")

        if len(parts) != 3:
            console.warn("[AUTH] ❌ Invalid JWT structure")
            return {"status": "unauthorized"}

        header_b64, payload_b64, signature_b64 = parts

        # 🔐 Recreate signature
        signing_input = f"{header_b64}.{payload_b64}".encode()
        secret = env.JWT_SECRET.encode()

        expected_sig = hmac.new(secret, signing_input, hashlib.sha256).digest()
        if not hmac.compare_digest(base64url(expected_sig), signature_b64):
            console.warn("[AUTH] ❌ JWT signature mismatch")
            return {"status": "unauthorized"}

        # 📦 Decode payload
        payload = json.loads(base64url_decode(payload_b64))
        now = int(time.time() * 1000)

        # ⏳ Expiry check
        if payload.get("exp", 0) < now:
            console.warn("[AUTH] ❌ Token expired")
            return {"status": "unauthorized"}

        # 🧾 Standard claim checks
        if payload.get("iss") != ISSUER:
            console.warn("[AUTH] ❌ Invalid issuer")
            return {"status": "forbidden"}

        if payload.get("aud") != AUDIENCE:
            console.warn("[AUTH] ❌ Invalid audience")
            return {"status": "forbidden"}

        if payload.get("type") != "access":
            console.warn("[AUTH] ❌ Invalid token type")
            return {"status": "forbidden"}

        if payload.get("token_id") != env.TOKEN_ID:
            console.warn(f"[AUTH] ❌ token_id mismatch | jwt={payload.get('token_id')}")
            return {"status": "forbidden"}

        console.log("[AUTH] ✅ Token verified successfully")
        return {"status": "ok", "payload": payload}

    except Exception as e:
        console.error(f"[AUTH] ❌ Token verification failed: {e}")
        return {"status": "unauthorized"}


def verify_static_token(request, env):
    try:
        auth_header = (request.headers.get("Authorization") or request.headers.get("authorization"))

        if not auth_header:
            console.warn("[AUTH] ❌ Missing or malformed Authorization header")
            return {"status": "unauthorized"}

        # Split 'Bearer <token>' safely
        parts = auth_header.split(" ", 1)
        if len(parts) != 2:
            console.warn("[AUTH] ❌ Invalid JWT structure")
            return {"status": "unauthorized"}

        scheme, token = parts
        if scheme.lower() != "bearer":
            return {"status": "unauthorized"}

        expected_token = getattr(env, "STATIC_JWT_TOKEN", None)
        if not expected_token:
            console.error("[AUTH] ❌ STATIC_JWT_TOKEN not configured")
            return {"status": "forbidden"}

        if token != expected_token:
            console.warn("[STATIC_AUTH] ❌ Invalid static token")
            return {"status": "forbidden"}

        console.log("[STATIC_AUTH] ✅ Static token verified")
        return {"status": "ok"}

    except Exception as e:
        console.error(f"[STATIC_AUTH] ❌ Verification failed: {e}")
        return {"status": "unauthorized"}
