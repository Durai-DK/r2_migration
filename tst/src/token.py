from helper import *
from js import Response, JSON, console
import time, json, hashlib, uuid, hmac, base64

TOKEN_TTL = 3600  # 1 hour
ISSUER = "pos-bucket-api"
AUDIENCE = "pos-bucket-client"


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


async def generate_token(request, env):
    console.log("[AUTH] 🔐 Token generation request received")

    secret = env.JWT_SECRET.encode()

    header = {"alg": "HS256",
              "typ": "JWT"
    }

    payload = {
                "iss": ISSUER,
                "aud": AUDIENCE,
                "iat": int(time.time()),
                "exp": int(time.time()) + TOKEN_TTL,
                "jti": str(uuid.uuid4()),  # unique token id
                "type": "access"
    }

    header_b64 = base64url(json.dumps(header).encode())
    payload_b64 = base64url(json.dumps(payload).encode())

    signing_input = f"{header_b64}.{payload_b64}".encode()

    signature = hmac.new(secret, signing_input, hashlib.sha256).digest()

    signature_b64 = base64url(signature)

    token = f"{header_b64}.{payload_b64}.{signature_b64}"

    console.log(f"[AUTH] ✅ Access token issued")

    return make_response({
            "status": "success", "token_type": "Bearer", "access_token": token, "expires_in": TOKEN_TTL}, status=200
    )


def base64url_decode(data):
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def verify_request_token(request, env) -> bool:
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            console.warn("[AUTH] ❌ Missing or malformed Authorization header")
            return False

        # Split 'Bearer <token>' safely
        token = auth_header.split(" ")[1]

        parts = token.split(".")

        if len(parts) != 3:
            console.warn("[AUTH] ❌ Invalid JWT format")
            return False

        header_b64, payload_b64, signature_b64 = parts

        # 🔐 Recreate signature
        signing_input = f"{header_b64}.{payload_b64}".encode()
        secret = env.JWT_SECRET.encode()

        expected_sig = hmac.new(secret, signing_input, hashlib.sha256).digest()

        expected_sig_b64 = base64url(expected_sig)

        if not hmac.compare_digest(expected_sig_b64, signature_b64):
            console.warn("[AUTH] ❌ JWT signature mismatch")
            return False

        # 📦 Decode payload
        payload_json = base64url_decode(payload_b64)
        payload = json.loads(payload_json)

        now = int(time.time())

        # ⏳ Expiry check
        if payload.get("exp", 0) < now:
            console.warn("[AUTH] ❌ Token expired")
            return False

        # 🧾 Standard claim checks
        if payload.get("iss") != ISSUER:
            console.warn("[AUTH] ❌ Invalid issuer")
            return False

        if payload.get("aud") != AUDIENCE:
            console.warn("[AUTH] ❌ Invalid audience")
            return False

        if payload.get("type") != "access":
            console.warn("[AUTH] ❌ Invalid token type")
            return False

        console.log("[AUTH] ✅ Token verified.")
        return True

    except Exception as e:
        console.error(f"[AUTH] ❌ Token verification failed: {e}")
        return False


def verify_static_token(request, env):
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        console.warn("[AUTH] ❌ Missing or malformed Authorization header")
        return False

    # Split 'Bearer <token>' safely
    parts = auth_header.split(" ")
    if len(parts) != 2:
        return False

    scheme, token = parts
    if scheme.lower() != "bearer":
        return False

    expected_token = getattr(env, "STATIC_JWT_TOKEN", None)
    if not expected_token:
        console.error("[AUTH] ❌ STATIC_JWT_TOKEN not configured")
        return False

    return token == expected_token
