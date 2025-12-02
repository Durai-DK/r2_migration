from starlette.middleware.base import BaseHTTPMiddleware
from datetime import datetime
import pytz

class DateHeaderMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request, call_next):

        response = await call_next(request)
        now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
        response.headers["Local-Date-Time"] = now_ist.strftime("%a, %d %b %Y %H:%M:%S %Z")
        return response
