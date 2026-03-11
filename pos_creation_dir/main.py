from fastapi import FastAPI
from Transaction import pos_url
from middleware.datetie_region import DateHeaderMiddleware

app = FastAPI(title="Cloudflare R2 Migrate API", version="1.0.0")


#   ----- middleware -----
app.add_middleware(DateHeaderMiddleware)

app.include_router(pos_url.router)
