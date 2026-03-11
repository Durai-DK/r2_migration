from fastapi import FastAPI
from routers.crm_bucket import crm_url
from routers.Pos_Transaction import pos_url
from middleware.datetie_region import DateHeaderMiddleware

app = FastAPI(title="Cloudflare R2 Migrate API", version="1.0.0")

#   ----- middleware -----
app.add_middleware(DateHeaderMiddleware)

app.include_router(crm_url.router)
app.include_router(pos_url.router)

# app.include_router(namespace.router)
# app.include_router(extract_mysql.router)
# app.include_router(transaction_table.router)
# app.include_router(routing_file.router)
# app.include_router(urls.router)
