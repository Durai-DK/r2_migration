from fastapi import FastAPI
from routers.transaction_post import urls
from routers.r2_bucket import routing_file
from middleware.datetie_region import DateHeaderMiddleware

app = FastAPI(title="Cloudflare R2 Migrate API", version="1.0.0")

#   ----- middleware -----
app.add_middleware(DateHeaderMiddleware)

# app.include_router(namespace.router)
# app.include_router(extract_mysql.router)
# app.include_router(transaction_table.router)
app.include_router(routing_file.router)
app.include_router(urls.router)
