# query_api.py (optional)
from fastapi import FastAPI, HTTPException
from decouple import config
from pyiceberg.catalog.rest import RestCatalog
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile
import os

app = FastAPI()
WAREHOUSE = config("WAREHOUSE")
CATALOG_URI = config("CATALOG_URI")
TOKEN = config("TOKEN")

NAMESPACE = config("ICEBERG_NAMESPACE", default="soc_media")
TABLE_NAME = config("ICEBERG_TABLE", default="customer_mobile_partitioned")

catalog = RestCatalog(name="r2", warehouse=WAREHOUSE, uri=CATALOG_URI, token=TOKEN)
table = catalog.load_table((NAMESPACE, TABLE_NAME))

@app.get("/by_mobile/{mobile}")
def get_by_mobile(mobile: str, limit: int = 100):
    """
    Simple approach:
    1. Use Iceberg table metadata to locate data files for partition mobilenumber=<mobile>.
    2. Read only those files (via HTTP/R2) and filter/limit.
    Note: the exact method to fetch file URLs depends on your storage/catalog setup.
    """
    # Step 1: find manifest / files that match partition value.
    try:
        # This API depends on pyiceberg internals (manifest/file scan). If not available in your version,
        # consider listing partition folder in R2 (e.g., "mobilenumber=<mobile>/" path) and reading parquet files directly.
        scan = table.new_scan().filter(f"mobilenumber = '{mobile}'").plan_files()
    except Exception:
        # Fallback: try to construct partition path on R2. This is example only and must be adapted.
        raise HTTPException(status_code=500, detail="Scan API not available in this pyiceberg version. Use direct parquet reads from the partition folder.")

    rows = []
    # The 'scan' object and its returned file entries vary by version. This block will likely need minor changes.
    for file_entry in scan:
        file_path = getattr(file_entry, "file_path", None) or getattr(file_entry, "path", None)
        if not file_path:
            continue
        # Read parquet file (can be HTTP(s) or r2:// depending on your storage)
        try:
            table_parquet = pq.read_table(file_path, columns=None)
            # filter by mobilenumber (should be already only that partition)
            # convert to pylist and extend
            rows.extend(table_parquet.to_pylist())
            if len(rows) >= limit:
                break
        except Exception as e:
            # Could be remote path access issue; log and continue
            print("Failed read:", file_path, e)
            continue

    return {"count": len(rows[:limit]), "rows": rows[:limit]}
