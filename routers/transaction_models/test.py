import time, json, os, math
from typing import Any, List
from datetime import datetime, date
from fastapi import APIRouter,HTTPException, Query
from core.creds import logger, r2_client, MysqlCatalog
from concurrent.futures import ThreadPoolExecutor, as_completed


router = APIRouter(prefix="/api/bucket", tags=["Store Data to Bucket "])

def make_json_data(record: dict[str, Any]):
    from decimal import Decimal

    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            record[k] = v.isoformat()
        elif isinstance(v, Decimal):
            record[k] = float(v)
        elif isinstance(v, bytes):
            record[k] = v.decode(errors="ignore")
    return record


@router.post("/id/")
def create_bucket(start_range, end_range):
    if end_range <= start_range:
        raise HTTPException(status_code=400, detail="end_range must be greater than start_range")

    total_start = time.time()
    mysql_creds = MysqlCatalog()
    mysql_db_name = "Transaction"

    try:
        mysql_start = time.time()
        rows = mysql_creds.get_range(mysql_db_name, start_range, end_range)
        mysql_duration = round(time.time() - mysql_start, 2)
        if not rows:
            logger.error(f"❌ No data found in the given range.")
            raise HTTPException(status_code=404, detail="No data found in the given range.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {e}")

    def process_record(record: dict[str, Any]):
        pri_id = record.get("pri_id")
        if not pri_id:
            return {"status": "error", "error": "Missing pri_id"}

        try:
            record_safe = make_json_data(record)
            key = f"id/{pri_id}.json"
            return {"status": "ok"}

        except Exception as e:
            return {"status": "error", "error": str(e)}