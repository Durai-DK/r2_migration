import pandas as pd
import os, json, time
from decimal import Decimal
from datetime import datetime, date
from core.creds import logger, MysqlCatalog
from fastapi import APIRouter, HTTPException
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(prefix="/api/extract", tags=["Extract MySQL → JSON"])

MAX_WORKERS = 4
BASE_STORE_PATH = "/Users/DK/Desktop/Data_Backup/stored_data/Transaction_Mysql_Dump"

def convert_value(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.decode(errors="ignore")
    return v

def save_json_file(output_dir, rec):
    pri_id = rec.get("pri_id")
    if not pri_id:
        return False

    path = f"{output_dir}/{pri_id}.json"

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"Failed to write {path}: {e}")
        return False


@router.post("/range/json/")
def extract_mysql_to_json(start_range: int, end_range: int):
    if end_range <= start_range:
        raise HTTPException(400, "end_range must be greater than start_range")

    base_folder = f"{BASE_STORE_PATH}_{start_range}_{end_range}"
    os.makedirs(base_folder, exist_ok=True)

    mysql = MysqlCatalog()

    # Step 1: Fetch
    start = time.time()
    rows = mysql.get_range("Transaction", start_range, end_range)
    mysql_duration = round(time.time() - start, 2)

    if not rows:
        raise HTTPException(404, "No data found in range")

    # Step 2: Pandas DataFrame
    df = pd.DataFrame(rows)
    for col in df.columns:
        df[col] = df[col].apply(convert_value)

    #Step 3: Parallel JSON Writing
    save_start = time.time()
    success = 0

    record_list = df.to_dict(orient="records")
    for rec in record_list:
        if save_json_file(base_folder, rec):
            success += 1

    save_duration = round(time.time() - save_start, 2)
    total_duration = round(time.time() - start, 2)

    return {
        "message": "JSON extraction completed",
        "total_saved": success,
        "output_dir": base_folder,
        "mysql_duration_sec": mysql_duration,
        "file_write_duration_sec": save_duration,
        "total_duration_sec": total_duration
    }
