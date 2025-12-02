import os, json, time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, HTTPException, Query

from core.creds import logger, r2_client

router = APIRouter(prefix="/api/transform", tags=["JSON → Parquet → R2"])


@router.post("/json-to-parquet/")
def convert_json_to_parquet_and_upload(
    input_dir: str = Query(..., description="Directory containing JSON files"),
    parquet_output: str = Query("output.parquet", description="Local parquet file name"),
    bucket_name: str = Query(...),
    object_key: str = Query(..., description="R2 key, example: parquet/data.parquet")
):
    if not os.path.isdir(input_dir):
        raise HTTPException(400, "Invalid input_dir, not a directory")

    start = time.time()

    # Step 1: Load all JSON files
    files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    if not files:
        raise HTTPException(404, "No JSON files found")

    all_rows = []
    for file in files:
        with open(os.path.join(input_dir, file), "r", encoding="utf-8") as f:
            all_rows.append(json.load(f))

    # Step 2: Convert → DataFrame
    df = pd.DataFrame(all_rows)

    # Step 3: Convert → Parquet locally
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_output)

    # Step 4: Upload to R2
    with open(parquet_output, "rb") as f:
        r2_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=f,
            ContentType="application/octet-stream"
        )

    return {
        "message": "JSON → Parquet → R2 upload completed",
        "json_files": len(files),
        "local_parquet": os.path.abspath(parquet_output),
        "r2_bucket": bucket_name,
        "r2_key": object_key,
        "duration_sec": round(time.time() - start, 2)
    }
