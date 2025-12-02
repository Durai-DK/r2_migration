import os, json, time
from datetime import datetime, date
from dateutil import parser
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    BooleanType,
    LongType,
    DoubleType,
    DateType,
    StringType,
)
from core.creds import cat_client
from pyiceberg.exceptions import NoSuchTableError
from fastapi import APIRouter, HTTPException, Query


router = APIRouter(prefix="/api/incert", tags=["incert"])

JSON_ROOT = "/Users/DK/Desktop/Data_Backup/stored_data/Transaction_Mysql_Dump_1_500000/"

def load_json_files(root: str, start: int, end: int):
    files = sorted([f for f in os.listdir(root) if f.endswith(".json")])

    if end > len(files):
        end = len(files)

    selected = files[start:end]

    rows = []
    for fname in selected:
        fpath = os.path.join(root, fname)
        try:
            with open(fpath, "r") as f:
                rows.append(json.load(f))
        except Exception:
            print(f"⚠️ Skipped invalid JSON: {fname}")
    return rows

def process_chunk(chunk, arrow_schema):
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}

        for field in arrow_schema:
            val = row.get(field.name, None)

            try:
                if val in ("", " ", None):
                    converted_row[field.name] = None
                    continue

                if pa.types.is_integer(field.type):
                    converted_row[field.name] = int(val)

                elif pa.types.is_floating(field.type):
                    converted_row[field.name] = float(val)

                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    parsed_date = None

                    if isinstance(val, (datetime, date)):
                        parsed_date = val
                    elif isinstance(val, str):
                        val = val.strip()
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(val, fmt)
                                break
                            except ValueError:
                                continue

                    if parsed_date:
                        converted_row[field.name] = (
                            parsed_date
                            if isinstance(parsed_date, datetime)
                            else datetime.combine(parsed_date, datetime.min.time())
                        )
                    else:
                        converted_row[field.name] = None

                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(f"❌ Row {row_idx}, Field '{field.name}', Value: {val}, Error: {e}")
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)


def infer_schema_from_record(record: dict):
    iceberg_fields = []
    arrow_fields = []

    field_overrides = {
        "pri_id": (LongType(), pa.int64(), True),
        "Invoice_Amount__c": (DoubleType(), pa.float64(), False),
        "Bill_Date__c": (DateType(), pa.date32(), False),
        "CreatedDate": (DateType(), pa.date32(), False),
    }

    for idx, (name, value) in enumerate(record.items(), start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()
            elif isinstance(value, int):
                ice_type, arrow_type = LongType(), pa.int64()
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()
            elif isinstance(value, (date, datetime)):
                ice_type, arrow_type = DateType(), pa.date32()
            else:
                ice_type, arrow_type = StringType(), pa.string()
            required = False

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema


@router.post("/data/insert")
def r2_catalog(start_range: int, end_range: int, chunk_size: int):
    total_start = time.time()

    namespace, table_name = "Pos_Transaction", "cus_mobile_partition"

    # ------------------------------
    json_start = time.time()
    print("json_start", json_start)
    rows = load_json_files(JSON_ROOT, start_range, end_range)

    if not rows:
        raise HTTPException(400, "No JSON loaded in given range")

    json_end = time.time()
    print("json_end", json_end)

    # ------------------------------
    schema_start = time.time()
    print("schema_start", schema_start)

    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])

    schema_end = time.time()
    print("schema_end", schema_end)

    # ------------------------------
    arrow_start = time.time()
    print("arrow_start", arrow_start)

    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    arrow_tables = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}

        for future in as_completed(futures):
            idx = futures[future]
            try:
                tbl = future.result()
                arrow_tables.append(tbl)
                print(f"Processed chunk {idx + 1}/{len(chunks)} rows={tbl.num_rows}")
            except Exception as e:
                print(f"Chunk {idx + 1} failed: {e}")
                raise HTTPException(500, f"Arrow conversion failed: {e}")

    arrow_end = time.time()
    print("arrow_end", arrow_end)

    # ------------------------------
    catalog = cat_client()
    table_identifier = f"{namespace}.{table_name}"

    catalog_start = time.time()
    print("catalog_start", catalog_start)
    try:
        tbl = catalog.load_table(table_identifier)
        catalog_end = time.time()
        print("catalog_end", catalog_end)
    except NoSuchTableError:
        raise HTTPException(404, f"Iceberg table not found: {table_identifier}")

    # ------------------------------
    append_start = time.time()
    print("append_start", append_start)
    try:
        for batch in arrow_tables:
            tbl.append(batch)
        append_end = time.time()
        print("append_end", append_end)
    except Exception as e:
        print(f"❌ error: {e}")
        raise HTTPException(500, f"Append failed: {e}")

    # ------------------------------
    return {
        "success": True,
        "rows_loaded": len(rows),
        "chunks": len(chunks),
        "execution_times": {
            "json_load": round(json_end - json_start, 2),
            "schema_infer": round(schema_end - schema_start, 2),
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append": round(append_end - append_start, 2),
        },
    }
