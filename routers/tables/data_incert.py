import time
from core.creds import MysqlCatalog
from fastapi import APIRouter,HTTPException
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.types import *
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError

router = APIRouter(prefix="/api/table", tags=["Data Incerts"])

@router.post("/data/insert")
def r2_catalog(start_range: int, end_range: int, chunk_size: int):
    total_start = time.time()
    # namespace, table_name = "pos_transactions_with_out", "iceberg_out_partitioning"
    namespace, table_name = "Pos_Transaction", "cus_mobile_partition"
    dbname = "Transaction"
    mysql_creds = MysqlCatalog()

    # -------------------------------------------------
    # Step 1: Fetch and Convert MySQL Data
    # -------------------------------------------------
    mysql_start = time.time()
    try:
        rows = mysql_creds.get_range_ph_bi(dbname, start_range, end_range)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        converted_rows = []


        for row in rows:
            # 1️⃣ Convert float fields safely
            float_fields = ["bill_tax__c", "bill_grand_total__c", "Invoice_Amount__c"]
            for f in float_fields:
                val = row.get(f)
                if isinstance(val, str):
                    try:
                        row[f] = float(val)
                    except ValueError:
                        row[f] = 0.0
                elif val is None:
                    row[f] = 0.0

            # Convert mobile numbers to int64
            mobile_val = row.get("customer_mobile__c")
            if isinstance(mobile_val, str):
                try:
                    row["customer_mobile__c"] = int(mobile_val)
                except ValueError:
                    row["customer_mobile__c"] = None

            # Convert Item_Code__c to int64
            item_val = row.get("Item_Code__c")
            if isinstance(item_val, str):
                try:
                    row["Item_Code__c"] = int(item_val)
                except ValueError:
                    row["Item_Code__c"] = 0

            # Convert date strings to Python `date` object (yyyy-mm-dd only)
            for date_field in ["Bill_Date__c",  "CreatedDate"]:
                val = row.get(date_field)

                if not val or str(val).strip() == "":
                    row[date_field] = None
                    continue

                try:
                    # use auto parser
                    dt = parser.parse(str(val))  # can parse both '6/24/2021 0:00' and '2021-06-24 00:00:00'
                    row[date_field] = dt
                except Exception as e:
                    print(f"⚠️ Error converting {date_field}: {val} ({e})")
                    row[date_field] = None

            converted_rows.append(row)



        mysql_end = time.time()
        print(f"MySQL fetch completed in {mysql_end - mysql_start:.2f} sec ({len(rows)} rows).")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    # -------------------------------------------------
    # Step 2: Infer Iceberg + Arrow Schema
    # -------------------------------------------------
    schema_start = time.time()
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    # print("iceberg_schema",iceberg_schema)
    # print("arrow_schema",arrow_schema)

    schema_end = time.time()
    print(f"Schema inference completed in {schema_end - schema_start:.2f} sec")

    # -------------------------------------------------
    # Step 3: Convert Rows to Arrow Tables (Multithreaded)
    # -------------------------------------------------
    arrow_start = time.time()
    chunks = [converted_rows[i:i + chunk_size] for i in range(0, len(converted_rows), chunk_size)]

    # print("chunks",chunks)
    arrow_tables = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_chunk, chunk, arrow_schema): idx for idx, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                tbl = future.result()
                arrow_tables.append(tbl)
                print(f"Chunk {idx + 1}/{len(chunks)} processed with {tbl.num_rows} rows")
            except Exception as e:
                print(f"Chunk {idx + 1} failed: {e}")
                raise HTTPException(status_code=500, detail=f"Arrow chunk conversion failed: {e}")


    arrow_end = time.time()
    print(f"Arrow conversion completed in {arrow_end - arrow_start:.2f} sec")

    # -------------------------------------------------
    # Step 4: Load Iceberg Table
    # -------------------------------------------------
    catalog_start = time.time()
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    # print(f"catalog table_identifier: {table_identifier}")
    try:
        tbl = catalog.load_table(table_identifier)
        catalog_end = time.time()
        print(f"Catalog load completed in {catalog_end - catalog_start:.2f} sec")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_identifier}")


    append_start = time.time()
    try:

        for i, batch in enumerate(arrow_tables, start=1):
            print(f"Appending batch {i}/{len(arrow_tables)} rows={batch.num_rows}")
            tbl.append(batch)  # commit each


        append_end = time.time()

    except Exception as e:
        error_message = str(e)
        error_code = "ICEBERG_APPEND_FAILED"
        print(f"❌ {error_code}: {error_message}")

        raise HTTPException(
            status_code=500,
            detail={
                "error_code": error_code,
                "message": f"Data append failed for table {table_identifier}",
                "exception": error_message,
            },
        )

    print(f"✅ Append completed in {append_end - append_start:.2f} sec")

    # -------------------------------------------------
    # Step 6: Return Response
    # -------------------------------------------------
    return {
        "success": True,
        "message": "Data appended successfully with multithreading",
        "rows_fetched": len(rows),
        "chunks": len(chunks),
        "execution_times": {
            "mysql_fetch": round(mysql_end - mysql_start, 2),
            "schema_infer": round(schema_end - schema_start, 2),
            "arrow_convert": round(arrow_end - arrow_start, 2),
            "catalog_load": round(catalog_end - catalog_start, 2),
            "append_refresh": round(append_end - append_start, 2),
            # "total_time": round(total_end - total_start, 2),
        },
    }
