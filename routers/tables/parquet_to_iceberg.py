# parquet_to_iceberg.py
import os
from decouple import config
from pyiceberg.catalog.rest import RestCatalog
import pyarrow.dataset as ds
import pyarrow as pa
from tqdm import tqdm

PARQUET_DIR = config("PARQUET_OUT_DIR", default="parquet_out")
BATCH_SIZE_ROWS = int(config("ICEBERG_BATCH_ROWS", default=200_000))  # rows per append operation

WAREHOUSE = config("WAREHOUSE")
CATALOG_URI = config("CATALOG_URI")
TOKEN = config("TOKEN")

NAMESPACE = config("ICEBERG_NAMESPACE", default="soc_media")
TABLE_NAME = config("ICEBERG_TABLE", default="customer_mobile_partitioned")

def list_parquet_files(parquet_dir):
    files = []
    for fn in sorted(os.listdir(parquet_dir)):
        if fn.endswith(".parquet"):
            files.append(os.path.join(parquet_dir, fn))
    return files

def append_parquet_file_to_iceberg(table, parquet_file_path):
    # We will stream parquet file in batches of BATCH_SIZE_ROWS rows to the Iceberg table
    dataset = ds.dataset(parquet_file_path, format="parquet")
    # iterate over batches
    for batch in dataset.to_batches(batch_size=BATCH_SIZE_ROWS):
        batch_table = pa.Table.from_batches([batch])
        # Using pyiceberg table append API. Many pyiceberg versions allow:
        # writer = table.new_append(); with writer as w: w.add_data(batch_table)
        # If your version requires writer.write_table(...) or different API, adapt accordingly.
        appender = table.new_append()
        try:
            # Many pyiceberg builds include an add_data() convenience to write a pyarrow table.
            with appender as w:
                w.add_data(batch_table)   # writes file(s) into partition(s)
        except AttributeError:
            # Fallback - older/newer pyiceberg versions might expect a different call.
            # You might need to use low-level writer provided by pyiceberg.io.pyarrow
            # OR persist the batch_table to a local parquet tmp file and call appender.add_file(parquet_path)
            # We'll implement fallback by writing a small temp parquet then calling add_file if available.
            import tempfile
            import pyarrow.parquet as pq
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpf:
                tmp_path = tmpf.name
            pq.write_table(batch_table, tmp_path, compression="snappy")
            # Try to add_file
            try:
                with table.new_append() as w2:
                    w2.add_file(tmp_path)
            except Exception as e2:
                print("Fallback add_file failed:", e2)
                raise
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

def main():
    catalog = RestCatalog(name="r2", warehouse=WAREHOUSE, uri=CATALOG_URI, token=TOKEN)
    table = catalog.load_table((NAMESPACE, TABLE_NAME))

    files = list_parquet_files(PARQUET_DIR)
    print(f"Found {len(files)} parquet files to append.")
    for p in tqdm(files, desc="parquet -> iceberg"):
        append_parquet_file_to_iceberg(table, p)

    print("✅ All parquet files appended to Iceberg!")

if __name__ == "__main__":
    main()
