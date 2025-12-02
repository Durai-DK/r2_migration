import os
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.partitioning import PartitionSpec
# from pyiceberg.transforms import Identity

WAREHOUSE = os.getenv("WAREHOUSE")
CATALOG_URI = os.getenv("CATALOG_URI")
TOKEN = os.getenv("TOKEN")

catalog = RestCatalog(name="r2", warehouse=WAREHOUSE, uri=CATALOG_URI,token=TOKEN)

# -------------------------------------------------------------
# Step 1: Create Namespace
namespace = "soc_media"

# try:
#     catalog.create_namespace(namespace)
#     print(f"Namespace '{namespace}' created.")
# except NamespaceAlreadyExistsError:
#     print(f"Namespace '{namespace}' already exists.")

# -------------------------------------------------------------
# Step 2: Define Schema (PyArrow)
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("mobilenumber", pa.string()),
])

iceberg_schema = schema.to_pyiceberg()

# -------------------------------------------------------------
# Step 3: Partition by mobilenumber (Identity)
partition_spec = PartitionSpec(
    iceberg_schema,
    Identity("mobilenumber")
)

# -------------------------------------------------------------
# Step 4: Create Table
table_identifier = (namespace, "customer_mobile_partitioned")

catalog.create_table(
    identifier=table_identifier,
    schema=iceberg_schema,
    partition_spec=partition_spec,
)

print("✅ Iceberg table created with mobilenumber partitioning!")
