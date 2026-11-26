# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_oyster_path"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_load_date = ""
p_root_path = "Files/oyster/landing_raw"
p_in_table_name = "Account"
p_debug = 0

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Oyster Core raw to Delta
# Rebuild each configured table from all raw batches, selecting the latest row per business key.
# This is idempotent for repeated runs and does not lose older ledger transactions.
# Only the four core entities are accepted. Missing files or invalid CSV data fail the activity.

# CELL ********************

import csv
import re
from datetime import datetime
from io import StringIO

from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, desc, lit, row_number
from pyspark.sql.types import StringType, StructField, StructType

core_tables = {
    "Customer": ("CustomerID", ["CustomerID", "FirstName", "LastName"]),
    "Account": ("AccountID", ["AccountID", "CustomerID", "Balance"]),
    "DebitCard": ("CardID", ["CardID", "AccountID", "CardTier"]),
    "Transaction_Ledger": (
        "TransactionID", ["TransactionID", "AccountID", "Amount", "Timestamp"],
    ),
}
if p_in_table_name not in core_tables:
    raise ValueError(f"Unsupported core table: {p_in_table_name}")
if not p_root_path:
    raise ValueError("p_root_path is required.")
valid_date = datetime.strptime(p_load_date, "%Y%m%d") if p_load_date else datetime.now()
business_key, required_columns = core_tables[p_in_table_name]
pattern = f"{p_root_path.rstrip('/')}/**/{p_in_table_name}_*.csv"
source_files = spark.sparkContext.binaryFiles(pattern).collect()
if not source_files:
    raise FileNotFoundError(f"No raw files match {pattern}")

rows = []
for path, content in source_files:
    match = re.search(r"/(\d{8}_\d{6}|\d{14}|\d{8})/[^/]+$", path)
    if not match:
        raise ValueError(f"Missing source batch timestamp in {path}")
    timestamp_text = match.group(1)
    timestamp_format = (
        "%Y%m%d_%H%M%S" if "_" in timestamp_text
        else "%Y%m%d%H%M%S" if len(timestamp_text) == 14
        else "%Y%m%d"
    )
    source_timestamp = datetime.strptime(timestamp_text, timestamp_format).isoformat(sep=" ")
    encoding = "utf-16" if content.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    reader = csv.DictReader(StringIO(content.decode(encoding)), strict=True)
    if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError(f"Missing or duplicate CSV header in {path}")
    missing_columns = set(required_columns) - set(reader.fieldnames)
    if missing_columns:
        raise ValueError(f"Missing required columns {sorted(missing_columns)} in {path}")
    for line_number, row in enumerate(reader, start=2):
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"Invalid CSV column count in {path}, row {line_number}")
        if not row[business_key].strip():
            raise ValueError(f"Missing {business_key} in {path}, row {line_number}")
        rows.append(
            [row[name] for name in required_columns] + [source_timestamp, path, str(line_number)]
        )
if not rows:
    raise ValueError(f"No data rows found for {p_in_table_name}")

schema = StructType([
    StructField(name, StringType(), False)
    for name in required_columns + ["m_source_dttm", "m_source_file", "_source_row"]
])
raw = spark.createDataFrame(rows, schema)
raw = raw.withColumn("m_source_dttm", col("m_source_dttm").cast("timestamp"))
latest = Window.partitionBy(business_key).orderBy(
    desc("m_source_dttm"), desc("m_source_file"), col("_source_row").cast("long").desc(),
)
output = (
    raw.withColumn("_rn", row_number().over(latest))
    .filter(col("_rn") == 1)
    .drop("_rn", "_source_row")
    .withColumn("m_valid_dt", lit(valid_date.strftime("%Y-%m-%d")))
    .withColumn("m_company_cd", lit("oyster"))
    .withColumn("m_source_system_cd", lit("oys"))
    .withColumn("m_table_nm", lit(p_in_table_name))
    .withColumn("m_loaded_at_dttm", current_timestamp())
)
table_name = f"ld_oys_{p_in_table_name.lower()}"
if int(p_debug):
    display(output)
output.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(table_name)
print(f"Loaded {p_in_table_name} into {table_name}")

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
