# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

# Parameters (pipelines override these; in the Fabric UI this shows as the "Parameters" cell).
p_root_path = "Files/landing/oyster"
p_batch_folder = ""   # "" = latest batch folder (yyyyMMddHHmmss names sort chronologically); or e.g. "20261117080003" to reload a specific batch
p_in_table_name = ""  # CSV file prefix, e.g. "Customer"
p_debug = 0           # 1 = display a preview of the loaded table


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Oyster CSV to Landing Delta
# Load one CSV entity into dbo.ld_oys_<lowercase source name>.
# The target table is overwritten; this notebook does not perform an incremental merge.
# Business-key and required-column validation is not performed.

# CELL ********************

import re
from datetime import datetime, timezone
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_prefix = p_in_table_name.strip()
if not re.fullmatch(r"[A-Za-z0-9_]+", file_prefix):
    raise ValueError("p_in_table_name must be a non-empty CSV file prefix containing only letters, digits and underscores")
table_name = f"dbo.ld_oys_{file_prefix.lower()}"

# Batch folders are named yyyyMMddHHmmss (UTC) by the generator; 14-digit names sort chronologically
batch_folders = [
    f.name.rstrip("/")
    for f in notebookutils.fs.ls(p_root_path)
    if f.isDir and re.fullmatch(r"\d{14}", f.name.rstrip("/"))
]
if not batch_folders:
    raise FileNotFoundError(f"No batch folders found under {p_root_path}")
if p_batch_folder and p_batch_folder not in batch_folders:
    raise FileNotFoundError(f"Batch folder {p_batch_folder} not found under {p_root_path}")
batch_folder = p_batch_folder or max(batch_folders)

batch_ts = datetime.strptime(batch_folder, "%Y%m%d%H%M%S")    # when the source extracted the data
load_ts = datetime.now(timezone.utc).replace(tzinfo=None)     # one load time for every table in this run
print(f"Batch {batch_folder} (of {len(batch_folders)}) | source: {file_prefix} | target: {table_name} | load_ts (UTC): {load_ts}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = f"{p_root_path}/{batch_folder}/{file_prefix}_*.csv"
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")   # raw STRING columns; types are applied in Silver
    .load(path)
)
if not df.take(1):
    raise ValueError(f"{file_prefix}: source file contains no data rows in {path}")

metadata_columns = {"meta_batch_timestamp", "meta_load_timestamp", "meta_source_file"}
collisions = [c for c in df.columns if c.lower() in metadata_columns]
if collisions:
    raise ValueError(f"{file_prefix}: source columns conflict with metadata columns: {collisions}")

out = (
    df.withColumn("meta_batch_timestamp", F.lit(batch_ts))
    .withColumn("meta_load_timestamp", F.lit(load_ts))
    .withColumn("meta_source_file", F.col("_metadata.file_name"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    out.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)
print(f"Loaded {file_prefix} -> {table_name}")
if int(p_debug):
    display(spark.table(table_name).limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Other ForEach iterations can be in flight; only read the target written by this invocation.
summary = (
    spark.table(table_name)
    .groupBy("meta_batch_timestamp", "meta_load_timestamp", "meta_source_file")
    .agg(F.count("*").alias("row_count"))
    .collect()
)
if not summary:
    raise ValueError(f"{table_name}: no rows after loading batch {batch_folder}")
for row in summary:
    if row.meta_batch_timestamp != batch_ts or row.meta_load_timestamp != load_ts:
        raise ValueError(
            f"{table_name}: expected batch {batch_ts} and load time {load_ts}; "
            f"found batch {row.meta_batch_timestamp} and load time {row.meta_load_timestamp}"
        )
    print(
        f"Verified {table_name}: batch={row.meta_batch_timestamp}, "
        f"load={row.meta_load_timestamp}, file={row.meta_source_file}, rows={row.row_count}"
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
