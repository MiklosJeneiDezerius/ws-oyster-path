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
p_batch_folder = ""   # "" = latest batch folder (manual runs only); pipelines always pass the generator's folder
p_entities = ""       # "" = all entities; or comma-separated, e.g. "Customer,Account"
p_debug = 0           # 1 = display a preview of each loaded table


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from datetime import datetime, timezone
from pyspark.sql import functions as F

# file prefix -> (target table, business key, required columns in source order)
ENTITIES = {
    "Customer": ("dbo.ld_oys_customer", "CustomerID",
                 ["CustomerID", "FirstName", "LastName"]),
    "CustomerAdress": ("dbo.ld_oys_customeraddress", "AddressID",
                       ["AddressID", "CustomerID", "AddressType", "Country",
                        "PostalCode", "City", "Street", "HouseNumber"]),
    "Account": ("dbo.ld_oys_account", "AccountID",
                ["AccountID", "CustomerID", "Balance"]),
    "DebitCard": ("dbo.ld_oys_debitcard", "CardID",
                  ["CardID", "AccountID", "CardTier", "IsActive"]),
    "Transaction_Ledger": ("dbo.ld_oys_transaction_ledger", "TransactionID",
                           ["TransactionID", "AccountID", "Amount", "Timestamp"]),
}

selected = [e.strip() for e in p_entities.split(",") if e.strip()] or list(ENTITIES)
unknown = sorted(set(selected) - set(ENTITIES))
if unknown:
    raise ValueError(f"Unknown entities: {unknown}. Allowed: {list(ENTITIES)}")

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
print(f"Batch {batch_folder} (of {len(batch_folders)}) | entities: {selected} | load_ts (UTC): {load_ts}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prepare(file_prefix):
    """Read one entity from the batch folder and validate it. Raises on any contract violation."""
    table_name, business_key, required_columns = ENTITIES[file_prefix]
    path = f"{p_root_path}/{batch_folder}/{file_prefix}_*.csv"

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")   # raw STRING columns; types are applied in Silver
        .load(path)
    )

    if not df.take(1):
        raise ValueError(f"{file_prefix}: source file contains no data rows in {path}")

    # 1) Header validation. Spark renames duplicate headers (e.g. CustomerID0), so duplicates fail here too.
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{file_prefix}: missing columns {missing}. Header was {df.columns}")
    extra = [c for c in df.columns if c not in required_columns]
    if extra:
        print(f"WARNING {file_prefix}: unexpected columns {extra} are kept (landing schema grows)")

    # 2) The business key must never be empty (the snapshot unique_key depends on it)
    empty_keys = df.filter(F.col(business_key).isNull() | (F.trim(F.col(business_key)) == "")).count()
    if empty_keys:
        raise ValueError(f"{file_prefix}: {empty_keys} row(s) with empty {business_key} in {path}")

    # 3) Required columns first, then any extras, then metadata
    out = df.select(
        *required_columns,
        *extra,
        F.lit(batch_ts).alias("meta_batch_timestamp"),
        F.lit(load_ts).alias("meta_load_timestamp"),
        F.col("_metadata.file_name").alias("meta_source_file"),
    )
    return table_name, out


# Phase 1: validate EVERY selected entity before writing anything (a bad batch changes nothing)
prepared = {e: prepare(e) for e in selected}

# Phase 2: write
for file_prefix, (table_name, out) in prepared.items():
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

# MAGIC %%sql
# MAGIC -- Expect one row per table, all with the SAME meta_batch_timestamp and meta_load_timestamp
# MAGIC SELECT 'customer' AS tbl, meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*) AS row_count
# MAGIC FROM dbo.ld_oys_customer GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
# MAGIC UNION ALL
# MAGIC SELECT 'customer_address', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
# MAGIC FROM dbo.ld_oys_customeraddress GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
# MAGIC UNION ALL
# MAGIC SELECT 'account', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
# MAGIC FROM dbo.ld_oys_account GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
# MAGIC UNION ALL
# MAGIC SELECT 'debitcard', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
# MAGIC FROM dbo.ld_oys_debitcard GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
# MAGIC UNION ALL
# MAGIC SELECT 'transaction_ledger', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
# MAGIC FROM dbo.ld_oys_transaction_ledger GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Machine-readable run summary for the pipeline (and the run history): which batch, how many rows per table.
# Must stay the LAST cell: notebook.exit stops execution.
summary = {
    "batch_folder": batch_folder,
    "batch_timestamp": batch_ts.isoformat(),
    "load_timestamp": load_ts.isoformat(),
    "row_counts": {e: spark.table(t).count() for e, (t, _) in prepared.items()},
}
print(json.dumps(summary, indent=2))
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
