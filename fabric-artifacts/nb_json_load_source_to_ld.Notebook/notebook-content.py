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
p_batch_folder = ""   # "" = latest batch folder (yyyyMMddHHmmss names sort chronologically); or e.g. "20261118072024"
p_target_table = "dbo.ld_oys_transaction_ledger_json"   # set to dbo.ld_oys_transaction_ledger to replace the CSV path
p_debug = 0           # 1 = display a preview of the loaded table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Nested transaction JSON to Landing Delta
# The generator writes `Transaction_Ledger_<ts>.json` as JSON Lines: one document per account, with the
# transactions split into arrays of 20 (`Data.TransactionBatches`, an array of arrays).
# `flatten()` merges the inner arrays into one array per account and `explode()` emits one row per
# transaction. The result has the same columns as the CSV-loaded `ld_oys_transaction_ledger`.

# CELL ********************

import re
from datetime import datetime, timezone
from functools import reduce
from operator import or_
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

REQUIRED_COLUMNS = ["TransactionID", "AccountID", "Amount", "Timestamp"]
FILE_PREFIX = "Transaction_Ledger"

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
load_ts = datetime.now(timezone.utc).replace(tzinfo=None)
print(f"Batch {batch_folder} (of {len(batch_folders)}) | target: {p_target_table} | load_ts (UTC): {load_ts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explicit schema: no inference cost, no surprise types. Every leaf is STRING, so JSON numbers keep
# their raw text (as in the CSV landing); types are applied in Silver.
transaction_schema = StructType([StructField(c, StringType()) for c in REQUIRED_COLUMNS])
document_schema = StructType([
    StructField("SourceSystem", StringType()),
    StructField("IngestDate", StringType()),
    StructField("Data", StructType([
        StructField("AccountID", StringType()),
        StructField("TransactionBatches", ArrayType(ArrayType(transaction_schema))),
    ])),
])

path = f"{p_root_path}/{batch_folder}/{FILE_PREFIX}_*.json"
documents = (
    spark.read.schema(document_schema)
    .option("mode", "FAILFAST")        # a malformed JSON line fails the run instead of becoming a null row
    .json(path)
    .select("*", F.col("_metadata.file_name").alias("meta_source_file"))
)
if not documents.take(1):
    raise ValueError(f"{FILE_PREFIX}: no JSON documents in {path}")

# 1) Envelope validation: every document must carry an account and at least one non-empty batch
bad_envelopes = documents.filter(
    F.col("Data.AccountID").isNull()
    | F.col("Data.TransactionBatches").isNull()
    | (F.size("Data.TransactionBatches") == 0)
    | F.exists("Data.TransactionBatches", lambda b: b.isNull() | (F.size(b) == 0))
).count()
if bad_envelopes:
    raise ValueError(f"{FILE_PREFIX}: {bad_envelopes} document(s) without AccountID or with empty batches in {path}")

# 2) flatten: array<array<struct>> -> array<struct>; explode: one row per transaction
transactions = (
    documents
    .select(
        F.col("Data.AccountID").alias("envelope_account_id"),
        F.explode(F.flatten("Data.TransactionBatches")).alias("tx"),
        "meta_source_file",
    )
    .select("envelope_account_id", "tx.*", "meta_source_file")
)

# 3) Row validation: required fields present, transaction belongs to its envelope account, unique key
missing = transactions.filter(
    reduce(or_, [F.col(c).isNull() | (F.trim(F.col(c)) == "") for c in REQUIRED_COLUMNS])
).count()
if missing:
    raise ValueError(f"{FILE_PREFIX}: {missing} transaction(s) with an empty required field in {path}")

foreign = transactions.filter(F.col("AccountID") != F.col("envelope_account_id")).count()
if foreign:
    raise ValueError(f"{FILE_PREFIX}: {foreign} transaction(s) nested under a different AccountID in {path}")

duplicates = transactions.groupBy("TransactionID").count().filter("count > 1").count()
if duplicates:
    raise ValueError(f"{FILE_PREFIX}: {duplicates} duplicated TransactionID value(s) in {path}")

# 4) Same column contract as the CSV loader
out = transactions.select(
    *REQUIRED_COLUMNS,
    F.lit(batch_ts).alias("meta_batch_timestamp"),
    F.lit(load_ts).alias("meta_load_timestamp"),
    "meta_source_file",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write only after every check above passed (a bad file changes nothing)
(
    out.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(p_target_table)
)
print(f"Loaded {FILE_PREFIX} JSON -> {p_target_table}: {spark.table(p_target_table).count()} rows")
if int(p_debug):
    display(spark.table(p_target_table).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reconcile with the CSV-loaded ledger of the same batch (skipped when the JSON path replaces it)
csv_table = "dbo.ld_oys_transaction_ledger"
if p_target_table.lower() != csv_table:
    json_df = spark.table(p_target_table).select(*REQUIRED_COLUMNS, "meta_batch_timestamp")
    csv_df = spark.table(csv_table).select(*REQUIRED_COLUMNS, "meta_batch_timestamp")
    only_json = json_df.exceptAll(csv_df).count()
    only_csv = csv_df.exceptAll(json_df).count()
    print(f"JSON rows: {json_df.count()} | CSV rows: {csv_df.count()} | only in JSON: {only_json} | only in CSV: {only_csv}")
    if only_json or only_csv:
        print("WARNING: JSON and CSV landings differ (different batches loaded, or a source mismatch)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
