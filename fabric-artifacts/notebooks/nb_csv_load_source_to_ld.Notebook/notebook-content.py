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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import re
from pyspark.sql import functions as F

# Each generator run writes a new batch folder: Files/landing/oyster/<yyyyMMddHHmmss UTC>/
p_root_path = "Files/landing/oyster"

# Pick ONLY the latest batch folder (14-digit names sort chronologically)
batch_folders = [
    f.name.rstrip("/")
    for f in notebookutils.fs.ls(p_root_path)
    if f.isDir and re.fullmatch(r"\d{14}", f.name.rstrip("/"))
]
if not batch_folders:
    raise FileNotFoundError(f"No batch folders found under {p_root_path}")
latest_folder = max(batch_folders)
print(f"Latest batch folder: {latest_folder} (out of {len(batch_folders)})")

# This targets the 'Customer' CSV file in the latest batch folder only
source_path = f"{p_root_path}/{latest_folder}/Customer_*.csv"

# Define the target managed Delta table name
target_table = "dbo.ld_oys_customer"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all matching CSV files across the date folders
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

# Add metadata columns for load time and source file name
df_with_meta = (
    df.withColumn("meta_load_timestamp", F.current_timestamp())
      .withColumn("meta_source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
)

# Optional: Preview the data and metadata columns
display(df_with_meta.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#If you want to repeat this exact process for Account, DebitCard, and Transaction_Ledger in a single script, wrap the logic in a loop:
entities = {
    "Account": "dbo.ld_oys_account",
    "Customer": "dbo.ld_oys_customer",
    "CustomerAdress": "dbo.ld_oys_customeraddress",
    "DebitCard": "dbo.ld_oys_debitcard",
    "Transaction_Ledger": "dbo.ld_oys_transaction_ledger"
}

for file_prefix, table_name in entities.items():
    path = f"{p_root_path}/{latest_folder}/{file_prefix}_*.csv"
    
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
        .withColumn("meta_load_timestamp", F.current_timestamp())
        .withColumn("meta_source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
    )
    
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Loaded {file_prefix} into {table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Write the DataFrame to a managed Delta table
(
    df_with_meta.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully loaded data into {target_table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     meta_load_timestamp,
# MAGIC     meta_source_file,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM dbo.ld_oys_customer
# MAGIC GROUP BY meta_load_timestamp, meta_source_file
# MAGIC ORDER BY meta_source_file DESC;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
