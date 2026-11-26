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

# CELL ********************

# %%
p_load_date = '20260513'
p_source_system_cd = "SAP"
p_subfolder_path = "SAP"
p_in_table_name = "YY1_DWH_DUEDATE_API"
p_lakehouse_name = "lh_oyster_path"
p_debug = 0
p_root_path = "Files/landing_raw"

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime, timedelta

# %%
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") 
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") 
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") 
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# %%
#check for parameter date, if there is none use yesterday's date
if not p_load_date:
    yesterday = datetime.now() - timedelta(days=1)
    v_valid_dt = yesterday.strftime("%Y%m%d")
else:
    v_valid_dt = p_load_date

print(v_valid_dt)

# %%
#check for parameter date, if there is none use yesterday's date
if not p_debug:
    v_debug = 0
else:
    v_debug = p_debug

print('v_debug:', v_debug)

# %%
# Get default lakehouse abfs path
for mp in notebookutils.fs.mounts():
    if mp.mountPoint == '/default':
       abfs = mp.source

# %%
# Path to the directory containing Parquet files
if p_in_table_name == 'Company':
    v_parquet_path = abfs + "/"+p_root_path+"/"+p_subfolder_path+"/"+p_in_table_name+"/*/"+v_valid_dt
else:
    v_parquet_path = abfs + "/"+p_root_path+"/"+p_subfolder_path+"/"+p_in_table_name+"/*/*/"+v_valid_dt
display(v_parquet_path)

# %%
v_out_table_name = "ld_"+p_source_system_cd.lower()+"_"+p_in_table_name.lower()
display(v_out_table_name)

# %%
# Path to the Delta table
v_out_table_name_path = "Tables/"+v_out_table_name
display(v_out_table_name_path)

# %%
# Read the Parquet files recursively
parquet_df = spark.read.format("parquet").option("mergeSchema", "true").option("recursiveFileLookup", "true").option("ignoreCorruptFiles", "true").load(v_parquet_path)

# %%
# Create m_updated_at_dttm
parquet_df = parquet_df.withColumn("m_updated_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest"))

# %%
if v_debug:
    display(parquet_df)

# %%
# Cast all columns to string
parquet_string_df = parquet_df.select([col(c).cast("string").alias(c) for c in parquet_df.columns])
display(parquet_string_df)

# %%
# Write the Delta table
parquet_string_df.write.format("delta")\
                .option("mergeSchema", "true")\
                .partitionBy("m_valid_dt")\
                .option("partitionOverwriteMode", "dynamic")\
                .mode("overwrite")\
                .saveAsTable(v_out_table_name)

# %%
# Optimize table
#sql_query = f"OPTIMIZE {p_lakehouse_name}.{v_out_table_name}"

# Execute SQL query
#spark.sql(sql_query)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
