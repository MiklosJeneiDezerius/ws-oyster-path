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

# PARAMETERS CELL ********************

# PARAMETERS CELL ********************

p_source_sytem_cd = "OYS"
p_subfolder_path = "oyster"
p_in_table_name = "Account_20261102122351_core.csv"
p_lakehouse_name = "lh_oyster_path"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Create a SparkSession
spark = SparkSession.builder.appName("MergeCSVToDelta").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Path to the directory containing Parquet files
v_parquet_path = "Files/landing/"+p_subfolder_path+"/"+p_in_table_name
display(v_parquet_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Generate a clean folder name without the .csv extension
v_out_table_name = "ld_" + p_source_sytem_cd.lower() + "_" + p_in_table_name.lower().replace(".csv", "")
display(v_out_table_name)
# Result: 'ld_oys_transaction_ledger'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Path inside the Tables directory structure
v_out_table_name_path = "Tables/oyster/" + v_out_table_name
display(v_out_table_name_path)
# Result: 'Tables/oyster/ld_oys_transaction_ledger'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Read the Parquet files recursively
parquet_df = spark.read.format("csv").option("header", "true").option("mergeSchema", "true").option("recursiveFileLookup", "true").option("ignoreCorruptFiles", "true").load(v_parquet_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta_df = parquet_df.withColumn("file_path", input_file_name())

# Step 3: Extract the subfolder names
# Assuming the folder structure is /Files/landing/NYT/ny_taxi_green_demo_client/puYears=YYYY/puMonths=MM/...
#year_df = meta_df.withColumn("m_year_num", regexp_extract("file_path", "puYear=([^/]+)", 1))
#month_df = year_df.withColumn("m_month_num", regexp_extract("file_path", "puMonth=([^/]+)", 1))
#year_month_df = month_df.withColumn("m_year_month_num", concat(month_df.m_year_num, lit('-'), lpad(month_df.m_month_num, 2, '0')))
#valid_dt_df = year_month_df.withColumn("m_valid_dt", to_date(year_month_df.lpepPickupDatetime))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Create m_updated_at_dttm
updated_df = meta_df.withColumn("m_updated_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output_df = updated_df.drop('file_path')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(output_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

# Write the Delta table
output_df.write.format("delta")\
                .option("overwriteSchema", "true")\
                .mode("overwrite")\
                .save(v_out_table_name_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
