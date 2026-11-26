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
p_load_date = "20260513"
p_root_path = "Files/landing_raw"
p_source_route_path = None
p_country_cd = "DE"
p_company_cd = "089 Immobilienmanagement GmbH"
p_source_system_cd = "DATEV"
p_subfolder_path = "Datev"
p_in_table_name = "MandantenDaten"
p_datev_filter = "Konto"
p_debug = 0
p_lakehouse_name = "lh_oyster_path"

# %%
from pyspark.sql import SparkSession, Row
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, current_timestamp, from_utc_timestamp, row_number, desc
from pyspark.sql import Window
import re
import csv
from io import StringIO

# %%
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") 
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") 
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") 
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# %%
# Check for parameter date; if not provided, use yesterday's date
if not p_load_date:
    yesterday = datetime.now() - timedelta(days=1)
    v_valid_dt = yesterday.strftime("%Y%m%d")
else:
    v_valid_dt = p_load_date
 
print('v_valid_dt:', v_valid_dt)

# %%
# Convert load date to formatted date string (YYYY-MM-DD)
v_m_valid_dt = datetime.strptime(v_valid_dt, "%Y%m%d").strftime("%Y-%m-%d")
 
print('v_m_valid_dt:', v_m_valid_dt)

# %%
if not p_root_path:
    v_root_path = "Files/landing_raw"
else:
    v_root_path = p_root_path

print('v_root_path:', v_root_path)

# %%
if not p_debug:
    v_debug = 0
else:
    v_debug = p_debug

print('v_debug: ', v_debug)

# %%
if p_lakehouse_name:
    v_lakehouse_name = p_lakehouse_name
else:
    # Retrieve the default Lakehouse name
    v_lakehouse_name = spark.conf.get("trident.lakehouse.name", "No default Lakehouse set")

if v_lakehouse_name == "No default Lakehouse set":
    raise ValueError("No default Lakehouse is set. Please specify a Lakehouse name.")

print(f"v_lakehouse_name:", v_lakehouse_name)

# %%
# Build source filepath; destination folder is cleared by the copy job's preceding Delete activity
if not p_company_cd:
    raise ValueError("p_company_cd parameter is required but not set.")

# MandantenDaten esetén csak az aktuális év almappáját olvassuk, a többi táblánál az összeset
if p_in_table_name == "MandantenDaten":
    filepath = f"Files/landing_raw/Datev/original/{p_company_cd}/current/{datetime.now().year}"
else:
    filepath = f"Files/landing_raw/Datev/original/{p_company_cd}/current"

print('filepath:', filepath)

# %%
# Create a SparkSession
spark = SparkSession.builder.appName("MergeParquetByPostingDateToDelta").getOrCreate()

# %%
# We assume we don't have errors
v_error = 0

# %%
def sanitize_column_name(name):
    """
    Sanitize a column name for use in a Spark DataFrame.
    - Replace German umlauts with ASCII equivalents
    - Replace all remaining special characters with underscores
    - Collapse multiple consecutive underscores into one
    - Strip leading and trailing underscores
    """
    umlaut_map = str.maketrans('äöüÄÖÜß', 'aouAOUs')
    name = name.translate(umlaut_map)
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name

# %%
def read_datev_csv_multi(file_path_pattern, table_name):
    """
    Reads Datev CSV files from year subfolders under /current/.

    For most tables, all year subfolders are read recursively and deduplicated:
    the Delete activity in the pipeline clears only the current year subfolder before
    copying, so older year subfolders (e.g. 2025) may still contain files. Rows from
    older years are only kept if they are not already present in the latest year's data.

    For "MandantenDaten", file_path_pattern already points directly at the current year
    folder (e.g. .../current/2026), so files are read non-recursively from that single
    folder and the year-based deduplication is effectively a no-op.

    Files are UTF-16 LE with BOM (ff fe), semicolon-delimited, Windows line endings (CRLF).
    All files are collected to the driver and parsed in Python to avoid Spark RDD
    string encoding issues with flatMap and toDF().
    Lines are split on \\n and \\r is stripped explicitly to handle CRLF correctly.
    CSV quoting is handled via Python's csv module to strip surrounding double quotes.
    Column names are sanitized to remove special characters and replace umlauts.
    The source year is extracted from the file path for use in deduplication.

    Args:
        file_path_pattern (str): Base path. For MandantenDaten this is the current year
            folder; otherwise the /current/ folder containing year subfolders.
        table_name (str): Table name prefix used to filter matching CSV files.

    Returns:
        pyspark.sql.DataFrame: DataFrame with a '_source_year' column added.
    """

    # MandantenDaten esetén a filepath már a konkrét év mappára mutat -> nincs almappa-szint
    if table_name == "MandantenDaten":
        glob_pattern = f"{file_path_pattern}/{table_name}_*.csv"
    else:
        glob_pattern = f"{file_path_pattern}/**/{table_name}_*.csv"

    rdd_raw = spark.sparkContext.binaryFiles(glob_pattern)

    def extract_year_from_path(path):
        """Extract 4-digit year from a .../current/<year>/... path segment."""
        match = re.search(r'/current/(\d{4})(?:/|$)', path)
        return match.group(1) if match else "0000"

    # Collect all files as (path, bytes) tuples to the driver
    all_files = rdd_raw.collect()

    if not all_files:
        raise ValueError(f"No files found matching pattern: {glob_pattern}")

    # Extract and sanitize header from first file
    first_lines = all_files[0][1].decode("utf-16").split("\n")
    raw_header = [c.strip("\r").strip() for c in first_lines[0].split(";")]
    header = [sanitize_column_name(c) for c in raw_header]
    num_cols = len(header)
    full_header = ["_source_year"] + header

    # Parse all files on the driver to avoid RDD encoding issues
    all_rows = []
    for path, content in all_files:
        year = extract_year_from_path(path)
        lines = content.decode("utf-16").split("\n")
        for line in lines[1:]:                           # Skip header row
            line = line.rstrip("\r")                     # Strip Windows CR
            if not line.strip():                         # Skip empty lines
                continue
            parts = next(csv.reader(StringIO(line), delimiter=";", quotechar='"'))
            if len(parts) < num_cols:
                parts += [""] * (num_cols - len(parts))  # Pad missing columns
            if len(parts) > num_cols:
                parts = parts[:num_cols]                  # Truncate extra columns
            all_rows.append(Row(**dict(zip(full_header, [year] + parts))))

    df = spark.createDataFrame(all_rows)
    return df

# %%
def merge_and_deduplicate(df, data_cols):
    """
    Merges rows from all year subfolders and deduplicates across years.
 
    Deduplication logic:
    - All rows from the latest year are kept.
    - Rows from earlier years are included only if they do not already exist
      in the latest year's data (based on all data columns as the key).
    - Effectively: partition by all data columns, order by year descending,
      keep only the first occurrence (= the one from the highest year).
 
    Args:
        df (pyspark.sql.DataFrame): DataFrame with '_source_year' column.
        data_cols (list): List of data column names (excluding '_source_year').
 
    Returns:
        pyspark.sql.DataFrame: Deduplicated DataFrame without '_source_year'.
    """
 
    window = Window.partitionBy(*data_cols).orderBy(desc("_source_year"))
 
    df_deduped = (
        df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn", "_source_year")
    )
 
    return df_deduped

# %%
try:
    # Read all CSV files across all year subfolders
    df_raw = read_datev_csv_multi(filepath, p_in_table_name)

    # Determine deduplication key:
    # - if p_datev_filter is set, dedupe on that single (sanitized) column
    # - otherwise fall back to all data columns
    if p_datev_filter:
        filter_col = sanitize_column_name(p_datev_filter.strip())
        if filter_col not in df_raw.columns:
            raise ValueError(f"p_datev_filter column '{filter_col}' not found in data. Available: {df_raw.columns}")
        data_cols = [filter_col]
    else:
        data_cols = [c for c in df_raw.columns if c != "_source_year"]

    # Deduplicate: prefer rows from the highest year; include older rows only if not already present
    df = merge_and_deduplicate(df_raw, data_cols)

    display(df)

except Exception as e:
    print("Data couldn't be retrieved")
    print(f"Error details: {e}")
    v_error = 1

# %%
# Enrich DataFrame with metadata columns
if not v_error:
    df_with_metadata = df.withColumn("m_valid_dt", lit(v_m_valid_dt)) \
                         .withColumn("m_country_cd", lit(p_country_cd)) \
                         .withColumn("m_company_cd", lit(p_company_cd)) \
                         .withColumn("m_updated_at_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest")) \
                         .withColumn("m_extracted_dttm", from_utc_timestamp(current_timestamp(), "Europe/Budapest")) \
                         .withColumn("m_source_system_cd", lit(p_source_system_cd)) \
                         .withColumn("m_table_nm", lit(p_in_table_name))

# %%
# Display enriched DataFrame in debug mode
if p_debug and not v_error:
    display(df_with_metadata)

# %%
# Cast all columns to string for uniform Parquet output
if not v_error:
    df_string = df_with_metadata.selectExpr(
        *[f"CAST(`{c}` AS STRING) AS `{c}`" for c in df_with_metadata.columns]
    )
    
display(df_string)

# %%
# Log row count
if not v_error:
    print('Rows loaded for', p_in_table_name, ':', df_string.count())

# %%
# Remove dots from company code to build a clean folder/file name segment
if p_company_cd:
    v_company_cd_cleared = re.sub(r'[.]', '', p_company_cd)
else:
    v_company_cd_cleared = ''
 
print('v_company_cd_cleared:', v_company_cd_cleared)

# %%
# Build output Parquet file path
v_out_file_path = (
    v_root_path + "/" +
    p_subfolder_path + "/" +
    p_in_table_name + "/" +
    p_country_cd + "/" +
    v_company_cd_cleared + "/" +
    v_valid_dt + "/" +
    p_in_table_name + "_" + p_country_cd + "_" + v_company_cd_cleared + "_" + v_valid_dt + ".parquet"
)
 
print('v_out_file_path:', v_out_file_path)

# %%
# Write output as Parquet (overwrite mode)
df_string.write.format("parquet") \
               .mode("overwrite") \
               .save(v_out_file_path)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
