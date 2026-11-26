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

p_root_path = "Files/landing/oyster"
p_batch_folder = ""  # Empty selects the latest batch; otherwise reload this batch.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from datetime import datetime

batch_folders = [
    f.name.rstrip("/")
    for f in notebookutils.fs.ls(p_root_path)
    if f.isDir and re.fullmatch(r"\d{14}", f.name.rstrip("/"))
]
if not batch_folders:
    raise FileNotFoundError(f"NO_BATCH_FOLDER: No yyyyMMddHHmmss batch folder found under {p_root_path}")
if p_batch_folder and p_batch_folder not in batch_folders:
    raise FileNotFoundError(f"Batch folder {p_batch_folder} not found under {p_root_path}")

batch_folder = p_batch_folder or max(batch_folders)
datetime.strptime(batch_folder, "%Y%m%d%H%M%S")
print(f"Resolved batch: {batch_folder} | root: {p_root_path}")
notebookutils.notebook.exit(batch_folder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
