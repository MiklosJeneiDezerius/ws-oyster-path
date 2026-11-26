# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f27d086e-be7b-4e40-8e62-ea4d210b4538",
# META       "default_lakehouse_name": "lh_oyster_path",
# META       "default_lakehouse_workspace_id": "5e32bf37-c2be-4676-94c7-97213238ed30",
# META       "known_lakehouses": [
# META         {
# META           "id": "f27d086e-be7b-4e40-8e62-ea4d210b4538"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import re 

for f in notebookutils.fs.ls(p_root_path)
    display(f)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
