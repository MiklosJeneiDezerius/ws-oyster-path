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
# MAGIC         "name": "lh_oyster_path" #your lakehouse name here
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
from datetime import datetime, timezone

CUSTOMER_COUNT = 102
CUSTOMER_ID_START = 1001
SOURCE_SYSTEM = "oyster"

run_started_at = datetime.now(timezone.utc)
ingest_date = run_started_at.strftime("%Y-%m-%d")
run_id = run_started_at.strftime("%Y%m%dT%H%M%S%fZ")

landing_root = f"Files/landing/{SOURCE_SYSTEM}"

random_generator = random.Random(20261109)

first_names = ["Anna", "Bence", "Eszter", "Gabor", "Juli", "Milan", "Nora", "Peter"]
last_names = ["Kovacs", "Nagy", "Toth", "Szabo", "Varga", "Kiss", "Molnar", "Farkas"]

customers = [
    {
        "CustomerID": customer_id,
        "FirstName": random_generator.choice(first_names),
        "LastName": random_generator.choice(last_names),
    }
    for customer_id in range(CUSTOMER_ID_START, CUSTOMER_ID_START + CUSTOMER_COUNT)
]

assert len(customers) == CUSTOMER_COUNT
assert len({customer["CustomerID"] for customer in customers}) == CUSTOMER_COUNT

customers_df = spark.createDataFrame(customers)
display(customers_df.orderBy("CustomerID"))

customer_output_path = (
    f"{landing_root}/customer/"
    f"ingest_date={ingest_date}/run_id={run_id}"
)

(
    customers_df.coalesce(1)
    .write
    .mode("errorifexists")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(customer_output_path)
)

print(f"Customer CSV mentve ide: {customer_output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
