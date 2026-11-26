# Fabric notebook source


# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_oyster_path"
# MAGIC     }
# MAGIC }

# PARAMETERS CELL ********************

num_customers = 83
anchor_date = "2026-09-01"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Oyster Core deterministic source data
# Customer, Account, DebitCard and Transaction_Ledger are generated.
# The seven spend scenarios match the full project's ledger amounts.

# CELL ********************

import csv
import json
import os
from datetime import datetime, timedelta

num_customers = int(num_customers)
if num_customers < 1:
    raise ValueError("num_customers must be positive.")
anchor = datetime.strptime(anchor_date, "%Y-%m-%d")
batch_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
output_dir = f"/lakehouse/default/Files/oyster/landing_raw/{batch_timestamp}"
os.makedirs(output_dir, exist_ok=True)

customer_rows = []
account_rows = []
debit_card_rows = []
transaction_rows = []
first_names = ["Jakub", "Tomasz", "Anna", "Katarzyna", "Lenka", "Luka"]
last_names = ["Nagy", "Radu", "Kowalski", "Popescu"]
scenarios = {
    1: (15, 60, 1, 10),
    2: (12, 300, 1, 14),
    3: (15, 550, 1, 8),
    4: (15, 600, 1, 9),
    5: (3, 250, 4, 11),
    0: (3, 225, 5, 15),
    6: (15, 550, 1, 16),
}
card_tiers = {
    1: "Standard", 2: "Gold", 3: "Standard", 4: "Gold",
    5: "Gold", 0: "Platinum", 6: "Platinum",
}
transaction_id = 1
for customer_id in range(1, num_customers + 1):
    account_id = customer_id * 10
    customer_rows.append((
        customer_id,
        first_names[customer_id % len(first_names)],
        last_names[customer_id % len(last_names)],
    ))
    account_rows.append((account_id, customer_id, customer_id * 111.11))
    debit_card_rows.append((customer_id * 100, account_id, card_tiers[customer_id % 7]))
    days, amount, day_step, hour = scenarios[customer_id % 7]
    for day in range(days):
        transaction_time = anchor + timedelta(days=day * day_step, hours=hour)
        transaction_rows.append((
            transaction_id, account_id, amount,
            transaction_time.strftime("%Y-%m-%d %H:%M:%S"),
        ))
        transaction_id += 1

files_config = {
    "Customer": (["CustomerID", "FirstName", "LastName"], customer_rows),
    "Account": (["AccountID", "CustomerID", "Balance"], account_rows),
    "DebitCard": (["CardID", "AccountID", "CardTier"], debit_card_rows),
    "Transaction_Ledger": (
        ["TransactionID", "AccountID", "Amount", "Timestamp"], transaction_rows,
    ),
}
for table_name, (header, rows) in files_config.items():
    path = os.path.join(output_dir, f"{table_name}_{batch_timestamp}_core.csv")
    with open(path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"{table_name}: {len(rows)} rows written to {path}")

    if table_name == "Transaction_Ledger":
        transactions_by_account = {}
        for transaction_id, account_id, amount, transaction_timestamp in rows:
            transactions_by_account.setdefault(account_id, []).append({
                "TransactionID": transaction_id,
                "AccountID": account_id,
                "Amount": amount,
                "Timestamp": transaction_timestamp,
            })

        json_path = os.path.splitext(path)[0] + ".json"
        with open(json_path, "w", encoding="utf-8") as json_file:
            for account_id, account_transactions in transactions_by_account.items():
                transaction_batches = [
                    account_transactions[index:index + 5]
                    for index in range(0, len(account_transactions), 5)
                ]
                document = {
                    "SourceSystem": "oyster",
                    "Data": {
                        "AccountID": account_id,
                        "TransactionBatches": transaction_batches,
                    },
                }
                json_file.write(json.dumps(document, ensure_ascii=False) + "\n")
        print(f"Transaction_Ledger JSON written to {json_path}")

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
