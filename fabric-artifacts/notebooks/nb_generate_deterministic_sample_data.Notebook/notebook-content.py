# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#%%configure
#{
#    "defaultLakehouse": {
#        "name": "lh_oyster_path"
#    }
#}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
import os
import random
from datetime import datetime, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CUSTOMER_COUNT = 102
CUSTOMER_ID_START = 1001
SOURCE_SYSTEM = "oyster"
anchor_date = date(2026, 11, 9)
run_started_at = datetime.now(timezone.utc)
ingest_date = run_started_at.strftime("%Y%m%d%H%M%S")
landing_root = f"Files/landing/{SOURCE_SYSTEM}"
output_path = f"{landing_root}/{ingest_date}"
output_dir = f"/lakehouse/default/{output_path}"
os.makedirs(output_dir, exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

random_generator = random.Random(20261109)

first_names = ["Anna", "Bence", "Eszter", "Gabor", "Juli", "Milan", "Nora", "Peter"]
last_names = ["Kovacs", "Nagy", "Toth", "Szabo", "Varga", "Kiss", "Molnar", "Farkas"]

customers = [
    {
        "CustomerID": customer_id,
        "FirstName": random_generator.choice(first_names),
        "LastName": random_generator.choice(last_names)
    }
    for customer_id in range(CUSTOMER_ID_START, CUSTOMER_ID_START + CUSTOMER_COUNT)
]

assert len(customers) == CUSTOMER_COUNT
assert len({customer["CustomerID"] for customer in customers}) == CUSTOMER_COUNT

file_name = f"Customer_{run_started_at:%Y%m%dT%H%M%S}.csv"
customer_csv_path = os.path.join(output_dir, file_name)

with open(customer_csv_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.DictWriter(
        csv_file,
        fieldnames=["CustomerID", "FirstName", "LastName"],
    )
    writer.writeheader()
    writer.writerows(customers)

print(f"Customer CSV mentve: {output_path}/{file_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ACCOUNT_ID_START = 2001

accounts = [
    {
        "AccountID": ACCOUNT_ID_START + index,
        "CustomerID": customer["CustomerID"],
        "Balance": random_generator.randint(1000, 100000),
    }
    for index, customer in enumerate(customers)
]

assert len(accounts) == len(customers)
assert {account["CustomerID"] for account in accounts} == {
    customer["CustomerID"] for customer in customers
}
assert len({account["AccountID"] for account in accounts}) == len(accounts)

accounts_df = spark.createDataFrame(accounts)

file_name = f"Account_{run_started_at:%Y%m%dT%H%M%S}.csv"
account_csv_path = os.path.join(output_dir, file_name)

with open(account_csv_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.DictWriter(
        csv_file,
        fieldnames=["AccountID", "CustomerID", "Balance"],
    )
    writer.writeheader()
    writer.writerows(accounts)

print(f"Account CSV mentve: {output_path}/{file_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CARD_ID_START = 9001
CARD_TIERS = {"Standard", "Gold", "Platinum"}

tier_plan = [
    (13, "Standard", "Standard"),
    (20, "Standard", "Gold"),
    (18, "Gold", "Gold"),
    (8, "Gold", "Platinum"),
    (10, "Gold", "Standard"),
    (15, "Platinum", "Platinum"),
    (18, "Platinum", "Gold"),
]

assert sum(count for count, _, _ in tier_plan) == len(accounts)
assert all(
    current_tier in CARD_TIERS and card_tier in CARD_TIERS
    for _, current_tier, card_tier in tier_plan
)

cards = []
current_tier_by_account = {}
account_index = 0

for count, current_tier, card_tier in tier_plan:
    selected_accounts = accounts[account_index:account_index + count]

    for account in selected_accounts:
        account_id = account["AccountID"]

        cards.append({
            "CardID": CARD_ID_START + len(cards),
            "AccountID": account_id,
            "CardTier": card_tier,
            "IsActive": True,
        })
        current_tier_by_account[account_id] = current_tier

    account_index += count

assert len(cards) == len(accounts)
assert len({card["CardID"] for card in cards}) == len(cards)
assert {card["AccountID"] for card in cards} == {
    account["AccountID"] for account in accounts
}

cards_df = spark.createDataFrame(cards)

file_name = f"DebitCard_{run_started_at:%Y%m%dT%H%M%S}.csv"
card_csv_path = os.path.join(output_dir, file_name)

with open(card_csv_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.DictWriter(
        csv_file,
        fieldnames=["CardID", "AccountID", "CardTier", "IsActive"],
    )
    writer.writeheader()
    writer.writerows(cards)

print(f"DebitCard CSV mentve: {output_path}/{file_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TRANSACTION_ID_START = 1_000_001

# A Standard: < 2000, Gold: 2000-<5000, Platinum: >=5000.
# Platinum esetén 10 000 a demóhoz választott felső összeg.
tier_amount_ranges_cents = {
    "Standard": (100_000, 199_999),
    "Gold": (200_000, 499_999),
    "Platinum": (500_000, 1_000_000),
}

if isinstance(anchor_date, datetime):
    anchor_day = anchor_date.date()
elif isinstance(anchor_date, date):
    anchor_day = anchor_date
else:
    anchor_day = date.fromisoformat(str(anchor_date)[:10])

# Az előző naptári hónap azonos napja, a hónap rövidebb volta esetén annak utolsó napja.
previous_month = anchor_day.month - 1
previous_month_year = anchor_day.year
if previous_month == 0:
    previous_month = 12
    previous_month_year -= 1

earliest_transaction_day = date(
    previous_month_year,
    previous_month,
    min(
        anchor_day.day,
        calendar.monthrange(previous_month_year, previous_month)[1],
    ),
)

def split_amount_into_cents(total_cents, transaction_count):
    if transaction_count == 1:
        return [total_cents]

    split_points = sorted(
        random_generator.sample(range(1, total_cents), transaction_count - 1)
    )
    boundaries = [0, *split_points, total_cents]
    return [
        boundaries[index + 1] - boundaries[index]
        for index in range(transaction_count)
    ]

transactions = []
total_cents_by_account = {}
transaction_id = TRANSACTION_ID_START
date_window_days = (anchor_day - earliest_transaction_day).days

for account in accounts:
    account_id = account["AccountID"]
    current_tier = current_tier_by_account[account_id]
    minimum_cents, maximum_cents = tier_amount_ranges_cents[current_tier]

    account_total_cents = random_generator.randint(minimum_cents, maximum_cents)
    transaction_count = random_generator.randint(1, 100)
    amounts_cents = split_amount_into_cents(account_total_cents, transaction_count)

    total_cents_by_account[account_id] = account_total_cents

    for amount_cents in amounts_cents:
        transaction_day = earliest_transaction_day + timedelta(
            days=random_generator.randint(0, date_window_days)
        )
        transaction_timestamp = datetime.combine(
            transaction_day,
            time(
                hour=random_generator.randrange(24),
                minute=random_generator.randrange(60),
                second=random_generator.randrange(60),
            ),
        )

        transactions.append({
            "TransactionID": transaction_id,
            "AccountID": account_id,
            "Amount": f"{amount_cents / 100:.2f}",
            "Timestamp": transaction_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        })
        transaction_id += 1

# Ellenőrizzük a sorszámot, a dátumablakot és az accountonkénti tierösszeget.
assert all(
    1 <= sum(tx["AccountID"] == account["AccountID"] for tx in transactions) <= 100
    for account in accounts
)
assert all(
    earliest_transaction_day
    <= datetime.strptime(tx["Timestamp"], "%Y-%m-%d %H:%M:%S").date()
    <= anchor_day
    for tx in transactions
)
assert all(
    tier_amount_ranges_cents[current_tier_by_account[account_id]][0]
    <= total_cents
    <= tier_amount_ranges_cents[current_tier_by_account[account_id]][1]
    for account_id, total_cents in total_cents_by_account.items()
)

transactions_df = spark.createDataFrame(transactions)

file_name = f"Transaction_Ledger_{run_started_at:%Y%m%dT%H%M%S}.csv"
transaction_csv_path = os.path.join(output_dir, file_name)

with open(transaction_csv_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.DictWriter(
        csv_file,
        fieldnames=["TransactionID", "AccountID", "Amount", "Timestamp"],
    )
    writer.writeheader()
    writer.writerows(transactions)

print(
    f"{len(transactions)} tranzakció mentve: "
    f"{output_path}/{file_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
from pyspark.sql import functions as F

# A JSON útvonala ugyanaz a mappa és fájlnév-törzs, mint a CSV-é.
file_name = f"Transaction_Ledger_{run_started_at:%Y%m%dT%H%M%S}.json"
transaction_json_path = os.path.join(output_dir, file_name)
transaction_json_file_name = os.path.basename(transaction_json_path)
transaction_json_relative_path = f"{output_path}/{transaction_json_file_name}"

transactions_by_account = {
    account["AccountID"]: []
    for account in accounts
}

for transaction in transactions:
    transactions_by_account[transaction["AccountID"]].append({
        "TransactionID": transaction["TransactionID"],
        "AccountID": transaction["AccountID"],
        "Amount": transaction["Amount"],
        "Timestamp": transaction["Timestamp"],
    })

# Az account tranzakciói 20-as tömbökbe kerülnek.
json_documents = []

for account_id, account_transactions in transactions_by_account.items():
    transaction_batches = [
        account_transactions[index:index + 20]
        for index in range(0, len(account_transactions), 20)
    ]

    json_documents.append({
        "SourceSystem": SOURCE_SYSTEM,
        "IngestDate": ingest_date,
        "Data": {
            "AccountID": account_id,
            "TransactionBatches": transaction_batches,
        },
    })

# JSON Lines mentése a CSV mellé.
os.makedirs(os.path.dirname(transaction_json_path), exist_ok=True)

with open(transaction_json_path, "w", encoding="utf-8") as json_file:
    for document in json_documents:
        json_file.write(json.dumps(document, ensure_ascii=False) + "\n")

print(f"JSON mentve: {transaction_json_path}")

# Visszaolvasás és kibontás táblás formába.
nested_transactions_df = spark.read.json(transaction_json_relative_path)

transactions_json_flat_df = (
    nested_transactions_df
    .select(
        F.explode(
            F.flatten(F.col("Data.TransactionBatches"))
        ).alias("Transaction")
    )
    .select(
        F.col("Transaction.TransactionID").cast("long").alias("TransactionID"),
        F.col("Transaction.AccountID").cast("long").alias("AccountID"),
        F.col("Transaction.Amount").cast("decimal(18, 2)").alias("Amount"),
        F.col("Transaction.Timestamp").alias("Timestamp"),
    )
)

assert transactions_json_flat_df.count() == len(transactions)

display(
    transactions_json_flat_df
    .orderBy("AccountID", "Timestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
