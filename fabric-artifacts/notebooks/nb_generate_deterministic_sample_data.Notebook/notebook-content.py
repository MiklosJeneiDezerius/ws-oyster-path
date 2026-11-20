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
# META   "frozen": false,
# META   "editable": true
# META }

# PARAMETERS CELL ********************

# A pipeline felülírhatja ezt a paramétercellát.
# 0 = változatlan alapadat; 1 = név- és címváltozások, valamint új tranzakciók.
# Egy külön kapcsolóval választható a próba az alapgenerálás átírása nélkül.
p_simulate_changes = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
import os
import random
from datetime import datetime, timezone, date, timedelta, time
import calendar

# Döntési napló – a notebook szintetikus, fejlesztési és betöltési tesztadatot
# készít; a szabályai nem valós ügyfélviselkedés statisztikai modelljét írják le.
#
# Reprodukálhatóság: rögzített seedet és stabil, sorozatos üzleti kulcsokat
# használunk. A seed nélküli véletlen és a futásonként új UUID-k változatosabb
# adatot adnának, de minden ismétlés más rekordokat és kulcsokat eredményezne,
# ami megnehezítené a snapshotok, az idempotencia és a változások ellenőrzését.
# Ezért az alapadat üzleti tartalma ismételhető; csak a batch metaadata új.
#
# Batch-azonosítás: egy UTC-ben, a futás elején képzett időbélyeg kerül minden
# fájlnévbe és egy közös, futásonkénti mappába. Az entitásonkénti mappák
# elkülönítenék a fájlokat, de elveszne az egy betöltéshez tartozás egyszerű
# jelölése. A fix időbélyeg nem különböztetné meg a futásokat; a másodperc
# pontosságú név viszont nem ütközésbiztos, ezért párhuzamos futást kerülünk.
#
# Fájlírás: a Python csv.DictWriterét választjuk explicit oszlopsorrenddel,
# UTF-8 kódolással és pontos fájlnevekkel. A Spark CSV-írás nagyobb adatmennyiség
# esetén skálázhatóbb, de fájlkimeneti/partíció-kezelési többletet hozna; a
# pandas kényelmes táblás API-ja pedig külön függőséget igényelne. Ehhez a kis,
# rögzített mintához a közvetlen írás egyszerűbb és kiszámíthatóbb.
#
# Két tranzakciós formátum: ugyanazokat a rekordokat CSV-ként és JSON Lines
# formában is kiírjuk. Egyetlen formátum kevesebb tárhelyet és írást igényelne,
# de nem lehetne mindkét betöltési útvonalat (sík CSV és beágyazott JSON)
# kipróbálni; külön generálásuk pedig eltérő adatot okozhatna. A JSON ezért a
# már elkészült tranzakciólistából épül fel, a két kimenet tartalma egyezik.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# A 102 rekord az ehhez a gyakorlathoz rögzített méret: gyorsan újrafuttatható,
# miközben elegendő ügyfelet ad a cím- és tier-csoportok tesztjéhez. Nagyobb
# minta jobban terhelné a betöltést, de lassítaná a visszajelzést; a kisebb
# gyorsabb, viszont kevesebb csoportot és szélső esetet mutatna.
CUSTOMER_COUNT = 102
CUSTOMER_ID_START = 1001
# A forrásrendszer neve a Landing útvonal namespace-e, nem a fájladatokból
# következtetjük ki; átnevezése a betöltő útvonal-konfigurációját is érintené.
SOURCE_SYSTEM = "oyster"
# Az üzleti dátumablak rögzített: ugyanaz az adat újrafuttatáskor sem csúszik
# a naptárral. A futási idő ettől külön, változó batch-metaadat.
anchor_date = date(2026, 11, 9)
run_started_at = datetime.now(timezone.utc)
ingest_date = run_started_at.strftime("%Y%m%d%H%M%S")
landing_root = f"Files/landing/{SOURCE_SYSTEM}"
output_path = f"{landing_root}/{ingest_date}"
output_dir = f"/lakehouse/default/{output_path}"
os.makedirs(output_dir, exist_ok=True)

# A névtartományok rögzítettek, hogy egy kulcs azonos entitást jelentsen minden
# alapfuttatásban. Véletlen kulcsok jobban emulálnák egyes forrásokat, de
# újrafuttatáskor újnak látszó rekordokat és nehezebben összevethető teszteket
# eredményeznének. A tartományok különállóságát a kezdőértékek teszik átláthatóvá.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Saját, rögzített seedű generátor: nem módosítja a notebook globális random
# állapotát, és az alapadat generálási sorrendje változatlanul ismételhető.
# A változáspróba csak a rekordok létrehozása után fut, így nem fogyaszt el
# további véletlen értékeket, és nem tolja el a többi entitás adatait.
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

# A kontrollált változást a teljes alaplista elkészülte után alkalmazzuk.
# Alternatíva lenne a seed megváltoztatása, de az a véletlen értékek nagy részét
# lecserélné, így nem különítené el a kívánt üzleti változást a zajtól. A rögzített
# névkészletből választott női ügyfél egyetlen vezetéknév-változása célzott
# snapshot-próba; a változás kikapcsolva az alapadat érintetlen marad.

# Change scenario: no random numbers are drawn; baseline records remain reproducible.

FEMALE_FIRST_NAMES = {"Anna", "Eszter", "Juli", "Nora"}
renamed_customer_id = None

if int(p_simulate_changes):
    renamed_customer = next(c for c in customers if c["FirstName"] in FEMALE_FIRST_NAMES)
    renamed_customer_id = renamed_customer["CustomerID"]
    old_last_name = renamed_customer["LastName"]
    renamed_customer["LastName"] = last_names[
        (last_names.index(old_last_name) + 1) % len(last_names)
    ]
    assert renamed_customer["LastName"] != old_last_name
    print(
        f"Névváltozás: CustomerID={renamed_customer_id} {renamed_customer['FirstName']} "
        f"{old_last_name} -> {renamed_customer['LastName']}"
    )

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

ADDRESS_ID_START = 3001
# Az elosztás nem véletlen: 32 ügyfélnél három, 28-nál két, 42-nél egy
# címtípust hoz létre. Egyenletesen egy címet adni egyszerűbb lenne, de nem
# tesztelné a többféle cím és a hiányzó címkapcsolatok kezelését; a terv
# rögzítése viszont megismételhetővé teszi az elvárt sor- és csoportszámokat.
address_plan = [
    (32, ("Permanent", "Mailing", "Billing")),
    (28, ("Permanent", "Mailing")),
    (42, ("Permanent",)),
]
address_locations = [
    ("1051", "Budapest", "Nador utca"),
    ("4024", "Debrecen", "Piac utca"),
    ("6720", "Szeged", "Karasz utca"),
    ("7621", "Pecs", "Kiraly utca"),
    ("9021", "Gyor", "Baross Gabor ut"),
    ("3525", "Miskolc", "Szechenyi utca"),
]
# A címeket körkörösen rendeljük ezekhez a helyszínekhez. A véletlen földrajzi
# kiosztás természetesebbnek tűnhet, de a körkörös sorrendnél könnyebb pontosan
# megismételni és ellenőrizni az egyes címeket.

assert sum(count for count, _ in address_plan) == len(customers)

customer_addresses = []
address_types_by_customer = {}
customer_index = 0

for count, address_types in address_plan:
    for customer in customers[customer_index:customer_index + count]:
        customer_id = customer["CustomerID"]
        address_types_by_customer[customer_id] = set(address_types)

        for address_type in address_types:
            address_index = len(customer_addresses)
            postal_code, city, street = address_locations[
                address_index % len(address_locations)
            ]
            customer_addresses.append({
                "AddressID": ADDRESS_ID_START + address_index,
                "CustomerID": customer_id,
                "AddressType": address_type,
                "Country": "HU",
                "PostalCode": postal_code,
                "City": city,
                "Street": street,
                "HouseNumber": str(address_index // len(address_locations) + 1),
            })

    customer_index += count

assert len(customer_addresses) == 194
assert len({address["AddressID"] for address in customer_addresses}) == 194
assert set(address_types_by_customer) == {
    customer["CustomerID"] for customer in customers
}
assert all(
    sum(types == set(address_types) for types in address_types_by_customer.values())
    == count
    for count, address_types in address_plan
)

# A változáspróba az utolsó két ügyfél Permanent címét módosítja, nem a
# névváltozással érintett ügyfélét. A cím azonosítója megmarad, így attribútum-
# változásként jelenik meg, nem új címként; az új AddressID létrehozása nem
# bizonyítaná a meglévő cím üzleti történetének SCD2-kezelését.

# Change scenario: move the Permanent address of the last two customers (not the renamed one).
# AddressID stays the same, so the snapshot records a new version of the existing address.
if int(p_simulate_changes):
    relocated_customer_ids = [customer["CustomerID"] for customer in customers[-2:]]
    assert renamed_customer_id not in relocated_customer_ids

    for customer_id in relocated_customer_ids:
        address = next(
            a for a in customer_addresses
            if a["CustomerID"] == customer_id and a["AddressType"] == "Permanent"
        )
        old_location = (address["PostalCode"], address["City"], address["Street"])
        new_location = address_locations[
            (address_locations.index(old_location) + 3) % len(address_locations)
        ]
        address["PostalCode"], address["City"], address["Street"] = new_location
        address["HouseNumber"] = str(int(address["HouseNumber"]) + 10)
        print(
            f"Címváltozás: CustomerID={customer_id} AddressID={address['AddressID']} "
            f"{old_location[1]} -> {address['PostalCode']} {address['City']}, "
            f"{address['Street']} {address['HouseNumber']}"
        )

# A CustomerAdress elírt forrás-prefixet a jelenlegi betöltési szerződés
# részeként megtartjuk. A helyesírási javítás tisztább lenne, de fájlnév- és
# entitásmódosítást igényelne a betöltőben is.
file_name = f"CustomerAdress_{run_started_at:%Y%m%dT%H%M%S}.csv"
customer_address_csv_path = os.path.join(output_dir, file_name)

with open(customer_address_csv_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.DictWriter(
        csv_file,
        fieldnames=[
            "AddressID", "CustomerID", "AddressType", "Country",
            "PostalCode", "City", "Street", "HouseNumber",
        ],
    )
    writer.writeheader()
    writer.writerows(customer_addresses)

print(f"CustomerAdress CSV mentve: {output_path}/{file_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ACCOUNT_ID_START = 2001

# A gyakorló adatmodellben ügyfelenként egy account készül, a balance pedig
# 1 000–100 000 közötti tesztérték. Több account ügyfelenként valósághűbb és
# gazdagabb kapcsolatot adna, de külön számossági és összerendelési szabályokat
# kívánna; az egy-az-egyhez minta a jelenlegi betöltési és tranzakciós
# ellenőrzéseket egyszerűsíti. Az egyenleg-tartomány nem pénzügyi szabály.
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

# Accountonként egy aktív kártya készül. Több vagy inaktív kártya bővítené az
# állapottesztet, de ebben a fixture-ben külön életciklus-próbát igényelne.
# A kártya CardTier értéke és az ügyfél aktuális tierje külön mezőfogalom:
# a terv szándékosan tartalmaz eltéréseket. A véletlen tier-hozzárendelés
# változatosabb lenne, de az elvárt csoportok és a tranzakciós limitek
# futásonként nehezebben lennének ellenőrizhetők.
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

# A tranzakciókat cent egész számokkal számoljuk és osztjuk fel, majd két
# tizedesre írjuk ki. Így a számítási lépésekben elkerülhető a float-alapú
# kerekítési hiba; a Decimal is pontos lenne, de itt az egész cent egyszerűbb.
#
# Az ügyfelenkénti teljes költés az aktuális account-tier tartományából jön.
# Ezek a korlátok tesztadat-kontraktust adnak, nem termékszabályt vagy valós
# költési modellt. A Platinum felső határa demóérték; valós adathoz üzleti
# szabály vagy forrásprofil alapján kellene meghatározni.
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

# Az adatablak az anchor_date előző havi megfelelő napjától indul; ha az a nap
# az előző hónapban nem létezik, annak utolsó napjára igazítjuk. Fix hosszúságú
# (például 30 napos) ablak más egyszerűséget adna, de nem naptári hónaphoz
# kötődne; itt a naptári megfeleltetés teszi értelmezhetővé a demó dátumait.
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

    # Véletlen pozitív egész határokra bontunk: a részösszegek összege pontosan
    # a teljes összeg, és egyik tranzakció sem nulla. Egyenlő vagy fix arányú
    # felosztás egyszerűbben magyarázható lenne, de kevésbé változatos; ez a
    # véletlen felosztás csak tesztadat, nem modellez valós tranzakcióeloszlást.
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
    # Számlánként 1–100 tétel: kis mintában is látszik a változó sorszám és az
    # egy-számlás aggregáció. Nagyobb felső határ több adatot adna, de a bemutató
    # méretét és a futási időt növelné; egy rögzített darabszám kevésbé változatos.
    transaction_count = random_generator.randint(1, 100)
    amounts_cents = split_amount_into_cents(account_total_cents, transaction_count)

    total_cents_by_account[account_id] = account_total_cents

    for amount_cents in amounts_cents:
        # A dátum és az idő egyenletes véletlen választás az ablakon belül.
        # Valósághű szezonális modellhez forrásadat vagy külön eloszlás kellene;
        # a jelenlegi egyszerű választás reprodukálható és könnyen ellenőrizhető.
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

# Az alaptranzakciók kulcsai és tartalma változatlan marad. A két kiválasztott
# ügyfél új tételei stabil kulcsot kapnak, így az ismétlés nem újabb esemény.
# A fenti darabszám- és tierösszeg-korlátok csak az alapadatra vonatkoznak.
if int(p_simulate_changes):
    extra_amounts_cents_by_customer = {
        CUSTOMER_ID_START: (100, 200, 300),
        CUSTOMER_ID_START + 1: (400, 500, 600, 700),
    }
    accounts_by_customer = {account["CustomerID"]: account for account in accounts}
    for customer_id, amounts_cents in extra_amounts_cents_by_customer.items():
        account_id = accounts_by_customer[customer_id]["AccountID"]
        for index, amount_cents in enumerate(amounts_cents):
            transactions.append({
                "TransactionID": transaction_id,
                "AccountID": account_id,
                "Amount": f"{amount_cents / 100:.2f}",
                "Timestamp": datetime.combine(
                    anchor_day, time(12, 0, index)
                ).strftime("%Y-%m-%d %H:%M:%S"),
            })
            transaction_id += 1
        print(f"Új tranzakciók: CustomerID={customer_id}, AccountID={account_id}, darab={len(amounts_cents)}")

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

# A JSON útvonala ugyanaz a mappa és fájlnév-törzs, mint a CSV-é.
# Így mindkét formátum ugyanahhoz a batchhez rendelhető; külön batch- vagy
# dátumképzés eltérő betöltési sorrendet és nehezebb egyeztetést okozhatna.
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

# A JSON Lines kimenetben accountonként egy dokumentum készül, a tranzakciókat
# pedig legfeljebb 20-as tömbökbe csoportosítjuk. Egyetlen nagy tömb kevesebb
# beágyazási szintet adna, de elveszne a kötegelt struktúra tesztelése; egy
# tranzakció/dokumentum egyszerűbb lenne, de megszüntetné az account-szintű
# csoportosítást. A 20-as méret teszt-fixture választás, nem teljesítményoptimum.
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

# JSON Lines mentése a CSV mellé. A soronkénti dokumentumformátum könnyen
# feldolgozható és egy rekordhatáronként olvasható; egyetlen JSON tömb is
# érvényes alternatíva lenne, de a feldolgozónak az egész dokumentumot kellene
# kezelnie. A kibontást és validálást a dedikált JSON betöltő notebook végzi,
# így a generátor csak a forrásfájlt állítja elő.
os.makedirs(os.path.dirname(transaction_json_path), exist_ok=True)

with open(transaction_json_path, "w", encoding="utf-8") as json_file:
    for document in json_documents:
        json_file.write(json.dumps(document, ensure_ascii=False) + "\n")

# A kibontást (flatten + explode) és az ellenőrzést az nb_json_load_source_to_ld végzi.
print(f"{len(json_documents)} JSON dokumentum mentve: {transaction_json_relative_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
