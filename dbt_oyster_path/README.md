# Oyster Core dbt project

## Chapter 4: Bronze snapshot historization

This walkthrough uses current-state Bronze tables and dbt snapshots to retain
source history. It does not modify the Word document. The available local
Chapter 4 copy was older than the attached document; verify its section wording
when transferring these instructions.

Bronze tables are rebuilt from the complete currently exposed source on each run.
Snapshots in the Bronze schema retain changed versions by business key; unchanged
rows do not create new versions. This is snapshot-based historization, not CDC.

### 4.1 Prepare the environment

From the repository root, create a virtual environment only if one does not
already exist. Otherwise activate the existing environment and skip installation
when its dependencies already work.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install "dbt-fabric==1.11.1" "dbt-core>=1.11,<1.12"
Set-Location dbt_oyster_demo_test_core
dbt --version
```

The dependency range matches the Fabric notebook runner. Do not run `dbt init`
over this existing project or replace its configuration with the older chapter's
project names. Ensure the organizational identity is signed into Azure CLI;
the profile uses CLI authentication. Use the organization's approved sign-in
process and grant that identity source SELECT and Warehouse DDL/DML permissions.

### 4.2 Configure the connection and target

Use [profiles.yml](profiles.yml) and [dbt_project.yml](dbt_project.yml):

- Project: `dbt_oyster_demo_test_core`; profile: `dbt_oyster_demo_test`.
- Source Lakehouse: `lh_oysterlake_core`, schema `dbo`.
- Target Warehouse: `dw_oyster_core`, not the Lakehouse SQL analytics endpoint.
- Bronze output: `oystr_core_bronze`; test failures: `oystr_core_dq`.
- Set `DBT_SERVER`, `DBT_DATABASE` or `DBT_SOURCE_DATABASE` only when the
  corresponding deployment differs from those defaults.

```powershell
dbt debug --profiles-dir .
```

Stop and resolve connection, identity or permissions errors before loading.
The Lakehouse SQL endpoint is a read source here; dbt writes Warehouse tables.

### 4.3 Verify the five landing sources

The four original sources must already exist and be visible through the core
Lakehouse SQL endpoint. Address is optional until its landing pipeline exists.

| Source in `dbo` | Required columns |
| --- | --- |
| `ld_oys_customer` | `CustomerID`, `FirstName`, `LastName` |
| `ld_oys_customeraddress` | `AddressID`, `CustomerID`, `AddressType`, `Country`, `PostalCode`, `City`, `Street`, `HouseNumber` |
| `ld_oys_account` | `AccountID`, `CustomerID`, `Balance` |
| `ld_oys_debitcard` | `CardID`, `AccountID`, `CardTier` |
| `ld_oys_transaction_ledger` | `TransactionID`, `AccountID`, `Amount`, `Timestamp` |

These are physical Delta table names, not the Python generator's list names.
The supplied Address generator fields replace the older chapter's
`AddressLine1` and `State/Region` schema. Postal codes and house numbers stay text.

The core generator, load configuration and pipeline still deliver four sources;
this change does not add Address extraction or create an Address Delta table.
Before enabling it, land the fifth table with the columns above, wait for SQL
endpoint visibility, and check its keys and row counts. If its physical name
differs, configure the source without changing the model:

```powershell
$env:DBT_ADDRESS_TABLE = 'ld_oys_customeraddress'
```

The definitions are in [models/000_source/src_landing_delta_core.yml](models/000_source/src_landing_delta_core.yml).
Address source/model enablement uses the `include_address` dbt variable.

### 4.4 Build current-state Bronze tables and snapshots

The Bronze models are full tables with no custom macros:

- [b_oys_customer.sql](models/010_bronze/b_oys_customer.sql)
- [b_oys_customer_address.sql](models/010_bronze/b_oys_customer_address.sql)
- [b_oys_account.sql](models/010_bronze/b_oys_account.sql)
- [b_oys_debit_card.sql](models/010_bronze/b_oys_debit_card.sql)
- [b_oys_transaction.sql](models/010_bronze/b_oys_transaction.sql)

Each uses `materialized='table'`; the Address model additionally defaults to
disabled. The five dbt snapshots are `b_oys_customer_history`,
`b_oys_customer_address_history`, `b_oys_account_history`,
`b_oys_debit_card_history` and `b_oys_transaction_history`. They are written to
`oystr_core_bronze` and track business columns with `strategy='check'`.
`batch_id` and `ingested_dttm` are observation metadata, not change-detection
columns. The existing `ldh_oys_customer` and `ldh_oys_debitcard` snapshots still
feed the SCD2 Silver dimensions and now read current versions from the Bronze
snapshots.

`batch_id` is the UUID generated for the latest dbt invocation, and
`ingested_dttm` is Warehouse observation time, not source event time or original
raw-file arrival. A snapshot version retains those values from when its tracked
business data was observed. The first snapshot run establishes the baseline; it
does not backfill historical versions from the old append-only Bronze tables.
Before applying this materialization change to an existing Warehouse, back up
or migrate the append-only Bronze tables: rebuilding them as full tables
replaces their accumulated rows, and the initial snapshot only records the
current source state.

Tests in [models/core_schema.yml](models/core_schema.yml) enforce uniqueness by
business key in each current-state table. Multiple address types per customer
are allowed; `address_id`, not `customer_id`, is the snapshot key. The demo
checks one card per account. Address IDs must remain stable across full extracts
for snapshot updates to be interpreted as changes rather than deletes and new
records.

### 4.5 Execute and verify ingestion

For all five sources, after Address is available:

```powershell
dbt parse --profiles-dir . --vars '{include_address: true}'
dbt build --profiles-dir . --select path:models/010_bronze path:snapshots --vars '{include_address: true}'
```

Without Address, omit `--vars`; the other four source tables and their snapshots
still build. Always run snapshots after a successful full table refresh.

In the Warehouse SQL query editor, inspect a Bronze history table:

```sql
SELECT account_id, customer_id, balance_amt, m_valid_from_dttm,
       m_valid_to_dttm, batch_id, ingested_dttm
FROM oystr_core_bronze.b_oys_account_history
ORDER BY account_id, m_valid_from_dttm;
```

With unchanged landing data, another snapshot run does not add versions. Change
a tracked field upstream, refresh landing Delta, and build Bronze plus snapshots
again; the old version should close and a new current version should appear.
Snapshots use `hard_deletes='invalidate'`, so the landing extracts must be
complete and successfully refreshed before snapshots run. A partial extract can
make still-existing keys appear deleted.

### Real-project decisions before adopting this pattern

1. History before landing Delta: if replication overwrites the only copy before
  dbt reads it, intermediate changes are lost. Preserve immutable raw deliveries
  or capture supported source CDC/CDF before that overwrite when every change
  matters. Bronze cannot recover changes it never observed. Delta time travel
  and retention are not a substitute for an explicit history contract.
2. Load volume: full observations are simple but grow with every execution.
  For large sources choose CDC, reliable watermarks with lookback and
  deduplication, or change detection. Deletes need explicit tombstones or
  reconciliation against a complete successful full extraction.
3. Time: decide between source-effective, raw-arrival, Delta-commit and Bronze
  observation time. Persist each required time separately, with UTC/time-zone
  rules. This demo currently captures only the latter in ingestion metadata.
4. Replay: a new invocation ID is a new observation, even for the same delivery.
  For idempotent retries use a stable upstream delivery/event ID, checkpoints
  and a completion log. No batch audit table is implemented in this demo.
5. Consistency: a shared dbt batch ID does not guarantee all source tables were
  read at one consistent source point. Coordinate complete landing batches
  before Bronze, especially for deletes, relationships and empty extracts.
6. Keys: the supplied AddressID generator is index-based. Ensure its assignment
  stays stable if customer ordering or address counts change. Define primary,
  mailing and registered address rules in Silver, not by arbitrarily selecting
  the highest AddressID. The current demo selects the highest AddressID per
  customer/type from the active address snapshot rows; replace this rule if the
  source exposes an explicit primary-address or effective-date rule.
7. Governance: choose retention, deletion/erasure rules for personal and address
  data, schema-change approval, access control and capacity budgets. Snapshot
  historization does not remove legal obligations to purge sensitive history.

## Existing Silver and Gold

The project has Bronze source snapshots plus customer/card dbt snapshots and
SCD2 dimensions. Bronze snapshots preserve source-field history; `ldh` snapshots
combine current customer and address values for the customer dimension. The
customer dimension includes formatted `permanent_address`, `mailing_address`
and `billing_address` attributes. Changes or removals of these fields create a
new customer SCD2 version. Missing address types remain null. Existing snapshot
history is retained and is not retroactively populated with today's addresses.
After the Address landing source exists, build the complete customer lineage:

```powershell
dbt build --profiles-dir . --select +s_dim_customer --vars '{include_address: true}'
```

When Address ingestion is disabled, snapshots retain previously observed address
values instead of interpreting the disabled source as address removal. Gold
customer exports are unchanged; these new attributes currently belong to Silver.
Silver account/transaction models continue to use current-state Bronze tables;
their complete version history is available in the corresponding Bronze
snapshots.
Gold calculates account/day rolling 30-day spend; 2000 qualifies for Gold and
5000 for Platinum. Its customer lookup carries the last known version forward
for retained historical accounts. These semantics are demo decisions, not a
general replacement for source-effective history or account-closure rules.

For orchestration/deployment see [the core artifacts guide](../fabric-artifacts-core/Readme.md).
Pass `--vars '{include_address: true}'` on later full builds only after enabling
Address end to end; the existing notebook runner does not set it automatically.
