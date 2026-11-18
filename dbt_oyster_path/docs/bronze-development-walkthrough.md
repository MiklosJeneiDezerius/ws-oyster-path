# Building the Oyster Bronze layer, one small change at a time

This guide establishes the data model and Fabric prerequisites, follows Day 3
extraction, then reconstructs our development from dbt setup to the current
Bronze implementation. It is a teaching sequence, not a verbatim transcript:
each iteration introduces one idea, explains its purpose, shows the code, and
gives an acceptance check. Applying the same pattern to all tables counts as
one iteration, not five.

**Scope:** raw-file generation and extraction orchestration, Fabric Lakehouse
landing tables, four Warehouse SCD2 snapshots, and
one append-only incremental transaction ledger, and the iterative Silver
extension. Silver currently has shared configuration and typed staging views;
its customer, account, and debit-card SCD2 dimensions are implemented.
Date/time dimensions, explicit customer/account Unknown members, and the
transaction star fact with validation tests are also implemented. The Gold
consumer models and account-tier daily snapshot are implemented and built in
development.

**Status:** the development stack is built and tested end to end. Landing
holds one complete `2026-11-18 07:20:24` batch, loaded during the
orchestration test in Iteration 62 (102 customers, 194 addresses, 102
accounts, 102 debit cards, 4,840 transactions). The Warehouse layers were
built from the identical-content `2026-11-17 08:00:03` batch. Upstream checks passed
27/27, the Silver build 129/129, and the Gold build 72/72. All 4,840
transactions resolve to account and customer versions under the initial-load
history policy (Iteration 56). The Gold daily snapshot covers 2026-10-09 to
2026-11-09 with 102 accounts per day and reconciles exactly with an
independent recalculation from Silver. The semantic model and report are
designed (Iteration 60) but not yet created. Orchestration design has started
(Part XI). Production builds have not been run.

## How to use this guide

1. Make only the change described by the current iteration.
2. Read its reason and decision point before proceeding.
3. Perform its check and stop on failure.
4. Continue only when you understand the result.

The project already contains the final implementations. Early snippets show
intermediate states for learning; do not overwrite final files with them.
Code blocks labelled as additions belong under the indicated YAML parent,
not at the document root.

### The destination

```text
CSV deliveries in Files/landing/oyster/<UTC batch folder>
    -> Spark loader: latest selected batch, all business values as strings
    -> lh_oyster_path.dbo.ld_oys_*
    -> dbt running against dw_oyster_path
         -> dev_bronze.bz_oys_customer             snapshot
         -> dev_bronze.bz_oys_customer_address     snapshot
         -> dev_bronze.bz_oys_account              snapshot
         -> dev_bronze.bz_oys_debitcard            snapshot
         -> dev_bronze.bz_oys_transaction_ledger  incremental append
```

In this exercise, “Bronze” means the Warehouse history layer after landing.
Other medallion implementations call the raw landed data Bronze. Agree on the
meaning rather than assuming the layer name defines storage semantics.

## Before Part 0: Model and provision the platform

The source exercise begins with conceptual and physical modelling, then sets
up the Fabric workspace and Lakehouse before extraction or dbt. This section
adds those prerequisites using the entities and resource names implemented in
this repository. It does not claim that a workspace or Lakehouse has been
provisioned by following this document.

### The Oyster Bank business story

Oyster Bank serves individual customers and organizes them into customer
types, such as Regular and Premium. A customer has identifying and profile
information, including a name, date of birth, and tax identifier, and may have
one or more recorded addresses. The bank must be able to relate each address
back to its customer and distinguish address records, including by address
type.

Customers open bank accounts. Each account belongs to a customer and has an
account number, an account type such as Checking or Savings, a currency, and a
balance. One customer may hold multiple accounts. An account can in turn have
one or more debit cards. A card has a tier, such as Gold or Platinum, a
lifecycle status such as Active, Blocked, or Expired, and an expiry date. The
bank needs to connect each card to the account it belongs to.

Customers use their accounts for everyday transactions. Each transaction is
recorded as a ledger event with an identifier, the related account, an amount,
a timestamp, a counterparty, and a status such as Approved, Pending, or
Denied. An account can therefore be associated with many transaction events.
Keeping the event and its status makes it possible to analyze account activity
without treating a transaction as another version of the account itself.

The data platform follows this journey from generated source records to
analytics: retain each raw delivery, load the selected delivery into queryable
Landing tables, preserve observed customer, address, account, and card states,
and accumulate transaction events. Downstream models can then relate customer
and account history to transaction activity, examine customer spending, and
compare card tiers with customer or account activity. These are analytical
questions supported by the model, not claims about particular real customers
or actual bank operations.

The exercise solution does not include a separate prose brief for “The Oyster
Bank Story.” This narrative expands its stated customer journey, entity
relationships, and example attributes into a readable scenario; it is a
summary of the exercise, not a quotation. Some conceptual entities in that
model are not separate inputs in this repository; the implemented scope is
listed below.

### Iteration P1. Turn the business story into a data model

Start from the customer journey: a customer opens an account, receives a debit
card, and makes transactions. Identify each entity, its stable key, and how
records relate before choosing table columns or types.

| Entity in this repository | Key and relationship |
|---|---|
| Customer | `CustomerID`; one customer can have multiple addresses and accounts |
| Customer address (`CustomerAdress` in the input file) | `AddressID`; `CustomerID` relates the address to its customer |
| Account | `AccountID`; `CustomerID` relates the account to its customer; one account can have multiple cards and transactions |
| Debit card | `CardID`; `AccountID` relates the card to its account |
| Transaction ledger | `TransactionID`; `AccountID` relates each event to its account |

These are the five entities generated by the current sample-data notebook.
The exercise's broader conceptual model also proposes reference entities such
as CustomerType, AccountType, Currency, CardTier, TransactionStatus, and
CounterParty. They are not separate inputs in this repository; for example,
`CardTier` is currently an attribute of DebitCard. Treat those additions as
future scope, not as tables already present in Landing.

The exercise's physical model suggests relational types such as integer keys,
decimal amounts, dates/timestamps, and variable-length text. Those are design
choices for typed downstream models, not the Landing contract: Landing keeps
business values as strings so identifiers and values such as postal codes
retain their original representation. Define and validate casts in Silver
rather than relying on CSV inference.

**Check:** trace every foreign key to its parent entity and confirm the key is
stable across deliveries. Decide how multiple customer addresses are
distinguished (the current input includes `AddressType`) before using them in
downstream joins.

### Iteration P2. Confirm the Fabric environment

Before creating resources, verify the intended workspace, assigned Fabric
capacity, and the operator's workspace permissions. The source exercise
recommends separate Development, UAT, and Production environments connected
to source control. For this walkthrough, use the existing development
workspace and repository setup; additional environments are a deployment
decision, not a prerequisite to run the documented local dbt checks.

Connect the intended Fabric workspace to the correct Git provider, repository,
branch, and folder when workspace Git integration is part of the team's
workflow. Confirm the branch and sync state before making workspace changes.
Do not create a duplicate workspace, Lakehouse, or generator just to match
names from the exercise document.

| Exercise document label | Resource used in this repository |
|---|---|
| `lh_OysterLake` / `lh_Oysterlake` | `lh_oyster_path` |
| `nb_GenerateOysterData` | `nb_generate_deterministic_sample_data` |
| `pl_Landing_Raw` | Proposed extraction pipeline name; verify the workspace asset before referencing it |

**Check:** the intended workspace has capacity and access, and
`lh_oyster_path` is available. The generator notebook must be saved in that
workspace and attached to `lh_oyster_path` as its default Lakehouse before it
writes to OneLake.

### Iteration P3. Agree on the layer boundaries

The intended path is:

```text
Synthetic source generation
    -> timestamped raw CSV delivery in Lakehouse Files
    -> selected delivery loaded into Landing Delta tables
    -> dbt reads Landing and stores history in the Warehouse Bronze layer
```

In the current implementation, generated files simulate extraction; they are
not read from a live banking system. The Landing loader selects a batch and
overwrites the current Landing tables, while dbt snapshots retain observed
entity history and the transaction ledger accumulates events. Keep the raw
deliveries for replay. These choices and their limits—including batch
completeness, deletes, CDC/CDF, and history between observations—are covered
in the extraction iterations below. Do not describe a full extract or snapshot
as capturing every source-side change.

## Part 0. Day 3 - Extraction

This section follows the document's **Day 3 - Extraction** sequence: save the
generator, land raw files, orchestrate it through `pl_Landing_Raw`, validate the
delivery, and load Landing Delta. It uses this repository's actual names rather
than copying the document's older examples literally.

The `E` iteration prefix keeps the existing dbt iteration numbers stable.
These are documentation and implementation instructions, not a claim that the
pipeline has been deployed or run.

### Iteration E1. Agree on the extraction boundary

Before creating anything, distinguish these three responsibilities:

```text
Landing Raw:   retain delivered files under Files/
Landing Delta: expose the selected delivery as queryable Delta tables
Bronze:        retain observed history using dbt snapshots / event accumulation
```

In the exercise, a notebook **simulates** source extraction by generating files.
It is not a connector reading a real operational banking database.

| Document example | Name used in this repository |
|---|---|
| lh_OysterLake / lh_Oysterlake | lh_oyster_path |
| nb_GenerateOysterData | nb_generate_deterministic_sample_data |
| Files/landing/Oyster | Files/landing/oyster |
| num_customers | CUSTOMER_COUNT |
| pl_Landing_Raw | Proposed pipeline name from the document |

**Why:** names and path casing must agree before the first read or write.
Do not create a second Lakehouse or generator just to match an older label.

**Check:** identify the intended workspace, attached capacity, and existing
`lh_oyster_path` Lakehouse. Confirm the operator has the required access.

### Iteration E2. Save the generator as a Fabric notebook

Open or import the existing
[generator notebook](../../fabric-artifacts/notebooks/nb_generate_deterministic_sample_data.Notebook/notebook-content.py)
in the Fabric workspace. For a fresh workspace, create a Notebook named
`nb_generate_deterministic_sample_data` and transfer its code cells.

Do not paste the exported `.py` file as one code cell: its `# CELL`, metadata,
and commented magic lines describe the notebook's Git representation.

**Why:** the pipeline must reference a saved workspace notebook, not a local
script or unsaved editor buffer.

**Check:** the notebook appears in the intended Fabric workspace with its cells
separated correctly.

### Iteration E3. Attach the destination Lakehouse

In the notebook's Lakehouse explorer, add `lh_oyster_path` and make it the
**default Lakehouse**. Save the notebook.

The existing notebook also contains this session-configuration example:

```python
%%configure
{
    "defaultLakehouse": {
        "name": "lh_oyster_path"
    }
}
```

If used, run it as the first session-configuration cell before other Spark work.
The exported cell is frozen, so do not assume Run all has executed it.
Persistent notebook attachment and a session setting are related but distinct.

**Why:** Python file writes use `/lakehouse/default/Files/...`. Without a valid
attachment, a local-looking directory is not evidence that files reached OneLake.

**Check:** inspect the notebook's attached default Lakehouse and later confirm
the output through the Lakehouse **Files** browser.

### Iteration E4. Put extraction size in one parameter cell

The repository currently defines `CUSTOMER_COUNT = 102` in its configuration
cell. For pipeline-controlled volume, a recommended small refactor is to put
only that assignment in a dedicated cell:

```python
CUSTOMER_COUNT = 102
```

Mark it **... -> Toggle parameter cell**, then save. Remove the duplicate
assignment from later cells if making this refactor; otherwise it would
overwrite the value injected by the pipeline.

**Status:** this is a documented improvement, not a notebook change made by this
guide. The document's `num_customers` parameter does not match the current code.

**Decision:** keep 102 for the existing deterministic exercise. Override it only
when intentionally testing a different volume; changing generation inputs can
change event identity/content assumptions downstream.

**Check:** after pipeline execution, the actual customer count matches the
requested value, not just the value visible in the parameter settings.

### Iteration E5. Create one timestamped raw delivery folder

The current generator resolves its output paths once:

```python
import os
from datetime import datetime, timezone

SOURCE_SYSTEM = "oyster"
run_started_at = datetime.now(timezone.utc)
ingest_date = run_started_at.strftime("%Y%m%d%H%M%S")
landing_root = f"Files/landing/{SOURCE_SYSTEM}"
output_path = f"{landing_root}/{ingest_date}"
output_dir = f"/lakehouse/default/{output_path}"
os.makedirs(output_dir, exist_ok=True)
```

**Why:** every entity from one run shares a folder and extraction timestamp.
Keeping previous deliveries supports investigation and replay.

**Decision:** this exercise's second-resolution folder name is simple, but not
collision-proof. Avoid overlapping runs; a production delivery ID or completion
manifest should support retries and uniquely identify deliveries.

**Check:** the printed output path is under
`Files/landing/oyster/<yyyyMMddHHmmss>`.

### Iteration E6. Write one raw entity, then apply the same writer pattern

After constructing `customers`, write Customer as CSV:

```python
import csv

file_name = f"Customer_{run_started_at:%Y%m%dT%H%M%S}.csv"
customer_csv_path = os.path.join(output_dir, file_name)

with open(customer_csv_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.DictWriter(
        csv_file,
        fieldnames=["CustomerID", "FirstName", "LastName"],
    )
    writer.writeheader()
    writer.writerows(customers)
```

Apply the same header/UTF-8/context-manager pattern to all five entities:

| Entity | Expected file in the delivery folder |
|---|---|
| Customer | Customer_&lt;run timestamp&gt;.csv |
| Customer address | CustomerAdress_&lt;run timestamp&gt;.csv |
| Account | Account_&lt;run timestamp&gt;.csv |
| Debit card | DebitCard_&lt;run timestamp&gt;.csv |
| Transaction ledger | Transaction_Ledger_&lt;run timestamp&gt;.csv |

The generator also writes `Transaction_Ledger_<run timestamp>.json` as an
alternative nested representation. The CSV loader does not consume that JSON;
the generator only writes it. See "Alternative: the source delivers JSON" at
the end of Part 0 for the loader that flattens it.

**Why one shared change:** the serialization mechanism is identical across the
entities; only the records, field list, and filename prefix differ.

**Document discrepancy:** its extraction inventory also mentions CustomerType,
AccountType, Currency, CardTier, and TransactionStatus. Separate files/tables
for those are not implemented by the current generator. Do not claim they were
loaded or invent empty source declarations for them. They require a separate
scope/model decision if the expanded exercise needs them.

### Iteration E7. Run and validate manually before adding orchestration

Run the generator once interactively. Then open the Lakehouse **Files** browser,
refresh it, and inspect the new delivery folder.

An optional notebook check, after a successful generator run:

```python
entries = notebookutils.fs.ls(output_path)
names = {entry.name.rstrip("/") for entry in entries if not entry.isDir}
expected = {
    f"{prefix}_{run_started_at:%Y%m%dT%H%M%S}.csv"
    for prefix in [
        "Customer", "CustomerAdress", "Account", "DebitCard", "Transaction_Ledger"
    ]
}
missing = expected - names
if missing:
    raise FileNotFoundError(f"Incomplete raw delivery: {sorted(missing)}")
print(f"Verified {len(expected)} expected CSV files in {output_path}")
```

**Check:** inspect headers and sample records as well as file existence.
The generator contains count/domain assertions; a green notebook run alone
still does not prove that downstream SQL tables exist.

**Best practice:** make one manual execution understandable before hiding it
behind a pipeline. File presence alone is not a production completion protocol.

### Iteration E8. Add the extraction pipeline with one activity

In Fabric:

1. Create a **Data pipeline** named `pl_Landing_Raw`.
2. Add a **Notebook** activity, for example `Generate_raw_files`.
3. In its settings, select the current workspace.
4. Select `nb_generate_deterministic_sample_data`.
5. Save the pipeline.

At this iteration, keep it to one activity and the notebook's defaults.

**Why:** establish that orchestration can run the exact notebook already tested.
No need to add schedules, branches, retries, or dbt execution simultaneously.

**Check:** confirm the selected notebook and its default Lakehouse, not merely
the activity's display name.

### Iteration E9. Optionally expose extraction volume through the pipeline

Only after completing E4, add a pipeline parameter and map it into the Notebook
activity's **Base parameters**:

| Setting | Value |
|---|---|
| Pipeline parameter name | customer_count |
| Pipeline parameter type | Int |
| Default | 102 |
| Notebook base parameter name | CUSTOMER_COUNT |
| Notebook base parameter type | Int |
| Notebook base parameter value (dynamic expression) | `@pipeline().parameters.customer_count` |

**Why:** pipeline controls invocation inputs; notebook defaults support
interactive development. Parameter names are exact identifiers.

**Check:** deliberately run with a small supported test count and verify the
output count. Restore the exercise's intended volume before downstream testing.
Never pass `num_customers` to code that only reads `CUSTOMER_COUNT`.

### Iteration E10. Save, run, monitor, and verify the raw delivery

1. Save `pl_Landing_Raw`.
2. Run it once.
3. Follow the Notebook activity in the pipeline's output/monitoring view.
4. Open notebook diagnostics if it fails; do not repeatedly submit new runs.
5. After success, find the corresponding new Lakehouse delivery folder.
6. Repeat the file/header/count checks from E7.

**Expected:** a successful notebook activity and the five CSVs in one delivery
folder. Retain the run ID and resolved batch folder for troubleshooting.

**Decision:** automatic retries are useful for transient faults only after
retry behaviour is understood. The current generator creates a new run timestamp
on a fresh execution, so retries are not automatically the same logical delivery.

**Status:** these pipeline steps are instructions. This documentation update
does not create, execute, or verify a live Fabric pipeline.

### Iteration E11. Introduce the simplest Landing Delta load

Open the existing
[loader notebook](../../fabric-artifacts/notebooks/nb_csv_load_source_to_ld.Notebook/notebook-content.py)
and attach the same default Lakehouse.

For teaching the basic file-to-table mechanism, start with one explicit,
successfully validated delivery and one entity:

```python
source_path = f"{output_path}/Customer_{run_started_at:%Y%m%dT%H%M%S}.csv"
target_table = "dbo.ld_oys_customer"

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(source_path)
)
df.write.format("delta").mode("overwrite").saveAsTable(target_table)
```

This snippet assumes the generator variables are available in the teaching
session. In the **separate loader notebook**, explicitly supply the verified
delivery path instead; notebook Python variables are not shared across pipeline
activities. The existing final loader discovers/selects its own batch.

**Why:** first understand one read and one write. Do not replace the final
validated loader with this stripped-down example.

**Check:** inspect `dbo.ld_oys_customer` from the attached Lakehouse and confirm
its count matches the CSV. The input files remain under **Files**; the Delta
table is a different object under **Tables**.

The document initially reads across all timestamped folders. We intentionally
avoid that for repeated full extracts: it would combine historical copies and
duplicate keys. The next iteration adds explicit latest-batch selection;
iterations 13-18 in the Day 3 loader improvements below generalize and validate
all five entities.

### Iteration E11a. Select one batch folder

Replace the manually supplied delivery path with explicit folder discovery
inside the loader notebook:

```python
import re

p_root_path = "Files/landing/oyster"
batch_folders = [
    f.name.rstrip("/")
    for f in notebookutils.fs.ls(p_root_path)
    if f.isDir and re.fullmatch(r"\d{14}", f.name.rstrip("/"))
]
if not batch_folders:
    raise FileNotFoundError(f"No batch folders found under {p_root_path}")
batch_folder = max(batch_folders)
source_path = f"{p_root_path}/{batch_folder}/Customer_*.csv"
```

Use this `source_path` in the reader from E11. The loader no longer depends on
Python variables from the generator notebook.

**Why:** folders use sortable UTC `yyyyMMddHHmmss` names. Combining all full
extracts would repeat keys and break snapshot assumptions.

**Decision:** latest-folder ingestion is appropriate for complete current-state
extracts. It is not a general solution for event batches: skipping a folder can
lose events. A snapshot cannot recover data it never read.

Do not depend on `/**/` as arbitrary-depth recursion in Spark file globs.
`recursiveFileLookup` is an alternative for intentionally reading all matching
files, not for selecting only the newest delivery.

**Check:** log the selected folder and verify that no older files were read.
A greatest folder name does not prove a producer has finished writing it.

### Iteration 9. Keep landing business values as strings

Apply the same reader setting to all five entities:

```python
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(path)
)
```

**Why:** inference can change types between batches, strip meaningful leading
zeros, and make raw ingestion dependent on sample contents.

**Decision:** explicit typed schemas are another valid approach when the source
contract is strong. We chose raw text and deferred casts and value-quality rules
to Silver. This does not mean all malformed CSV records are now validated.

**Fabric prerequisite:** attach `lh_oyster_path` as the default Lakehouse before
running either notebook. A frozen `%%configure` cell and empty dependency
metadata do not establish that the Lakehouse is attached.

### Iteration E12. Choose load and history semantics before expanding

| Decision | Choice here | Choose differently when |
|---|---|---|
| Full overwrite vs merge | Overwrite current-state Landing Delta | Deliveries are changes-only, or volume requires incremental processing |
| Raw preservation | Retain timestamped input folders | A governed alternative archive provides equivalent replay/audit |
| History location | Four Bronze snapshots; append-only ledger | Different history consumers or source semantics justify another design |
| CDF | Not explicitly enabled by this implementation | A supported downstream consumer and checkpoint/retention design need it |
| Next orchestration step | Loader after a successful complete delivery | Independent event sources require separate batch coordination |

**ODS:** the entity landing tables are a lightweight current-state view, not a
history archive. For the ledger, “current landing” means the selected event
delivery; it is not guaranteed to contain every event accumulated in Bronze.

**CDC vs CDF:** CDC captures source-side changes. Delta CDF exposes changes made
to a Delta table. Enabling CDF does not automatically make this dbt Warehouse
implementation consume it, nor does a full overwrite necessarily describe
minimal business changes. A real CDC design must represent deletes explicitly;
an update timestamp alone generally cannot reveal deleted source rows.

**History limitation:** snapshots retain versions they observe, not every
intermediate source change. Retained raw batches can support replay, but must
be processed in order; merely overwriting landing repeatedly can hide unobserved
states.

**Completion boundary:** before selecting the greatest timestamp folder in an
automated load, establish that the producer has finished all files. A manifest
or equivalent handoff is a future improvement, not implemented protection.
If adding the loader as another pipeline activity, use a success dependency and
an explicit delivery handoff rather than assuming “latest” identifies this run.

### Improve the loader, one responsibility at a time

These are shared changes for every entity. They replace the duplicated
single-Customer example and frozen multi-table loop with one execution path.
Their existing iteration numbers are retained even though this section now
appears before dbt setup.

### Iteration 13. Centralize the entity configuration

```python
ENTITIES = {
    "Customer": ("dbo.ld_oys_customer", "CustomerID",
                 ["CustomerID", "FirstName", "LastName"]),
    "CustomerAdress": ("dbo.ld_oys_customeraddress", "AddressID",
                       ["AddressID", "CustomerID", "AddressType", "Country",
                        "PostalCode", "City", "Street", "HouseNumber"]),
    "Account": ("dbo.ld_oys_account", "AccountID",
                ["AccountID", "CustomerID", "Balance"]),
    "DebitCard": ("dbo.ld_oys_debitcard", "CardID",
                  ["CardID", "AccountID", "CardTier", "IsActive"]),
    "Transaction_Ledger": ("dbo.ld_oys_transaction_ledger", "TransactionID",
                           ["TransactionID", "AccountID", "Amount", "Timestamp"]),
}
```

**Why:** table names, required columns, and keys have one shared mechanism.
The generator writes both transaction CSV and JSON; this loader uses CSV
(the JSON alternative is described at the end of Part 0).

### Iteration 14. Add pipeline parameters

Put these defaults in a dedicated cell:

```python
p_root_path = "Files/landing/oyster"
p_batch_folder = ""
p_entities = ""
p_debug = 0
```

Mark it **Toggle parameter cell** in Fabric and save/sync the resulting metadata.
The current exported notebook still instructs the user to do this; do not assume
pipeline overrides are enabled merely because the variables exist.

Selection logic:

```python
selected = [e.strip() for e in p_entities.split(",") if e.strip()] or list(ENTITIES)
unknown = sorted(set(selected) - set(ENTITIES))
if unknown:
    raise ValueError(f"Unknown entities: {unknown}. Allowed: {list(ENTITIES)}")

if p_batch_folder and p_batch_folder not in batch_folders:
    raise FileNotFoundError(f"Batch folder {p_batch_folder} not found under {p_root_path}")
batch_folder = p_batch_folder or max(batch_folders)
```

**Why:** replay an explicit batch or diagnose one entity without editing logic.
Partial entity loads can leave tables on different batches: do not run snapshots
until cross-table consistency is restored.

### Iteration 15. Validate required headers and keep extra columns

Inside the shared `prepare(file_prefix)` function:

```python
missing = [c for c in required_columns if c not in df.columns]
if missing:
    raise ValueError(f"{file_prefix}: missing columns {missing}. Header was {df.columns}")
extra = [c for c in df.columns if c not in required_columns]
if extra:
    print(f"WARNING {file_prefix}: unexpected columns {extra} are kept (landing schema grows)")
```

**Decision:** we chose to preserve unexpected columns, not drop them. Required
columns come first, extras follow. Schema changes still need review downstream.

**Limit:** this is a required-name check, not a full CSV header/record validator.
Spark may rename duplicate headers; do not rely on that as complete duplicate
validation, especially for unexpected columns or multiple input files.

### Iteration 16. Reject empty business keys

```python
from pyspark.sql import functions as F

empty_keys = df.filter(
    F.col(business_key).isNull() | (F.trim(F.col(business_key)) == "")
).count()
if empty_keys:
    raise ValueError(f"{file_prefix}: {empty_keys} row(s) with empty {business_key} in {path}")
```

**Why:** snapshots and event deduplication require usable keys. This rejects blank
keys but does not normalize valid keys or reject duplicates.

### Iteration 17. Add batch time, shared load time, and source filename

Resolve these once per notebook execution:

```python
from datetime import datetime, timezone

batch_ts = datetime.strptime(batch_folder, "%Y%m%d%H%M%S")
load_ts = datetime.now(timezone.utc).replace(tzinfo=None)
```

Project the metadata from the file-source DataFrame:

```python
out = df.select(
    *required_columns,
    *extra,
    F.lit(batch_ts).alias("meta_batch_timestamp"),
    F.lit(load_ts).alias("meta_load_timestamp"),
    F.col("_metadata.file_name").alias("meta_source_file"),
)
```

When documenting dbt sources later, include `meta_batch_timestamp` with
`data_type: datetime2(6)` for all five tables.

| Field | Meaning |
|---|---|
| meta_batch_timestamp | UTC source extract observation time encoded in the folder |
| meta_load_timestamp | Shared UTC loader execution time |
| meta_source_file | Input filename |

**Important:** extract time is not necessarily business-effective change time.
Verify Spark session timezone handling is UTC; a naive Python datetime alone
does not establish the session timezone.

**Why `_metadata.file_name`:** select file metadata directly from the file reader
rather than deriving a filename from `input_file_name()`. Verify support in the
chosen Fabric runtime. Source fields named `_metadata` or our `meta_*` fields
would need an explicit collision policy.

### Iteration 18. Validate every selected entity before writing any table

```python
prepared = {e: prepare(e) for e in selected}

for file_prefix, (table_name, out) in prepared.items():
    (
        out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Loaded {file_prefix} -> {table_name}")
    if int(p_debug):
        display(spark.table(table_name).limit(10))
```

**Why:** detect the implemented input checks before starting overwrites.
Do not say this makes the batch atomic: Spark is lazy, data can be reread, and a
write can fail after earlier writes succeeded. Delta protects each individual
table commit, not all five as one transaction.

**Check:** inspect counts and metadata for all tables. Multiple files can produce
multiple groups per table; the important shared value is the selected batch.

### Iteration 26. Reject empty entity deliveries before deletion detection

In the loader, immediately after reading `df`:

```python
if not df.take(1):
    raise ValueError(f"{file_prefix}: source file contains no data rows in {path}")
```

**Why:** a header-only delivery must not silently become mass deletion when
downstream snapshots compare complete extracts.

**Trade-off:** zero rows could be legitimate. For this exercise we fail closed.
A production completion manifest should distinguish a successful empty extract
from a broken one. This guard cannot detect a non-empty but incomplete extract.

### Complete loader: assemble the cells before moving on to dbt

All loader changes are now together. Historical iteration labels are retained;
follow their order here rather than their numeric values.
The following cells assemble the current loader's behaviour without the Git
export metadata. Keep the default Lakehouse attachment from E3. If using
`%%configure`, it belongs before these cells, as shown in E3.

**Cell 1 - parameters:** mark this cell as the Fabric parameter cell.

```python
p_root_path = "Files/landing/oyster"
p_batch_folder = ""
p_entities = ""
p_debug = 0
```

**Cell 2 - configuration and batch selection:**

```python
import re
from datetime import datetime, timezone
from pyspark.sql import functions as F

ENTITIES = {
    "Customer": ("dbo.ld_oys_customer", "CustomerID",
                 ["CustomerID", "FirstName", "LastName"]),
    "CustomerAdress": ("dbo.ld_oys_customeraddress", "AddressID",
                       ["AddressID", "CustomerID", "AddressType", "Country",
                        "PostalCode", "City", "Street", "HouseNumber"]),
    "Account": ("dbo.ld_oys_account", "AccountID",
                ["AccountID", "CustomerID", "Balance"]),
    "DebitCard": ("dbo.ld_oys_debitcard", "CardID",
                  ["CardID", "AccountID", "CardTier", "IsActive"]),
    "Transaction_Ledger": ("dbo.ld_oys_transaction_ledger", "TransactionID",
                           ["TransactionID", "AccountID", "Amount", "Timestamp"]),
}

selected = [e.strip() for e in p_entities.split(",") if e.strip()] or list(ENTITIES)
unknown = sorted(set(selected) - set(ENTITIES))
if unknown:
    raise ValueError(f"Unknown entities: {unknown}. Allowed: {list(ENTITIES)}")

batch_folders = [
    f.name.rstrip("/")
    for f in notebookutils.fs.ls(p_root_path)
    if f.isDir and re.fullmatch(r"\d{14}", f.name.rstrip("/"))
]
if not batch_folders:
    raise FileNotFoundError(f"No batch folders found under {p_root_path}")
if p_batch_folder and p_batch_folder not in batch_folders:
    raise FileNotFoundError(f"Batch folder {p_batch_folder} not found under {p_root_path}")
batch_folder = p_batch_folder or max(batch_folders)

batch_ts = datetime.strptime(batch_folder, "%Y%m%d%H%M%S")
load_ts = datetime.now(timezone.utc).replace(tzinfo=None)
print(f"Batch {batch_folder} (of {len(batch_folders)}) | entities: {selected} | load_ts (UTC): {load_ts}")
```

**Cell 3 - prepare every entity, then write:**

```python
def prepare(file_prefix):
    table_name, business_key, required_columns = ENTITIES[file_prefix]
    path = f"{p_root_path}/{batch_folder}/{file_prefix}_*.csv"

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(path)
    )

    if not df.take(1):
        raise ValueError(f"{file_prefix}: source file contains no data rows in {path}")

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{file_prefix}: missing columns {missing}. Header was {df.columns}")
    extra = [c for c in df.columns if c not in required_columns]
    if extra:
        print(f"WARNING {file_prefix}: unexpected columns {extra} are kept (landing schema grows)")

    empty_keys = df.filter(
        F.col(business_key).isNull() | (F.trim(F.col(business_key)) == "")
    ).count()
    if empty_keys:
        raise ValueError(f"{file_prefix}: {empty_keys} row(s) with empty {business_key} in {path}")

    out = df.select(
        *required_columns,
        *extra,
        F.lit(batch_ts).alias("meta_batch_timestamp"),
        F.lit(load_ts).alias("meta_load_timestamp"),
        F.col("_metadata.file_name").alias("meta_source_file"),
    )
    return table_name, out


prepared = {e: prepare(e) for e in selected}

for file_prefix, (table_name, out) in prepared.items():
    (
        out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Loaded {file_prefix} -> {table_name}")
    if int(p_debug):
        display(spark.table(table_name).limit(10))
```

**Cell 4 - verify the five landing tables:** use this after loading all entities.
On a partial first load, some tables may not yet exist; after a partial reload,
older tables can legitimately show different batch times until the load is completed.

```sql
%%sql
SELECT 'customer' AS tbl, meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*) AS row_count
FROM dbo.ld_oys_customer GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
UNION ALL
SELECT 'customer_address', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
FROM dbo.ld_oys_customeraddress GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
UNION ALL
SELECT 'account', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
FROM dbo.ld_oys_account GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
UNION ALL
SELECT 'debitcard', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
FROM dbo.ld_oys_debitcard GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file
UNION ALL
SELECT 'transaction_ledger', meta_batch_timestamp, meta_load_timestamp, meta_source_file, COUNT(*)
FROM dbo.ld_oys_transaction_ledger GROUP BY meta_batch_timestamp, meta_load_timestamp, meta_source_file;
```

**Acceptance:** after a successful full load, all five tables show the same batch
and load times. Business values remain strings, and all required columns exist.
These checks do not add cross-table atomicity, uniqueness enforcement, or a
producer-completion protocol; the limits discussed above still apply.

### Alternative: the source delivers JSON instead of CSV

**Goal:** show how the same Landing table is produced when a source sends
nested JSON instead of a flat CSV file. The example is the transaction ledger,
which the generator also writes as `Transaction_Ledger_<run timestamp>.json`.
The notebook is `nb_json_load_source_to_ld`, placed next to the CSV loader.

**Why it matters:** many sources (APIs, event exports) deliver documents, not
rows. Landing must still expose one row per business record with the same
column contract, so that dbt sources, snapshots and tests stay unchanged.

**The input shape:** JSON Lines, one document per account. Transactions are
grouped into arrays of 20, so the payload is an array of arrays:

```json
{"SourceSystem": "oyster", "IngestDate": "20261118072024",
 "Data": {"AccountID": 1001,
          "TransactionBatches": [
            [{"TransactionID": 1, "AccountID": 1001, "Amount": "12.30", "Timestamp": "2026-10-02 14:05:11"}, "... up to 20"],
            [{"TransactionID": 21, "AccountID": 1001, "Amount": "4.10", "Timestamp": "2026-10-09 08:41:52"}]
          ]}}
```

#### Step 1. Read with an explicit schema

```python
transaction_schema = StructType([StructField(c, StringType()) for c in REQUIRED_COLUMNS])
document_schema = StructType([
    StructField("SourceSystem", StringType()),
    StructField("IngestDate", StringType()),
    StructField("Data", StructType([
        StructField("AccountID", StringType()),
        StructField("TransactionBatches", ArrayType(ArrayType(transaction_schema))),
    ])),
])

documents = (
    spark.read.schema(document_schema)
    .option("mode", "FAILFAST")        # a malformed JSON line fails the run instead of becoming a null row
    .json(f"{p_root_path}/{batch_folder}/Transaction_Ledger_*.json")
    .select("*", F.col("_metadata.file_name").alias("meta_source_file"))
)
```

**Why:** schema inference costs an extra pass and can change types between
deliveries. Every leaf is `STRING`, exactly like the CSV Landing; JSON numbers
keep their raw text, and typing stays in Silver. `FAILFAST` turns a broken line
into a failed run; the default `PERMISSIVE` mode would load it as a null row.

#### Step 2. Validate the envelope before exploding

```python
bad_envelopes = documents.filter(
    F.col("Data.AccountID").isNull()
    | F.col("Data.TransactionBatches").isNull()
    | (F.size("Data.TransactionBatches") == 0)
    | F.exists("Data.TransactionBatches", lambda b: b.isNull() | (F.size(b) == 0))
).count()
if bad_envelopes:
    raise ValueError(f"{bad_envelopes} document(s) without AccountID or with empty batches")
```

**Why this check comes first:** `flatten` and `explode` lose data silently.

- `flatten` returns `NULL` for the whole account if any inner array is `NULL`.
- `explode` emits no row for a `NULL` or empty array.

Without this check, a damaged document would make an entire account disappear
from Landing without any error.

#### Step 3. `flatten` + `explode`: from documents to rows

```python
transactions = (
    documents
    .select(
        F.col("Data.AccountID").alias("envelope_account_id"),
        F.explode(F.flatten("Data.TransactionBatches")).alias("tx"),
        "meta_source_file",
    )
    .select("envelope_account_id", "tx.*", "meta_source_file")
)
```

| Function | Input | Output | Effect on row count |
|---|---|---|---|
| `flatten` | `array<array<struct>>` | `array<struct>` | none; it concatenates the inner arrays of one row |
| `explode` | `array<struct>` | `struct` | one output row per array element |
| `tx.*` | `struct` | flat columns | none; it promotes the struct fields to columns |

For one account with 45 transactions in batches of 20 + 20 + 5: `flatten`
produces one array of 45 elements, `explode` produces 45 rows, and `tx.*`
turns each struct into the columns `TransactionID`, `AccountID`, `Amount`,
`Timestamp`. The envelope `AccountID` is carried along so that it can be
checked against the transaction.

**Alternatives and when to use them:**

| Function | Use when |
|---|---|
| `explode` | Every parent must have children; missing children are rejected earlier (our case) |
| `explode_outer` | A parent without children is legitimate and must still produce a row (with `NULL` children) |
| `posexplode` | The position inside the array carries meaning (line number, ordering) |
| `inline` | Shortcut for `explode` + `tx.*` on an array of structs |
| Two `explode` calls instead of `flatten` | The batch number itself is needed (explode the outer array with `posexplode`, then the inner one) |

#### Step 4. Validate rows and project the CSV contract

```python
missing = transactions.filter(
    reduce(or_, [F.col(c).isNull() | (F.trim(F.col(c)) == "") for c in REQUIRED_COLUMNS])
).count()
foreign = transactions.filter(F.col("AccountID") != F.col("envelope_account_id")).count()
duplicates = transactions.groupBy("TransactionID").count().filter("count > 1").count()
# each non-zero count raises ValueError

out = transactions.select(
    *REQUIRED_COLUMNS,
    F.lit(batch_ts).alias("meta_batch_timestamp"),
    F.lit(load_ts).alias("meta_load_timestamp"),
    "meta_source_file",
)
```

The write (`overwrite`, Delta) runs only after every check has passed.

**Why the AccountID check:** nesting introduces a new kind of error that a flat
file cannot have, a transaction placed under the wrong parent document.

#### Decision points

| Decision | Options | Choice |
|---|---|---|
| Target table | a) separate table `dbo.ld_oys_transaction_ledger_json`; b) replace `dbo.ld_oys_transaction_ledger` | **a)** by default, so both paths can be compared; set `p_target_table = "dbo.ld_oys_transaction_ledger"` once the source switches to JSON. dbt does not change. |
| Parsing | Python on the driver (`json.loads`) or Spark reader | **Spark with an explicit schema**: it scales, and the schema is a contract |
| Duplicated `TransactionID` | Keep the latest, or fail | **Fail**: Landing does not repair the source silently. Deduplication is a deliberate downstream decision. |
| Malformed line | `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST` | **`FAILFAST`** |
| Batch selection | Same as the CSV loader | **Same** (`p_batch_folder`, empty = latest folder) |

#### Best practices

1. The output column contract is the same as for CSV; only the reader differs.
   The file format is a Landing concern, and dbt must not see it.
2. Keep every leaf a string in Landing, and type in Silver.
3. Validate the parent structure before `explode`. `flatten` and `explode`
   drop data without raising an error.
4. Validate parent/child consistency (the envelope `AccountID`).
5. Write only after all checks have passed.
6. Reconcile a new ingestion path against the old one before switching. The
   last notebook cell compares the JSON table with the CSV table using
   `exceptAll` in both directions.
7. Refresh the SQL analytics endpoint metadata before reading a new table
   through it (see O2).

**Verification run (batch `2026-11-18 07:20:24`):**

| Table | Rows | Distinct TransactionID | SUM(Amount) | Rows only in this table |
|---|---|---|---|---|
| `ld_oys_transaction_ledger_json` | 4,840 | 4,840 | 417,485.75 | 0 |
| `ld_oys_transaction_ledger` (CSV) | 4,840 | 4,840 | 417,485.75 | 0 |

The column names and types are identical as well, so the JSON path can replace
the CSV path without changing dbt.

Continue with dbt setup below. No later section introduces another loader edit.

## Part I. Make dbt work before modelling anything

### Iteration 1. Create an isolated Python environment

**Change:** create a virtual environment at the repository root.

```powershell
python -m venv .venv
```

Use the environment's executable explicitly; activation is optional. This avoids
accidentally using a globally installed dbt.

The versions used in this walkthrough are:

```text
dbt-core==1.12.6
dbt-fabric==1.12.0
```

**Reproducibility step:** these pins were originally recorded in
`dbt_oyster_path/requirements.txt`, but that file is absent in the current
checkout. To recreate the environment, first restore a dependency manifest with
these contents, then install from the repository root:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\dbt_oyster_path\requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\dbt.exe --version
```

**Best practice:** pin tested dependencies; do not add `packages.yml` or run
`dbt deps` before a dbt package is actually needed. The built-in tests used here
require no external package.

**Check:** dbt reports the expected core and Fabric adapter versions.

### Iteration 2. Initialize the dbt project

For a fresh repository, from its root:

```powershell
.\.venv\Scripts\dbt.exe init dbt_oyster_path
```

Do not initialize over the existing project. Keep the generated comments,
resource-path settings, and example model configuration.

**Why:** scaffolding gives us standard folders without premature layer logic.

**Check:** [dbt_project.yml](../dbt_project.yml) has
`name: dbt_oyster_path` and `profile: dbt_oyster_path`.

### Iteration 3. Exclude local and generated files

Add these entries to the repository ignore rules without replacing existing ones:

```gitignore
.venv
.vscode
logs/
~$*
~WRL*.tmp
.user.yml
```

Ensure dbt's generated `target/` and `dbt_packages/` directories are also ignored
by the project's ignore rules. Do not ignore source models or snapshots.

**Why:** environments, logs, build output, and Office lock files are not source
code. Never put credentials or access tokens in Git.

### Iteration 4. Create the Fabric Warehouse and a dev profile

In Fabric, create a Warehouse called `dw_oyster_path` in the intended workspace.
Copy its SQL connection endpoint from Fabric; do not construct the host yourself.
The Lakehouse SQL analytics endpoint supplies landing reads, while the
**Warehouse** is the writable dbt target.

Create [profiles.yml](../profiles.yml) in the project folder. Minimal shape:

```yaml
dbt_oyster_path:
  target: dev
  outputs:
    dev:
      type: fabric
      server: "<Warehouse SQL endpoint copied from Fabric>"
      database: dw_oyster_path
      schema: oyster
      authentication: CLI
      threads: 4
```

`target` and `outputs` are siblings. `dev` is inside `outputs`. The `profile`
setting in the project selects `dbt_oyster_path`; `target` selects an output.

**Adapter-specific note:** the installed dbt-fabric 1.12 uses `mssql-python`.
The current profile still contains `driver` and `port` from the exercise;
those are not the adapter fields selecting the driver/port here.

Authenticate in a terminal that can reach the Warehouse:

```powershell
az login --allow-no-subscriptions
Set-Location .\dbt_oyster_path
..\.venv\Scripts\dbt.exe debug --profiles-dir .
```

All following dbt commands assume the project folder as the working directory.
Use `--profiles-dir .` consistently because our profile is not in the default
user-level location.

**Check:** configuration and connection checks pass. This does not yet validate
landing permissions or prove that models can build.

### Iteration 5. Add a prod output

Add `prod` alongside `dev`, not inside it:

```yaml
    prod:
      type: fabric
      server: "{{ env_var('DBT_PROD_HOST', 'prod-host-not-configured.invalid') }}"
      database: "{{ env_var('DBT_PROD_DATABASE', 'dw_oyster_path') }}"
      schema: oyster
      authentication: CLI
      threads: 4
```

This is a **safer tutorial example**, not an exact copy of the current file.
Currently prod defaults to the same endpoint/database as dev. Configure real
environment separation before any production execution.

**Why the defaults:** dbt renders profile outputs even when another target is
selected. A missing required environment variable can therefore break dev.
A deliberately invalid prod host avoids accidentally connecting to dev.

**Decision:** separate Warehouses/workspaces are recommended for production.
Schema prefixes alone are not a security or deployment boundary.

```powershell
$env:DBT_PROD_HOST = "<production Warehouse SQL endpoint>"
$env:DBT_PROD_DATABASE = "<production Warehouse database>"
# Run only after verifying these point to the intended production environment:
..\.venv\Scripts\dbt.exe debug --target prod --profiles-dir .
```

### Iteration 6. Give snapshots a shared Bronze schema

Append to [dbt_project.yml](../dbt_project.yml), retaining all existing settings:

```yaml
snapshots:
  dbt_oyster_path:
    +schema: bronze
```

**Why:** one shared configuration applies to every snapshot. Do not repeat the
same setting on four entities.

**Important distinction:** the project file configures resource defaults.
Table declarations and column tests belong in resource YAML files, not as table
definitions inside `dbt_project.yml`.

### Iteration 7. Make custom schemas environment-aware

Create [generate_schema_name.sql](../macros/generate_schema_name.sql):

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name | lower == 'prod' -%}
        {{ custom_schema_name | trim | lower }}
    {%- else -%}
        {{ target.name | trim | lower }}_{{ custom_schema_name | trim | lower }}
    {%- endif -%}
{%- endmacro %}
```

| Target | Custom schema | Result |
|---|---|---|
| dev | bronze | dev_bronze |
| prod | bronze | bronze |
| either | absent | profile's `schema`, currently oyster |

**Decision:** we chose `target.name` as the environment parameter and no prefix
for prod. The default dbt schema naming convention is intentionally overridden.

**Check:** `dbt parse --profiles-dir .` passes. After resources exist, inspect
their resolved schema in `target/manifest.json`.

## Part III. Describe the data, then add checks

### Iteration 10. Register all five source tables, without columns or tests

Start [the landing sources file](../models/landing/_landing__sources.yml) with:

```yaml
version: 2

sources:
  - name: landing
    database: "{{ env_var('DBT_LANDING_DATABASE', 'lh_oyster_path') }}"
    schema: dbo
    tables:
      - name: customer
        identifier: ld_oys_customer
      - name: customer_address
        identifier: ld_oys_customeraddress
      - name: account
        identifier: ld_oys_account
      - name: debitcard
        identifier: ld_oys_debitcard
      - name: transaction_ledger
        identifier: ld_oys_transaction_ledger
```

**Why:** first establish that dbt can name and resolve the inputs. A source
declaration does not create its table.

`name` is dbt's logical name; `identifier` is the physical table. For example:

```sql
select *
from {{ source('landing', 'customer') }}
```

resolves to `[lh_oyster_path].[dbo].[ld_oys_customer]`.

```powershell
..\.venv\Scripts\dbt.exe ls --resource-type source --profiles-dir .
..\.venv\Scripts\dbt.exe show --inline "select top 5 * from {{ source('landing', 'customer') }}" --profiles-dir .
```

**Check:** five sources are discovered and the live sample read succeeds.
`dbt show --select` selects executable nodes, so use `--inline` to sample a source.

### Iteration 11. Add column names and descriptions to all sources

Under each table, add its business columns. Initially, do not add tests.

| Source | Business columns |
|---|---|
| customer | CustomerID, FirstName, LastName |
| customer_address | AddressID, CustomerID, AddressType, Country, PostalCode, City, Street, HouseNumber |
| account | AccountID, CustomerID, Balance |
| debitcard | CardID, AccountID, CardTier, IsActive |
| transaction_ledger | TransactionID, AccountID, Amount, Timestamp |

Example addition under `customer`:

```yaml
        description: Customer master data. Current full state, one row per customer.
        columns:
          - name: CustomerID
            description: Business key of the customer.
          - name: FirstName
          - name: LastName
```

**Why:** clarify meaning and keys before enforcing assumptions.
Preserve source spelling and case. In particular, the address CSV prefix is
`CustomerAdress`, while its logical dbt source is `customer_address`.

### Iteration 12. Document types for all columns

Add `data_type: varchar(8000)` to business fields and filename metadata.
Use `datetime2(6)` for load timestamps.

```yaml
          - name: Amount
            data_type: varchar(8000)
            description: Money. Cast to decimal(18,2) in Silver.
          - name: meta_load_timestamp
            data_type: datetime2(6)
          - name: meta_source_file
            data_type: varchar(8000)
```

**Why:** the YAML documents what the SQL endpoint exposes. It does not cast the
Spark data, enforce source types, or prove that a live column is `varchar(8000)`.
Inspect the SQL endpoint schema to confirm actual physical types.

```powershell
..\.venv\Scripts\dbt.exe parse --profiles-dir .
```

## Part V. Turn source assumptions into dbt tests

### Iteration 19. Add key tests to every source

Under the business key column of each table:

```yaml
          - name: CustomerID
            data_type: varchar(8000)
            description: Business key of the customer.
            data_tests:
              - unique
              - not_null
```

Apply identically to `AddressID`, `AccountID`, `CardID`, and `TransactionID`.
That is **one change introducing ten tests**, not ten development iterations.

**Why both layers check keys:** loader checks protect this ingestion route; dbt
tests document assumptions and protect against other writers.
`not_null` does not reject blank strings, and neither test trims whitespace.

### Iteration 20. Add one cross-table batch test

Create [assert_landing_single_batch.sql](../tests/landing/assert_landing_single_batch.sql):

```sql
with batches as (
    select distinct meta_batch_timestamp from {{ source('landing', 'customer') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'customer_address') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'account') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'debitcard') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'transaction_ledger') }}
)
select meta_batch_timestamp
from batches
where (select count(*) from batches) > 1
```

**Why:** a singular dbt test can express a rule across tables. Returning any rows
means failure; default severity is `error`.

**Limits:** it does not detect an empty table contributing no timestamp, or a
single shared NULL timestamp. It also does not prove that extraction was
transactionally consistent. Row-count and timestamp-not-null checks are future
improvements, not implemented guarantees.

```powershell
..\.venv\Scripts\dbt.exe test --select source:landing assert_landing_single_batch --profiles-dir .
```

**Check:** eleven landing tests pass.

**Decision:** defer relationship tests to Silver, where key normalization and
orphan-handling rules are defined. This does not mean every orphan is acceptable.

### Iteration 21. Add one source-level freshness policy

Under `landing`, after `schema: dbo`:

```yaml
    config:
      loaded_at_field: meta_batch_timestamp
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 48, period: hour}
```

**Why:** source-level configuration is inherited by all tables. Batch time
prevents reloading an old extract from looking fresh merely because the loader
ran today.

**Decision:** 24/48 hours suits the exercise's daily schedule, not all workloads.
`meta_load_timestamp` would instead measure ingestion recency.

```powershell
..\.venv\Scripts\dbt.exe source freshness --profiles-dir .
```

Freshness is separate from `dbt test` and is not automatically executed by
`dbt build`. It checks the newest timestamp, not every row, and cannot prove that
no delivery was missed or that batches were processed in order.

## Part VI. Introduce SCD2 history, then improve it

### Iteration 22. Start with one minimal customer snapshot

Create [the snapshot YAML](../snapshots/bronze/_bronze__snapshots.yml) initially as:

```yaml
snapshots:
  - name: bz_oys_customer
    relation: source('landing', 'customer')
    config:
      unique_key: CustomerID
      strategy: check
      check_cols: [FirstName, LastName]
```

This is an instructional intermediate version; do not execute the first
snapshot until the current-row sentinel and deletion choices below are settled.

**Why `check`:** the source has no reliable per-row modification timestamp.
`meta_batch_timestamp` changes for every extract; it is not such a timestamp.

**Why explicit columns:** `check_cols: all` would compare ingestion metadata and
produce artificial versions every load. Extra source columns can be stored, but
do not become change-driving attributes until added deliberately.

**Decision:** use the `timestamp` strategy when a reliable source update
timestamp advances for every relevant change. YAML snapshots are supported here;
SQL snapshot blocks remain another valid form, not something inherently broken.

### Iteration 23. Choose a consistent current-row representation

Add one shared project setting before the first live run:

```yaml
snapshots:
  dbt_oyster_path:
    +schema: bronze
    +dbt_valid_to_current: "cast('9999-12-31' as datetime2(6))"
```

**Why:** current rows are easy to select consistently. An alternative is dbt's
default NULL end date. Do not switch representation on populated snapshots
without planning the migration of existing rows and downstream filters.

### Iteration 24. Use batch observation time for changed versions

Add under the customer snapshot's `config`:

```yaml
      updated_at: meta_batch_timestamp
```

The installed `check` strategy accepts `updated_at`; this does not change it into
the timestamp strategy. Business values still decide whether a row changed.

**Decision:** batch time captures the extract observation rather than the dbt
execution time. Run time is simpler if observation-time reconstruction is not
required.

**Critical constraints:**

- Process observations in order. Never replay an older changed batch directly
  into a populated snapshot.
- Changed data must not reuse an old batch timestamp.
- Freshness does not enforce either rule.
- In the installed Fabric snapshot macros, hard-delete events use snapshot run
  time even when updates use batch time. Reappearance after deletion therefore
  needs care: its batch time must not precede the deletion observation.
- Unchanged rows retain metadata from their last stored version; snapshot
  metadata is not a log of every successful load.

### Iteration 25. Make disappeared entities explicit

Add under snapshot `config`:

```yaml
      hard_deletes: new_record
```

| Setting | Meaning |
|---|---|
| ignore | Missing keys stay current |
| invalidate | Close the previous version, no current tombstone |
| new_record | Close the previous version and create a deletion version |

**Decision:** complete current-state extracts justify `new_record`. Never infer
deletion from a partial event batch. In this adapter, `dbt_is_deleted` is text
`'True'`/`'False'`, not a SQL boolean.

The resulting customer config is:

```yaml
snapshots:
  - name: bz_oys_customer
    description: SCD2 observed history of landing.customer.
    relation: source('landing', 'customer')
    config:
      unique_key: CustomerID
      strategy: check
      check_cols: [FirstName, LastName]
      updated_at: meta_batch_timestamp
      hard_deletes: new_record
```

**Terminology:** a dbt snapshot reads source state but stores detected versions.
It is not a complete physical copy of every row for every batch.

### Iteration 27. Apply the snapshot pattern to the other three entities

Append to the same snapshot YAML:

```yaml
  - name: bz_oys_customer_address
    description: SCD2 history of landing.customer_address.
    relation: source('landing', 'customer_address')
    config:
      unique_key: AddressID
      strategy: check
      check_cols: [CustomerID, AddressType, Country, PostalCode, City, Street, HouseNumber]
      updated_at: meta_batch_timestamp
      hard_deletes: new_record

  - name: bz_oys_account
    description: SCD2 history of landing.account.
    relation: source('landing', 'account')
    config:
      unique_key: AccountID
      strategy: check
      check_cols: [CustomerID, Balance]
      updated_at: meta_batch_timestamp
      hard_deletes: new_record

  - name: bz_oys_debitcard
    description: SCD2 history of landing.debitcard.
    relation: source('landing', 'debitcard')
    config:
      unique_key: CardID
      strategy: check
      check_cols: [AccountID, CardTier, IsActive]
      updated_at: meta_batch_timestamp
      hard_deletes: new_record
```

**Why one iteration:** the algorithm is unchanged; only keys and attributes
differ. No separate SQL models are required for these snapshot tables.

```powershell
..\.venv\Scripts\dbt.exe ls --resource-type snapshot --profiles-dir .
```

**Check:** four snapshots discovered, targeting `dev_bronze` in dev.

## Part VII. Treat immutable transaction events differently

### Iteration 28. Choose event accumulation instead of SCD2

We deliberately kept the transaction ledger incremental after comparing:

| Approach | Correct when |
|---|---|
| Append | Events are immutable; corrections are separate events |
| Merge | Existing rows may change; only latest values are required |
| Snapshot | Existing rows may change; observed versions must be retained |
| Full rebuild | All necessary history is available and rebuild cost is acceptable |

**Generator finding:** it uses a fixed random seed and fixed anchor date, resets
IDs to the same start, and regenerates the same sample events. Those recurring
IDs are not new events requiring new IDs. We chose to keep that deterministic
behaviour.

Changing the anchor date, seed, or upstream generation can break that identity
assumption. New business events need globally stable, non-reused event IDs.

### Iteration 29. Add append with an existing-ID filter

Create [bz_oys_transaction_ledger.sql](../models/bronze/bz_oys_transaction_ledger.sql).
At this iteration its config does not yet include the later full-refresh guard:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='bronze',
    alias='bz_oys_transaction_ledger'
) }}

with source_transactions as (
    select *
    from {{ source('landing', 'transaction_ledger') }}
)

select source_transactions.*
from source_transactions

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} as existing
    where existing.TransactionID = source_transactions.TransactionID
)
{% endif %}
```

**Why:** append alone would duplicate recurring inputs. The filter skips
already-loaded IDs. It also allows late-arriving events with old business times;
there is no maximum-timestamp watermark excluding them.

**Limits:** no automatic uniqueness enforcement; duplicate new IDs in the input
can both be inserted. Concurrent writers can race. Run landing tests first and
serialize ledger writes. Checking IDs against a growing target still costs
query work, even though it avoids rewriting old events.

### Iteration 30. Protect accumulated events from accidental full refresh

Add to the model's `config`:

```sql
    full_refresh=false,
```

**Why:** current landing is not a complete archive. A rebuild from just the
latest batch may discard older accumulated transactions.

This makes the model ignore `--full-refresh`; it does not prevent manual table
deletion. A deliberate recovery requires a planned rebuild from retained files
and controlled replay, not simply removing the guard and running the latest batch.

### Iteration 31. Detect same-ID/different-values conflicts

Create [assert_transaction_events_immutable.sql](../tests/bronze/assert_transaction_events_immutable.sql):

```sql
select incoming.TransactionID
from {{ source('landing', 'transaction_ledger') }} as incoming
inner join {{ ref('bz_oys_transaction_ledger') }} as stored
    on incoming.TransactionID = stored.TransactionID
where exists (
    select incoming.AccountID, incoming.Amount, incoming.[Timestamp]
    except
    select stored.AccountID, stored.Amount, stored.[Timestamp]
)
```

**Why:** append otherwise silently ignores corrected existing IDs. `EXCEPT`
provides NULL-aware comparison without substituting a fake business value.
Comparison still follows the Warehouse's string/collation semantics; it is not
a byte-for-byte raw payload audit.

**Execution rule:** after initial table creation, run this test explicitly
before subsequent ledger loads, and stop if it fails. Its `ref` dependency means
a normal `dbt build` does not turn it into a pre-load gate automatically.

## Part VIII. Test the stored history

### Iteration 32. Add generic snapshot version tests to all four snapshots

Under each snapshot, alongside `config`, add `columns`:

```yaml
    columns:
      - name: CustomerID
        data_tests:
          - not_null
      - name: dbt_scd_id
        data_tests:
          - unique
          - not_null
```

Substitute `AddressID`, `AccountID`, or `CardID` as appropriate.
This shared change introduces twelve tests.

**Why:** historical business keys repeat by design. Do not add a plain
business-key `unique` test across all snapshot versions. `dbt_scd_id` identifies
the version row.

### Iteration 33. Check that each snapshot key has at most one current version

Create [assert_bronze_current_keys_unique.sql](../tests/bronze/assert_bronze_current_keys_unique.sql):

```sql
with current_versions as (
    select 'customer' as entity_name, cast(CustomerID as varchar(8000)) as business_key
    from {{ ref('bz_oys_customer') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))

    union all
    select 'customer_address', cast(AddressID as varchar(8000))
    from {{ ref('bz_oys_customer_address') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))

    union all
    select 'account', cast(AccountID as varchar(8000))
    from {{ ref('bz_oys_account') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))

    union all
    select 'debitcard', cast(CardID as varchar(8000))
    from {{ ref('bz_oys_debitcard') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))
)
select entity_name, business_key, count(*) as current_row_count
from current_versions
group by entity_name, business_key
having business_key is null or count(*) > 1
```

A current deletion tombstone counts too. The invariant is “at most one current
version,” not “at most one current non-deleted version.”

**Limit:** this test does not detect missing current rows, invalid date ranges,
overlapping historical intervals, or out-of-order input.

### Iteration 34. Add ledger output-key tests

Create [the Bronze model YAML](../models/bronze/_bronze__models.yml):

```yaml
version: 2

models:
  - name: bz_oys_transaction_ledger
    description: Append-only transaction events captured from landing.
    columns:
      - name: TransactionID
        data_tests:
          - unique
          - not_null
```

The current file also documents full-refresh and pre-load immutability rules.
The ledger key is unique across its stored events, unlike an SCD2 business key.

**Expected final inventory:** 5 sources, 4 snapshots, 1 model, 27 data tests.
Freshness checks are separate from that data-test count.

## Part IX. Execute deliberately and observe the outcome

### Iteration 35. Create all five Bronze tables

Run the generator and loader in Fabric with the correct Lakehouse attached.
For recurring event ingestion, process every required delivery, not merely
whatever folder happens to be greatest when the pipeline runs.

From the project directory, execute each command separately and stop on failure:

```powershell
..\.venv\Scripts\dbt.exe debug --profiles-dir .
..\.venv\Scripts\dbt.exe source freshness --profiles-dir .
..\.venv\Scripts\dbt.exe test --select source:landing assert_landing_single_batch --profiles-dir .
..\.venv\Scripts\dbt.exe snapshot --profiles-dir .
..\.venv\Scripts\dbt.exe run --select bz_oys_transaction_ledger --profiles-dir .
..\.venv\Scripts\dbt.exe test --profiles-dir .
```

For **subsequent** runs, insert this command before the ledger `run`:

```powershell
..\.venv\Scripts\dbt.exe test --select assert_transaction_events_immutable --profiles-dir .
```

It cannot run before the ledger table exists for the first time.
Plainly pasting all commands into PowerShell does not stop on native executable
failure. Run manually one by one, or gate a script explicitly:

```powershell
& ..\.venv\Scripts\dbt.exe source freshness --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw "Freshness failed; stopping before Bronze writes." }
```

Apply the same exit-code check after each automated step. Prevent overlapping
pipeline runs and concurrent landing replacements while dbt is reading.

**Expected first-run results:**

- Four snapshot tables and one incremental ledger exist in `dev_bronze`.
- Snapshot rows are current, with `dbt_is_deleted = 'False'`.
- Current snapshot end dates use the sentinel.
- All 27 data tests pass.

### Iteration 36. Inspect rather than trusting a green command

In the Warehouse SQL editor, for a dev target:

```sql
select CustomerID, FirstName, LastName,
       meta_batch_timestamp, dbt_valid_from, dbt_valid_to, dbt_is_deleted
from dev_bronze.bz_oys_customer
order by CustomerID, dbt_valid_from;

select count(*) as row_count,
       count(distinct TransactionID) as distinct_ids
from dev_bronze.bz_oys_transaction_ledger;
```

Use `bronze` instead of `dev_bronze` only when intentionally inspecting prod.
Check all four snapshot tables, not only customer.

### Iteration 37. Prove repeat-run safety without changing data

**Starting point:** deterministic data is already loaded into the Lakehouse
Delta tables. Do not clear landing or Bronze. Existing data is exactly what
allows us to test repeat-run behaviour.

First establish a successful Bronze baseline using iteration 35. If the ledger
already exists, run its immutability test before loading it. Record all five
counts in the dev Warehouse SQL editor:

```sql
select 'customer' as entity, count(*) as stored_rows
from dev_bronze.bz_oys_customer
union all
select 'customer_address', count(*)
from dev_bronze.bz_oys_customer_address
union all
select 'account', count(*)
from dev_bronze.bz_oys_account
union all
select 'debitcard', count(*)
from dev_bronze.bz_oys_debitcard
union all
select 'transaction_ledger', count(*)
from dev_bronze.bz_oys_transaction_ledger;
```

These are total stored rows, including historical snapshot versions, not only
current rows. Keep the results for comparison.

Now run the deterministic generator and loader again. A new delivery folder
and new ingestion metadata should not change the business values. Execute
these commands from the dbt project directory, one at a time, stopping on failure:

```powershell
..\.venv\Scripts\dbt.exe source freshness --profiles-dir .
..\.venv\Scripts\dbt.exe test --select source:landing assert_landing_single_batch --profiles-dir .
..\.venv\Scripts\dbt.exe test --select assert_transaction_events_immutable --profiles-dir .
..\.venv\Scripts\dbt.exe snapshot --profiles-dir .
..\.venv\Scripts\dbt.exe run --select bz_oys_transaction_ledger --profiles-dir .
..\.venv\Scripts\dbt.exe test --profiles-dir .
```

Rerun the count query and compare it with the baseline.

**Expected:** no extra snapshot versions and no extra ledger events. Unchanged
snapshot metadata remains unchanged too.

**Why:** snapshots compare explicit business columns rather than load metadata;
the ledger filters out IDs already present. An additional useful check is to
rerun dbt against the exact same landing batch without rerunning the generator
or loader. Counts must remain unchanged in that case too.

This proves the tested sequential repeat case, not exactly-once delivery under
concurrent writers or arbitrary failures. It does not yet prove that genuine
changes or new events are captured.

### Iteration 38. Demonstrate one real business change

This is a controlled **future verification exercise**, not a data change already
performed. The generator supports it through its `p_simulate_changes` parameter
cell (default `0` = baseline):

1. Run the generator with `p_simulate_changes = 1`. It creates a new complete
   delivery folder with a strictly later batch timestamp and, compared with the
   baseline, changes only:
   - customer `1001` (Eszter): `LastName` `Kovacs` -> `Nagy`;
   - the Permanent address of customers `1101` (`AddressID` 3193) and `1102`
     (`AddressID` 3194): new postal code, city, street, and house number.
   Every other entity, key, and transaction is identical to the baseline.
2. Load that delivery and pass landing tests/freshness.
3. Snapshot and inspect the changed customer and addresses.

Inspect the deliberately changed customer in the Warehouse:

```sql
select CustomerID, FirstName, LastName,
       dbt_valid_from, dbt_valid_to, dbt_is_deleted
from dev_bronze.bz_oys_customer
where CustomerID = '1001'
order by dbt_valid_from;
```

Use the actual chosen key if it is not `1001`.

Inspect the moved addresses the same way:

```sql
select AddressID, CustomerID, PostalCode, City, Street, HouseNumber,
       dbt_valid_from, dbt_valid_to, dbt_is_deleted
from dev_bronze.bz_oys_customer_address
where AddressID in ('3193', '3194')
order by AddressID, dbt_valid_from;
```

**Expected:** one additional version for that customer (two versions if there
was only one before the test), and likewise for each of the two addresses. The
old version closes at the new batch observation time; the new version is
current. The other snapshots do not gain versions merely because their load
metadata changed.

Record the counts before and after: the customer snapshot increases by exactly
one, the address snapshot by exactly two, and other snapshot counts stay
unchanged. Run snapshots again without changing landing; no further versions
should appear. Running the generator again with `p_simulate_changes = 0`
reverts these values, which produces another version for the same three keys.

Do not mutate an already-snapshotted delivery and reuse its timestamp.
Because the generator is deterministic, rerunning it alone is not proof of
business-change history. Do not change the random seed just to manufacture one
change: that could alter many unrelated rows.

### Iteration 39. Demonstrate deletion and event conflict separately

**Snapshot deletion:** in another complete delivery, remove one customer, keeping
the table non-empty. After validation and snapshotting, inspect the closed row
and current `'True'` tombstone. Its deletion time follows dbt execution time.
Keep related test data consistent, or explicitly document intended orphans.

**Ledger conflict:** in controlled dev input, change the amount for an existing
transaction ID and run only:

```powershell
..\.venv\Scripts\dbt.exe test --select assert_transaction_events_immutable --profiles-dir .
```

It should fail; do not proceed to the load. The stored Bronze event must remain
unchanged. Restore a valid later delivery before continuing.

**New ledger event:** in another complete controlled delivery, preserve existing
transactions and add one event with a genuinely new, unused `TransactionID`.
Record the ledger count. Run the loader and landing validation, then:

```powershell
..\.venv\Scripts\dbt.exe test --select assert_transaction_events_immutable --profiles-dir .
..\.venv\Scripts\dbt.exe run --select bz_oys_transaction_ledger --profiles-dir .
```

Stop if either command fails. The ledger count should increase by exactly one.
Repeat the test and model run against unchanged landing; the count must not
increase again. Finish with the full dbt test suite.

| Test scenario | Expected snapshot effect | Expected ledger effect |
|---|---|---|
| Same deterministic business data, new load metadata | No new versions | No new events |
| Same landing batch processed twice | No new versions | No new events |
| One changed customer surname | One additional customer version | No new events |
| One new transaction ID | No dimension versions if dimensions unchanged | One additional event, once |
| Changed amount for an existing transaction ID | Not applicable | Immutability test fails before load |

Use dev data only. Keep complete deliveries and process their timestamps in
order. Immediately regenerating the unchanged deterministic sample after a
deliberate edit would undo that edit in landing; snapshotting it could record
an unintended reversal. Restore test data intentionally through a later valid
delivery instead.

Never use `--full-refresh` or delete Bronze tables as a shortcut to make these
tests pass. No live data mutations or successful live results are implied by
these instructions.

## Part X. Build the Silver Kimball star incrementally

**Approved direction:** preserve the existing Bronze history and derive
business SCD2 dimension versions from it. Do not add a second snapshot layer
over current state or remove the Bronze snapshots.

The proposed star has one transaction fact row per `TransactionID`:

| Model | Intended grain and responsibility |
|---|---|
| dim_customer | One business version of a customer, including selected address attributes |
| dim_account | One business version of an account's descriptive attributes |
| dim_debitcard | One business version of a card |
| dim_date | One calendar date |
| dim_time | One second of the day, reusable across dates |
| fct_transaction | One immutable transaction event with applicable dimension keys |

These are planned models, not tables already created by this documentation.
Use separate date and time dimensions, retaining the complete transaction
timestamp on the fact. A combined date-by-second dimension would grow
unnecessarily large.

### Iteration 40. Configure the Silver model folder once

Under `models.dbt_oyster_path` in the project configuration, preserve the
existing example settings and add:

```yaml
    silver:
      +schema: silver
      +materialized: table
      staging:
        +materialized: view
```

**Why:** every model under `models/silver` receives the same schema and default
table materialization. Models under its `staging` subfolder use views instead.
This is one shared change, not one change per future dimension.

The existing environment schema macro resolves `dev_silver` for dev and `silver`
for prod. The configuration alone creates neither a schema nor tables.

**Initial materialization decision:** rebuild derived Silver tables from retained
Bronze history rather than introducing another incremental state machine.
Version surrogate keys must be deterministic and stable across rebuilds.

**Check:**

```powershell
..\.venv\Scripts\dbt.exe parse --profiles-dir .
```

An unused Silver configuration warning is expected until the first Silver model
is added. It does not verify physical Warehouse creation or SQL execution.

### Next small iterations and modelling decisions

1. Add typed staging projections over all five Bronze entities as one shared
   pattern; preserve raw values needed to expose conversion errors.
2. Add conversion/key validation tests before dimension joins.
3. Decide customer address denormalization: the sample includes Permanent,
   Mailing, and Billing addresses. Select an explicit business rule, and reject
   ambiguous matches rather than taking an arbitrary address.
4. Derive customer version boundaries from both customer and selected-address
   history, then coalesce adjacent equal business states. Joining only current
   addresses would misrepresent historical customer versions.
5. Derive account and card dimensions with explicit SCD2 attributes. Balance is
   a time-varying measure, not a descriptive dimension attribute; decide its
   separate fact/grain rather than silently discarding it.
6. Add calendar and time-of-day dimensions, including explicit coverage and
   unknown-member policies.
7. Build the transaction fact with temporal dimension lookups and reconciliation
   checks ensuring joins do not multiply transactions or amounts.
8. Add shared dimension version tests, relationship tests, and controlled
   change/rebuild demonstrations.

**Open decisions before those implementations:**

- Transactions have `AccountID`, but no `CardID`. Do not infer the paying card
  by joining every card for an account; that can duplicate transaction measures.
  A card dimension needs a separate defensible analytical relationship or fact.
- Transaction event times can precede the earliest observed dimension version.
  Choose an explicit unknown-member or other approved inference policy; do not
  present observed history as known business history before observation began.
- Bronze version times represent observations. Temporal joins need documented
  half-open intervals (`valid_from <= time AND time < valid_to`), explicit delete
  handling, and non-overlap checks.
- Building Silver SCD2 from Bronze must preserve changes between Silver runs,
  not merely compare the latest current rows.

### Iteration 41. Add typed staging views for all five Bronze entities

**Change:** create five models under `models/silver/staging`, with descriptions
in [the staging YAML](../models/silver/staging/_silver__staging.yml):

| Model | Typed/normalized business fields |
|---|---|
| [stg_oys_customer](../models/silver/staging/stg_oys_customer.sql) | customer_id, first_name, last_name |
| [stg_oys_customer_address](../models/silver/staging/stg_oys_customer_address.sql) | address_id, customer_id, address_type, country, postal_code, city, street, house_number |
| [stg_oys_account](../models/silver/staging/stg_oys_account.sql) | account_id, customer_id, balance |
| [stg_oys_debitcard](../models/silver/staging/stg_oys_debitcard.sql) | card_id, account_id, card_tier, is_active |
| [stg_oys_transaction_ledger](../models/silver/staging/stg_oys_transaction_ledger.sql) | transaction_id, account_id, amount, transaction_timestamp |

This is one iteration because all five apply the same staging responsibility:
rename, normalize, convert, and preserve lineage without joining or filtering.
The shared folder configuration creates views in `dev_silver` for dev.

#### Why staging views instead of going directly to dimensions?

These are **views, not additional stored tables**. They separate two
responsibilities:

1. **Staging:** clean and type individual Bronze entities while preserving their
   history and lineage.
2. **Dimensions and facts:** join entities, denormalize attributes, and construct
   business SCD2 versions and fact relationships.

```text
Bronze account history
    -> staging view: trim IDs, convert Balance, preserve every version
    -> Silver dimension: determine descriptive SCD2 versions
```

**Benefits in this project:**

- Customer SCD2 combines customer and address history. Keeping conversion logic
  separate makes the temporal joins easier to understand and test.
- Conversion failures can be inspected before dimension construction.
- Cleaned account data can be reused by an account dimension and a balance fact
  without duplicating conversion expressions.
- Bronze remains unchanged and available for replay.

**Trade-off:** five view definitions add another dependency layer to navigate.
Views do not persist a second copy of the data or cache the conversion results;
queries still execute their underlying logic.

**Decision point:** Kimball and SCD2 do not require staging views. The same
transformations could live in CTEs inside dimension and fact models. We chose
lightweight views because historical joins will be complex and the cleaned
inputs are reusable. They are an implementation convenience, not another
business layer or a prerequisite for historization.

**IDs remain text:** use `nullif(trim(CustomerID), '')`, not a numeric cast.
Leading zeros can be meaningful. Preserve the original value as
`customer_id_raw` (and equivalent raw columns for every key/reference).
Trimming can collapse distinct raw keys; validation is required before joins.

**Example: the complete transaction staging projection**

```sql
select
    TransactionID as transaction_id_raw,
    nullif(trim(TransactionID), '') as transaction_id,
    AccountID as account_id_raw,
    nullif(trim(AccountID), '') as account_id,
    Amount as amount_raw,
    try_convert(decimal(18,2), nullif(trim(Amount), '')) as amount,
    [Timestamp] as transaction_timestamp_raw,
    try_convert(
        datetime2(6), nullif(trim([Timestamp]), ''), 120
    ) as transaction_timestamp,
    meta_batch_timestamp,
    meta_load_timestamp,
    meta_source_file
from {{ ref('bz_oys_transaction_ledger') }}
```

Balance uses the same decimal conversion. Invalid conversions produce NULL,
not zero or another valid-looking fallback. Decimal conversion can round excess
fractional digits, so successful conversion alone is not a precision-quality
test. The original amount/balance strings remain available for diagnostics.

Transaction timestamps use the generator's format, with explicit style 120.
No timezone conversion is performed or inferred from this timezone-less field.
Date/time dimension design must establish the business timezone convention.

**Boolean conversion:** card activity maps case-normalized `true`/`1` to bit 1
and `false`/`0` to bit 0; all other values become NULL. The raw activity value is
retained. Do not equate an inactive card with a deleted card.

**History preservation:** the four snapshot-backed views retain every row,
including tombstones, with:

```sql
dbt_scd_id as bronze_version_id,
dbt_valid_from as observed_valid_from,
dbt_valid_to as observed_valid_to,
dbt_is_deleted as source_deleted_raw,
case lower(trim(dbt_is_deleted))
    when 'true' then cast(1 as bit)
    when 'false' then cast(0 as bit)
    else cast(null as bit)
end as is_deleted
```

They also retain all three ingestion metadata fields. No `is_current` filter,
deduplication, or history coalescing is introduced here. Bronze version IDs are
lineage identifiers, not yet Silver dimension surrogate keys.

**Checks:**

```powershell
..\.venv\Scripts\dbt.exe parse --profiles-dir .
..\.venv\Scripts\dbt.exe ls --select path:models/silver/staging --resource-type model --profiles-dir .
```

On an authenticated, approved network, after Bronze exists:

```powershell
..\.venv\Scripts\dbt.exe run --select path:models/silver/staging --profiles-dir .
```

**Expected:** five views, with the same row counts as their Bronze inputs.
View creation and parsing do not prove valid conversions. These views are
inspectable preparation, not approved inputs to a published star yet.

### Iteration 42. Validate staging before building dimensions

Add these tests to [the staging YAML](../models/silver/staging/_silver__staging.yml).
This is one shared validation change across all five views, not five separate
modelling steps.

| View | Required normalized/converted columns (`not_null`) |
|---|---|
| Customer | customer_id |
| Address | address_id, customer_id, address_type |
| Account | account_id, customer_id, balance |
| Debit card | card_id, account_id, is_active |
| Transactions | transaction_id, account_id, amount, transaction_timestamp |

Example:

```yaml
      - name: amount
        description: Amount converted to decimal(18,2); failed conversion becomes NULL.
        data_tests: [not_null]
```

**Why:** blank normalized keys, unconvertible money/timestamps, and unrecognized
activity flags must produce visible test failures. We require these fields
for this exercise; descriptive fields such as surname and postal code do not
become mandatory without a business requirement.

For each snapshot-backed staging view, add `unique` and `not_null` on
`bronze_version_id`, plus `not_null` on `observed_valid_from`,
`observed_valid_to`, and `is_deleted`. Test normalized `transaction_id` for
uniqueness in the ledger view. Do not test historical dimension business keys
for uniqueness across versions.

Create [the shared interval test](../tests/silver/assert_staging_history_intervals.sql).
It combines all four histories and applies this window:

```sql
max(observed_valid_to) over (
    partition by entity_name, business_key
    order by observed_valid_from, bronze_version_id
    rows between unbounded preceding and 1 preceding
) as previous_max_end
```

Fail rows where:

```sql
observed_valid_from >= observed_valid_to
or observed_valid_from < previous_max_end
```

**Why:** reject zero/negative intervals and overlapping versions before temporal
joins can duplicate facts. Comparing against the maximum preceding end also
detects nested overlaps that a simple `lag` can miss. Adjacent intervals are
valid under half-open interval semantics. Tombstones participate in validation.

This catches overlapping histories collapsed onto the same trimmed key; it
does not prove that distinct raw keys with disjoint intervals identify the same
business entity. Missing timestamps are handled by the separate `not_null`
tests. Decimal rounding/scale checks and relationship rules remain future work.

Run after Bronze and the five staging views exist:

```powershell
..\.venv\Scripts\dbt.exe test --select path:models/silver/staging assert_staging_history_intervals --profiles-dir .
```

**Expected:** 36 Silver staging tests pass (35 generic tests and one interval
test). Severity defaults to `error`. Stop on failure before building dimensions.
Parsing or test discovery is not evidence that these data checks passed live.

### Iteration 43. Establish the address contract before denormalization

**Approved dimension layout:** include Permanent, Mailing, and Billing address
attributes as three separate column groups in `dim_customer`. An absent type
remains NULL; do not silently substitute another address type.

```text
customer_id, first_name, last_name
permanent_country, permanent_postal_code, permanent_city, ...
mailing_country, mailing_postal_code, mailing_city, ...
billing_country, billing_postal_code, billing_city, ...
```

**Why:** retain all available types while keeping one row per customer version.
A direct one-to-many address join could multiply transaction amounts.

Add a built-in `accepted_values` test to `address_type`:

```yaml
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['Permanent', 'Mailing', 'Billing']
```

Staging trims but does not case-normalize address types; comparisons follow
Warehouse collation semantics. Unexpected labels must be investigated rather
than silently ignored during denormalization.

Create [the customer/address-type interval test](../tests/silver/assert_customer_address_type_intervals.sql).
For non-deleted address versions, calculate:

```sql
max(observed_valid_to) over (
    partition by customer_id, address_type
    order by observed_valid_from, bronze_version_id
    rows between unbounded preceding and 1 preceding
) as previous_max_end
```

Fail rows where `observed_valid_from < previous_max_end`.

**Why:** the existing interval test partitions by address ID. This additional
rule rejects two address IDs occupying the same customer's same address type
at the same instant, as well as overlapping versions of one address. Adjacent
intervals remain valid. Deleted-address tombstones do not occupy address slots.

This does not require every customer to have all three address types or prove
customer referential integrity. The separate interval/not-null tests still
validate basic history integrity and required fields.

```powershell
..\.venv\Scripts\dbt.exe test --select path:models/silver/staging assert_staging_history_intervals assert_customer_address_type_intervals --profiles-dir .
```

**Expected:** 38 Silver staging tests pass, including the two new contract
checks. Stop on failure; never resolve ambiguity by taking an arbitrary address.

**Next dimension algorithm:** combine customer and address interval boundaries,
resolve the three address groups at each boundary, and coalesce adjacent equal
business states. Changes to any included address attribute can produce a
customer SCD2 version; load-metadata-only differences must not.

### Iteration 44. Construct the customer history timeline

Create [int_customer_intervals](../models/silver/intermediate/int_customer_intervals.sql)
as a view. Before joining attributes, combine boundary dates from both histories:

```sql
{{ config(materialized='view') }}

with boundaries as (
    select customer_id, observed_valid_from as boundary_at
    from {{ ref('stg_oys_customer') }}
    union
    select customer_id, observed_valid_to
    from {{ ref('stg_oys_customer') }}
    union
    select customer_id, observed_valid_from
    from {{ ref('stg_oys_customer_address') }}
    union
    select customer_id, observed_valid_to
    from {{ ref('stg_oys_customer_address') }}
),
intervals as (
    select
        customer_id,
        boundary_at as valid_from,
        lead(boundary_at) over (
            partition by customer_id order by boundary_at
        ) as valid_to
    from boundaries
)
select intervals.customer_id, intervals.valid_from, intervals.valid_to
from intervals
where intervals.valid_to is not null
  and exists (
      select 1
      from {{ ref('stg_oys_customer') }} as customer
      where customer.customer_id = intervals.customer_id
        and intervals.valid_from >= customer.observed_valid_from
        and intervals.valid_from < customer.observed_valid_to
  )
```

**Why:** customer-only version dates would miss an address addition, removal,
or attribute change. `union` deduplicates simultaneous boundaries; `lead`
constructs the candidate half-open periods. Address ends matter as much as starts.

The `exists` condition retains only intervals covered by an observed customer
version, including tombstones. An earlier address cannot invent earlier customer
history, and gaps in customer observation coverage remain gaps. Partitioning by
customer prevents one customer's changes from splitting another customer's history.

These are not final dimension rows. The next step resolves customer attributes
and Permanent/Mailing/Billing addresses for each candidate interval, followed by
coalescing adjacent periods with identical business state.

**Preconditions:** staging interval, required-field, and address ambiguity tests
must pass. The view constructs boundaries; it does not repair invalid histories
or choose between overlapping address versions.

```powershell
..\.venv\Scripts\dbt.exe run --select int_customer_intervals --profiles-dir .
```

Run on the approved Fabric network after the staging views exist.
Acceptance cases include simultaneous customer/address changes, address removal,
customer coverage gaps, retained deletion periods, and separate customers.
Local fixture verification is not a substitute for live Fabric SQL validation.

### Iteration 45. Resolve customer attributes and all three address groups

Create [int_customer_states](../models/silver/intermediate/int_customer_states.sql)
as a view over the candidate timeline. Keep one state per customer and interval.
The model uses Jinja to apply this address join identically to Permanent,
Mailing, and Billing:

```sql
left join {{ ref('stg_oys_customer_address') }} as permanent
    on permanent.customer_id = timeline.customer_id
   and permanent.address_type = 'Permanent'
   and permanent.is_deleted = 0
   and timeline.valid_from >= permanent.observed_valid_from
   and timeline.valid_from < permanent.observed_valid_to
```

Resolve customer names and `is_deleted` through an inner temporal join to
customer staging using the same start-inclusive/end-exclusive comparisons.
The timeline already includes every relevant boundary, so state is constant
within each candidate interval, provided the staging history tests pass.

Project these address fields for each prefix (`permanent_`, `mailing_`,
`billing_`): country, postal_code, city, street, and house_number.
For example:

```sql
permanent.country as permanent_country,
permanent.postal_code as permanent_postal_code,
permanent.city as permanent_city,
permanent.street as permanent_street,
permanent.house_number as permanent_house_number
```

**Why left joins:** a missing type or a deleted address produces NULL fields
without dropping its customer. Do not select an arbitrary matching row if
addresses overlap; run the address contract tests before this model.

**Approved deletion semantics:** preserve customer tombstones and the names
carried by Bronze. Resolve address histories independently, even during customer
deletion intervals. Do not invent address deletions from customer deletion.
Consumers must honor customer `is_deleted`.

Address IDs and ingestion metadata are not business attributes in this
projection. Replacing an address ID with identical address attributes does not
by itself need a new final customer business version. Candidate intervals can
still split there; later coalescing will remove redundant adjacent states.

Add [state uniqueness](../tests/silver/assert_customer_states_unique.sql) and
[timeline reconciliation](../tests/silver/assert_customer_states_reconcile.sql)
tests. The first rejects duplicate `(customer_id, valid_from)` rows. The second
combines expected and actual intervals and requires exactly one of each for
every `(customer_id, valid_from, valid_to)`, detecting lost, multiplied, extra,
or altered intervals.

```powershell
..\.venv\Scripts\dbt.exe run --select int_customer_states --profiles-dir .
..\.venv\Scripts\dbt.exe test --select assert_customer_states_unique assert_customer_states_reconcile --profiles-dir .
```

Run after upstream views exist and their validation tests pass. Local fixtures
cover missing types, address changes/removal, boundary equality, tombstones, and
the reconciliation failure cases; live Fabric execution remains a separate check.

**Next:** coalesce adjacent equal states and assign deterministic dimension
version keys. This intermediate view is not yet the final SCD2 dimension.

### Iteration 46. Materialize the customer business SCD2 dimension

Create [dim_customer](../models/silver/dim_customer.sql), inheriting Silver's
table materialization. It contains customer names, all three address groups,
`customer_id`, `customer_key`, `valid_from`, `valid_to`, `is_current`, and
`is_deleted`. No unknown member is introduced yet.

**First identify a business change:** use `lag` for each included attribute and
the preceding interval's end. These attributes are first/last name, customer
deletion state, and country/postal_code/city/street/house_number for Permanent,
Mailing, and Billing addresses. Compare previous and current attribute tuples
using NULL-aware `EXCEPT`. Start a new group unless the tuples match and
`previous_valid_to = valid_from`.

**Then merge each contiguous group:** a running sum of change markers assigns
the group; `min(valid_from)` and `max(valid_to)` produce the version range.
Take attributes from the group's first state. Never group globally by business
attributes: A -> B -> A must remain three separate versions. Identical states
on opposite sides of a coverage gap must also remain separate.

**Stable version key:**

```sql
convert(
    varchar(64),
    hashbytes(
        'SHA2_256',
        concat(
            len(versions.customer_id), ':', versions.customer_id, ':',
            convert(varchar(27), cast(versions.valid_from as datetime2(6)), 126)
        )
    ),
    2
) as customer_key
```

The length-prefixed normalized customer ID and explicit-format version start
form an unambiguous input. For unchanged input/history, rebuilding produces
the same key. This is a text hash surrogate, not an incrementing integer.
No external package is needed.

**Historical corrections:** inserting older observations or changing business
rules can alter version boundaries and therefore keys. Coordinate rebuilding
dependent facts; deterministic keys do not make arbitrary backfills harmless.

**Current versus active:** `is_current` is bit 1 only when the end is the
configured `9999-12-31` sentinel. A current tombstone is both current and deleted.
Do not confuse “latest observed row” with “currently active customer.”

Add seven generic tests in
[the dimension YAML](../models/silver/_silver__dimensions.yml): unique/non-null
version key and non-null customer ID, boundaries, and flags. Add
[interval validation](../tests/silver/assert_dim_customer_intervals.sql) and
[current-key uniqueness](../tests/silver/assert_dim_customer_current_unique.sql)
as two singular tests. Nullability tests complement, not replace, temporal tests.

Run all upstream history/address/state validations first. After their views
exist and tests pass, execute:

```powershell
..\.venv\Scripts\dbt.exe run --select dim_customer --profiles-dir .
..\.venv\Scripts\dbt.exe test --select dim_customer --profiles-dir .
```

**Expected:** nine customer dimension tests pass. Local fixture checks cover
adjacent equality (including NULL), attribute changes, A -> B -> A, gaps,
address changes, deletion transitions, and empty input. Native Fabric hashing,
table creation, and live data tests still require a Fabric-connected run.

### Iteration 47. Apply business SCD2 to accounts and debit cards

Create [dim_account](../models/silver/dim_account.sql) and
[dim_debitcard](../models/silver/dim_debitcard.sql). These are rebuilt Silver
tables derived from complete Bronze observation histories, not new snapshots.

| Model | Business key | Attributes defining versions |
|---|---|---|
| dim_account | account_id | customer_id, is_deleted |
| dim_debitcard | card_id | account_id, card_tier, is_active, is_deleted |

**Why exclude balance:** money is a measure rather than an account description.
A balance-only change in Bronze must not create another account dimension
version. Preserve balance history in staging until its fact grain is decided.
An ownership change, by contrast, creates a new account version.

Both use [coalesce_observed_dimension](../macros/coalesce_observed_dimension.sql),
a shared macro applying the established customer coalescing algorithm directly
to staging observation intervals:

```sql
{{ coalesce_observed_dimension(
    relation=ref('stg_oys_account'),
    business_key='account_id',
    version_key='account_key',
    attributes=['customer_id', 'is_deleted']
) }}
```

```sql
{{ coalesce_observed_dimension(
    relation=ref('stg_oys_debitcard'),
    business_key='card_id',
    version_key='debitcard_key',
    attributes=['account_id', 'card_tier', 'is_active', 'is_deleted']
) }}
```

**Why a shared macro now:** two models require the exact same algorithm with
different keys/attributes. Reuse that algorithm rather than copying it twice.
The existing customer model is unchanged because it already consumes its
denormalized customer-state timeline.

The macro projects only the selected descriptive attributes, compares adjacent
tuples with NULL-aware `EXCEPT`, and coalesces only touching equal intervals.
It preserves A -> B -> A and coverage gaps. Version keys use the same SHA-256,
length-prefixed business key plus explicit-format version start as customer.
Keys are stable for unchanged history; historical corrections require
coordinated downstream rebuilds.

**Relationships:** customer/account IDs in these tables are source business
references, not dimension surrogate foreign keys. Do not copy customer names
into account/card or join dimensions to other dimensions at this step. Future
facts resolve dimension keys directly. In particular, transaction data lacks
CardID, so these dimensions do not invent a card-to-transaction relationship.

**Deletion and activity:** tombstones remain included; `is_current` denotes an
open-ended version, not an active entity. Card `is_active` and `is_deleted`
remain independent flags.

Add eight generic tests for account and nine for debit card in
[dimension YAML](../models/silver/_silver__dimensions.yml). Required fields
include their business references; card tier remains nullable because no
mandatory-tier business rule has been agreed.
Add shared [interval validation](../tests/silver/assert_account_card_dimension_intervals.sql)
and [current-version uniqueness](../tests/silver/assert_account_card_current_unique.sql)
tests, partitioning by entity and normalized business key.

After staging validation passes and its views exist:

```powershell
..\.venv\Scripts\dbt.exe run --select dim_account dim_debitcard --profiles-dir .
..\.venv\Scripts\dbt.exe test --select dim_account dim_debitcard --profiles-dir .
```

**Expected:** 19 new dimension tests pass. Locally check balance-only changes,
ownership changes, tier/activity/account changes, deletion transitions, gaps,
NULL attributes, repeat rebuilds, and coalescing. Native Fabric hashing and
live table creation still require the authenticated Fabric-connected run.

### Iteration 48. Add date and time-of-day reference dimensions

Create [dim_date](../models/silver/dim_date.sql) and
[dim_time](../models/silver/dim_time.sql) as Silver tables.
These are static reference dimensions, not SCD2 histories.

| Model | Grain | Key |
|---|---|---|
| dim_date | One calendar day | Integer YYYYMMDD |
| dim_time | One second of the day | Integer seconds since midnight, 0-86399 |

Add these project variables:

```yaml
vars:
  silver_date_start: '2020-01-01'
  silver_date_end: '2035-12-31'
```

Both bounds are inclusive. This default produces 5844 calendar rows; the time
dimension always contains 86400 rows. Calendar generation is independent of
transaction activity, so dates with no activity and future dates remain available.

The [calendar bounds macro](../macros/silver_calendar_bounds.sql) validates
the ISO date strings, ascending bounds, and a maximum of 100000 inclusive days.
The SQL generators cross join five ten-row digit sets to produce integers
0-99999; invalid/oversized ranges fail compilation rather than truncate silently.

Date attributes are calendar year, quarter, month, day, ISO weekday, and weekend
flag. Weekdays use days elapsed since Monday 1900-01-01, with modulo normalized
for earlier dates, rather than session-dependent DATEFIRST or language settings.
No holiday or fiscal-calendar rules are invented.

```sql
((datediff(day, cast('19000101' as date), calendar_date) % 7 + 7) % 7) + 1
    as iso_weekday
```

Time attributes are hour, minute, and second. For example, 14:35:12 has key
52512 (14*3600 + 35*60 + 12). The future fact retains its full timestamp,
including fractional seconds; the time dimension groups it at second precision.
Do not treat the date/time split as a timezone conversion. The source timezone
convention remains an explicit decision before fact modelling.

Add unique/non-null tests for date/time keys and values, and non-null tests on
every attribute. Add [calendar validation](../tests/silver/assert_dim_date_calendar.sql)
and [clock validation](../tests/silver/assert_dim_time_clock.sql) to check complete
coverage, extrema, and key/attribute consistency. The calendar count, distinct
date count, and inclusive extrema together enforce contiguous daily coverage.

```powershell
..\.venv\Scripts\dbt.exe run --select dim_date dim_time --profiles-dir .
..\.venv\Scripts\dbt.exe test --select dim_date dim_time --profiles-dir .
```

**Expected:** 19 tests pass. Unknown members and fact lookups are not introduced
by this step. Out-of-calendar transaction handling must be validated explicitly
before the fact is published; do not silently discard unmatched dates.

### Iteration 49. Make missing historical coverage explicit

**Problem:** generated transactions can predate our first source extraction.
Dimension intervals begin when attributes were observed, not necessarily when
they became true in the business. Assigning the first known version to every
earlier transaction would invent historical ownership and address knowledge.

**Chosen policy:** strict event-time lookups with explicit Unknown members,
not first-known historical inference.

| Lookup situation | Planned fact treatment |
|---|---|
| Exactly one non-deleted version covers transaction time | Use its dimension version key |
| No matching observed version | Use Unknown and expose lookup status |
| Multiple matching versions | Fail validation, never select an arbitrary match |
| Date outside calendar bounds | Fail coverage validation; extend the calendar |
| No CardID in the transaction | Do not invent a debit-card relationship |

Customer lookup must first resolve account ownership at transaction time, then
the owner's customer version at the same time. Without historical account
ownership, historical customer attribution is also unknown.

**Alternative considered:** first-known inference assigns the earliest observed
version to earlier events and explicitly flags it as inferred. This improves
attribution but assumes the earlier ownership/attributes were the same. We do
not make that assumption. Initial reports can therefore legitimately contain
many Unknown transactions. Their amounts must still be retained and reconciled.

**Implemented in this iteration:** add one synthetic row to each of
[dim_customer](../models/silver/dim_customer.sql) and
[dim_account](../models/silver/dim_account.sql):

| Attribute | Unknown customer | Unknown account |
|---|---|---|
| Surrogate key | UNKNOWN_CUSTOMER | UNKNOWN_ACCOUNT |
| Business IDs and descriptive attributes | NULL | NULL |
| is_unknown | 1 | 1 |
| is_deleted | NULL (unknown, not assumed active) | NULL |
| valid_from | 0001-01-01 | 0001-01-01 |
| valid_to | 9999-12-31 | 9999-12-31 |
| is_current | 1 | 1 |

All observed rows have `is_unknown = 0`. Reserved surrogate keys cannot collide
with the hexadecimal SHA-256 version keys. No source business ID is invented.
The synthetic date range is a technical convention, **not historical evidence**.
Future temporal joins must exclude `is_unknown = 1`, then explicitly use the
reserved key when no observed version matches.

The shared account/card coalescing macro accepts an optional `unknown_key`:

```sql
{{ coalesce_observed_dimension(
    relation=ref('stg_oys_account'),
    business_key='account_id',
    version_key='account_key',
    attributes=['customer_id', 'is_deleted'],
    unknown_key='UNKNOWN_ACCOUNT'
) }}
```

Card behaviour remains unchanged. It has no transaction lookup requirement
because the event source supplies no CardID.

**Adapt tests without weakening observed-data rules:** business ID and deletion
flag `not_null` tests apply only to observed rows:

```yaml
        data_tests:
          - not_null:
              config:
                where: "is_unknown = 0"
```

Surrogate-key uniqueness/non-null, boundary non-null, and current-flag non-null
tests still cover all rows. Historical overlap/current-key tests exclude the
synthetic members. Add non-null `is_unknown` tests and
[the Unknown-member contract test](../tests/silver/assert_dimension_unknown_members.sql):
exactly one Unknown per dimension, correct reserved keys, NULL business IDs and
deletion status, expected technical boundaries, and no observed reserved-key use.

After rebuilding customer/account (and with existing card available):

```powershell
..\.venv\Scripts\dbt.exe run --select dim_customer dim_account --profiles-dir .
..\.venv\Scripts\dbt.exe test --select dim_customer dim_account --profiles-dir .
```

**Acceptance:** one additional synthetic row per affected dimension; existing
observed version keys/attributes remain unchanged. Even an empty observed
history produces its Unknown member. Native Fabric execution remains a live
verification step.

**Local verification completed:** `dbt parse` passed with only the existing
unused example-model configuration warning. Rendered SQL fixtures in SQLite
(with replacements for native T-SQL hash/date primitives) verified empty and
populated histories, observed-only test filters, and rejection of missing or
duplicate Unknown members, invented IDs, assumed deletion status, invalid
reserved keys/bounds, NULL flags, and observed reserved-key use. These are local
logic checks, not successful live Warehouse data tests.

**Next step, not implemented yet:** the transaction fact will preserve event
ID, source account ID, full timestamp, amount, and lookup statuses. Left temporal
lookups must retain unmatched transactions, reject ambiguous matches, and
reconcile event counts/amounts. Adding Unknown rows alone does not implement any
of those lookups or error gates.

### Iteration 50. Build the transaction star fact

**Grain:** one immutable source `TransactionID` per row. The ID is a degenerate
dimension; there is no need to invent another transaction surrogate key.
[fct_transaction](../models/silver/fct_transaction.sql) inherits Silver's
`table` materialization, not incremental append. Bronze retains immutable events;
Silver rebuilds their dimensional attribution when new history becomes available.
This avoids permanently freezing an early Unknown assignment.

**Star relationships:** the fact references account, customer, date, and time
dimensions directly. Account/customer business IDs retained in the fact are
lineage, not additional dimension-to-dimension relationships. No `CardID` exists
in the event source; no card attribution is inferred.

**Business-effective limitation:** event-time lookup uses observation intervals,
not proven business-effective dates. We neither extend the first known version
backwards nor select current ownership for old transactions. Timestamps remain
as supplied; establish a source timezone contract before mixing differently
zoned event feeds.

The account lookup retains every transaction through a left join:

```sql
with account_lookup as (
    select
        transactions.*,
        accounts.account_key,
        accounts.customer_id
    from {{ ref('stg_oys_transaction_ledger') }} as transactions
    left join {{ ref('dim_account') }} as accounts
        on accounts.account_id = transactions.account_id
       and accounts.is_unknown = 0
       and accounts.is_deleted = 0
       and transactions.transaction_timestamp >= accounts.valid_from
       and transactions.transaction_timestamp < accounts.valid_to
)
```

The customer join uses that historical owner's business ID and the same
half-open `[valid_from, valid_to)` comparison. Unknown synthetic members are
excluded from both temporal joins. A missing match maps explicitly to
`UNKNOWN_ACCOUNT` / `UNKNOWN_CUSTOMER`; deletion versions are not eligible.
`no_account_history` therefore means no eligible non-deleted coverage, including
pre-observation events, gaps, deleted periods, or absent IDs. It does not claim
which of those causes occurred.

| Account status | Customer status | Meaning |
|---|---|---|
| matched | matched | Both observed event-time versions resolved |
| matched | no_customer_history | Owner is known, but eligible customer coverage is not |
| no_account_history | account_unresolved | No historical owner can be inferred |

**Do not hide ambiguity:** no `top 1`, `row_number` deduplication, or arbitrary
version selection is used. Overlapping matches can multiply rows; upstream
interval tests, fact `unique(transaction_id)`, and reconciliation must fail
before publication. A successful `dbt run` alone is not acceptance.

**Date/time:** left join calendar date and integer seconds since midnight.
Do not cast the timestamp to `time(0)`: fractional seconds may round to the next
second/day. Instead use:

```sql
datepart(hour, transaction_timestamp)*3600
+ datepart(minute, transaction_timestamp)*60
+ datepart(second, transaction_timestamp)
```

The full `datetime2(6)` timestamp remains in the fact. Missing calendar coverage
leaves a NULL key and fails required-key validation rather than dropping the
event. Extend configured date bounds and rebuild the calendar/fact.

**Measures and lineage:** retain the signed decimal amount without inventing
currency or balance semantics. Retain normalized/raw transaction and account
IDs plus original batch/load/file metadata; do not replace them with rebuild
timestamps. Balance remains a separate future grain decision.

**Tests added** in [fact metadata](../models/silver/_silver__facts.yml) and
[Silver tests](../tests/silver): 20 tests (17 generic and three singular).

- Unique/non-null transaction IDs, required source fields and dimension keys.
- Four dimension relationship checks and accepted, non-null lookup statuses.
- [Reconciliation](../tests/silver/assert_transaction_reconciliation.sql):
  bidirectional null-aware row comparison of event values/lineage, row counts,
  and signed total amounts using decimal(38,2) aggregation.
- [Temporal attribution](../tests/silver/assert_transaction_temporal_lookups.sql):
  resolved versions must match source ID, observed ownership, interval, and
  non-deleted state; Unknown cannot hide an eligible observed match; statuses
  must agree with the resulting keys.
- [Calendar mapping](../tests/silver/assert_transaction_calendar_keys.sql):
  keys must correspond to the retained event timestamp.

Unknown is valid, not an automatic test failure. Monitor its volume and amount:

```sql
select
    account_lookup_status,
    customer_lookup_status,
    count_big(*) as transaction_count,
    sum(cast(amount as decimal(38,2))) as signed_amount
from dev_silver.fct_transaction
group by account_lookup_status, customer_lookup_status;
```

Use the target's actual schema (`dev_silver` for development, `silver` for
production). No arbitrary Unknown-rate threshold has been invented.

**Build/acceptance sequence** (requires reachable authenticated Fabric):

```powershell
..\.venv\Scripts\dbt.exe build --select +fct_transaction --profiles-dir .
..\.venv\Scripts\dbt.exe test --select path:tests/silver --profiles-dir .
```

The first command includes upstream models/snapshots and their relevant tests;
it can update Bronze snapshots, so run it only against the intended source
batch/target. The second assumes all Silver models, including the independent
card dimension, already exist. Sources are not loaded by dbt.
Do not publish to reporting until tests pass. Tests are detection gates, not an
atomic deployment: an ordinary table rebuild can be visible before subsequent
tests fail. Isolated build schemas and controlled promotion remain future work.

**Local verification completed:** `dbt parse` passed with only the preserved
unused example configuration warning. Manifest checks confirmed the table
materialization, 20 attached fact tests, and all four relationships.
Rendered SQL fixtures in SQLite, substituting T-SQL date/count primitives,
verified 12 event-time scenarios: pre-observation events, exact boundaries,
owner/address changes, customer coverage starting later, gaps, deleted states,
absent accounts, and fractional seconds without rounding. Negative mutations
detected wrong historical versions/owners/statuses, hidden eligible matches,
missing/duplicated events, changed amounts/lineage, incorrect calendar keys,
and broken relationships. Separate fixtures detected overlapping account or
customer matches and missing date/time coverage; late history resolved an
Unknown assignment after rebuild, and an empty ledger stayed empty.
These checks do not substitute for native Fabric execution or verify native
decimal arithmetic/adapter deployment.

### Iteration 51. Make Silver acceptance repeatable

**Reason:** `+fct_transaction` selects its ancestors, including mutable Bronze
resources, but does not include the independent card dimension. A Silver-only
acceptance run should be explicit about what it reads and writes.

Add [named selectors](../selectors.yml):

- `silver_upstream_checks`: generic and singular tests for existing Landing and
  Bronze relations, including snapshot and source column tests. It selects only
  tests, not the models/snapshots themselves.
- `silver`: every model under `models/silver` plus every shared singular test
  under `tests/silver`. Generic model tests are included by dbt's normal indirect
  selection. Includes card, calendar, staging, intermediate models, dimensions,
  and transaction fact.

Neither selector runs the loader or updates Bronze. Existing Landing/Bronze
relations must already exist, and their data must represent the intended
processed batch. A skipped source delivery is not repaired by rebuilding Silver.
Do not run concurrent loaders/snapshots during acceptance: views read their
current inputs, and these commands do not pin a cross-table source version.

**Preview before writes**, from the dbt project directory:

```powershell
& ..\.venv\Scripts\dbt.exe parse --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Project parsing failed.' }
& ..\.venv\Scripts\dbt.exe ls --selector silver_upstream_checks --output name --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Upstream selection failed.' }
& ..\.venv\Scripts\dbt.exe ls --selector silver --output name --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Silver selection failed.' }
```

`parse` and `ls` verify project structure/selection, not table existence, SQL
execution, or data quality. The upstream list must contain only tests. The
Silver list must contain only Silver models and their tests, with no executable
snapshots or Bronze models. Recheck the list after adding new resources.

**Execute acceptance, stopping on failure:**

```powershell
& ..\.venv\Scripts\dbt.exe test --selector silver_upstream_checks --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Upstream validation failed; stop before rebuilding Silver.' }
& ..\.venv\Scripts\dbt.exe build --selector silver --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Silver build or validation failed; do not publish.' }
```

`dbt build` orders models and tests by dependency. Do not replace it with an
unchecked `run` followed much later by `test`. Skipped downstream nodes after an
upstream failure are not successful acceptance. Inspect the console and
`target/run_results.json` immediately after each command; the next command can
overwrite that artifact. Source freshness is separate from these data tests;
when required, run the existing freshness checks against the delivery SLA.

**Acceptance checklist:**

| Area | Required result |
|---|---|
| Upstream | All selected tests pass; no key, immutability, or batch violations |
| History | Positive-length, non-overlapping observed intervals; adjacency allowed |
| Customer | No simultaneous ambiguous address type; state reconciliation passes |
| SCD2 | Stable observed keys for unchanged input; no duplicate current versions |
| Unknown | Exactly one customer/account Unknown member, correct reserved-key contract |
| Calendar | Configured inclusive date coverage; 86400 second-grain time rows |
| Fact | One row per source transaction; source values, count, signed total preserved |
| Relationships | All four fact keys resolve; no missing date/time coverage |
| Temporal mapping | Event-time versions/ownership/statuses agree; no arbitrary overlap resolution |
| Reporting | Review Unknown counts/amounts explicitly; no invented zero-Unknown requirement |

**Understanding the shared interval test:**
[assert_staging_history_intervals](../tests/silver/assert_staging_history_intervals.sql)
uses the maximum end of *all* earlier intervals for each entity/business key,
not only `lag(valid_to)`. This detects nested overlap:
`[1,10)`, `[2,3)`, `[4,5)`; the third interval still overlaps the first.
`[1,2)` followed by `[2,3)` is valid adjacency. `from >= to` is invalid.
NULL bounds are rejected by separate required-field tests.
The test includes deletion versions because tombstones also occupy observed
history; business eligibility is a separate fact-lookup rule.
It does not reject gaps or infer coverage before first observation. Unknown
members and temporal fact tests address unmatched event times.

**Controlled history scenarios** (development only, retain raw test deliveries):

1. Rebuild Silver with unchanged Bronze: observed dimension keys and fact
   assignments/counts/amounts must remain unchanged.
2. Process an approved attribute-change batch through the loader and Bronze
   snapshots, then repeat acceptance. Check the relevant SCD2 boundary;
   unchanged business states must not gain unnecessary Silver versions.
3. Use events at an observed version start and end: start is inclusive, end
   exclusive. Ownership lookup must use the version at the event time.
4. Check pre-observation/gap/deleted-period events: retain each event and amount,
   use Unknown where no eligible history exists, expose lookup status.
5. Replay a fixture with eligible history arriving later: rebuild Silver;
   attribution can change from Unknown to matched, never the source amount.
6. In isolated fixtures only, introduce an overlap or out-of-calendar event:
   acceptance must fail, not silently choose a version or drop the transaction.

Do not edit production history merely to demonstrate a failure. Fixtures must
use controlled observation boundaries; replaying an old extraction alone does
not guarantee historically accurate snapshot effective times, especially for
hard deletes. None of these live scenarios has been claimed as executed here.

**Local verification completed:** `dbt parse` passed with only the preserved
unused example configuration warning. Actual `dbt ls --selector ... --output
json` output was compared with manifest resource paths, not just row counts:
`silver_upstream_checks` selects exactly 27 tests; `silver` selects exactly
13 models and 110 tests. The selectors are disjoint and together cover all
137 current data tests, including shared singular tests. Neither selects
executable Bronze models or snapshots. These are selection checks, not live
Warehouse builds or passing data-test results.

**Publication boundary:** successful validation is necessary but does not hide
in-progress table rebuilds from readers. Coordinate a maintenance window or
implement isolated build schemas and promotion separately before treating this
as a production release process.

**Current implementation boundary:** iterations 40-51 are implemented.
Five dimensions, explicit customer/account Unknown members, and the transaction
star fact with validation tests and repeatable acceptance selectors are present.
Live Warehouse execution and controlled publication remain external verification steps.

### Iteration 52. Attempt live development acceptance and stop at the input gate

The user approved live validation against the configured development target.
Run the upstream gate explicitly against `dev`:

```powershell
& ..\.venv\Scripts\dbt.exe test --selector silver_upstream_checks --target dev --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Upstream validation failed; do not rebuild Silver.' }
```

**Observed result:** connection succeeded; 27 tests executed, eight passed and
19 errored. These are execution errors, not 19 demonstrated data-quality
violations.

| Result | Live evidence |
|---|---|
| Eight passes | Unique/non-null business IDs for Landing customer, account, debit card, and transaction ledger |
| 16 errors | The five expected `dw_oyster_path.dev_bronze.bz_oys_*` relations could not be resolved |
| Two errors | `lh_oyster_path.dbo.ld_oys_customeraddress` could not be resolved |
| One error | The mixed-batch test could not resolve `meta_batch_timestamp` |

The metadata error establishes that the expected column is unavailable in at
least one referenced Landing relation; it does not identify all affected
tables. An invalid-object error does not prove that equivalent data is absent
from every other schema or target.

**Safety decision:** do not run the Silver build, loader, snapshots, or Bronze
ledger append after this failed gate. No production target was selected and no
Silver model was rebuilt by this attempt. Do not remove tests, substitute load
time for extract time, or point development at production merely to turn the
gate green.

**Follow-up status at that time:** a read-only inspection confirmed four
Landing tables and no `meta_batch_timestamp`; the Warehouse had only the
transaction ledger and date/time dimensions among the relations expected by
Silver. The active-card cardinality assumption was supported by the data: all
102 accounts had exactly one active card. The transaction ledger contained
4,840 distinct transactions spanning 2026-10-09 through 2026-11-09, with no
NULL transaction IDs, account IDs, amounts, or timestamps. These Landing
findings were addressed in iteration 54 below.

### Iteration 53. Create local Gold consumer models

**Goal:** prepare a stable Gold projection of the Silver star schema and add
the account/day dataset needed by the exercise's tier-alignment report.

**Why:** Silver owns conformed history and event-time attribution. Gold should
provide an explicit, consumer-facing projection rather than duplicate or
redefine that logic. The daily account snapshot is a different fact from the
transaction ledger: each row represents one account at the end of one
snapshot date.

Configure Gold tables in [dbt_project.yml](../dbt_project.yml):

```yaml
vars:
  gold_tier_gold_threshold: 2000
  gold_tier_platinum_threshold: 5000
  gold_snapshot_end_date: '2026-11-09'

models:
  dbt_oyster_path:
    gold:
      +schema: gold
      +materialized: table
```

The thresholds are configurable starting values from the exercise SQL, not
approved business policy. Platinum must be greater than Gold; dbt compilation
rejects invalid threshold ordering.

The daily snapshot end date is an explicit reporting parameter, defaulting to
`2026-11-09`, the latest date used by the generated example data. The model
does not use the current date. It keeps daily account snapshots from the first
transaction date through the configured end date, bounded by the latest
transaction date in the fact. This preserves daily history while keeping the
exercise's as-of date stable when the project is run later.

Create explicit `ref()` projections for the customer, account, debit-card,
date, and time dimensions, plus the transaction fact:

| Gold model | Grain/purpose |
|---|---|
| `gold_dim_customer` | Silver customer SCD2 versions for consumption |
| `gold_dim_account` | Silver account SCD2 versions for consumption |
| `gold_dim_debitcard` | Silver debit-card SCD2 versions for consumption |
| `gold_dim_date`, `gold_dim_time` | Shared reporting calendar and clock |
| `gold_fct_transaction` | One row per source transaction |
| `gold_fct_account_tier_daily_snapshot` | One row per active account and snapshot date |

Explicit column lists are intentional: they stabilize the reporting contract
and prevent an upstream schema addition from silently changing the Gold
interface. The transaction fact retains the Silver event-time keys. It does
not invent a card key, because the transaction source contains no card ID.

The account/day snapshot uses the account, customer, and active-card SCD2
versions valid at the end of the snapshot day. Intervals remain half-open,
`[valid_from, valid_to)`. Rolling spend includes the snapshot date and the
previous 29 calendar days:

```sql
transactions.transaction_timestamp >= dateadd(
    day, -29, cast(snapshot_date as datetime2(6))
)
and transactions.transaction_timestamp < dateadd(
    day, 1, cast(snapshot_date as datetime2(6))
)
```

Missing cards, missing tiers, and unrecognized tiers are exposed as statuses;
they are not silently changed to `Standard`. Candidate flags are NULL when a
valid active tier cannot be compared. A source-window flag is provided for
report filtering, but it does not prove upstream source completeness.

**Decision points:**

- **Transaction snapshot versus periodic snapshot:** keep transactions at
  transaction grain for event-level analysis; use the separate daily
  account-grain snapshot for daily rolling-spend/tier comparisons.
- **Snapshot reporting date:** configure `gold_snapshot_end_date` for the
  report's intended as-of date. Use `2026-11-09` for the generated exercise
  data; do not substitute today's date.
- **Account/card relationship:** the current data supports one active card per
  account. The model still avoids selecting an arbitrary card; its grain test
  fails if multiple active cards create duplicate account/day rows.
- **Thresholds:** use 2,000/5,000 only as provisional configurable values until
  the business owner confirms amounts, currency, and tier-transition policy.
- **Aggregate metrics:** keep total-account counts and mismatch rates out of
  the row-level fact. Define them as Power BI measures so filters and the
  selected snapshot date control aggregation.

**Quality checks:** YAML tests cover required/unique keys, dimension
relationships, tier/status values, and fact grain. Singular tests check one
row per account/date, the rolling-window sum, tier classification, and
event-date SCD2 key alignment, and the configured snapshot end date. These
tests are executable checks, not a substitute for a successful live build.

**Local acceptance:** `dbt parse` and `dbt ls --select path:models/gold
path:tests/gold` recognize the Gold resources. A live Gold compile/build is
not acceptance yet: the upstream Landing contract and Silver relations must
first be repaired and pass their gates. No Gold data has been published.

**Next step:** run the upstream dbt acceptance tests against the updated
Landing. If they pass, build and validate Bronze/Silver in dependency order
before attempting Gold. Do not skip directly to Gold or treat a passing local
parse as proof of live data quality.

### Iteration 54. Complete the Landing source contract

**Goal:** ensure every Landing source table exists with the batch, load, and
source-file metadata required by dbt snapshots, freshness checks, and lineage.

**Why:** adding only the missing customer-address table would leave the other
source tables without the `meta_batch_timestamp` required by the shared dbt
source freshness configuration. The loader already defines all five source
entities and attaches metadata; selecting entities explicitly lets the
missing table and the existing four tables be refreshed in separate,
controlled writes.

**Step 1 — Load customer address only.** Run the loader with
`p_entities=CustomerAdress`. The spelling follows the source CSV prefix; the
destination is `dbo.ld_oys_customeraddress`. This creates the missing table
without overwriting the other four Landing tables.

**Step 2 — Add metadata to the other four tables.** Run the loader with
`p_entities=Customer,Account,DebitCard,Transaction_Ledger`. The overwrite is
limited to those four full-state/ledger Landing tables and uses the same latest
batch. The customer-address table is not rewritten in this step.

**Step 3 — Verify the Landing contract.** Read the table catalog and bounded
aggregates; confirm all five tables exist, all three metadata columns are
present, and every table reports the same `meta_batch_timestamp`.

**Observed acceptance result:** both loader runs completed. The read-only
verification returned:

| Landing table | Rows | Batch timestamp | Source files |
|---|---:|---|---:|
| `ld_oys_customer` | 102 | `2026-11-17 08:00:03` | 1 |
| `ld_oys_customeraddress` | 194 | `2026-11-17 08:00:03` | 1 |
| `ld_oys_account` | 102 | `2026-11-17 08:00:03` | 1 |
| `ld_oys_debitcard` | 102 | `2026-11-17 08:00:03` | 1 |
| `ld_oys_transaction_ledger` | 4,840 | `2026-11-17 08:00:03` | 1 |

All five tables contain `meta_batch_timestamp`, `meta_load_timestamp`, and
`meta_source_file`. No dbt snapshots, dbt models, or Gold data were run or
changed by this step.

**Next step:** run `silver_upstream_checks`. Stop on any failure; only after
the upstream tests pass should the development Bronze snapshots/ledger and
Silver acceptance build be considered.

### Iteration 55. Pass the upstream gate and build Bronze history

**Goal:** prove that Landing and Bronze satisfy every assumption Silver relies
on before any Silver model is written.

**Why:** a green Silver build on top of incomplete Bronze would only move the
problem downstream. The `silver_upstream_checks` selector is the agreed gate.

**Step 1 — Run the gate first, read-only.**

```powershell
cd dbt_oyster_path
& '..\.venv\Scripts\dbt.exe' test --selector silver_upstream_checks --target dev --profiles-dir .
```

All Landing source tests passed. Tests on the account and debit-card snapshots
could not run because those snapshot tables did not exist yet: a missing
relation, not a data defect.

**Step 2 — Build all four snapshots from the same batch.**

```powershell
& '..\.venv\Scripts\dbt.exe' snapshot --threads 1 --target dev --profiles-dir .
```

All four snapshots run, not only the missing two, so every SCD2 history has
observed the same Landing batch. Snapshots are idempotent: unchanged rows
open no new versions.

**Decision point — parallelism on Fabric Warehouse.** With `threads: 4`,
concurrent snapshot transactions can deadlock on Warehouse metadata locks.
Use `--threads 1` for snapshot and build runs that write several tables;
the volume here makes the cost negligible.

**Observed acceptance result:** gate 27/27 passed. Bronze holds 102 / 194 /
102 / 102 current versions (customer / address / account / card), all with
`dbt_valid_from = 2026-11-17 08:00:03` and `dbt_valid_to = 9999-12-31`.

### Iteration 56. Choose an initial-load history policy

**Goal:** let events that happened before the first observation resolve to a
dimension version, without hiding that this is an assumption.

**Why:** every first version starts at the batch time (2026-11-17), but the
transactions span 2026-10-09 to 2026-11-09. Under strict observation
semantics (Iteration 49) every transaction would be `no_account_history`, and
the Gold snapshot would carry no tier for any account. The sources contain no
business effective date (for example an account opening date) to use instead.

**Decision point — how to treat time before the first observation:**

| Option | Behaviour | When to use |
|---|---|---|
| A. Strict observation | No coverage before the first snapshot; facts stay unresolved | Audit-grade history where assumptions are unacceptable |
| B. Inferred initial history (chosen) | Each key's first version starts at `0001-01-01` and is flagged | Initial load of an existing system (Kimball initial-load practice) |
| C. Rewrite the batch timestamp | Pretend the batch arrived earlier | Never: it falsifies lineage metadata |

**Change 1 — make the policy a project setting** in `dbt_project.yml`:

```yaml
vars:
  # Initial-load policy: extend each business key's first observed version
  # back to 0001-01-01 and flag it with is_inferred_start.
  silver_infer_initial_history: true
```

**Change 2 — define the policy once** in `macros/initial_history_policy.sql`.
`version_group = 1` is the first observed version of a business key:

```sql
{% macro inferred_valid_from(alias) %}
    {%- if var('silver_infer_initial_history', false) -%}
    case when {{ alias }}.version_group = 1
        then cast('0001-01-01' as datetime2(6))
        else {{ alias }}.valid_from
    end
    {%- else -%}
    {{ alias }}.valid_from
    {%- endif -%}
{% endmacro %}

{% macro inferred_start_flag(alias) %}
    {%- if var('silver_infer_initial_history', false) -%}
    cast(case when {{ alias }}.version_group = 1 then 1 else 0 end as bit)
    {%- else -%}
    cast(0 as bit)
    {%- endif -%}
{% endmacro %}
```

**Change 3 — apply it in both dimension builders**
(`coalesce_observed_dimension` for account/card, and `dim_customer`):

```sql
    {{ inferred_valid_from('versions') }} as valid_from,
    versions.valid_to,
    versions.valid_from as observed_valid_from,
    {{ inferred_start_flag('versions') }} as is_inferred_start,
```

Unknown members get `observed_valid_from = 0001-01-01` and
`is_inferred_start = 0`. The Gold dimensions project both new columns, and
`not_null` tests cover them in Silver and Gold.

**Design rules behind the change:**

- Only the first version is extended. Later changes keep their observed start,
  so real SCD2 history is not distorted.
- The surrogate key is still hashed from the *observed* start. Keys therefore
  stay stable if the policy is switched off.
- Inferred data is always flagged (`is_inferred_start`) and the true
  observation time is preserved (`observed_valid_from`), so reports can
  separate assumed from observed coverage.

**Acceptance:** `dbt parse` passes and the compiled SQL contains the
`version_group = 1` branch.

### Iteration 57. Migrate the protected development ledger schema

**Goal:** bring the development transaction ledger onto the current Landing
contract, including `meta_batch_timestamp`.

**Why:** the ledger was created before Iteration 54 and is append-only with
`full_refresh=false`. That protection is deliberate, but it also means dbt will
not add the new column. All 4,840 existing ledger rows are present in Landing,
so rebuilding loses no events.

**Decision point — schema change on a protected append-only table:**

| Option | Effect |
|---|---|
| Dev-only drop and rebuild (chosen for dev) | Clean table, metadata on every row; protection stays in the model |
| `on_schema_change='append_new_columns'` | Column added, but existing rows keep NULL metadata |
| Production | Controlled migration: `ALTER TABLE` plus a reviewed backfill, never a drop |

**Change — a guarded development macro** in `macros/dev_drop_relation.sql`:

```sql
{% macro dev_drop_relation(schema, identifier) %}
    {% if target.name != 'dev' %}
        {{ exceptions.raise_compiler_error("dev_drop_relation is allowed only on target 'dev', got '" ~ target.name ~ "'.") }}
    {% endif %}
    {% if not schema.startswith('dev_') %}
        {{ exceptions.raise_compiler_error("dev_drop_relation only drops relations in dev_* schemas, got '" ~ schema ~ "'.") }}
    {% endif %}
    {% set relation = adapter.get_relation(database=target.database, schema=schema, identifier=identifier) %}
    {% if relation is none %}
        {{ log("Relation " ~ schema ~ "." ~ identifier ~ " does not exist; nothing to drop.", info=true) }}
    {% else %}
        {% do adapter.drop_relation(relation) %}
        {{ log("Dropped " ~ relation, info=true) }}
    {% endif %}
{% endmacro %}
```

Two guards (target name and `dev_` schema prefix) make an accidental
production drop a compile error.

```powershell
& '..\.venv\Scripts\dbt.exe' run-operation dev_drop_relation --args "{schema: dev_bronze, identifier: bz_oys_transaction_ledger}" --target dev --profiles-dir .
& '..\.venv\Scripts\dbt.exe' run --select bz_oys_transaction_ledger --target dev --profiles-dir .
```

**Acceptance:** the ledger is rebuilt with 4,840 rows and all three metadata
columns.

### Iteration 58. Build Silver and Gold, then reconcile independently

**Goal:** build both layers and prove that the Gold result is correct, not only
that the commands succeeded.

```powershell
& '..\.venv\Scripts\dbt.exe' build --selector silver --threads 1 --target dev --profiles-dir .
& '..\.venv\Scripts\dbt.exe' build --select path:models/gold path:tests/gold --threads 1 --target dev --profiles-dir .
```

`dbt build` runs each model's tests right after the model, so a failing
dimension stops its dependants.

**Observed acceptance result:**

| Check | Result |
|---|---|
| Silver build | 129/129 passed |
| Gold build | 72/72 passed |
| `fct_transaction` lookup status | 4,840 `matched` / `matched` |
| Inferred first versions | 102 account, 102 customer, 102 card |
| Daily snapshot span | 2026-10-09 to 2026-11-09, 32 days |
| Accounts per snapshot day | 102 on every day (3,264 rows) |

Tier distribution on 2026-11-09 (thresholds 2,000 / 5,000, not yet
business-approved):

| Actual tier | Calculated tier | Alignment | Accounts |
|---|---|---|---:|
| Gold | Standard | misaligned (downgrade) | 22 |
| Gold | Platinum | misaligned (upgrade) | 17 |
| Gold | Gold | aligned | 17 |
| Platinum | Platinum | aligned | 13 |
| Standard | Standard | aligned | 13 |
| Standard | Gold | misaligned (upgrade) | 10 |
| Platinum | Gold | misaligned (downgrade) | 10 |

**Independent reconciliation:** recompute the 30-day spend for 2026-11-09
directly from Silver, outside the Gold model logic, and compare:

```sql
with ref_spend as (
  select account_id, sum(cast(amount as decimal(38,4))) as spend
  from dev_silver.fct_transaction
  where transaction_timestamp >= '2026-10-11'
    and transaction_timestamp <  '2026-11-10'
  group by account_id
), g as (
  select account_id, rolling_30_day_spend, calculated_spending_tier
  from dev_gold.gold_fct_account_tier_daily_snapshot
  where snapshot_date = '2026-11-09'
)
select count(*) as accounts,
  sum(case when abs(coalesce(r.spend, 0) - g.rolling_30_day_spend) > 0.01
      then 1 else 0 end) as spend_mismatch,
  sum(case when (case when coalesce(r.spend, 0) >= 5000 then 'Platinum'
                      when coalesce(r.spend, 0) >= 2000 then 'Gold'
                      else 'Standard' end) <> g.calculated_spending_tier
      then 1 else 0 end) as tier_mismatch
from g
left join ref_spend as r on r.account_id = g.account_id;
```

Result: 102 accounts, 0 spend mismatches, 0 tier mismatches; both sides total
389,823.91. Early snapshot days have `has_full_30_day_source_window = 0`
because the data starts on 2026-10-09; report pages should filter or label them.

**Next step:** make the plain full build reliable (Iteration 59), then build
the Power BI semantic model on the Gold tables.

### Iteration 59. Make the default full build deterministic

**Goal:** `dbt build --profiles-dir .` must succeed without extra flags, so
every developer and scheduler runs the same way.

**Why:** with the original `threads: 4`, the full build (233 nodes) failed with
deadlock victims on snapshots, Silver staging views, and Gold tables. Passing
`--threads 1` by hand worked, but a safety setting that depends on remembering
a flag is not a safety setting. Concurrency belongs in the profile.

**Decision point — parallelism versus reliability:**

| Option | Trade-off |
|---|---|
| `threads: 1` (chosen) | Serial DDL, no lock contention; about 3 minutes for the full project |
| `threads: 4` with retries | Faster when it works, but failures cascade into skipped dependants |
| Split runs (snapshots serial, models parallel) | More orchestration for little gain at this volume |

**Change** in `profiles.yml`, for both `dev` and `prod`:

```yaml
      threads: 1  # Fabric Warehouse deadlocks on parallel DDL; see walkthrough Iteration 59
```

Revisit when data volume makes run time matter; measure deadlocks before
raising the value.

**Observed acceptance result:** `dbt build --profiles-dir .` reports
`Concurrency: 1 threads (target='dev')` and passes 233/233 (snapshots, Bronze,
Silver, Gold, and all tests).

**Next step:** build the Power BI semantic model on the Gold tables and author
the report described at the end of the exercise document.

### Iteration 60. Design the semantic model and report (plan, not yet built)

**Goal:** agree on the semantic model, measures, and report layout before
creating anything, so the report reproduces verifiable numbers for the
2026-11-09 as-of date.

**Why:** the exercise document's final chapter (Semantic Model & Power BI
Report Creation) targets a differently named table set and quotes example
figures (37.0%, "27 Platinum vs 10 Gold") from another dataset. Mapping it to
our Gold contract first prevents wrong relationships and, above all, wrong
aggregation of a snapshot fact.

**Status:** design only. No semantic model, measure, or report exists yet.

#### Build sequence

| # | Step | Where |
|---|---|---|
| 1 | Create a Direct Lake semantic model `sm_OysterBank_Analytics` from the `dev_gold` schema | Fabric workspace, Warehouse → New semantic model |
| 2 | Add the tables with friendly names | Table mapping below |
| 3 | Create 1:N relationships with single direction; mark the date table | Model view |
| 4 | Hide technical columns; set formats | Model view |
| 5 | Create measures in a dedicated `_Measures` table with display folders | Measures below |
| 6 | Build the report page: KPI cards, 100% stacked bar, donut, date slicer | Report layout below |
| 7 | Security (RLS) | Separate decision, see decision points |

#### Table mapping

| Model table | Source (`dev_gold`) | Role |
|---|---|---|
| **Account Tier Snapshot** | `gold_fct_account_tier_daily_snapshot` | Main fact; grain: account × day |
| Account | `gold_dim_account` | SCD2 dimension |
| Customer | `gold_dim_customer` | SCD2 dimension |
| Debit Card | `gold_dim_debitcard` | SCD2 dimension |
| Date | `gold_dim_date` | Date table (Mark as date table on `calendar_date`) |
| *(optional)* Transaction, Time | `gold_fct_transaction`, `gold_dim_time` | Later transaction drill-down page |

The document's `gold_fact_periodic_spend_snapshot` corresponds to our
`gold_fct_account_tier_daily_snapshot`: the purpose-made table without
aggregate metrics. Aggregation happens in the measures.

**Relationships** (all one-to-many, single direction, dimension filters fact):

```
Date[date_key]                  1 ──► N  Account Tier Snapshot[date_key]
Account[account_key]            1 ──► N  Account Tier Snapshot[account_key]
Customer[customer_key]          1 ──► N  Account Tier Snapshot[customer_key]
Debit Card[debitcard_key]       1 ──► N  Account Tier Snapshot[debitcard_key]
```

A read-only check confirmed that the snapshot fact has no NULL keys and no
orphan keys against any of the four dimensions.

**Hide:** every `*_key` column, `valid_from`, `valid_to`,
`observed_valid_from`, the `meta_*` columns, and `is_unknown`. Keep
`is_inferred_start` visible so users can see where a version start was assumed.

#### Measures

**Key rule — the snapshot fact is semi-additive.** The document's
`Total Spend = SUM(Rolling30DaySpend)` would add the rolling totals of every
day in the selection. Likewise a `DISTINCTCOUNT` filtered on `Misaligned`
would count an account that was misaligned on *any* of the 32 days. Every
measure therefore evaluates on the **last snapshot date** of the current
selection.

```dax
-- Display folder: 0 Context
Latest Snapshot Date =
CALCULATE (
    MAX ( 'Account Tier Snapshot'[snapshot_date] ),
    ALL ( 'Account Tier Snapshot' ),
    VALUES ( 'Date'[calendar_date] )
)

-- Display folder: 1 Cardholders
Total Cardholders =
VAR _asOf = [Latest Snapshot Date]
RETURN
    CALCULATE (
        DISTINCTCOUNT ( 'Account Tier Snapshot'[account_id] ),
        'Date'[calendar_date] = _asOf,
        KEEPFILTERS ( 'Account Tier Snapshot'[actual_card_status] = "active" )
    )

Misaligned Cardholders =
CALCULATE (
    [Total Cardholders],
    KEEPFILTERS ( 'Account Tier Snapshot'[alignment_status] = "misaligned" )
)

Aligned Cardholders =
CALCULATE (
    [Total Cardholders],
    KEEPFILTERS ( 'Account Tier Snapshot'[alignment_status] = "aligned" )
)

-- Display folder: 2 Tier alignment
Tier Mismatch Rate =
DIVIDE ( [Misaligned Cardholders], [Total Cardholders] )

Upgrade Candidates =
CALCULATE (
    [Total Cardholders],
    KEEPFILTERS ( 'Account Tier Snapshot'[upgrade_candidate] = TRUE () )
)

Downgrade Candidates =
CALCULATE (
    [Total Cardholders],
    KEEPFILTERS ( 'Account Tier Snapshot'[downgrade_candidate] = TRUE () )
)

-- Display folder: 3 Spend
Rolling 30-Day Spend =
VAR _asOf = [Latest Snapshot Date]
RETURN
    CALCULATE (
        SUM ( 'Account Tier Snapshot'[rolling_30_day_spend] ),
        'Date'[calendar_date] = _asOf
    )

Avg Rolling Spend per Cardholder =
DIVIDE ( [Rolling 30-Day Spend], [Total Cardholders] )
```

**Why these patterns:**

- `ALL(fact)` plus `VALUES(Date)`: the "latest day" depends only on the date
  filter. A legend segment (for example `aligned`) can never fall back to an
  earlier day just because it has no rows on the last day.
- `KEEPFILTERS`: the `alignment_status` legend filter in the 100% stacked bar
  is intersected, not overwritten, so the bars split correctly.
- `DIVIDE` without a third argument returns blank rather than a misleading
  0% when there is no data.
- Formats: `Tier Mismatch Rate` as `0.0%`, counts as `#,0`, spend as `#,0.00`.

#### Report layout and expected results (as-of 2026-11-09)

Add a `Date[calendar_date]` slicer (Between), defaulting to 2026-10-09 –
2026-11-09. All measures then show the 2026-11-09 state.

| Visual | Fields | Expected value |
|---|---|---|
| Card "TOTAL ACTIVE CARDHOLDERS" (soft blue) | `[Total Cardholders]` | **102** |
| Card "MISMATCHED TIER RATE" (soft red, %) | `[Tier Mismatch Rate]` | **57.8%** (59/102) |
| Card "UPGRADE CANDIDATES" (soft green) | `[Upgrade Candidates]` | **27** |
| *(recommended 4th)* Card "DOWNGRADE CANDIDATES" | `[Downgrade Candidates]` | **32** |

**100% stacked bar chart (bottom left).** Y-axis
`Account Tier Snapshot[actual_card_tier]`, X-axis `[Total Cardholders]`,
legend `Account Tier Snapshot[alignment_status]`, data labels as percentages.

| Actual tier | Total | Misaligned | Rate |
|---|---:|---:|---:|
| Gold | 56 | 39 | 69.6% |
| Platinum | 23 | 10 | 43.5% |
| Standard | 23 | 10 | 43.5% |

**Donut chart (bottom right).** Legend
`Account Tier Snapshot[calculated_spending_tier]`, values
`[Misaligned Cardholders]`. The donut has no native centre label: place a
transparent card showing `[Tier Mismatch Rate]` over its centre.

| Calculated tier | Misaligned |
|---|---:|
| Standard (down from Gold) | 22 |
| Gold (10 up from Standard, 10 down from Platinum) | 20 |
| Platinum (up from Gold) | 17 |

**Recommended addition:** a line chart of `[Tier Mismatch Rate]` over
`Date[calendar_date]` for all 32 days. It shows why the daily snapshot exists:
the mismatch rate changes from day to day.

The document's 37.0% and "27 Platinum vs 10 Gold" are illustrative figures
from another dataset. The values above, derived from our Gold data for
2026-11-09, are the acceptance criteria.

#### Decision points

| Decision | Options | Recommendation |
|---|---|---|
| Direct Lake mode | *On OneLake*: no DirectQuery fallback, fast. *On SQL endpoint*: honours Warehouse RLS/DDM, but falls back to DirectQuery | **On OneLake**, with security in the model |
| Dynamic data masking on `TaxID` (document) | Source has no `TaxID` column | Skip and record the gap |
| RLS on `CustomerType` (document) | Source has no such field. Skip, or a demo role on `permanent_country` | Skip in the first iteration; add a demo role as a separate step if required |
| Full 30-day window | On 2026-11-09, `has_full_30_day_source_window = 1` for 87 of 102 accounts; the document's KPIs count all accounts | Count all 102; expose the flag as a slicer or tooltip |
| Source schema | `dev_gold` now | Bind production only after fixing the prod target in `profiles.yml` |
| Transaction page | Include `Transaction` fact now or later | Later, once the first page is accepted |

#### How it will be built

Create the model (tables, relationships, measures, formats) directly in the
workspace, then run DAX queries to confirm the measures return the expected
values above before any visual is built. Report visuals are assembled in the
Fabric report editor with the field assignments listed here.

**Next step:** create the semantic model as designed (Direct Lake on OneLake,
without RLS/DDM) and validate the measures with DAX queries.

## Part XI. Orchestrate the end-to-end run

The source document ends with an Azure Data Factory master pipeline that
chains three steps on success: generate data, land raw files, run dbt. This
part rebuilds that idea in Fabric, one small iteration at a time. Each
iteration reuses artefacts that are already tested; no new transformation
logic is written inside the pipeline.

### Iteration 61 (O1). Map the document to Fabric and decide how dbt runs

**Goal:** decide, before creating any item, what each document step becomes
in Fabric and under which identity dbt runs.

**Why:** the document uses a Databricks notebook, a Copy Data activity and
"Publish All". Copying it literally would bypass the loader's validation and
`meta_*` columns, and would leave the dbt identity undefined.

| Document | Fabric implementation | Reason |
|---|---|---|
| ADF / Synapse Studio | Fabric Data pipeline `pl_master_orchestration` | One control plane, in the same workspace as the data |
| `01_Generate_Data` (Databricks) | Notebook activity, `nb_generate_deterministic_sample_data` | Existing, tested notebook |
| `02_Landing_Raw` (Copy Data) | Notebook activity, `nb_csv_load_source_to_ld` | Copy Data neither validates nor writes batch metadata |
| `03_Run_dbt_Notebook` | Notebook activity, `nb_run_dbt` (new): clone from Azure DevOps, then `dbt build` | Chosen run method |
| Green "On success" arrows | On success dependencies | Landing, then Bronze, Silver, Gold |
| Validate, Debug, Publish All | Validate, Run, Save, then commit through Git integration | Fabric has no publish step |

Target flow:

```text
pl_master_orchestration  (triggered manually)
  01_generate_data  --on success-->  02_landing_raw  --on success-->  03_run_dbt
  (generator)                        (validating loader)               (nb_run_dbt: refresh Lakehouse SQL endpoint metadata, then dbt build)
                                                                         --on success-->  04_refresh_semantic_model
                                                                                         (sm_reporting)
```

Iteration 63 (O3) refines this into one pipeline per layer and a master
(`pl_Master`) that chains the layer pipelines, the dbt/DW build, and the
semantic model refresh.

`nb_run_dbt` performs these steps:

1. Install pinned versions, `dbt-core==1.12.6` and `dbt-fabric==1.12.0`,
   matching the local environment.
2. Read the Azure DevOps personal access token (PAT).
3. Shallow-clone `main` from
   `https://dev.azure.com/pulze-azure-devops/Oyster/_git/Oyster`. The PAT
   travels only in an HTTP header; it is never put in the URL or printed.
4. Refresh the Lakehouse SQL endpoint metadata through the Fabric
   `refreshMetadata` API. This makes the latest Landing Delta versions visible
   to dbt sources; it is a metadata refresh, not a separate DW refresh.
5. Run `dbt build` against a dedicated `fabric_notebook` target with
   `authentication: notebookutils` and `threads: 1`.
6. Copy the run results and dbt log to Lakehouse Files as run artefacts.
7. Raise an exception when dbt fails, so the pipeline activity fails.

The dbt `build` creates or updates the Warehouse objects and data; there is no
separate DW refresh step. In `pl_Master`, the Semantic model refresh
activity runs only after `DW create and test` succeeds, waits for completion,
and refreshes the full `sm_reporting` model. See Microsoft's
[Semantic model refresh activity documentation](https://learn.microsoft.com/en-us/fabric/data-factory/semantic-model-refresh-activity).

#### Decision point: which run method

| Option | Pros | Cons |
|---|---|---|
| A. Fabric dbt job item | Native, no install step, supports SPN/managed identity | Newer item type, less control over the runtime |
| **B. Notebook (chosen)** | Full control, same commands as locally, easy to debug | Installs packages per run; we own the clone and auth code |
| C. External runner (Azure DevOps agent) | Natural fit for CI | A second runtime to operate; the orchestration leaves Fabric |

#### Decision point: under which identity dbt runs

`dbt-fabric` supports `authentication: notebookutils`, which asks
`notebookutils.credentials.getToken` for a token of whoever executes the
notebook. When the notebook is run by hand, that is the developer.

| | Now: developer's own identity | Target: technical identity |
|---|---|---|
| Warehouse auth | `notebookutils`, the executing user's Entra token | Service principal (`ActiveDirectoryServicePrincipal`) or Fabric workspace identity |
| Azure DevOps auth | Personal PAT, scope *Code (Read)*, short expiry | Service principal or managed identity added to the Azure DevOps organisation, no PAT |
| Trigger | Manual run of the notebook or the pipeline | Scheduled pipeline with a connection owned by the technical identity |
| Risk | Stops when the person leaves, the PAT expires or a policy changes; every data change is audited under a personal name | Independent of people; least privilege; clear audit trail |
| Fit | Development, training, manual demos | Test and production |

The developer-identity setup is accepted for this exercise only. The proper
solution is a technical identity, for four reasons:

- the run must not depend on one person's account, password, MFA or
  employment;
- permissions can be scoped to the job (write only the dbt schemas, read only
  the repository);
- the audit log shows "the pipeline changed the data", not a developer;
- nobody needs personal write access to production data.

Note: when the notebook runs from a pipeline, the executing identity is
defined by the pipeline (for example its last modifier), not necessarily by
the person who clicked Run. Iteration O4 verifies this.

#### Decision point: where the PAT comes from

No Key Vault is available yet. The PAT is therefore passed as a notebook
parameter at run time and is never saved in the notebook or the repository.
The notebook code keeps a Key Vault branch (`notebookutils.credentials.getSecret`)
as a placeholder. Switching to it is a configuration change, not a rewrite.

**Best practices**

1. Orchestrate tested artefacts; do not put new logic in the pipeline.
2. Do not replace a validating load with a plain Copy activity.
3. One run equals one batch, and it is clear which batch was loaded.
4. Never store secrets in a notebook, a pipeline definition or Git. Prefer
   Key Vault; give a PAT the minimum scope and a short expiry.
5. Pin dbt and adapter versions to match local development.
6. Keep `threads: 1` in every target, because Fabric Warehouse deadlocks on
   parallel DDL (Iteration 59).
7. Use a personal identity only for development; use a technical identity
   for test and production.

**Acceptance check:** every document step has a Fabric counterpart, and the
run method, the run identity, the secret source and the target technical
identity are recorded.

### Iteration 62 (O2). Decide how the loader selects the batch

**Goal:** make the batch the loader reads explicit and controllable, without
coupling the generator to the pipeline.

**Why:** the generator only writes files into a new `yyyyMMddHHmmss` folder;
it returns nothing to its caller, just as a real source system would not. The
loader therefore decides which batch to read:

- `p_batch_folder` set: load exactly that folder (reload or replay);
- `p_batch_folder` empty: load the latest folder. Folder names are UTC
  timestamps, so the lexicographic maximum is the most recent batch.

The loader's parameter cell was marked `# CELL` in the source file, not
`# PARAMETERS CELL`. Fabric therefore did not treat it as a parameter cell,
and a pipeline override of `p_batch_folder` would have been silently ignored.

**Loader parameter cell** (`nb_csv_load_source_to_ld`):

```python
# PARAMETERS CELL ********************

# Parameters (pipelines override these; in the Fabric UI this shows as the "Parameters" cell).
p_root_path = "Files/landing/oyster"
p_batch_folder = ""   # "" = latest batch folder (yyyyMMddHHmmss names sort chronologically); or e.g. "20261117080003" to reload a specific batch
p_entities = ""       # "" = all entities; or comma-separated, e.g. "Customer,Account"
p_debug = 0           # 1 = display a preview of each loaded table
```

The existing selection logic stays unchanged:

```python
batch_folders = [
    f.name.rstrip("/")
    for f in notebookutils.fs.ls(p_root_path)
    if f.isDir and re.fullmatch(r"\d{14}", f.name.rstrip("/"))
]
if not batch_folders:
    raise FileNotFoundError(f"No batch folders found under {p_root_path}")
if p_batch_folder and p_batch_folder not in batch_folders:
    raise FileNotFoundError(f"Batch folder {p_batch_folder} not found under {p_root_path}")
batch_folder = p_batch_folder or max(batch_folders)
```

**Pipeline wiring** (configured in O3): the loader activity exposes
`p_batch_folder` as a pipeline parameter with an empty default. A normal run
loads the latest batch; a replay passes a folder name.

#### Decision points

| Decision | Options | Choice |
|---|---|---|
| How the loader learns the batch | a) the generator returns the folder name and the pipeline passes it; b) the loader takes the latest folder unless a folder is given | **b)**: the generator stays a pure file producer, independent of the pipeline, and behaves like an external source. |
| Empty `p_batch_folder` | Latest folder, or an error | **Latest folder**; an unknown explicit folder fails the run |
| Replay of an older batch | Not supported, or an explicit parameter | **Explicit parameter** |

Trade-off of b): "latest" is only safe when one run at a time writes and reads
the landing folder. A concurrent run or a manual upload between generation and
loading would make the loader pick a different folder. O6 handles this with a
concurrency guard on the pipeline.

**Best practices**

1. Name batch folders with a sortable UTC timestamp (`yyyyMMddHHmmss`), so
   "latest" is a simple and reliable maximum.
2. Keep the default convenient (latest) and the override explicit (a folder
   name); fail on an unknown folder instead of falling back silently.
3. Check the parameter cell marker in the source file, not only in the UI.
4. Do not freeze a `%%configure` cell. A frozen cell is skipped at run time.
   A notebook that relies on `%%configure` for its default Lakehouse then has
   no Lakehouse, and its first `Files/...` access fails.
5. Compare the live definition with the repository copy before uploading,
   and upload only the intended change.

**Verification run:** the generator created folder `20261118072024`. The
loader loaded exactly that batch. All five Landing tables show
`meta_batch_timestamp = 2026-11-18 07:20:24` with the same load timestamp and
the expected counts (102, 194, 102, 102, 4,840).

**Finding for the next steps:** the Lakehouse SQL analytics endpoint exposed
the new Delta version only some time after the loader finished. dbt reads
Landing through that endpoint, so a dbt step started immediately after the
loader can snapshot the previous batch. The pipeline must refresh the SQL
endpoint metadata before dbt runs (O3/O4).

**Acceptance check:** the generator only writes files; the loader with an
empty `p_batch_folder` loads the newest folder, with a folder name loads that
folder, and with an unknown folder fails before writing anything.

### Iteration 63 (O3). One pipeline per layer, a config-driven Landing load and a master

**Goal:** replace the single `pl_master_orchestration` sketched in O1 with one
pipeline per layer, plus a master that only chains them:

```text
pl_Master
  Generate Data  --on success-->  Load Delta  --on success-->  DW create  --on success-->  Refresh semantic model
  (pl_Landing_Raw)                (pl_Landing_Delta)           (pl_Create_DW)                 (Semantic model refresh activity)
```

The original staged plan added the DW step in O4 and the semantic refresh in
O7. The current workspace definition includes both: the DW child is followed
by a Semantic model refresh activity for `sm_reporting`; the DW child must
succeed before that refresh starts. The semantic refresh is an activity in the
master, not a separate child pipeline.

**Why:**

- One pipeline per layer means each layer can be run on its own. After a
  failure, only the failed layer is rerun (for example Landing without
  generating new files), and monitoring shows which layer broke.
- The master holds no logic, only the order and the on-success dependencies.
- The Landing load is metadata-driven: a new source is one row in
  `Files/config/load_config.csv`, not a pipeline change.

The repository copies are in `fabric-artifacts/pipelines/<name>.DataPipeline/`
(`pipeline-content.json` and `.platform`).

#### The config file

`Files/config/load_config.csv` in `lh_oyster_path` (CSV, as the source
document's `load_config` but in CSV form):

```text
source_object_name,is_active,is_active_delta,type
Account,1,1,csv
Customer,1,1,csv
CustomerAdress,1,1,csv
DebitCard,1,1,csv
Transaction_Ledger,1,1,csv
```

`CustomerAdress` keeps the source's spelling, because it is the file prefix.
A Lookup returns every CSV value as a string, so the pipeline compares with
`'1'`, not `1`.

#### Step 1: `pl_Landing_Raw`

A single notebook activity that runs `nb_generate_deterministic_sample_data`.
The pipeline parameter `p_simulate_changes` (int, default `0`) is passed to
the notebook parameter of the same name.

#### Step 2: `pl_Landing_Delta`

```text
Set v_load_date
 -> Set v_batch_folder (= p_batch_folder)
 -> get_BatchFolders (Get Metadata, childItems of Files/<p_root_folder>)
 -> flt_BatchFolders (folders with a 14-character name)
 -> forEach_ResolveLatestBatch (sequential; If newer -> Set v_batch_folder)
 -> if_NoBatchFound (Fail: NO_BATCH_FOLDER)
 -> lkp_GetLoadConfig (Lookup, load_config.csv, firstRowOnly = false)
 -> Filter - active tables (is_active = '1' and is_active_delta = '1')
 -> forEach_FileLoader (sequential)
      sw_FileType on toLower(trim(item().type))
        csv     -> nb_csv_load_source_to_ld (p_batch_folder, p_entities = item().source_object_name)
        json    -> nb_json_load_source_to_ld (p_batch_folder)
        default -> Fail: UNSUPPORTED_SOURCE_TYPE
```

Parameters: `p_batch_folder` (string, default empty = latest) and
`p_root_folder` (default `landing/oyster`, relative to `Files`).
Variables: `v_load_date` and `v_batch_folder`.

The key expressions:

```text
v_load_date        @convertFromUtc(utcNow(), 'Central Europe Standard Time', 'yyyy-MM-dd HH:mm:ss')
v_batch_folder     @coalesce(pipeline().parameters.p_batch_folder, '')
batch folders      @and(equals(item().type, 'Folder'), equals(length(item().name), 14))
resolve items      @if(empty(pipeline().parameters.p_batch_folder), activity('flt_BatchFolders').output.value, createArray())
newer?             @greater(item().name, variables('v_batch_folder'))
active tables      @and(equals(item().is_active, '1'), equals(item().is_active_delta, '1'))
```

The Lakehouse is referenced inline in the Get Metadata and Lookup activities
(`linkedService` of type `Lakehouse` with `workspaceId`, `artifactId` and
`rootFolder: Files`), so no separate connection is needed inside the same
workspace:

```json
"linkedService": {
  "name": "lh_oyster_path",
  "properties": {
    "type": "Lakehouse",
    "typeProperties": {
      "workspaceId": "5e32bf37-c2be-4676-94c7-97213238ed30",
      "artifactId": "f27d086e-be7b-4e40-8e62-ea4d210b4538",
      "rootFolder": "Files"
    }
  }
}
```

The csv branch, the notebook activity inside the ForEach:

```json
{
  "name": "nb_csv_load_source_to_ld",
  "type": "TridentNotebook",
  "policy": { "timeout": "0.01:00:00", "retry": 0 },
  "typeProperties": {
    "notebookId": "730f3dc7-e69c-4c3f-af0b-cfb20ffc4433",
    "workspaceId": "5e32bf37-c2be-4676-94c7-97213238ed30",
    "parameters": {
      "p_batch_folder": { "value": { "value": "@variables('v_batch_folder')", "type": "Expression" }, "type": "string" },
      "p_entities":     { "value": { "value": "@item().source_object_name", "type": "Expression" }, "type": "string" }
    }
  }
}
```

#### Step 3: `pl_Master`

Three child activities are chained on success: `Generate Data`, `Load Delta`,
and `DW create and test`. The first two are Execute Pipeline activities (shown
as "Invoke pipeline (Legacy)" in the UI), both with `waitOnCompletion: true`.
`referenceName` is the child pipeline's item id. `p_simulate_changes` is
passed to the generator; `p_batch_folder` is passed empty to Landing, so a
normal run loads the batch that was just generated. The DW child receives the
secure `p_ado_token` parameter. A Semantic model refresh activity follows the
DW child, uses the Power BI connection, and waits for the full refresh to
complete.

#### Decision points

| Decision | Options | Choice and reason |
|---|---|---|
| Pipeline layout | One master with all activities, or one pipeline per layer with a master | **Per layer:** layers can be rerun alone; failures are localised |
| Loader in the ForEach | Reuse `nb_csv_load_source_to_ld` with `p_entities`, or write a new per-table notebook | **Reuse:** one tested code base, same validation and `meta_*` columns |
| Who picks "latest" | Each loader call on its own, or once in the pipeline | **Once in the pipeline:** every entity gets the same folder, even if a new folder appears during the run |
| How "latest" is found | A small notebook, or Get Metadata + ForEach + If | **Native activities:** no Spark session, runs in seconds; `greater()` compares strings, correct for `yyyyMMddHHmmss` |
| ForEach mode | Sequential or parallel (`batchCount`) | **Sequential:** predictable, one Spark session at a time; parallel is a later optimisation |
| File type routing | CSV only, or Switch on `type` | **Switch:** `csv` and `json` loaders; any other value fails the run |
| `v_load_date` | Batch folder name, or run time | **Run time in Budapest local time:** identifies the run in monitoring and later in notifications (O5) |
| Child call | Execute Pipeline (legacy) or the new Invoke pipeline | **Execute Pipeline:** same workspace, no connection needed; the new activity needs a connection but can call across workspaces |

**Legacy support clarification:** “Legacy” identifies the older activity; it does
not by itself mean that the activity is unsupported. Microsoft currently
documents both activities. The legacy activity can invoke Fabric pipelines in
the same workspace and monitors the parent pipeline. The new Invoke pipeline
activity can also invoke pipelines across Fabric workspaces (and ADF or
Synapse), and supports monitoring the child pipeline; it requires a Connection
object. Since this master and its child pipelines are in the same workspace,
the legacy activity remains a supported fit for this design. See Microsoft's
[Invoke pipeline activity documentation](https://learn.microsoft.com/en-us/fabric/data-factory/invoke-pipeline-activity)
for current capabilities.

**Trade-off of per-entity loader calls:** called with all entities, the loader
validates every file before writing any table. Called per entity, an error in
the fourth entity leaves the first three tables already on the new batch. This
is acceptable because every iteration gets the same batch folder, so one rerun
of `pl_Landing_Delta` (with the same `p_batch_folder` if needed) restores
a consistent state, and the dbt step only starts when the whole Landing
pipeline succeeded.

**Best practices**

1. Keep layer pipelines independently runnable; the master only orders them.
2. Use on-success dependencies and `waitOnCompletion: true` in the master, so
   the next layer never reads a half-loaded previous layer.
3. Resolve the batch once per run and pass it to every activity.
4. Fail loudly: no batch folder and an unknown `type` both stop the run with
   a clear error code, instead of loading nothing or the wrong thing.
5. Normalise config values (`toLower(trim(...))`) and remember that Lookup
   returns CSV values as strings.
6. Set explicit activity timeouts (1 hour per notebook, 10 minutes for
   metadata activities) instead of the 12-hour default.
7. Keep retries at 0 until retry behaviour is designed (O5).
8. Store each pipeline in Git with a unique `logicalId` in `.platform`.

**Verification run:** `pl_Master` ran with default parameters and
completed in about 23.5 minutes. Generate Data created folder
`20261118093359`. Load Delta resolved the latest batch from four folders to
`20261118093359`. The Lookup returned five config rows, all five passed the
filter, and the csv branch loaded each entity. All five Landing tables show
`meta_batch_timestamp = 2026-11-18 09:33:59` with the expected counts (102,
102, 194, 102, 4,840).

Two observations follow from the per-entity design:

- `meta_load_timestamp` now differs per table (09:38 to 09:52), because each
  table is loaded by its own notebook run. `meta_batch_timestamp` is the
  column that ties a consistent load together.
- Each notebook activity starts its own Spark session, about 3.5 minutes per
  entity, so the Landing step takes about 18 minutes for five small files.
  Options for later: run the ForEach in parallel (`batchCount`), enable
  high-concurrency sessions for pipelines with a shared session tag, or call
  the loader once with all active entities as a comma-separated `p_entities`.

**Acceptance check:** each layer pipeline runs on its own and from the master;
all Landing tables carry the same, newest `meta_batch_timestamp`; a config row
with an unknown `type` fails the run.

## What we deliberately have not implemented

- Additional facts (for example balances), remaining business rules, and
  business approval of Gold tier thresholds/transition policy.
- Production Silver and Gold builds, Gold data publication, and Power BI
  semantic model/report authoring.
- A production schema-migration procedure for protected append-only tables.
- A delivery completion manifest and expected-count/anomaly validation.
- Cross-table atomic publication or pinned source versions.
- An enforced snapshot batch-order/high-water mark.
- Interval tests for future dimensions (customer dimension and Bronze-backed
  staging interval tests are implemented).
- Recovery automation for replaying retained batches.
- CSV parser corruption checks and a comprehensive schema-collision policy.
- An explicit downstream policy for new extra columns.
- Scheduling, notifications, permissions, and retention automation.

These are real next decisions, not guarantees supplied by the existing tests.

## Troubleshooting encountered during development

| Symptom | Interpretation/action |
|---|---|
| Wrong target principal | Re-copy and verify the SQL hostname |
| Named Pipes / host not found | Check spelling, including accidental double dots |
| Failed to invoke Azure CLI | Check CLI availability/login in that exact execution environment |
| Private-link/IP restriction | Use the approved network path; valid credentials alone are insufficient |
| Landing table not found | Check Lakehouse attachment, loader run, SQL endpoint visibility, database/schema/case |
| Files under a local driver path | Attach the Lakehouse; local `/lakehouse/default` assumptions are not proof of OneLake persistence |
| Unused `models.dbt_oyster_path.example` | Preserved starter configuration has no matching resource; unrelated to snapshot correctness |
| `dbt compile` needs a connection | Adapter introspection and `is_incremental()` can require live relation lookup; parse success is not a live SQL check |
| Transaction deadlocked, chosen as victim | Concurrent writes on Fabric Warehouse; the profile uses `threads: 1` (Iteration 59) |
| Invalid column on an append-only model | The protected table predates a source-contract change; migrate its schema (Iteration 57) |

## Final source index

- [Project defaults](../dbt_project.yml)
- [Dev/prod profiles](../profiles.yml)
- [Schema naming macro](../macros/generate_schema_name.sql)
- [Landing sources, column documentation, key tests, freshness](../models/landing/_landing__sources.yml)
- [Generator notebook](../../fabric-artifacts/notebooks/nb_generate_deterministic_sample_data.Notebook/notebook-content.py)
- [Loader notebook](../../fabric-artifacts/notebooks/nb_csv_load_source_to_ld.Notebook/notebook-content.py)
- [Four snapshots and their column tests](../snapshots/bronze/_bronze__snapshots.yml)
- [Incremental ledger](../models/bronze/bz_oys_transaction_ledger.sql)
- [Ledger column tests and operational description](../models/bronze/_bronze__models.yml)
- [Landing batch test](../tests/landing/assert_landing_single_batch.sql)
- [Current snapshot version test](../tests/bronze/assert_bronze_current_keys_unique.sql)
- [Transaction immutability test](../tests/bronze/assert_transaction_events_immutable.sql)
- [Gold dimensions, facts, and snapshot tests](../models/gold/)
- [Gold account-tier snapshot model](../models/gold/gold_fct_account_tier_daily_snapshot.sql)
- [Gold account-tier snapshot tests](../tests/gold/)
- [Gold snapshot end-date test](../tests/gold/assert_gold_snapshot_end_date.sql)
- [Initial-load history policy macros](../macros/initial_history_policy.sql)
- [Guarded development drop macro](../macros/dev_drop_relation.sql)

**The development principle:** first name the data, then describe it, then test
the assumptions, then store history, then test that history. Add shared behaviour
once for every affected table; introduce a different mechanism only when the
data's meaning actually requires it.
