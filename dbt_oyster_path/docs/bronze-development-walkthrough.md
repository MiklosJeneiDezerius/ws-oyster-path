# Building the Oyster Bronze layer, one small change at a time

This guide starts with Day 3 extraction, then reconstructs our development from dbt setup to the current Bronze
implementation. It is a teaching sequence, not a verbatim transcript: each
iteration introduces one idea, explains its purpose, shows the code, and gives
an acceptance check. Applying the same pattern to all tables counts as one
iteration, not five.

**Scope:** raw-file generation and extraction orchestration, Fabric Lakehouse
landing tables, four Warehouse SCD2 snapshots, and
one append-only incremental transaction ledger, and the iterative Silver
extension. Silver currently has shared configuration and typed staging views;
its customer, account, and debit-card SCD2 dimensions are implemented.
Date/time dimensions, explicit customer/account Unknown members, and the
transaction star fact with validation tests are also implemented. Gold is not
yet implemented.

**Status:** project parsing and resource discovery have passed. Earlier live
attempts were blocked by authentication/private-link access. In iteration 52,
the dev connection succeeded: upstream validation returned eight passes and
19 execution errors from missing relations/columns. Silver was not rebuilt.
Live Silver creation and history demonstrations remain unverified. Expected
outcomes below are acceptance criteria, not claims of successful runs.

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
alternative nested representation. The CSV loader does not consume that JSON.

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
The generator writes both transaction CSV and JSON; this loader uses CSV.

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
performed:

1. Create a new complete delivery folder with a strictly later batch timestamp.
2. Change one customer's `LastName`; preserve every other entity and key.
3. Load that delivery and pass landing tests/freshness.
4. Snapshot and inspect that customer's versions.

Inspect the deliberately changed customer in the Warehouse:

```sql
select CustomerID, FirstName, LastName,
       dbt_valid_from, dbt_valid_to, dbt_is_deleted
from dev_bronze.bz_oys_customer
where CustomerID = '1001'
order by dbt_valid_from;
```

Use the actual chosen key if it is not `1001`.

**Expected:** one additional version for that customer (two versions if there
was only one before the test). The old version closes at the new batch
observation time; the new version is current. The other snapshots do not gain
versions merely because their load metadata changed.

Record the counts before and after: the customer snapshot increases by exactly
one, and other snapshot counts stay unchanged. Run snapshots again without
changing landing; no further versions should appear.

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

**Next prerequisite step:** inspect deployed Landing table names/columns and
compare them with the updated loader/source contract. Publish/run the approved
loader only after confirming the intended Lakehouse, source batch, and overwrite
scope. Once Landing matches the contract and source tests pass, initialize or
update the approved development Bronze snapshots/ledger, then rerun the upstream
gate before attempting Silver. These writes are separate from the completed
validation attempt and require their own review.

## What we deliberately have not implemented

- Additional facts (for example balances), remaining business rules, and Gold
  reporting models. The transaction star and its relationship tests are implemented.
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

**The development principle:** first name the data, then describe it, then test
the assumptions, then store history, then test that history. Add shared behaviour
once for every affected table; introduce a different mechanism only when the
data's meaning actually requires it.
