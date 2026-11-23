Welcome to your new dbt project!

### Oyster Bronze and Silver walkthrough

See the [iterative development guide](docs/bronze-development-walkthrough.md)
for setup, landing ingestion, source definitions, snapshots, the incremental
ledger, Silver SCD2 dimensions, the transaction star, tests, and step-by-step
verification.

### Current Landing loaders

The [CSV loader](../fabric-artifacts/notebooks/nb_csv_load_source_to_ld.Notebook/notebook-content.py)
loads one `p_in_table_name` per invocation and overwrites
`dbo.ld_oys_<lowercase source name>`. It adds `meta_batch_timestamp`,
`meta_load_timestamp`, and `meta_source_file` with `withColumn`; it rejects
empty input and metadata-name collisions, but does not validate business keys
or required CSV columns. Batch time is shared across a pipeline run; load time
is specific to each notebook invocation.

The [JSON loader](../fabric-artifacts/notebooks/nb_json_load_source_to_ld.Notebook/notebook-content.py)
currently supports `Transaction_Ledger` and validates its nested data. The
[Landing pipeline](../fabric-artifacts/pipelines/pl_jb_landing_delta.DataPipeline/pipeline-content.json)
resolves the batch once and passes the root, batch, and source name to either
loader. Its ForEach runs up to two iterations in parallel; enable only one
format per source because both formats overwrite the same target.

Run the local loader and pipeline tests from the repository root:

```powershell
python -B -m unittest discover -s .\fabric-artifacts\tests -v
```

These tests use mocks and do not replace a Fabric execution.

### Validating and rebuilding Silver

From this directory, with existing Landing/Bronze relations and a working Fabric
connection:

```powershell
& ..\.venv\Scripts\dbt.exe test --selector silver_upstream_checks --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Upstream validation failed; stop before rebuilding Silver.' }
& ..\.venv\Scripts\dbt.exe build --selector silver --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw 'Silver build or validation failed; do not publish.' }
```

These [selectors](selectors.yml) do not run the loader, append Bronze events,
or execute snapshots. They validate the existing history and rebuild all BIS
and Silver models, including the independent card dimension. BIS models are
tables and must be rebuilt after Bronze changes before downstream models or
tests can use fresh data. The view-to-table change has not yet been validated
in Fabric; approve and verify the conversion before publishing. A passing run
is required before reporting publication; this is not an atomic publication
mechanism.
See iteration 51 of the [walkthrough](docs/bronze-development-walkthrough.md)
for selection previews, acceptance criteria, and historical test scenarios.

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
