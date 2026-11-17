Welcome to your new dbt project!

### Oyster Bronze and Silver walkthrough

See the [iterative development guide](docs/bronze-development-walkthrough.md)
for setup, landing ingestion, source definitions, snapshots, the incremental
ledger, Silver SCD2 dimensions, the transaction star, tests, and step-by-step
verification.

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
or execute snapshots. They validate the existing history and rebuild all Silver
models, including the independent card dimension. A passing run is required
before reporting publication; this is not an atomic publication mechanism.
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
