import ast
import csv
import hashlib
import json
import re
import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / "dbt_oyster_demo_test_core"
ARTIFACTS = ROOT / "fabric-artifacts-core"
NOTEBOOKS = ARTIFACTS / "040_Notebooks"


def notebook_code(name):
    path = next(NOTEBOOKS.rglob(f"{name}.Notebook")) / "notebook-content.py"
    source = path.read_text(encoding="utf-8")
    return re.sub(
        r"(?ms)^%%configure\n.*?(?=^# (?:PARAMETERS CELL|CELL|METADATA|MARKDOWN)\b)",
        "",
        source,
    )


class CoreContractTests(unittest.TestCase):
    def test_four_source_config_and_pipeline_bindings(self):
        config = json.loads((ARTIFACTS / "config" / "load_config.json").read_text())
        self.assertEqual(
            [row["source_object_name"] for row in config],
            ["Customer", "Account", "DebitCard", "Transaction_Ledger"],
        )
        self.assertTrue(all(row["is_active_delta"] == "1" for row in config))
        platforms = [
            json.loads(path.read_text()) for path in ARTIFACTS.rglob(".platform")
        ]
        logical_ids = {item["config"]["logicalId"] for item in platforms}
        self.assertEqual(len(platforms), len(logical_ids))
        for path, item in zip(ARTIFACTS.rglob(".platform"), platforms):
            if item["metadata"]["type"] == "Notebook":
                self.assertTrue((path.parent / "notebook-content.py").is_file(), path.parent)
        full_ids = {
            json.loads(path.read_text())["config"]["logicalId"]
            for path in (ROOT / "fabric-artifacts").rglob(".platform")
        }
        self.assertFalse(logical_ids & full_ids)

        def check_references(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in ("notebookId", "pipelineId", "artifactId", "datasetId"):
                        self.assertIn(item, logical_ids, key)
                    check_references(item)
            elif isinstance(value, list):
                for item in value:
                    check_references(item)

        for path in ARTIFACTS.rglob("pipeline-content.json"):
            pipeline = json.loads(path.read_text())
            check_references(pipeline)
            activities = pipeline["properties"]["activities"]
            activity_names = {activity["name"] for activity in activities}
            for activity in activities:
                for dependency in activity["dependsOn"]:
                    self.assertIn(dependency["activity"], activity_names)
            if path.parent.name == "pl_Landing_Delta_Core.DataPipeline":
                filter_activity = next(a for a in activities if a["type"] == "Filter")
                self.assertIn("is_active_delta", filter_activity["typeProperties"]["condition"]["value"])

    def test_notebook_syntax_and_runner_parity(self):
        for name in (
            "Oyster_Deterministic_Data_Generator_Core",
            "Oyster_Json_Transaction_Generator_Core",
            "nb_datev_normal_csv_load_source_to_lr_core",
            "nb_create_delta_from_raw_core",
            "nb_bc_create_delta_from_parquet_timestamp_core",
            "nb_normal_csv_load_source_to_ld_core",
            "nb_json_load_source_to_ld_core",
            "nb_dbt_run_python_core",
        ):
            ast.parse(notebook_code(name))
        reference = (
            ROOT / "fabric-artifacts" / "040_Notebooks" / "030_Framework_notebooks"
            / "040_dbt" / "nb_dbt_run_python.Notebook" / "notebook-content.py"
        ).read_text(encoding="utf-8")
        core = notebook_code("nb_dbt_run_python_core")
        self.assertEqual(
            core,
            reference.replace(
                "dbt_subfolder = globals().get('dbt_subfolder', 'dbt_oyster_demo_test')",
                "dbt_subfolder = globals().get('dbt_subfolder', 'dbt_oyster_demo_test_core')",
            ),
        )

    def test_core_warehouse_is_independent(self):
        warehouse = ARTIFACTS / "dw_oyster_core.Warehouse"
        platform = json.loads((warehouse / ".platform").read_text())
        project = (warehouse / "dw_oyster_core.sqlproj").read_text()
        profile = (PROJECT / "profiles.yml").read_text()
        model_expression = (
            ARTIFACTS / "sm_oyster_core.SemanticModel" / "definition" / "expressions.tmdl"
        ).read_text()
        pipeline = json.loads(
            (
                ARTIFACTS / "030_Pipelines" / "040_Jobs" / "030_dbt"
                / "pl_Create_DW_Core.DataPipeline" / "pipeline-content.json"
            ).read_text()
        )
        self.assertEqual(platform["metadata"]["displayName"], "dw_oyster_core")
        self.assertIn("<Name>dw_oyster_core</Name>", project)
        core_server = (
            "xe6b73af6wce5cpkwv3dpk3vzi-htsck64qztlehhjsieqb3d7vzi"
            ".datawarehouse.fabric.microsoft.com"
        )
        self.assertIn(f"env_var('DBT_SERVER', '{core_server}')", profile)
        self.assertIn("env_var('DBT_DATABASE', 'dw_oyster_core')", profile)
        self.assertIn('"dw_oyster_core"', model_expression)
        self.assertIn("REPLACE_WITH_CORE_WAREHOUSE_SQL_ENDPOINT", model_expression)
        notebook_parameters = pipeline["properties"]["activities"][0]["typeProperties"]["parameters"]
        self.assertIn("dbt_server", notebook_parameters)

    def test_core_snapshot_warehouse_contains_card_surrogate_key(self):
        table = (
            ARTIFACTS
            / "dw_oyster_core.Warehouse"
            / "oystr_core_gold"
            / "Tables"
            / "g_fct_periodic_spend_snapshot.sql"
        ).read_text()
        self.assertRegex(table, r"\[s_dim_card_sk\]\s+VARCHAR\s*\(64\)\s+NULL")

    def test_full_warehouse_server_is_pipeline_parameterized(self):
        full_profile = (ROOT / "dbt_oyster_demo_test" / "profiles.yml").read_text()
        full_runner = (
            ROOT / "fabric-artifacts" / "040_Notebooks" / "030_Framework_notebooks"
            / "040_dbt" / "nb_dbt_run_python.Notebook" / "notebook-content.py"
        ).read_text(encoding="utf-8")
        pipeline = json.loads(
            (
                ROOT / "fabric-artifacts" / "030_Pipelines" / "040_Jobs" / "030_dbt"
                / "pl_Create_DW.DataPipeline" / "pipeline-content.json"
            ).read_text()
        )
        notebook_parameters = pipeline["properties"]["activities"][0]["typeProperties"]["parameters"]
        self.assertIn("env_var('DBT_SERVER')", full_profile)
        self.assertIn("dbt_environment['DBT_SERVER'] = dbt_server", full_runner)
        self.assertEqual(
            notebook_parameters["dbt_server"]["value"],
            "xe6b73af6wce5cpkwv3dpk3vzi-ufjmp33zdiqudnk6b6giyfzpxi.datawarehouse.fabric.microsoft.com",
        )

    def test_dbt_runner_pins_compatible_adapter_and_core(self):
        runner_paths = (
            ARTIFACTS / "040_Notebooks" / "030_Framework_notebooks" / "040_dbt"
            / "nb_dbt_run_python_core.Notebook" / "notebook-content.py",
            ROOT / "fabric-artifacts" / "040_Notebooks" / "030_Framework_notebooks"
            / "040_dbt" / "nb_dbt_run_python.Notebook" / "notebook-content.py",
        )
        for path in runner_paths:
            code = path.read_text(encoding="utf-8")
            self.assertIn("'dbt-fabric==1.11.1'", code)
            self.assertIn("'dbt-core<1.12'", code)

    def test_generator_has_four_entities_and_aligned_platinum_scenario(self):
        code = notebook_code("Oyster_Deterministic_Data_Generator_Core")
        code = code.replace("num_customers = 83", "num_customers = 7")
        real_open = open
        with tempfile.TemporaryDirectory() as directory:
            def local_open(path, *args, **kwargs):
                return real_open(Path(directory) / Path(path).name, *args, **kwargs)

            with patch("os.makedirs"), patch("builtins.open", side_effect=local_open):
                namespace = {}
                exec(compile(code, "<core generator>", "exec"), namespace)
            files = list(Path(directory).glob("*.csv"))
            self.assertEqual(len(files), 4)
            self.assertEqual(len(namespace["customer_rows"]), 7)
            self.assertEqual(len(namespace["account_rows"]), 7)
            self.assertEqual(namespace["debit_card_rows"], [
                (100, 10, "Standard"), (200, 20, "Gold"),
                (300, 30, "Standard"), (400, 40, "Gold"),
                (500, 50, "Gold"), (600, 60, "Platinum"),
                (700, 70, "Platinum"),
            ])
            totals = {}
            for _, account, amount, _ in namespace["transaction_rows"]:
                totals[account] = totals.get(account, 0) + amount
            self.assertEqual(
                totals,
                {10: 900, 20: 3600, 30: 8250, 40: 9000, 50: 750, 60: 8250, 70: 675},
            )
            self.assertGreaterEqual(totals[60], 5000)
            self.assertLess(totals[70], 2000)
            for path in files:
                with real_open(path, newline="", encoding="utf-8") as stream:
                    self.assertTrue(list(csv.DictReader(stream)))

    def test_core_json_generator_emits_nested_transaction_batches(self):
        code = notebook_code("Oyster_Json_Transaction_Generator_Core")
        code = code.replace("num_customers = 83", "num_customers = 7")
        real_open = open
        with tempfile.TemporaryDirectory() as directory:
            def local_open(path, *args, **kwargs):
                return real_open(Path(directory) / Path(path).name, *args, **kwargs)

            with patch("os.makedirs"), patch("builtins.open", side_effect=local_open):
                namespace = {}
                exec(compile(code, "<core JSON transaction generator>", "exec"), namespace)
            files = list(Path(directory).iterdir())
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].suffix, ".json")
            with real_open(files[0], encoding="utf-8") as stream:
                document = json.load(stream)
        batches = document["transaction_batches"]
        transactions = [entry["transaction"] for batch in batches for entry in batch]
        self.assertEqual(len(transactions), 78)
        self.assertEqual(list(transactions[0]), [
            "TransactionID", "AccountID", "Amount", "Timestamp",
        ])
        self.assertEqual(transactions[0]["TransactionID"], 1)
        self.assertEqual(transactions[0]["AccountID"], 10)
        self.assertEqual(transactions[0]["Amount"], 60)

    def test_core_json_loader_uses_flatten_and_explode(self):
        notebook = next(NOTEBOOKS.rglob("nb_json_load_source_to_ld_core.Notebook"))
        platform = json.loads((notebook / ".platform").read_text())
        code = notebook_code("nb_json_load_source_to_ld_core")
        ast.parse(code)
        self.assertEqual(platform["metadata"]["displayName"], "nb_json_load_source_to_ld_core")
        self.assertIn('explode(flatten(col("transaction_batches")))', code)
        self.assertIn('table_name = "ld_oys_transaction_ledger"', code)

    def test_full_json_generator_preserves_full_ledger_data(self):
        csv_generator = (
            ROOT / "fabric-artifacts" / "040_Notebooks" / "020_Utility_notebooks"
            / "c26cc17a-260e-8c59-44fc-d6cd231fc926.Notebook" / "notebook-content.py"
        ).read_text(encoding="utf-8")
        ast.parse(csv_generator)
        self.assertNotIn("json.dump(", csv_generator)
        code = (
            ROOT / "fabric-artifacts" / "040_Notebooks" / "020_Utility_notebooks"
            / "Oyster_Json_Transaction_Generator.Notebook" / "notebook-content.py"
        ).read_text(encoding="utf-8")
        ast.parse(code)
        code = code.replace("num_customers = 83", "num_customers = 7")
        real_open = open
        with tempfile.TemporaryDirectory() as directory:
            def local_open(path, *args, **kwargs):
                return real_open(Path(directory) / Path(path).name, *args, **kwargs)

            with patch("os.makedirs"), patch("builtins.open", side_effect=local_open):
                namespace = {}
                exec(compile(code, "<full JSON transaction generator>", "exec"), namespace)
            files = list(Path(directory).iterdir())
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].suffix, ".json")
            with real_open(files[0], encoding="utf-8") as stream:
                document = json.load(stream)
        transactions = [entry["transaction"] for batch in document["transaction_batches"] for entry in batch]
        self.assertEqual(len(transactions), 78)
        self.assertEqual(list(transactions[0]), [
            "TransactionID", "AccountID", "Amount", "Timestamp",
            "Counterparty", "TransactionStatusID",
        ])
        self.assertEqual(transactions[0]["Counterparty"], "Kaufland")
        self.assertEqual(transactions[0]["TransactionStatusID"], 1)

    def test_full_json_loader_uses_flatten_and_explode(self):
        notebook = (
            ROOT / "fabric-artifacts" / "040_Notebooks" / "030_Framework_notebooks"
            / "020_Landing_delta" / "nb_json_load_source_to_ld.Notebook"
        )
        platform = json.loads((notebook / ".platform").read_text())
        code = (notebook / "notebook-content.py").read_text(encoding="utf-8")
        ast.parse(code)
        self.assertEqual(platform["metadata"]["displayName"], "nb_json_load_source_to_ld")
        self.assertIn('explode(flatten(col("transaction_batches")))', code)
        self.assertIn('"Counterparty"', code)
        self.assertIn('"TransactionStatusID"', code)
        self.assertIn('table_name = "ld_oys_transaction_ledger"', code)

    def test_full_generator_has_matching_aligned_platinum_scenario(self):
        generator = (
            ROOT / "fabric-artifacts" / "040_Notebooks" / "020_Utility_notebooks"
            / "c26cc17a-260e-8c59-44fc-d6cd231fc926.Notebook" / "notebook-content.py"
        ).read_text(encoding="utf-8")
        scenario = re.search(
            r"(?ms)^    elif scenario == 6:\n(.*?)(?=^# =+|\Z)", generator,
        )
        self.assertIsNotNone(scenario)
        self.assertIn("account_id, f\"40071012100", scenario.group(1))
        self.assertIn("range(15)", scenario.group(1))
        self.assertIn("550.00", scenario.group(1))

    def test_loader_rejects_unsupported_or_malformed_sources(self):
        code = notebook_code("nb_normal_csv_load_source_to_ld_core")
        code = code.split("\nschema = StructType", 1)[0]
        tree = ast.parse(code)
        tree.body = [
            node for node in tree.body
            if not (isinstance(node, ast.ImportFrom) and node.module.startswith("pyspark"))
        ]
        path = "Files/oyster/landing_raw/20261105120000/Customer_20261105120000_core.csv"

        def run(files, table="Customer"):
            source = SimpleNamespace(collect=lambda: files)
            spark = SimpleNamespace(sparkContext=SimpleNamespace(binaryFiles=lambda _: source))
            namespace = {"spark": spark}
            parameter_node = next(
                node for node in tree.body
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "p_in_table_name"
            )
            parameter_node.value = ast.Constant(value=table)
            exec(compile(ast.fix_missing_locations(tree), "<core loader>", "exec"), namespace)
            return namespace

        valid = b"CustomerID,FirstName,LastName\n1,Anna,Nagy\n"
        self.assertEqual(run([(path, valid)])["rows"][0][:3], ["1", "Anna", "Nagy"])
        utf16 = "CustomerID,FirstName,LastName\n1,Anna,Nagy\n".encode("utf-16")
        self.assertEqual(run([(path, utf16)])["rows"][0][:3], ["1", "Anna", "Nagy"])
        card_path = path.replace("Customer_", "DebitCard_")
        card_csv = b"CardID,AccountID,CardTier\n100,10,Gold\n"
        self.assertEqual(
            run([(card_path, card_csv)], "DebitCard")["rows"][0][:3],
            ["100", "10", "Gold"],
        )
        with self.assertRaises(ValueError):
            run([(path, valid)], "CardTier")
        with self.assertRaises(FileNotFoundError):
            run([])
        for invalid in (
            b"CustomerID,FirstName\n1,Anna\n",
            b"CustomerID,FirstName,LastName\n,Anna,Nagy\n",
            b"CustomerID,FirstName,LastName\n1,Anna,Nagy,extra\n",
        ):
            with self.assertRaises(ValueError):
                run([(path, invalid)])

    def test_actual_rolling_cte_includes_zero_days_and_exact_boundaries(self):
        sql = (PROJECT / "models" / "030_gold" / "g_fct_periodic_spend_snapshot.sql").read_text()
        ctes = sql.split("\nSELECT\n    CONVERT", 1)[0]
        ctes = re.sub(r"\{\{ ref\('(\w+)'\) \}\}", r"\1", ctes)
        ctes = ctes.replace(
            "DATEADD(day, -29, CAST('2025-01-01' AS DATE))",
            "date('2025-01-01', '-29 days')",
        ).replace("CAST(GETDATE() AS DATE)", "'2025-02-01'")
        ctes = ctes.replace(
            "DATEADD(day, 1, CAST(d.[FullDate] AS DATETIME2(6)))",
            "datetime(d.[FullDate], '+1 day')",
        )
        ctes = ctes.replace("CAST([transaction_dttm] AS DATE)", "date([transaction_dttm])")
        with sqlite3.connect(":memory:") as connection:
            connection.executescript("""
                CREATE TABLE s_dim_date(s_dim_date_sk INTEGER, full_dt TEXT);
                CREATE TABLE s_dim_account(
                    s_dim_account_sk TEXT,
                    s_dim_customer_sk TEXT,
                    account_id INTEGER,
                    customer_id INTEGER,
                    card_id INTEGER,
                    card_tier_nm TEXT
                );
                CREATE TABLE s_dim_customer(
                    s_dim_customer_sk TEXT,
                    customer_id INTEGER,
                    valid_from_dttm TEXT,
                    valid_to_dttm TEXT
                );
                CREATE TABLE s_dim_card(
                    s_dim_card_sk TEXT,
                    s_dim_account_sk TEXT,
                    card_id INTEGER,
                    card_tier_nm TEXT,
                    valid_from_dttm TEXT,
                    valid_to_dttm TEXT
                );
                CREATE TABLE s_fct_transaction(s_dim_account_sk TEXT, transaction_dttm TEXT, transaction_amt NUMERIC);
                INSERT INTO s_dim_account VALUES
                    ('a1', 'c1', 1, 1, 101, 'Standard'),
                    ('a2', 'c2', 2, 2, NULL, NULL);
                INSERT INTO s_dim_customer VALUES
                    ('c1-v1', 1, '1900-01-01 00:00:00', '9999-12-31 00:00:00'),
                    ('c2-v1', 2, '1900-01-01 00:00:00', '9999-12-31 00:00:00');
                INSERT INTO s_dim_card VALUES
                    ('card-1-v1', 'a1', 101, 'Standard', '1900-01-01 00:00:00', '9999-12-31 00:00:00');
                INSERT INTO s_fct_transaction VALUES
                    ('a1', '2024-12-02 12:00:00', 10000),
                    ('a1', '2024-12-03 00:00:00', 2000),
                    ('a1', '2025-01-01 23:59:59', 3000);
            """)
            start = date(2024, 12, 2)
            connection.executemany(
                "INSERT INTO s_dim_date VALUES (?, ?)",
                [(int((start + timedelta(days=n)).strftime("%Y%m%d")),
                  (start + timedelta(days=n)).isoformat()) for n in range(62)],
            )
            query = ctes + "\nSELECT FullDate, AccountID, Rolling30DaySpend FROM rolling_spend"
            values = {(day, account): amount for day, account, amount in connection.execute(query)}
            self.assertEqual(values[("2025-01-01", 1)], 5000)
            self.assertEqual(values[("2025-01-02", 1)], 3000)
            self.assertEqual(values[("2025-01-30", 1)], 3000)
            self.assertEqual(values[("2025-01-31", 1)], 0)
            self.assertTrue(all(amount == 0 for (day, account), amount in values.items() if account == 2))
            tier_case = re.search(
                r"    CASE\n.*?    END AS \[calculated_spending_tier_nm\]", sql, re.S,
            ).group().replace("[Rolling30DaySpend]", "[rolling_30_day_spend_amt]")
            tiers = dict(connection.execute(
                "SELECT rolling_30_day_spend_amt, " + tier_case +
                " FROM (SELECT 1999.99 AS rolling_30_day_spend_amt UNION ALL SELECT 2000 "
                "UNION ALL SELECT 4999.99 UNION ALL SELECT 5000)"
            ))
            self.assertEqual(tiers, {1999.99: "Standard", 2000: "Gold", 4999.99: "Gold", 5000: "Platinum"})

    def test_dbt_and_semantic_report_contract(self):
        models = {path.stem: path.read_text() for path in (PROJECT / "models").rglob("*.sql")}
        self.assertEqual(set(models), {
            "b_oys_customer", "b_oys_account", "b_oys_debit_card", "b_oys_transaction",
            "b_oys_customer_address",
            "s_dim_customer", "s_dim_card", "s_dim_account", "s_dim_date", "s_fct_transaction",
            "ga_dim_customer", "ga_dim_account", "g_dim_date",
            "g_fct_periodic_spend_snapshot", "fail_transactions_without_accountid",
        })
        snapshot_names = {path.stem for path in (PROJECT / "snapshots").glob("*.sql")}
        bronze_snapshot_names = {
            "b_oys_customer_history",
            "b_oys_customer_address_history",
            "b_oys_account_history",
            "b_oys_debit_card_history",
            "b_oys_transaction_history",
        }
        self.assertTrue(bronze_snapshot_names.issubset(snapshot_names))
        for snapshot_name in bronze_snapshot_names:
            snapshot_sql = (PROJECT / "snapshots" / f"{snapshot_name}.sql").read_text()
            self.assertIn(f"{{% snapshot {snapshot_name} %}}", snapshot_sql)
            self.assertIn("target_schema='core_bronze'", snapshot_sql)
            self.assertIn("strategy='check'", snapshot_sql)
            self.assertIn("hard_deletes='invalidate'", snapshot_sql)
        for model_name, sql in models.items():
            for reference in re.findall(r"\{\{ ref\('(\w+)'\) \}\}", sql):
                self.assertIn(reference, set(models) | snapshot_names)
            if model_name.startswith("b_oys_"):
                self.assertIn("materialized='table'", sql)
                self.assertNotIn("materialized='incremental'", sql)
            else:
                self.assertNotIn("{{ config", sql)

        definition = ARTIFACTS / "sm_oyster_core.SemanticModel" / "definition"
        fields = {}
        for path in (definition / "tables").glob("*.tmdl"):
            source = path.read_text(encoding="utf-8-sig")
            self.assertIn("schemaName: oystr_core_gold", source)
            for physical in re.findall(r"sourceColumn: (\w+)", source):
                self.assertIn(physical, models[path.stem])
            fields[path.stem] = set(re.findall(r"^\tcolumn ([^\r\n]+)", source, re.M))
            fields[path.stem].update(
                re.findall(r"^\tmeasure '([^']+)'", source, re.M)
            )
        self.assertEqual(set(fields), {
            "g_dim_date", "ga_dim_customer", "ga_dim_account",
            "g_fct_periodic_spend_snapshot",
        })
        measures = (definition / "tables" / "g_fct_periodic_spend_snapshot.tmdl").read_text()
        self.assertIn("REMOVEFILTERS('g_fct_periodic_spend_snapshot'[CalculatedSpendingTier])", measures)
        for measure in (
            "Total No. of Cards", "Upgrade Candidate Cards", "Downgrade Candidate Cards",
            "Aligned Cards", "Misaligned Cards", "Tier Mismatch Rate",
        ):
            self.assertIn(f"measure '{measure}'", measures)
        self.assertIn(
            "DISTINCTCOUNTNOBLANK ( 'g_fct_periodic_spend_snapshot'[CardID] )",
            measures,
        )
        snapshot_sql = models["g_fct_periodic_spend_snapshot"]
        snapshot_output = snapshot_sql.split("\n)\nSELECT\n    CONVERT", 1)[1]
        for column in (
            "card_id", "actual_card_tier_nm", "is_upgrade_candidate_flag",
            "is_low_spend_premium_anomaly_flag", "alignment_status_cd",
        ):
            self.assertIn(f"[{column}]", snapshot_output)
        report = ARTIFACTS / "050_Reports" / "rpt_oyster_core.Report"
        reference = json.loads((report / "definition.pbir").read_text())
        self.assertEqual(
            (report / reference["datasetReference"]["byPath"]["path"]).resolve(),
            definition.parent.resolve(),
        )

        def check_fields(value):
            if isinstance(value, dict):
                for kind in ("Column", "Measure"):
                    if kind in value:
                        field = value[kind]
                        entity = field["Expression"]["SourceRef"]["Entity"]
                        self.assertIn(field["Property"], fields[entity])
                for item in value.values():
                    check_fields(item)
            elif isinstance(value, list):
                for item in value:
                    check_fields(item)

        for path in report.rglob("visual.json"):
            check_fields(json.loads(path.read_text()))

    def test_semantic_model_ai_dimension_guidance_and_card_relationship(self):
        full_definition = (
            ROOT
            / "fabric-artifacts"
            / "b11cafb7-422c-90d5-4788-69fc1488a6d0.SemanticModel"
            / "definition"
        )
        relationships = (full_definition / "relationships.tmdl").read_text()
        self.assertIn(
            "fromColumn: ga_dim_card.ga_dim_card_sk\n"
            "\ttoColumn: g_fct_periodic_spend_snapshot.s_dim_card_sk",
            relationships,
        )
        self.assertNotIn("fromColumn: ga_dim_card.s_dim_account_sk", relationships)

        model_definitions = (
            ARTIFACTS / "sm_oyster_core.SemanticModel",
            full_definition.parent,
        )
        for model in model_definitions:
            settings = json.loads((model / "definition.pbism").read_text())["settings"]
            self.assertTrue(settings.get("qnaEnabled"))

        for table_name, label in (
            ("ga_dim_account", "AccountID"),
            ("ga_dim_customer", "FullName"),
            ("g_dim_date", "FullDate"),
        ):
            table = (
                ARTIFACTS
                / "sm_oyster_core.SemanticModel"
                / "definition"
                / "tables"
                / f"{table_name}.tmdl"
            ).read_text()
            self.assertIn(f"column {label}\n\t\tdataType:", table)
            self.assertIn("\t\tisDefaultLabel", table)
            self.assertTrue(table.startswith("///"))
            self.assertRegex(table, rf"\t///[^\r\n]*\n\tcolumn {label}\n")

    def test_account_denormalization_retains_cardless_accounts_and_grain(self):
        sql = (PROJECT / "models" / "020_silver" / "s_dim_account.sql").read_text()
        sql = re.sub(r"\{\{ ref\('(\w+)'\) \}\}", r"\1", sql)
        hash_expression = (
            "CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', "
            "CAST(a.[account_id] AS VARCHAR(50))), 2)"
        )
        self.assertIn(hash_expression, sql)
        sql = sql.replace(hash_expression, "account_hash(a.account_id)")
        with sqlite3.connect(":memory:") as connection:
            connection.create_function(
                "account_hash", 1,
                lambda account: hashlib.sha256(str(account).encode()).hexdigest().upper(),
            )
            connection.executescript("""
                CREATE TABLE b_oys_account(account_id INTEGER, customer_id INTEGER, balance_amt NUMERIC);
                CREATE TABLE s_dim_customer(customer_id INTEGER, s_dim_customer_sk TEXT, is_current_version_flag INTEGER);
                CREATE TABLE s_dim_card(card_id INTEGER, account_id INTEGER, card_tier_nm TEXT, is_current_version_flag INTEGER);
                CREATE TABLE b_oys_debit_card(card_id INTEGER, account_id INTEGER, card_tier_nm TEXT);
                INSERT INTO b_oys_account VALUES (10, 1, 100), (20, 2, 200);
                INSERT INTO s_dim_customer VALUES (1, 'c1', 1), (2, 'c2', 1);
                INSERT INTO s_dim_card VALUES (100, 10, 'Gold', 1);
                INSERT INTO b_oys_debit_card VALUES (100, 10, 'Gold');
            """)
            rows = connection.execute(sql).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0][-2:], (100, "Gold"))
            self.assertEqual(rows[1][-2:], (None, None))
            self.assertEqual(rows[0][0], hashlib.sha256(b"10").hexdigest().upper())
            import yaml

            schema = yaml.safe_load((PROJECT / "models" / "core_schema.yml").read_text())
            card = next(m for m in schema["models"] if m["name"] == "b_oys_debit_card")
            account_column = next(c for c in card["columns"] if c["name"] == "account_id")
            self.assertIn("unique", account_column["data_tests"])
            connection.execute("INSERT INTO b_oys_debit_card VALUES (101, 10, 'Platinum')")
            duplicates = connection.execute(
                "SELECT account_id FROM b_oys_debit_card GROUP BY account_id HAVING COUNT(*) > 1"
            ).fetchall()
            self.assertEqual(duplicates, [(10,)])


if __name__ == "__main__":
    unittest.main()
