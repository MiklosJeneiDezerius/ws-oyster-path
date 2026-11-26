import contextlib
import csv
import importlib.util
import io
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace


NOTEBOOKS = (Path(__file__).resolve().parents[1] / "040_Notebooks" / "030_Framework_notebooks"
             / "020_Landing_delta")
HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
FIELDS = ["TransactionID", "AccountID", "Amount", "Timestamp"]
BATCH = "20261117080003"


def notebook_cells(name):
    source = (NOTEBOOKS / name / "notebook-content.py").read_text(encoding="utf-8")
    return [
        cell.split("# METADATA ********************", 1)[0]
        for cell in source.split("# CELL ********************")[1:]
    ]


@unittest.skipUnless(HAS_PYSPARK, "requires PySpark and a local Java runtime")
class LandingLoaderSparkParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("landing-loader-parity-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_matching_ledger_files_produce_matching_dataframes(self):
        rows = [
            {
                "TransactionID": i,
                "AccountID": 1001 if i <= 21 else 1002,
                "Amount": f"{i}.30",
                "Timestamp": "2026-10-02 14:05:11",
            }
            for i in range(1, 24)
        ]
        with tempfile.TemporaryDirectory() as root:
            batch_dir = Path(root) / BATCH
            batch_dir.mkdir()
            with (batch_dir / "Transaction_Ledger_sample.csv").open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            with (batch_dir / "Transaction_Ledger_sample.json").open("w", encoding="utf-8") as file:
                for account_id in (1001, 1002):
                    account_rows = [row for row in rows if row["AccountID"] == account_id]
                    batches = [account_rows[i:i + 20] for i in range(0, len(account_rows), 20)]
                    file.write(json.dumps({
                        "SourceSystem": "oyster", "IngestDate": BATCH,
                        "Data": {"AccountID": account_id, "TransactionBatches": batches},
                    }) + "\n")

            def execute_loader(name, cell_indices):
                cells = notebook_cells(name)
                scope = {
                    "spark": self.spark,
                    "notebookutils": SimpleNamespace(
                        fs=SimpleNamespace(ls=lambda _: [SimpleNamespace(name=BATCH + "/", isDir=True)])
                    ),
                    "p_root_path": Path(root).as_uri(),
                    "p_batch_folder": BATCH,
                    "p_in_table_name": "Transaction_Ledger",
                    "p_debug": 0,
                }
                with contextlib.redirect_stdout(io.StringIO()):
                    for index in cell_indices:
                        exec(compile(cells[index], name, "exec"), scope)
                return scope

            csv_scope = execute_loader("nb_csv_load_source_to_ld.Notebook", (1, 2, 3))
            json_scope = execute_loader("nb_json_load_source_to_ld.Notebook", (1, 2))
            csv_out, json_out = csv_scope["out"], json_scope["out"]

            self.assertEqual(csv_scope["table_name"], json_scope["table_name"])
            self.assertEqual(
                [(field.name, field.dataType) for field in csv_out.schema],
                [(field.name, field.dataType) for field in json_out.schema],
            )
            columns = FIELDS + ["meta_batch_timestamp"]
            csv_rows = Counter(tuple(row) for row in csv_out.select(*columns).collect())
            json_rows = Counter(tuple(row) for row in json_out.select(*columns).collect())
            self.assertEqual(len(rows), sum(csv_rows.values()))
            self.assertEqual(csv_rows, json_rows)
            for scope, extension in ((csv_scope, "csv"), (json_scope, "json")):
                metadata = scope["out"].select("meta_load_timestamp", "meta_source_file").distinct().collect()
                self.assertEqual(len(metadata), 1)
                self.assertEqual(metadata[0].meta_load_timestamp, scope["load_ts"])
                self.assertEqual(metadata[0].meta_source_file, f"Transaction_Ledger_sample.{extension}")


if __name__ == "__main__":
    unittest.main()
