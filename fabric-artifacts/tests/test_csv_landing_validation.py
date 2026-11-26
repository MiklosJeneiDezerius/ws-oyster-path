import contextlib
import io
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


NOTEBOOK = (
    Path(__file__).resolve().parents[1]
    / "040_Notebooks" / "030_Framework_notebooks" / "020_Landing_delta"
    / "nb_csv_load_source_to_ld.Notebook" / "notebook-content.py"
)
BATCH = datetime(2026, 11, 19, 12, 12, 33)
LOAD = datetime(2026, 11, 19, 12, 19, 22)
ADDRESS = "dbo.ld_oys_customeradress"
LEDGER = "dbo.ld_oys_transaction_ledger"
TABLES = ("dbo.ld_oys_customer", ADDRESS, "dbo.ld_oys_account", "dbo.ld_oys_debitcard", LEDGER)


class CsvLandingValidationTests(unittest.TestCase):
    def setUp(self):
        source = NOTEBOOK.read_text(encoding="utf-8")
        self.validation = compile(source.rsplit("# CELL ********************", 1)[1], str(NOTEBOOK), "exec")
        self.reader = Mock()
        self.count = Mock()
        self.group = self.reader.groupBy
        self.result = self.group.return_value.agg.return_value
        self.result.collect.return_value = [
            SimpleNamespace(
                meta_batch_timestamp=BATCH, meta_load_timestamp=LOAD,
                meta_source_file="source.csv", row_count=3,
            ),
        ]
        self.spark = Mock()

    def verify(self, table):
        available = {table: self.reader}
        self.spark.table.side_effect = available.__getitem__
        scope = {
            "table_name": table,
            "spark": self.spark,
            "F": SimpleNamespace(count=self.count),
            "batch_ts": BATCH,
            "load_ts": LOAD,
            "batch_folder": "20261119121233",
        }
        with contextlib.redirect_stdout(io.StringIO()) as output:
            exec(self.validation, scope)
        return output.getvalue()

    def test_partial_run_does_not_read_unavailable_ledger(self):
        output = self.verify(ADDRESS)
        self.spark.table.assert_called_once_with(ADDRESS)
        self.group.assert_called_once_with("meta_batch_timestamp", "meta_load_timestamp", "meta_source_file")
        self.count.assert_called_once_with("*")
        self.count.return_value.alias.assert_called_once_with("row_count")
        self.assertIn(f"Verified {ADDRESS}", output)
        self.assertIn("file=source.csv, rows=3", output)

    def test_each_invocation_checks_only_its_target(self):
        for table in TABLES:
            with self.subTest(table=table):
                self.spark.reset_mock()
                self.verify(table)
                self.spark.table.assert_called_once_with(table)

    def test_empty_selected_target_fails(self):
        self.result.collect.return_value = []
        with self.assertRaisesRegex(ValueError, "no rows after loading batch 20261119121233"):
            self.verify(ADDRESS)

    def test_unexpected_batch_or_load_timestamp_fails(self):
        for field in ("meta_batch_timestamp", "meta_load_timestamp"):
            with self.subTest(field=field):
                row = SimpleNamespace(
                    meta_batch_timestamp=BATCH, meta_load_timestamp=LOAD,
                    meta_source_file="source.csv", row_count=3,
                )
                setattr(row, field, datetime(2026, 11, 18))
                self.result.collect.return_value = [row]
                with self.assertRaisesRegex(ValueError, "expected batch .* and load time"):
                    self.verify(ADDRESS)
