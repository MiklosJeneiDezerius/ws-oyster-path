import contextlib
import io
import re
import unittest
from datetime import datetime, timezone
from functools import reduce
from operator import or_
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "nb_json_load_source_to_ld.Notebook" / "notebook-content.py"
SOURCE = NOTEBOOK.read_text(encoding="utf-8")
CELLS = SOURCE.split("# CELL ********************")
BATCH_CELL = CELLS[2].split('REQUIRED_COLUMNS =', 1)[1].split("# METADATA ********************", 1)[0]
BATCH_CELL = "REQUIRED_COLUMNS =" + BATCH_CELL
READ_CELL = CELLS[3].split("# METADATA ********************", 1)[0]
WRITE_CELL = CELLS[4].split("# METADATA ********************", 1)[0]
VERIFY_CELL = CELLS[5].split("# METADATA ********************", 1)[0]


class JsonLandingLoaderTests(unittest.TestCase):
    def setUp(self):
        self.fs = MagicMock()
        self.fs.ls.return_value = [SimpleNamespace(name="20261117080003/", isDir=True)]
        self.spark = MagicMock()
        self.functions = MagicMock()
        self.documents = self.spark.read.schema.return_value.option.return_value.json.return_value.select.return_value
        self.documents.take.return_value = [object()]
        self.documents.filter.return_value.count.return_value = 0
        self.transactions = self.documents.select.return_value.select.return_value
        self.transactions.filter.return_value.count.return_value = 0
        self.transactions.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
        self.scope = {
            "re": re, "datetime": datetime, "timezone": timezone, "reduce": reduce, "or_": or_,
            "notebookutils": SimpleNamespace(fs=self.fs), "spark": self.spark, "F": self.functions,
            "ArrayType": MagicMock(), "StringType": MagicMock(), "StructField": MagicMock(),
            "StructType": MagicMock(), "p_root_path": "Files/landing/custom",
            "p_batch_folder": "", "p_in_table_name": "Transaction_Ledger", "p_debug": 0,
        }

    def run_cell(self, source):
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(source, str(NOTEBOOK), "exec"), self.scope)

    def test_reads_source_and_overwrites_derived_target(self):
        self.run_cell(BATCH_CELL)
        self.run_cell(READ_CELL)
        self.spark.read.schema.return_value.option.return_value.json.assert_called_once_with(
            "Files/landing/custom/20261117080003/Transaction_Ledger_*.json"
        )
        self.run_cell(WRITE_CELL)
        out = self.transactions.select.return_value
        out.write.format.assert_called_once_with("delta")
        out.write.format.return_value.mode.assert_called_once_with("overwrite")
        out.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            "dbo.ld_oys_transaction_ledger"
        )
        self.spark.table.return_value.groupBy.return_value.agg.return_value.collect.return_value = [
            SimpleNamespace(meta_batch_timestamp=self.scope["batch_ts"],
                            meta_load_timestamp=self.scope["load_ts"],
                            meta_source_file="Transaction_Ledger_test.json", row_count=1)
        ]
        self.run_cell(VERIFY_CELL)
        self.spark.table.assert_called_once_with("dbo.ld_oys_transaction_ledger")

    def test_invalid_or_unsupported_source_fails_before_batch_listing(self):
        for value in ("", "../Transaction_Ledger", "Customer", "transaction_ledger"):
            with self.subTest(value=value):
                scope = self.scope.copy()
                scope["p_in_table_name"] = value
                with self.assertRaises(ValueError):
                    exec(BATCH_CELL, scope)
        self.fs.ls.assert_not_called()

    def test_missing_batch_fails(self):
        self.scope["p_batch_folder"] = "20261118080003"
        with self.assertRaisesRegex(FileNotFoundError, "not found"):
            self.run_cell(BATCH_CELL)

    def test_json_validation_prevents_write(self):
        self.run_cell(BATCH_CELL)
        self.documents.filter.return_value.count.return_value = 1
        with self.assertRaisesRegex(ValueError, "document\\(s\\)"):
            self.run_cell(READ_CELL)
        self.transactions.select.return_value.write.format.assert_not_called()

    def test_verify_rejects_empty_or_wrong_batch(self):
        self.run_cell(BATCH_CELL)
        result = self.spark.table.return_value.groupBy.return_value.agg.return_value
        result.collect.return_value = []
        with self.assertRaisesRegex(ValueError, "no rows after loading"):
            self.run_cell(VERIFY_CELL)
        result.collect.return_value = [
            SimpleNamespace(meta_batch_timestamp=datetime(2020, 1, 1),
                            meta_load_timestamp=self.scope["load_ts"], meta_source_file="test.json", row_count=1)
        ]
        with self.assertRaisesRegex(ValueError, "expected batch"):
            self.run_cell(VERIFY_CELL)


if __name__ == "__main__":
    unittest.main()
