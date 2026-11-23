import contextlib
import io
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "nb_csv_load_source_to_ld.Notebook" / "notebook-content.py"
SOURCE = NOTEBOOK.read_text(encoding="utf-8")


def cell(start):
    return SOURCE.split(start, 1)[1].split("# METADATA ********************", 1)[0]


BATCH_CELL = "file_prefix =" + cell("file_prefix =")
READ_CELL = 'path = f"{p_root_path}' + cell('path = f"{p_root_path}')
WRITE_CELL = '(\n    out.write.format("delta")' + cell('(\n    out.write.format("delta")')
VERIFY_CELL = "summary = (" + cell("summary = (")


class CsvLandingLoaderTests(unittest.TestCase):
    def setUp(self):
        self.fs = MagicMock()
        self.fs.ls.return_value = [SimpleNamespace(name="20261117080003/", isDir=True)]
        self.functions = MagicMock()
        self.spark = MagicMock()
        self.frame = self.spark.read.format.return_value.option.return_value.option.return_value.load.return_value
        self.frame.take.return_value = [object()]
        self.frame.columns = ["CustomerID", "FirstName"]
        self.scope = {
            "re": re, "datetime": datetime, "timezone": timezone,
            "notebookutils": SimpleNamespace(fs=self.fs),
            "spark": self.spark, "F": self.functions,
            "p_root_path": "Files/landing/oyster", "p_batch_folder": "",
            "p_in_table_name": "Customer",
            "p_debug": 0,
        }

    def run_cell(self, source):
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(source, str(NOTEBOOK), "exec"), self.scope)

    def test_parameterized_read_write_and_verification(self):
        self.run_cell(BATCH_CELL)
        self.run_cell(READ_CELL)
        self.spark.read.format.return_value.option.return_value.option.return_value.load.assert_called_once_with(
            "Files/landing/oyster/20261117080003/Customer_*.csv"
        )
        self.functions.lit.assert_any_call(self.scope["batch_ts"])
        self.functions.lit.assert_any_call(self.scope["load_ts"])
        self.frame.withColumn.assert_called_once_with("meta_batch_timestamp", self.functions.lit.return_value)
        batch_frame = self.frame.withColumn.return_value
        batch_frame.withColumn.assert_called_once_with("meta_load_timestamp", self.functions.lit.return_value)
        load_frame = batch_frame.withColumn.return_value
        self.functions.col.assert_called_once_with("_metadata.file_name")
        load_frame.withColumn.assert_called_once_with("meta_source_file", self.functions.col.return_value)
        output_frame = load_frame.withColumn.return_value
        self.run_cell(WRITE_CELL)
        writer = output_frame.write.format.return_value.mode.return_value.option.return_value
        writer.saveAsTable.assert_called_once_with("dbo.ld_oys_customer")
        output_frame.write.format.assert_called_once_with("delta")
        output_frame.write.format.return_value.mode.assert_called_once_with("overwrite")
        self.spark.table.return_value.groupBy.return_value.agg.return_value.collect.return_value = [
            SimpleNamespace(meta_batch_timestamp=self.scope["batch_ts"],
                            meta_load_timestamp=self.scope["load_ts"],
                            meta_source_file="Customer_test.csv", row_count=1)
        ]
        self.run_cell(VERIFY_CELL)
        self.spark.table.assert_called_with("dbo.ld_oys_customer")

    def test_missing_or_unsafe_source_fails_before_listing(self):
        for value in ("", "../Customer", "Customer*", "Customer,Account"):
            with self.subTest(value=value):
                scope = self.scope.copy()
                scope["p_in_table_name"] = value
                with self.assertRaises(ValueError):
                    exec(BATCH_CELL, scope)
        self.fs.ls.assert_not_called()

    def test_target_derived_from_source_name(self):
        for source, target in (
            ("Customer", "dbo.ld_oys_customer"),
            ("CustomerAdress", "dbo.ld_oys_customeradress"),
            ("Transaction_Ledger", "dbo.ld_oys_transaction_ledger"),
        ):
            with self.subTest(source=source):
                self.scope["p_in_table_name"] = source
                self.run_cell(BATCH_CELL)
                self.assertEqual(self.scope["table_name"], target)

    def test_empty_source_and_metadata_collision_fail_before_write(self):
        self.run_cell(BATCH_CELL)
        self.frame.take.return_value = []
        with self.assertRaisesRegex(ValueError, "no data rows"):
            self.run_cell(READ_CELL)
        self.frame.take.return_value = [object()]
        self.frame.columns = ["META_SOURCE_FILE"]
        with self.assertRaisesRegex(ValueError, "conflict with metadata"):
            self.run_cell(READ_CELL)
        self.frame.withColumn.assert_not_called()

    def test_missing_batch_fails(self):
        self.scope["p_batch_folder"] = "20261118080003"
        with self.assertRaisesRegex(FileNotFoundError, "not found"):
            self.run_cell(BATCH_CELL)


if __name__ == "__main__":
    unittest.main()
