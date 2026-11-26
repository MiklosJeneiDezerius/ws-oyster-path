import contextlib
import csv
import io
import json
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


NOTEBOOK = (
    Path(__file__).resolve().parents[1]
    / "040_Notebooks" / "030_Framework_notebooks" / "010_Landing_raw"
    / "nb_generate_deterministic_sample_data.Notebook"
    / "notebook-content.py"
)
SOURCE = NOTEBOOK.read_text(encoding="utf-8")


def generate(simulate_changes):
    with tempfile.TemporaryDirectory() as output_dir:
        source, count = re.subn(
            r"(?m)^p_simulate_changes = [01]$",
            f"p_simulate_changes = {simulate_changes}",
            SOURCE,
        )
        if count != 1:
            raise ValueError(f"Expected one p_simulate_changes default, found {count}")
        source = source.replace(
            'output_dir = f"/lakehouse/default/{output_path}"',
            f"output_dir = {output_dir!r}",
            1,
        )
        scope = {"spark": SimpleNamespace(createDataFrame=Mock())}
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(source, str(NOTEBOOK), "exec"), scope)

        files = list(Path(output_dir).iterdir())
        csv_path = next(path for path in files if path.name.startswith("Transaction_Ledger_") and path.suffix == ".csv")
        json_path = next(path for path in files if path.suffix == ".json")
        with csv_path.open(encoding="utf-8", newline="") as file:
            csv_rows = list(csv.DictReader(file))
        with json_path.open(encoding="utf-8") as file:
            documents = [json.loads(line) for line in file]
        json_rows = [
            {key: str(value) for key, value in transaction.items()}
            for document in documents
            for batch in document["Data"]["TransactionBatches"]
            for transaction in batch
        ]
        return scope, csv_rows, json_rows


class SampleDataChangeTests(unittest.TestCase):
    def test_transactions_are_new_deterministic_events_in_both_formats(self):
        baseline, baseline_csv, baseline_json = generate(0)
        changed, changed_csv, changed_json = generate(1)
        repeated, repeated_csv, repeated_json = generate(1)

        original = baseline["transactions"]
        updated = changed["transactions"]
        self.assertEqual(updated[:len(original)], original)
        self.assertEqual(updated, repeated["transactions"])
        extra = updated[len(original):]
        self.assertEqual(len(extra), 7)
        self.assertEqual([tx["AccountID"] for tx in extra], [2001] * 3 + [2002] * 4)
        self.assertEqual([tx["Amount"] for tx in extra], [
            "1.00", "2.00", "3.00", "4.00", "5.00", "6.00", "7.00",
        ])
        original_ids = {tx["TransactionID"] for tx in original}
        extra_ids = {tx["TransactionID"] for tx in extra}
        self.assertEqual(len(extra_ids), 7)
        self.assertTrue(original_ids.isdisjoint(extra_ids))
        self.assertEqual(len({tx["TransactionID"] for tx in updated}), len(updated))

        by_id = lambda rows: sorted(rows, key=lambda row: int(row["TransactionID"]))
        self.assertEqual(by_id(baseline_csv), by_id(baseline_json))
        self.assertEqual(by_id(changed_csv), by_id(changed_json))
        self.assertEqual(changed_csv, repeated_csv)
        self.assertEqual(changed_json, repeated_json)
        self.assertEqual(changed_csv[:len(baseline_csv)], baseline_csv)
        self.assertEqual(
            {int(row["TransactionID"]) for row in changed_csv} - original_ids, extra_ids
        )


if __name__ == "__main__":
    unittest.main()
