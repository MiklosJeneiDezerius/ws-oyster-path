import contextlib
import io
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


ARTIFACTS = Path(__file__).resolve().parents[1]
RESOLVER = ARTIFACTS / "notebooks" / "nb_resolve_landing_batch.Notebook"
SOURCE = (RESOLVER / "notebook-content.py").read_text(encoding="utf-8")
BODY = SOURCE.rsplit("# CELL ********************", 1)[1]
OLDER = "20261117080003"
NEWER = "20261118080003"


def directory(name):
    return SimpleNamespace(name=name, isDir=True)


class NotebookExit(BaseException):
    pass


class LandingBatchTests(unittest.TestCase):
    def resolve(self, entries, batch="", root="Files/landing/oyster"):
        fs = SimpleNamespace(ls=Mock(return_value=entries))
        notebook = SimpleNamespace(exit=Mock(side_effect=NotebookExit))
        scope = {
            "p_root_path": root,
            "p_batch_folder": batch,
            "notebookutils": SimpleNamespace(fs=fs, notebook=notebook),
        }
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(NotebookExit):
                exec(compile(BODY, str(RESOLVER / "notebook-content.py"), "exec"), scope)
        fs.ls.assert_called_once_with(root)
        notebook.exit.assert_called_once_with(scope["batch_folder"])
        return scope["batch_folder"]

    def test_latest_ignores_non_batch_directories_and_files(self):
        entries = [
            directory(NEWER + "/"),
            directory("config"),
            directory("z" * 14),
            directory(OLDER),
            SimpleNamespace(name="20261119080003", isDir=False),
        ]
        self.assertEqual(self.resolve(entries), NEWER)

    def test_explicit_batch_does_not_change_when_newer_batch_exists(self):
        self.assertEqual(self.resolve([directory(NEWER), directory(OLDER)], OLDER), OLDER)

    def test_custom_root(self):
        self.assertEqual(self.resolve([directory(OLDER)], root="Files/landing/custom"), OLDER)

    def test_no_batch_fails_before_returning(self):
        for entries in ([], [directory("config")], [directory("x" * 14)]):
            with self.subTest(entries=entries):
                with self.assertRaisesRegex(FileNotFoundError, "NO_BATCH_FOLDER"):
                    self.resolve(entries)

    def test_missing_explicit_batch_fails(self):
        with self.assertRaisesRegex(FileNotFoundError, "not found"):
            self.resolve([directory(NEWER)], OLDER)

    def test_invalid_timestamp_fails(self):
        with self.assertRaises(ValueError):
            self.resolve([directory("20261318080003")])

    def test_storage_errors_propagate(self):
        utils = SimpleNamespace(fs=SimpleNamespace(ls=Mock(side_effect=PermissionError("denied"))))
        with self.assertRaisesRegex(PermissionError, "denied"):
            exec(BODY, {"p_root_path": "Files/landing/oyster", "p_batch_folder": "", "notebookutils": utils})

    def test_pipeline_wiring(self):
        pipeline_path = ARTIFACTS / "pipelines" / "pl_jb_landing_delta.DataPipeline" / "pipeline-content.json"
        properties = json.loads(pipeline_path.read_text(encoding="utf-8"))["properties"]
        activities = {activity["name"]: activity for activity in properties["activities"]}
        self.assertEqual(list(activities), [
            "Set v_load_date", "nb_resolve_landing_batch", "lkp_GetLoadConfig",
            "Filter - active tables", "forEach_FileLoader",
        ])
        resolver = activities["nb_resolve_landing_batch"]
        platform = json.loads((RESOLVER / ".platform").read_text(encoding="utf-8"))
        self.assertEqual(resolver["typeProperties"]["notebookId"], platform["config"]["logicalId"])
        self.assertEqual(resolver["typeProperties"]["workspaceId"], "00000000-0000-0000-0000-000000000000")
        for current, previous in zip(list(activities.values())[1:], activities):
            self.assertEqual(current["dependsOn"], [
                {"activity": previous, "dependencyConditions": ["Succeeded"]}
            ])
        cases = activities["forEach_FileLoader"]["typeProperties"]["activities"][0]["typeProperties"]["cases"]
        self.assertEqual({case["value"] for case in cases}, {"csv", "json"})
        for case in cases:
            parameters = case["activities"][0]["typeProperties"]["parameters"]
            self.assertEqual(parameters["p_batch_folder"], {
                "value": {"value": "@activity('nb_resolve_landing_batch').output.result.exitValue", "type": "Expression"},
                "type": "string",
            })
            if case["value"] == "csv":
                self.assertEqual(parameters["p_root_path"], resolver["typeProperties"]["parameters"]["p_root_path"])
        self.assertNotIn("v_batch_folder", properties["variables"])
        self.assertIn("# PARAMETERS CELL ********************", SOURCE)
        compile(SOURCE, str(RESOLVER / "notebook-content.py"), "exec")


if __name__ == "__main__":
    unittest.main()
