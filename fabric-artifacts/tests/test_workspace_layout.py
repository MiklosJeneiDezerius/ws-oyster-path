import json
import unittest
from pathlib import Path


ARTIFACTS = Path(__file__).resolve().parents[1]


class WorkspaceLayoutTests(unittest.TestCase):
    def test_item_locations_and_display_names(self):
        items = {
            "030_Pipelines/020_Flows/pl_fl_master.DataPipeline": "pl_fl_master",
            "030_Pipelines/040_Jobs/010_Landing_Raw/pl_jb_landing_raw.DataPipeline": "pl_jb_landing_raw",
            "030_Pipelines/040_Jobs/020_Landing_delta/pl_jb_landing_delta.DataPipeline": "pl_jb_landing_delta",
            "030_Pipelines/040_Jobs/pl_jb_create_dw.DataPipeline": "pl_jb_create_dw",
            "030_Pipelines/040_Jobs/pl_jb_refresh_semantic_model_core.DataPipeline": "pl_jb_refresh_semantic_model_core",
            "040_Notebooks/030_Framework_notebooks/010_Landing_raw/nb_generate_deterministic_sample_data.Notebook": "nb_generate_deterministic_sample_data",
            "040_Notebooks/030_Framework_notebooks/020_Landing_delta/nb_csv_load_source_to_ld.Notebook": "nb_csv_load_source_to_ld",
            "040_Notebooks/030_Framework_notebooks/020_Landing_delta/nb_json_load_source_to_ld.Notebook": "nb_json_load_source_to_ld",
            "040_Notebooks/030_Framework_notebooks/020_Landing_delta/nb_resolve_landing_batch.Notebook": "nb_resolve_landing_batch",
            "040_Notebooks/030_Framework_notebooks/040_dbt/nb_run_dbt.Notebook": "nb_run_dbt",
            "040_Notebooks/030_Framework_notebooks/040_dbt/nb_run_dbt_devops.Notebook": "nb_run_dbt_devops",
            "030_Reports/rpt_oyster_core.Report": "rpt_oyster_core",
            "sm_reporting.SemanticModel": "sm_reporting",
            "lh_oyster_path.Lakehouse": "lh_oyster_path",
            "dw_oyster_path.Warehouse": "dw_oyster_path",
        }
        ids = set()
        for path, name in items.items():
            with self.subTest(path=path):
                platform = json.loads((ARTIFACTS / path / ".platform").read_text(encoding="utf-8"))
                self.assertEqual(platform["metadata"]["displayName"], name)
                self.assertNotIn(platform["config"]["logicalId"], ids)
                ids.add(platform["config"]["logicalId"])

    def test_report_reference_resolves_to_semantic_model(self):
        report = ARTIFACTS / "030_Reports" / "rpt_oyster_core.Report"
        definition = json.loads((report / "definition.pbir").read_text(encoding="utf-8"))
        target = (report / definition["datasetReference"]["byPath"]["path"]).resolve()
        self.assertEqual(target, (ARTIFACTS / "sm_reporting.SemanticModel").resolve())
        self.assertTrue(target.is_dir())


if __name__ == "__main__":
    unittest.main()
