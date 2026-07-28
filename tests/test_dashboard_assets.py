import csv
import json
from argparse import Namespace
from pathlib import Path

from dashboards.datasets import DashboardDatasetBuilder
from pipeline.local import run_pipeline


def test_dashboard_builder_writes_three_stakeholder_datasets(tmp_path):
    data_dir = tmp_path / "lakehouse"
    output_dir = tmp_path / "superset"
    run_pipeline(
        Namespace(
            data_dir=data_dir,
            students=20,
            weeks=1,
            seed=808,
            limit=180,
            malformed_rate=0.05,
            clean=True,
            skip_quality_gates=False,
        )
    )

    result = DashboardDatasetBuilder(data_dir / "gold", output_dir).build()

    assert {dataset.dataset_name for dataset in result.datasets} == {
        "advisor_at_risk_students",
        "faculty_content_engagement",
        "admin_adoption_weekly",
    }
    assert all(dataset.row_count > 0 for dataset in result.datasets)
    assert (output_dir / "manifest.json").exists()


def test_advisor_dataset_contains_review_workflow_columns(tmp_path):
    data_dir = tmp_path / "lakehouse"
    output_dir = tmp_path / "superset"
    run_pipeline(
        Namespace(
            data_dir=data_dir,
            students=12,
            weeks=1,
            seed=909,
            limit=120,
            malformed_rate=0.05,
            clean=True,
            skip_quality_gates=False,
        )
    )
    DashboardDatasetBuilder(data_dir / "gold", output_dir).build()

    with (output_dir / "advisor_at_risk_students.csv").open(encoding="utf-8") as input_file:
        rows = list(csv.DictReader(input_file))

    assert rows
    assert {
        "student_id",
        "engagement_score",
        "dropout_risk_band",
        "needs_advisor_review",
    }.issubset(rows[0])


def test_dashboard_manifest_maps_datasets_to_dashboards(tmp_path):
    data_dir = tmp_path / "lakehouse"
    output_dir = tmp_path / "superset"
    run_pipeline(
        Namespace(
            data_dir=data_dir,
            students=14,
            weeks=1,
            seed=1001,
            limit=140,
            malformed_rate=0.05,
            clean=True,
            skip_quality_gates=False,
        )
    )
    DashboardDatasetBuilder(data_dir / "gold", output_dir).build()

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))

    assert [dashboard["name"] for dashboard in manifest["dashboards"]] == [
        "Advisor Risk Monitor",
        "Faculty Content Engagement",
        "Department Adoption Weekly",
    ]
    assert {dataset["file_name"] for dataset in manifest["datasets"]} == {
        "advisor_at_risk_students.csv",
        "faculty_content_engagement.csv",
        "admin_adoption_weekly.csv",
    }


def test_superset_specs_define_expected_dashboards():
    specs = Path("superset/dashboard_specs.yml").read_text(encoding="utf-8")

    assert "Advisor Risk Monitor" in specs
    assert "Faculty Content Engagement" in specs
    assert "Department Adoption Weekly" in specs
    assert "advisor_at_risk_students" in specs
    assert "faculty_content_engagement" in specs
    assert "admin_adoption_weekly" in specs
