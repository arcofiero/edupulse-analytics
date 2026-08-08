import csv
import json
import sqlite3
from argparse import Namespace
from pathlib import Path

from dashboards.datasets import DashboardDatasetBuilder
from pipeline.local import run_pipeline


def test_dashboard_builder_writes_stakeholder_analytics_datasets(tmp_path):
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
        "advisor_engagement_signals",
        "faculty_content_engagement",
        "faculty_content_difficulty",
        "admin_adoption_weekly",
        "admin_cohort_engagement_summary",
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
        "risk_score",
        "engagement_score",
        "engagement_percentile",
        "dropout_risk_band",
        "needs_advisor_review",
        "risk_reasons",
        "recommended_action",
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
        "advisor_engagement_signals.csv",
        "faculty_content_engagement.csv",
        "faculty_content_difficulty.csv",
        "admin_adoption_weekly.csv",
        "admin_cohort_engagement_summary.csv",
    }
    assert manifest["dashboards"][0]["datasets"] == [
        "advisor_at_risk_students",
        "advisor_engagement_signals",
    ]
    assert manifest["database"]["sqlalchemy_uri"] == (
        "sqlite:////app/edupulse_superset_data/edupulse_dashboards.db"
    )


def test_dashboard_builder_writes_queryable_sqlite_database(tmp_path):
    data_dir = tmp_path / "lakehouse"
    output_dir = tmp_path / "superset"
    run_pipeline(
        Namespace(
            data_dir=data_dir,
            students=16,
            weeks=1,
            seed=1111,
            limit=150,
            malformed_rate=0.05,
            clean=True,
            skip_quality_gates=False,
        )
    )
    DashboardDatasetBuilder(data_dir / "gold", output_dir).build()

    with sqlite3.connect(output_dir / "edupulse_dashboards.db") as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "select name from sqlite_master where type = 'table'"
            )
        }
        advisor_count = connection.execute(
            "select count(*) from advisor_at_risk_students"
        ).fetchone()[0]

    assert tables == {
        "advisor_at_risk_students",
        "advisor_engagement_signals",
        "faculty_content_engagement",
        "faculty_content_difficulty",
        "admin_adoption_weekly",
        "admin_cohort_engagement_summary",
    }
    assert advisor_count > 0


def test_faculty_and_admin_datasets_include_analytical_measures(tmp_path):
    data_dir = tmp_path / "lakehouse"
    output_dir = tmp_path / "superset"
    run_pipeline(
        Namespace(
            data_dir=data_dir,
            students=18,
            weeks=1,
            seed=1212,
            limit=160,
            malformed_rate=0.05,
            clean=True,
            skip_quality_gates=False,
        )
    )
    DashboardDatasetBuilder(data_dir / "gold", output_dir).build()

    with (output_dir / "faculty_content_difficulty.csv").open(encoding="utf-8") as input_file:
        difficulty_rows = list(csv.DictReader(input_file))
    with (output_dir / "admin_cohort_engagement_summary.csv").open(encoding="utf-8") as input_file:
        cohort_rows = list(csv.DictReader(input_file))

    assert difficulty_rows
    assert {"difficulty_index", "content_health_band", "quiz_accuracy"}.issubset(
        difficulty_rows[0]
    )
    assert cohort_rows
    assert {"avg_risk_score", "high_risk_rate", "avg_attendance_rate"}.issubset(
        cohort_rows[0]
    )


def test_superset_specs_define_expected_dashboards():
    specs = Path("superset/dashboard_specs.yml").read_text(encoding="utf-8")

    assert "Advisor Risk Monitor" in specs
    assert "Faculty Content Engagement" in specs
    assert "Department Adoption Weekly" in specs
    assert "advisor_at_risk_students" in specs
    assert "advisor_engagement_signals" in specs
    assert "faculty_content_engagement" in specs
    assert "faculty_content_difficulty" in specs
    assert "admin_adoption_weekly" in specs
    assert "admin_cohort_engagement_summary" in specs
