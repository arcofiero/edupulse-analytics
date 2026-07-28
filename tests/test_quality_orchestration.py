from argparse import Namespace
from pathlib import Path

import pytest

from orchestration.pipeline_tasks import run_local_pipeline_task, run_quality_gate_task
from pipeline.local import run_pipeline
from soda.checks import LocalQualityRunner, QualityGateError


def test_quality_gates_pass_for_local_pipeline(tmp_path):
    summary = run_pipeline(
        Namespace(
            data_dir=tmp_path,
            students=20,
            weeks=1,
            seed=505,
            limit=150,
            malformed_rate=0.05,
            clean=True,
            skip_quality_gates=False,
        )
    )

    assert summary["quality"]["bronze"]["passed"] is True
    assert summary["quality"]["silver"]["passed"] is True
    assert summary["quality"]["gold"]["passed"] is True


def test_failed_bronze_quality_gate_blocks_downstream_layers(tmp_path):
    with pytest.raises(QualityGateError) as error:
        run_pipeline(
            Namespace(
                data_dir=tmp_path,
                students=5,
                weeks=1,
                seed=606,
                limit=0,
                malformed_rate=0,
                clean=True,
                skip_quality_gates=False,
            )
        )

    assert error.value.report.layer == "bronze"
    assert not (tmp_path / "silver").exists()
    assert not (tmp_path / "gold").exists()


def test_quality_gate_task_returns_summary(tmp_path):
    run_local_pipeline_task(
        data_dir=str(tmp_path),
        students=10,
        weeks=1,
        seed=707,
        limit=80,
        malformed_rate=0.05,
        clean=True,
    )

    summary = run_quality_gate_task("gold", data_dir=str(tmp_path))

    assert summary["passed"] is True
    assert summary["failed"] == 0


def test_quality_runner_reports_missing_gold_outputs(tmp_path):
    report = LocalQualityRunner(tmp_path).run_layer("gold")

    assert report.passed is False
    assert {check.name for check in report.failed_checks} >= {
        "student_scores_not_empty",
        "content_engagement_not_empty",
        "adoption_not_empty",
    }


def test_airflow_dag_files_define_quality_gated_tasks():
    dag_dir = Path("airflow/dags")
    local_dag = (dag_dir / "edupulse_local_pipeline.py").read_text(encoding="utf-8")
    stakeholder_dag = (dag_dir / "edupulse_stakeholder_refresh.py").read_text(encoding="utf-8")

    assert "edupulse_local_pipeline" in local_dag
    assert "check_bronze_quality" in local_dag
    assert "check_silver_quality" in local_dag
    assert "check_gold_quality" in local_dag
    assert "edupulse_stakeholder_refresh" in stakeholder_dag
    assert "advisor_refresh_every_5_min" in stakeholder_dag
