from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from orchestration.pipeline_tasks import run_quality_gate_task


DEFAULT_ARGS = {
    "owner": "edupulse",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="edupulse_stakeholder_refresh",
    description="Quality-gated stakeholder refresh schedules for advisor, faculty, and admin views.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["edupulse", "stakeholders", "quality"],
) as dag:
    check_gold = PythonOperator(
        task_id="check_gold_quality",
        python_callable=run_quality_gate_task,
        op_kwargs={"layer": "gold"},
    )

    advisor_refresh = EmptyOperator(task_id="advisor_refresh_every_5_min")
    faculty_refresh = EmptyOperator(task_id="faculty_refresh_daily_6am")
    admin_refresh = EmptyOperator(task_id="admin_refresh_monday_7am")

    check_gold >> [advisor_refresh, faculty_refresh, admin_refresh]
