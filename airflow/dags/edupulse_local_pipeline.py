from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from orchestration.pipeline_tasks import run_local_pipeline_task, run_quality_gate_task


DEFAULT_ARGS = {
    "owner": "edupulse",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="edupulse_local_pipeline",
    description="Run the local EduPulse Bronze, Silver, Gold pipeline with quality gates.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["edupulse", "local", "quality"],
) as dag:
    run_pipeline = PythonOperator(
        task_id="run_local_pipeline",
        python_callable=run_local_pipeline_task,
    )

    check_bronze = PythonOperator(
        task_id="check_bronze_quality",
        python_callable=run_quality_gate_task,
        op_kwargs={"layer": "bronze"},
    )

    check_silver = PythonOperator(
        task_id="check_silver_quality",
        python_callable=run_quality_gate_task,
        op_kwargs={"layer": "silver"},
    )

    check_gold = PythonOperator(
        task_id="check_gold_quality",
        python_callable=run_quality_gate_task,
        op_kwargs={"layer": "gold"},
    )

    run_pipeline >> check_bronze >> check_silver >> check_gold
