"""Airflow orchestration for the local Project 8 lakehouse."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parent

with DAG(
    dag_id="retail_lakehouse_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["project8", "iceberg", "minio", "pyspark"],
) as dag:
    verify_infrastructure = BashOperator(
        task_id="verify_infrastructure",
        bash_command=f"{REPOSITORY_ROOT}/venv/bin/python {PROJECT_ROOT}/scripts/bootstrap.py",
    )
    run_lakehouse_pipeline = BashOperator(
        task_id="run_lakehouse_pipeline",
        bash_command=f"{REPOSITORY_ROOT}/venv/bin/python {PROJECT_ROOT}/run_pipeline.py",
    )
    verify_infrastructure >> run_lakehouse_pipeline
