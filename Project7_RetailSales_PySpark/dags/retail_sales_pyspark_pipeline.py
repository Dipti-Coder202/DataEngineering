"""Airflow orchestration for the Project 7 incremental PySpark pipeline."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


PROJECT_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_DIR = PROJECT_DIR.parent
PYTHON_BIN = REPOSITORY_DIR / "venv" / "bin" / "python"
PIPELINE_RUNNER = PROJECT_DIR / "run_pipeline.py"
BRONZE_ROOT = PROJECT_DIR / "data" / "bronze" / "retail_sales"
PIPELINE_COMMAND = f'"{PYTHON_BIN}" "{PIPELINE_RUNNER}"'
BATCH_XCOM = "{{ ti.xcom_pull(task_ids='generate_batch_id') }}"


def generate_batch_id(**context) -> str:
    """Return a safe ID that remains stable across retries of one DAG run."""
    run_id = context["run_id"]
    logical_date = context["logical_date"].astimezone(timezone.utc)
    run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:8]
    batch_id = f"airflow_{logical_date:%Y%m%dT%H%M%S}_{run_hash}"
    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError("Generated an unsafe batch ID")
    return batch_id


def choose_processing_path(**context) -> str:
    """Skip Spark processing when extraction did not create a Bronze batch."""
    batch_id = context["ti"].xcom_pull(task_ids="generate_batch_id")
    matching_batches = list(
        BRONZE_ROOT.glob(f"ingestion_date=*/batch_id={batch_id}")
    )
    if len(matching_batches) > 1:
        raise RuntimeError(f"Batch ID appears in multiple partitions: {batch_id}")
    return "transform_to_silver" if matching_batches else "no_changes"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retail_sales_pyspark_pipeline",
    description="Incremental PostgreSQL to Bronze/Silver/Gold PySpark pipeline",
    start_date=datetime(2026, 8, 16, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["retail", "pyspark", "postgresql", "data-lake"],
) as dag:
    start = EmptyOperator(task_id="start")

    create_batch_id = PythonOperator(
        task_id="generate_batch_id",
        python_callable=generate_batch_id,
    )

    extract = BashOperator(
        task_id="extract_to_bronze",
        bash_command=(
            f"{PIPELINE_COMMAND} --stage extract --batch-id \"{BATCH_XCOM}\""
        ),
        cwd=str(REPOSITORY_DIR),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    branch = BranchPythonOperator(
        task_id="check_for_source_changes",
        python_callable=choose_processing_path,
    )

    no_changes = EmptyOperator(task_id="no_changes")

    transform = BashOperator(
        task_id="transform_to_silver",
        bash_command=(
            f"{PIPELINE_COMMAND} --stage transform --batch-id \"{BATCH_XCOM}\""
        ),
        cwd=str(REPOSITORY_DIR),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    aggregate = BashOperator(
        task_id="create_gold",
        bash_command=f"{PIPELINE_COMMAND} --stage aggregate",
        cwd=str(REPOSITORY_DIR),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    load_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=f"{PIPELINE_COMMAND} --stage load",
        cwd=str(REPOSITORY_DIR),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    quality = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=(
            f"{PIPELINE_COMMAND} --stage quality --batch-id \"{BATCH_XCOM}\""
        ),
        cwd=str(REPOSITORY_DIR),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    commit = BashOperator(
        task_id="commit_watermark",
        bash_command=(
            f"{PIPELINE_COMMAND} --stage commit --batch-id \"{BATCH_XCOM}\""
        ),
        cwd=str(REPOSITORY_DIR),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> create_batch_id >> extract >> branch
    branch >> no_changes >> end
    branch >> transform >> aggregate >> load_postgres >> quality >> commit >> end
