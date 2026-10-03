"""Airflow DAG that runs the incremental customer ETL.

This DAG is a thin *orchestration* layer: it does not re-implement any pipeline
logic. It imports the existing ``src.pipeline.run`` entry point and lets Airflow
handle scheduling, retries, alerting and run history.

Why a single task?
------------------
The pipeline shares one Spark session across extract/transform/load and passes
*lazy* Spark DataFrames between stages. Those DataFrames can't be serialised and
handed from one Airflow task to another, so splitting the flow into separate
tasks would mean re-architecting the pipeline. Running the whole `run()` inside
one task keeps the pipeline intact while still giving us scheduling + retries.
The pipeline already records fine-grained per-stage metrics in ``etl_run_log``.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

# ---------------------------------------------------------------------------
# Make the project importable.
#
# This file lives at  <project_root>/orchestration/dags/customer_etl_dag.py
# so the project root (which contains `config/` and `src/`) is two levels up.
# Airflow loads DAG files from the dags folder, not from the project root, so we
# add the project root to sys.path before importing the pipeline.
# NOTE: the folder is deliberately NOT named `airflow/` — adding the project
# root to sys.path would otherwise shadow the installed `apache-airflow`.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Absolute path to the credentials file, so the task works regardless of the
# worker's current working directory.
ENV_FILE = str(PROJECT_ROOT / "config" / "db.env")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # "email": ["you@example.com"],
    # "email_on_failure": True,
}


@dag(
    dag_id="customer_etl",
    description="Incremental SCD Type-2 customer load (PySpark -> MySQL).",
    schedule="0 2 * * *",  # daily at 02:00; set to None for manual-only runs
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # never let two loads overlap on the same tables
    default_args=default_args,
    tags=["etl", "customer", "scd2"],
)
def customer_etl():
    @task(task_id="run_pipeline")
    def run_pipeline() -> dict:
        """Invoke the existing pipeline and return its run metrics."""
        # Imported inside the task so DAG *parsing* never needs pyspark/mysql
        # installed on the scheduler — only the worker that runs the task does.
        from config.settings import Settings
        from src.pipeline import run

        settings = Settings.from_env(env_file=ENV_FILE)
        metrics = run(settings)

        # Returned value is pushed to XCom for visibility in the Airflow UI.
        return {
            "extracted": metrics.extracted,
            "standardized": metrics.standardized,
            "valid_rows": metrics.valid_rows,
            "inserts": metrics.inserts,
            "updates": metrics.updates,
            "rejects": metrics.rejects,
        }

    run_pipeline()


dag = customer_etl()
