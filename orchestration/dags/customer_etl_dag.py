from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


ENV_FILE = str(PROJECT_ROOT / "config" / "db.env")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="customer_etl",
    description="Incremental SCD Type-2 customer load (PySpark -> MySQL).",
    schedule="0 2 * * *",  # daily at 02:00
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
        from config.settings import Settings
        from src.pipeline import run

        settings = Settings.from_env(env_file=ENV_FILE)
        metrics = run(settings)

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
