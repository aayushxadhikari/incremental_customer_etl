# Airflow orchestration

The `customer_etl` DAG calls `src.pipeline.run()` once per day at 02:00 UTC.
It processes READY batches, retries failures twice with a five-minute delay,
and returns aggregate processing metrics to XCom. Detailed run audits are
stored in MySQL's `etl_run_log` table.

## Setup

Use Python 3.11 and Java 17+. On Windows, run Airflow through WSL2 or Docker.
Run these commands from the project root in an activated virtual environment:

```bash
pip install "apache-airflow==2.10.4" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
pip install "apache-airflow==2.10.4" -r requirements.txt
pip check
python -m scripts.download_jdbc

export AIRFLOW_HOME="$(pwd)/orchestration"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/orchestration/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

if [ ! -f config/db.env ]; then
  cp config/db.env.example config/db.env
fi
```

Configure database credentials in `config/db.env`, start MySQL, then launch:

```bash
airflow standalone
```

Open http://localhost:8080 and log in using the generated admin credentials.
Unpause `customer_etl` to enable scheduling, or select **Trigger DAG** for a
manual run. CSV ingestion is separate from the DAG.

## Execution

The single `run_pipeline` task retains the Spark session and DataFrames within
one process. `max_active_runs=1` prevents overlapping DAG runs; the pipeline's
MySQL advisory lock also prevents overlap with CLI invocations. Customer changes,
rejects, the batch checkpoint and the successful audit commit together.

The DAG imports pipeline dependencies inside the task so the scheduler can parse
it independently. The directory is named `orchestration` to avoid shadowing the
installed `airflow` package when the project root is added to `sys.path`.

Airflow runtime files and metadata are ignored by Git. Airflow's metadata
database is separate from the customer database.
