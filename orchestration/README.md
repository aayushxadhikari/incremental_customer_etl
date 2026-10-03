# Airflow orchestration

This folder adds [Apache Airflow](https://airflow.apache.org/) scheduling on top
of the existing pipeline **without changing any pipeline code**. The DAG simply
calls `src.pipeline.run()` — Airflow provides scheduling, retries, alerting and
run history; the pipeline still does all the real work.

```
orchestration/                  # NOT named `airflow/` on purpose — see note below
├── dags/
│   └── customer_etl_dag.py     # the DAG (one task: run the whole pipeline)
├── requirements-airflow.txt    # Airflow install pin + constraints note
└── README.md                   # this file
```

> **Why `orchestration/` and not `airflow/`?** The DAG adds the project root to
> `sys.path` so it can `import src.pipeline`. If this folder were named `airflow`,
> a plain `import airflow` would resolve to *this* directory instead of the
> installed `apache-airflow` package and break everything. The folder name just
> can't collide with the package name.

## One-time setup (local, standalone)

> Use a dedicated **Python 3.11** virtualenv with **Java 17+**. Do not reuse the
> project's old Python 3.13 environment with Airflow 2.10.4.
> Airflow does **not** support Windows natively — on
> Windows use WSL2 or Docker.

1. **Install Airflow** (with the official constraints) and the project deps into
   the *same* environment:
   ```bash
   pip install "apache-airflow==2.10.4" \
     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
   pip install "apache-airflow==2.10.4" -r requirements.txt
   pip check
   ```

2. **Point Airflow at this project** so it discovers the DAG. From the project
   root:
   ```bash
   export AIRFLOW_HOME="$(pwd)/orchestration"
   export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/orchestration/dags"
   export AIRFLOW__CORE__LOAD_EXAMPLES=False
   ```
   (Put these in your shell profile or an `.envrc` so every session has them.)

3. **Make sure `config/db.env` exists** (the DAG reads it by absolute path):
   ```bash
   # Keep existing credentials; create the file only if it is missing.
   if [ ! -f config/db.env ]; then
     cp config/db.env.example config/db.env
   fi
   ```

4. **Start Airflow** in standalone mode (runs webserver + scheduler, prints a
   generated admin password on first launch):
   ```bash
   airflow standalone
   ```
   Open http://localhost:8080, log in, and you'll see the **`customer_etl`** DAG.

## Running it

- **From the UI:** unpause `customer_etl`, then click ▶ "Trigger DAG".
- **From the CLI:**
  ```bash
  airflow dags test customer_etl 2026-06-14   # run once, no scheduler needed
  ```

The single `run_pipeline` task invokes the pipeline. Its metrics
(extracted / valid / inserts / updates / rejects) are pushed to **XCom**, so you
can see them in the UI under the task's *XCom* tab. Detailed per-run history also
lands in the `etl_run_log` MySQL table, exactly as before.

## How it's wired

- **Schedule:** `0 2 * * *` (daily 02:00 UTC). Change the `schedule=` arg in
  [`dags/customer_etl_dag.py`](dags/customer_etl_dag.py), or set it to `None` for
  manual-only runs.
- **Retries:** 2 retries after the initial attempt, 5 minutes apart.
- **No overlap:** `max_active_runs=1` serializes DAG runs; the pipeline's MySQL
  advisory lock also guards manual CLI runs. The successful audit, SCD changes,
  rejects and batch checkpoint commit together, making failed batches retryable.
- **Imports inside the task:** `pyspark` / `mysql-connector` are imported only
  when the task *runs*, so the scheduler can parse the DAG even if those aren't
  installed on the scheduler node.

## Why one task instead of extract → transform → load tasks?

The pipeline shares a single Spark session and passes **lazy Spark DataFrames**
between stages. Those can't be serialised and handed across Airflow task
boundaries, so splitting them would require re-architecting the pipeline (e.g.
each stage writing to and re-reading from intermediate tables). Keeping the flow
in one task preserves the current design; the in-DB `etl_run_log` already gives
you per-stage observability.
