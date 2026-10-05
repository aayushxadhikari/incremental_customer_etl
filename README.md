# Incremental Customer ETL

A customer data pipeline built with Python, PySpark, MySQL and Docker, with
Airflow scheduling and a Streamlit dashboard.

## What the project does

- Loads customer CSV deliveries into MySQL staging tables.
- Cleans and validates records, handles duplicates and detects changes.
- Updates customer records while preserving history using SCD Type 2.
- Stores rejected records and tracks batch processing results.
- Displays customers, history, pipeline runs and rejection metrics in a dashboard.
- Includes automated transformation and database integration tests.

## How to run

Run the commands from the project root.

### 1. Start Docker Desktop

Install Docker Desktop and wait for the engine to start.

### 2. Configure the database

Create the configuration file if it does not exist:

```bash
if [ ! -f config/db.env ]; then
  cp config/db.env.example config/db.env
fi
```

Set `MYSQL_PASSWORD` and `MYSQL_ROOT_PASSWORD` in `config/db.env` before the first
startup. For an existing database, keep its current credentials.

### 3. Start MySQL and build the ETL image

```bash
docker compose --env-file config/db.env up -d --wait mysql
docker compose --env-file config/db.env build etl
```

### 4. Prepare a CSV delivery

Create `data/customers.csv` with these columns. Data files are not included in
the repository.

```text
customer_id,name,email,phone,address,source_updated_at,source_sequence
```

Use a timezone-aware timestamp such as `2026-10-05T00:00:00Z` for
`source_updated_at` and an integer for `source_sequence`.

### 5. Ingest the delivery and run ETL

```bash
docker compose --env-file config/db.env run --rm etl \
  python -m scripts.ingest data/customers.csv --source-key delivery-001
docker compose --env-file config/db.env run --rm etl
```

Use a new source key for each new delivery. Reuse the same key when retrying a
delivery.

### 6. Launch the dashboard

Install Python 3.11 or newer, then create a virtual environment if needed:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r dashboard/requirements.txt
.venv/bin/python -m streamlit run dashboard/app.py --server.address 127.0.0.1
```

Open http://localhost:8501. Click **Refresh data** after processing another batch.

### 7. Stop the services

Stop Streamlit with `Ctrl+C`, then stop MySQL:

```bash
docker compose --env-file config/db.env down
```
