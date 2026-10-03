# Incremental Customer ETL

PySpark normalizes and validates sealed customer batches. MySQL applies SCD
Type 2 changes, rejects, the batch checkpoint and the successful audit record in
one transaction. Replaying a processed batch does not write more history.

## Start here

Run all commands from this project directory. The recommended way is Docker:
it supplies MySQL, Python, Java, and Spark without installing them separately.

Your current `config/db.env` is already configured for the Docker database.
Keep this file; do not overwrite it with the example. The older
`config/db.env.local-backup` is unused by the application. It is only a copy of
previous connection settings, not a database backup.

The normal workflow is: start MySQL → ingest a CSV → run the ETL → inspect results.
Running the ETL without any READY batches has no new customer data to process.

## Run with Docker

Docker Desktop must be running. Compose 2.24.4+ is required for the isolated test
port override. Run commands below from the project root.

1. Install and open Docker Desktop, then wait for its engine to start.
2. Create settings only if `config/db.env` is missing:

```bash
if [ ! -f config/db.env ]; then
  cp config/db.env.example config/db.env
fi
```

If you just copied the example, edit `config/db.env` and replace
`MYSQL_PASSWORD` and `MYSQL_ROOT_PASSWORD` with different local passwords.
Keep your existing passwords when using an existing database volume.

3. Start the database and build the ETL image:

```bash
docker compose --env-file config/db.env up -d --wait mysql
docker compose --env-file config/db.env build etl
```

4. Load the included sample and run the pipeline:

```bash
docker compose --env-file config/db.env run --rm etl \
  python -m scripts.ingest data/example.csv --source-key example-delivery-001
docker compose --env-file config/db.env run --rm etl
```

5. Inspect the results using the MySQL commands below.

For your own delivery, place a CSV in `data/` and run:

```bash
docker compose --env-file config/db.env run --rm etl \
  python -m scripts.ingest data/customers.csv --source-key customers-delivery-002
docker compose --env-file config/db.env run --rm etl
```

Replace the filename and delivery key each time you receive a new delivery.
To retry the same delivery, keep its original key.

On later runs, start MySQL, ingest your new delivery, and run ETL again; rebuilding
is needed when code or dependencies change.

The MySQL database is `customer_db`. Host clients connect to `127.0.0.1:3307`
as `customer_etl` using `MYSQL_PASSWORD` from the gitignored `config/db.env`.
The ETL container connects to `mysql:3306`; it includes Python 3.11, Java 17 and
the JDBC jar. Neither credentials nor the host virtualenv enter the image.

```bash
docker compose --env-file config/db.env ps
docker compose --env-file config/db.env logs mysql
docker compose --env-file config/db.env exec mysql \
  mysql -u customer_etl -p customer_db
```

Inside MySQL:

```sql
SELECT batch_id, source_key, status FROM ingestion_batches;
SELECT * FROM customer_master ORDER BY customer_id, start_date;
SELECT run_id, batch_id, status, inserts, updates, rejects FROM etl_run_log;
SELECT reason, COUNT(*) FROM customer_rejects GROUP BY reason;
```

`docker compose ... down` retains the named volume. `down -v` deletes database
data. Initialization SQL and environment credentials run only on a fresh volume;
use migrations or SQL account changes for an existing one. Pin image digests
when promoting a tested environment to production.

## Ingestion contract

CSV columns are `customer_id,name,email,phone,address,source_updated_at,source_sequence`.
Put local CSVs in `data/`, mounted read-only in the runner. Source timestamps
must include a timezone; ingestion converts them to UTC. Empty optional values
become null. Missing source metadata becomes a reject; malformed timestamp or
sequence values roll back the entire ingestion transaction.

`--source-key` is the upstream delivery's stable unique identity. Reusing it
returns the existing batch without rereading the CSV. Use a new key for a new
delivery, and never reuse a key for different contents.

The ingestion command writes rows into an OPEN batch and seals it READY in a
single transaction. Upstream integrations can follow the same contract directly
in SQL. Database triggers block writes to sealed batches and modifications to
staging rows. The pipeline extracts only READY batches, in batch-ID order, and
commits each separately as PROCESSED. Staging history is retained; archival needs
a deliberate maintenance migration.

Within a batch, the newest source timestamp wins, then the highest source
sequence. Identical ties keep the lowest source-row ID. Conflicting winning
ties are rejected rather than choosing arbitrary customer values. Superseded
rows are also retained as rejects. A late batch cannot overwrite a newer master
source version; conflicting equal source versions across batches are rejected.
Source sequence numbers must have a stable ordering for each customer.

## Correctness and recovery

- Required customer fields must be nonempty after trimming. Field lengths match
  master limits. Reject reasons identify failing fields and preserve duplicates.
- SHA-256 hashes use JSON with named fields and explicit nulls. `hash_version=2`
  distinguishes this representation from legacy delimiter-based hashes.
- Spark writes disposable `customer_work` rows keyed by run and source-row ID.
  MySQL joins this work table to the master, avoiding driver-side ID collection.
- One connection-owned MySQL advisory lock serializes CLI and Airflow loads.
  All writers of this dimension must use this protocol; the unique generated
  active-customer key also prevents multiple active versions.
- Expiration and replacement use the same UTC timestamp and transaction.
  Rejects, checkpoint and successful audit commit with them. Failure rolls all
  of those back; the READY batch is retryable. Each attempt gets a new run ID.
- Failed partial work is cleaned on the next invocation. Under the lock,
  abandoned RUNNING audits are marked FAILED before processing resumes.
- Metrics count input rows and final rejects; `valid_rows + rejects = extracted`.
  Superseded/ambiguous/stale rows count as rejects. Inserts/updates on FAILED
  attempts are zero because their transaction did not commit.

For a network failure during COMMIT, the client may not know whether the server
committed. Retry the pipeline: the persisted batch status is authoritative and
prevents replaying a completed batch. Investigate the audit and batch together.

## Existing databases

The Docker database is separate from a previous host MySQL instance. Back up and
export/import that instance before switching if you need its existing history.
The migration command upgrades the original four-table project schema without
deleting history. Stop old workers and back up first: MySQL ALTER TABLE commits
immediately and can lock/rebuild tables.

The local Compose server trusts database-scoped trigger creators so the migration
account can install ingestion guards with binary logging enabled. On a separately
managed server, have its DBA create those triggers or approve that server setting.

```bash
# Run with settings pointing to the database being upgraded.
python -m scripts.migrate
```

For a database imported into this Docker instance:

```bash
docker compose --env-file config/db.env run --rm etl python -m scripts.migrate
```

Migration checks for multiple active versions and invalid legacy history before
DDL. It preserves old hashes as version 1; unchanged legacy rows upgrade their
hash without manufacturing a new SCD version. Existing staging becomes one
READY legacy batch. Because old staging lacks source ordering, conflicting
duplicate IDs in that batch are rejected for inspection. Source timestamps for
legacy staging are migration time, not original business event time.

If this workspace has `config/db.env.local-backup`, it contains the connection
settings from before Docker setup and is gitignored. Do not publish it. The old
database's data is not part of that backup.

## Local Python and Airflow

Use Python 3.11 and Java 17+ for the shared ETL/Airflow environment. The previous
Python 3.13 virtualenv is not a supported target for pinned Airflow 2.10.4.

Start the Docker MySQL service first if using the current settings:

```bash
docker compose --env-file config/db.env up -d --wait mysql
```

Then install dependencies and run locally:

```bash
python3.11 -m venv .venv311
source .venv311/bin/activate
pip install -r requirements.txt
python -m scripts.ingest data/example.csv --source-key example-delivery-001
python main.py
```

Ensure `JAVA_HOME` points to Java 17+, and retain the valid host `MYSQL_JAR` path.
Configuration resolves relative jar paths from the project root. Environment
variables override the dotenv file, including for container workers.

See [orchestration/README.md](orchestration/README.md) for Airflow setup. The DAG
runs daily at 02:00 UTC (07:45 Nepal), returns aggregate committed-batch metrics
to XCom, and retries safely through the same database protocol. Airflow's own
metadata database is separate from this customer database.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Cannot connect to the Docker daemon | Open Docker Desktop and wait until its engine is running. |
| Connection refused / MySQL error 2003 | Start MySQL with the command above and check `docker compose --env-file config/db.env ps` and `logs mysql`. Local Python uses `127.0.0.1:3307`; the container uses `mysql:3306`. |
| Access denied / MySQL error 1045 | Check the username and password against the existing database accounts. Changing `db.env` does not change passwords in an initialized volume. |
| Missing environment variables | Ensure `config/db.env` exists and has the keys shown in `config/db.env.example`. Exported shell variables take precedence; clear stale `MYSQL_*` exports if needed. |
| JDBC jar not found | Ensure `drivers/mysql-connector-j.jar` exists and `MYSQL_JAR` points to it. |
| Java gateway fails locally | Check `java -version` and `JAVA_HOME`; Spark needs Java 17+. Use Docker if you do not want a local Java setup. |
| No new rows after rerunning a delivery | A processed batch is skipped. Use a new source key for a new delivery and inspect `customer_rejects` for rejected rows. |
| Missing tables on an existing database | Back up the database and follow the migration instructions above. |

To stop the Docker services while keeping your data:

```bash
docker compose --env-file config/db.env down
```

## Verification

The integration suite runs in its own Docker project on port 13307. Do not set
`ETL_TEST_DATABASE=1` against your working database: migration tests intentionally
replace tables in the disposable test instance.

```bash
docker compose -p customer-etl-test -f compose.yaml -f compose.test.yaml \
  --env-file config/db.env up -d --wait mysql
docker compose -p customer-etl-test -f compose.yaml -f compose.test.yaml \
  --env-file config/db.env build etl
docker compose -p customer-etl-test -f compose.yaml -f compose.test.yaml \
  --env-file config/db.env run --rm etl python -m unittest discover -v
# Removes ONLY the disposable test project's data:
docker compose -p customer-etl-test -f compose.yaml -f compose.test.yaml \
  --env-file config/db.env down -v
```

GitHub Actions runs these checks on pushes and pull requests. Coverage includes
normalization, hash boundaries, duplicate resolution, new/unchanged/changed
loads, transactional rollback, retry, locking, database constraints, ingestion
rollback and legacy migration. Tune `JDBC_PARTITIONS`, `JDBC_FETCH_SIZE`,
`JDBC_BATCH_SIZE`, `SPARK_MASTER` and `SPARK_SHUFFLE_PARTITIONS` after measuring
representative workloads. The default runner uses two local Spark threads.

## Layout

```text
config/               validated configuration, private dotenv and template
src/etl/              batch extraction, transformations and work-table writes
src/infrastructure/   Spark, JDBC and native MySQL connection factories
src/repositories/     transactional merge and run audits
src/pipeline.py       batch loop, recovery and cleanup
scripts/              CSV ingestion and legacy migration
sql/schema.sql        fresh schema, constraints and ingestion guards
tests/                transformation and isolated database integration tests
orchestration/        optional Airflow DAG
compose.yaml          persistent MySQL and one-shot ETL runner
```
