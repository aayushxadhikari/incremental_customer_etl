"""Upgrade the original four-table schema in place without deleting data.

    python -m scripts.migrate

Stop old workers and take a backup first. MySQL DDL commits immediately; this
command checks individual columns/indexes so interrupted upgrades can be rerun.
Fresh Docker installs already have the current schema.
"""

from pathlib import Path

from config.settings import Settings
from src.infrastructure.db import get_connection

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


def statements(text):
    """Read our SQL files including DELIMITER blocks (not arbitrary SQL)."""
    delimiter, buffer = ";", []
    for line in text.splitlines():
        if line.strip().startswith("--") or not line.strip():
            continue
        if line.strip().startswith("DELIMITER "):
            delimiter = line.strip().split()[1]
            continue
        buffer.append(line)
        if line.rstrip().endswith(delimiter):
            yield "\n".join(buffer).rstrip()[:-len(delimiter)]
            buffer.clear()
    if buffer:
        raise ValueError("Unterminated schema SQL")


def migrate(settings):
    if settings.mysql_db != "customer_db":
        raise ValueError("This schema is for customer_db; use a separate test MySQL instance")
    with get_connection(settings) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s,0)", (f"{settings.mysql_db}:customer_etl",))
        if cursor.fetchone()[0] != 1:
            raise RuntimeError("Stop the running pipeline before migrating")
        cursor.execute("SHOW TABLES")
        tables = {row[0] for row in cursor}
        if "customer_master" in tables:
            cursor.execute("SELECT customer_id FROM customer_master WHERE active_flag=1 GROUP BY customer_id HAVING COUNT(*)>1 LIMIT 1")
            if cursor.fetchone():
                raise RuntimeError("Multiple active versions exist; resolve them before migration")
            cursor.execute("SELECT COUNT(*) FROM customer_master WHERE BINARY customer_id<>BINARY TRIM(customer_id) OR customer_id='' OR active_flag NOT IN (0,1) OR start_date IS NULL OR (active_flag=0 AND end_date IS NULL) OR (active_flag=1 AND end_date IS NOT NULL) OR end_date<start_date")
            if cursor.fetchone()[0]:
                raise RuntimeError("Noncanonical IDs or invalid history exist; repair them before migration")

        sql = list(statements(SCHEMA.read_text()))
        # Create new supporting tables before adding foreign keys to old ones.
        for statement in sql:
            if not statement.startswith("CREATE TRIGGER"):
                cursor.execute(statement)

        def columns(table):
            cursor.execute(f"SHOW COLUMNS FROM {table}")
            return {row[0] for row in cursor}

        def add(table, name, definition):
            if name not in columns(table):
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

        def index(table, name, definition):
            cursor.execute(f"SHOW INDEX FROM {table} WHERE Key_name=%s", (name,))
            if not cursor.fetchall():
                cursor.execute(f"ALTER TABLE {table} ADD {definition}")

        add("customer_staging", "source_row_id", "BIGINT AUTO_INCREMENT PRIMARY KEY")
        add("customer_staging", "batch_id", "BIGINT NULL")
        add("customer_staging", "source_updated_at", "DATETIME(6)")
        add("customer_staging", "source_sequence", "BIGINT")
        cursor.execute("SELECT COUNT(*) FROM customer_staging WHERE batch_id IS NULL")
        if cursor.fetchone()[0]:
            cursor.execute("INSERT IGNORE INTO ingestion_batches (source_key) VALUES ('legacy-staging-migration-v2')")
            cursor.execute("SELECT batch_id,status FROM ingestion_batches WHERE source_key='legacy-staging-migration-v2'")
            legacy_id, status = cursor.fetchone()
            if status != "OPEN":
                raise RuntimeError("Legacy batch already sealed but unmigrated rows remain")
            cursor.execute("UPDATE customer_staging SET batch_id=%s,source_updated_at=UTC_TIMESTAMP(6),source_sequence=0 WHERE batch_id IS NULL", (legacy_id,))
            cursor.execute("UPDATE ingestion_batches SET status='READY' WHERE batch_id=%s", (legacy_id,))
            conn.commit()
        cursor.execute("ALTER TABLE customer_staging MODIFY batch_id BIGINT NOT NULL, MODIFY customer_id TEXT, MODIFY name TEXT, MODIFY email TEXT, MODIFY phone TEXT, MODIFY address TEXT")
        index("customer_staging", "idx_staging_batch", "KEY idx_staging_batch (batch_id,source_row_id)")

        add("customer_master", "hash_version", "INT NOT NULL DEFAULT 1")
        add("customer_master", "run_id", "BIGINT")
        add("customer_master", "source_updated_at", "DATETIME(6)")
        add("customer_master", "source_sequence", "BIGINT")
        add("customer_master", "active_customer_id", "VARCHAR(64) GENERATED ALWAYS AS (CASE WHEN active_flag=1 THEN customer_id ELSE NULL END) STORED")
        index("customer_master", "uq_one_active_customer", "UNIQUE KEY uq_one_active_customer (active_customer_id)")
        for name in ("batch_id",):
            add("etl_run_log", name, "BIGINT")
        for name in ("extracted", "standardized", "valid_rows", "inserts", "updates", "rejects"):
            cursor.execute(f"ALTER TABLE etl_run_log MODIFY {name} BIGINT DEFAULT 0")
        add("customer_rejects", "reject_id", "BIGINT AUTO_INCREMENT PRIMARY KEY")
        for name in ("source_row_id", "batch_id", "run_id"):
            add("customer_rejects", name, "BIGINT")
        index("customer_rejects", "uq_reject_source", "UNIQUE KEY uq_reject_source (source_row_id)")
        cursor.execute("ALTER TABLE customer_rejects MODIFY customer_id TEXT, MODIFY name TEXT, MODIFY email TEXT, MODIFY phone TEXT, MODIFY address TEXT, MODIFY reason TEXT")
        for table, date_fields in {
            "customer_master": ("start_date", "end_date", "updated_at", "etl_loaded_at"),
            "customer_rejects": ("rejected_at",), "etl_run_log": ("started_at", "finished_at"),
        }.items():
            for field in date_fields:
                cursor.execute(f"ALTER TABLE {table} MODIFY {field} DATETIME(6) NULL")
        # Match Spark's case-sensitive ID semantics. Existing hashes stay version 1.
        for table in ("customer_master", "customer_staging", "customer_rejects", "etl_run_log"):
            cursor.execute(f"ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        cursor.execute("SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA=%s AND TABLE_NAME='customer_master' AND CONSTRAINT_TYPE='CHECK'", (settings.mysql_db,))
        checks = {row[0] for row in cursor}
        for name, expression in {
            "ck_master_active_flag": "active_flag IN (0,1)",
            "ck_master_version_state": "(active_flag=1 AND end_date IS NULL) OR (active_flag=0 AND end_date IS NOT NULL)",
            "ck_master_dates": "start_date IS NOT NULL AND (end_date IS NULL OR end_date>=start_date)",
        }.items():
            if name not in checks:
                cursor.execute(f"ALTER TABLE customer_master ADD CONSTRAINT {name} CHECK ({expression})")
        cursor.execute("SHOW TRIGGERS")
        triggers = {row[0] for row in cursor}
        for statement in sql:
            if statement.startswith("CREATE TRIGGER") and statement.split()[2] not in triggers:
                cursor.execute(statement)
    print("Schema upgraded; existing history preserved, legacy staging sealed for processing")


if __name__ == "__main__":
    migrate(Settings.from_env())
