"""Database-side SCD2 merge, rejects, checkpoint and audit in one transaction."""

from contextlib import contextmanager

from config.settings import Settings
from src.infrastructure.db import get_connection
from src.repositories.run_log import RunMetrics, RunLogRepository


class CustomerMasterRepository:
    def __init__(self, settings: Settings):
        self._settings = settings

    @contextmanager
    def locked_connection(self):
        with get_connection(self._settings) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT GET_LOCK(%s, 0)", (f"{self._settings.mysql_db}:customer_etl",))
                if cursor.fetchone()[0] != 1:
                    raise RuntimeError("Another customer ETL run holds the database lock")
            # Connection-owned lock is released on close, after commit/rollback.
            yield conn

    def next_batch(self, conn):
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT batch_id FROM ingestion_batches WHERE status='READY' ORDER BY batch_id LIMIT 1")
            batch = cursor.fetchone()
            if batch is None:
                return None
            cursor.execute(
                "SELECT MIN(source_row_id) AS lower_id, MAX(source_row_id) AS upper_id "
                "FROM customer_staging WHERE batch_id=%s", (batch["batch_id"],)
            )
            return {**batch, **cursor.fetchone()}

    def merge(self, conn, run_id: int, batch_id: int, metrics: RunMetrics) -> None:
        """Caller commits on leaving locked_connection; any failure rolls back."""
        with conn.cursor() as cursor:
            cursor.execute("SET @etl_time = UTC_TIMESTAMP(6)")
            # Late source deliveries must not restore older customer values.
            cursor.execute(
                """UPDATE customer_work w JOIN customer_master m
                   ON m.customer_id=w.customer_id AND m.active_flag=1
                   SET w.reason=CASE
                     WHEN w.source_updated_at<m.source_updated_at OR
                       (w.source_updated_at=m.source_updated_at AND w.source_sequence<m.source_sequence)
                       THEN 'Stale source version'
                     ELSE 'Conflicting source version across batches' END
                   WHERE w.run_id=%s AND w.reason IS NULL AND m.source_updated_at IS NOT NULL AND
                     (w.source_updated_at<m.source_updated_at OR
                      (w.source_updated_at=m.source_updated_at AND w.source_sequence<m.source_sequence) OR
                      (w.source_updated_at=m.source_updated_at AND w.source_sequence=m.source_sequence AND
                       NOT ((BINARY m.name <=> BINARY w.name) AND (BINARY m.email <=> BINARY w.email) AND
                            (BINARY m.phone <=> BINARY w.phone) AND (BINARY m.address <=> BINARY w.address))))""",
                (run_id,),
            )
            cursor.execute(
                """UPDATE customer_work w LEFT JOIN customer_master m
                   ON m.customer_id=w.customer_id AND m.active_flag=1
                   SET w.action=CASE
                     WHEN m.master_id IS NULL THEN 'INSERT'
                     WHEN m.hash_version=2 AND m.hash_val=w.hash_val THEN 'UNCHANGED'
                     WHEN m.hash_version<>2 AND
                          (BINARY m.name <=> BINARY w.name) AND (BINARY m.email <=> BINARY w.email) AND
                          (BINARY m.phone <=> BINARY w.phone) AND (BINARY m.address <=> BINARY w.address)
                       THEN 'UNCHANGED'
                     ELSE 'UPDATE' END
                   WHERE w.run_id=%s AND w.reason IS NULL""", (run_id,)
            )
            cursor.execute("SELECT action, COUNT(*) FROM customer_work WHERE run_id=%s AND reason IS NULL GROUP BY action", (run_id,))
            counts = dict(cursor.fetchall())
            inserts, updates = counts.get("INSERT", 0), counts.get("UPDATE", 0)
            cursor.execute(
                """UPDATE customer_master m JOIN customer_work w ON w.customer_id=m.customer_id
                   SET m.active_flag=0, m.end_date=@etl_time, m.updated_at=@etl_time
                   WHERE m.active_flag=1 AND w.run_id=%s AND w.action='UPDATE'""", (run_id,)
            )
            cursor.execute(
                """INSERT INTO customer_master
                   (customer_id,name,email,phone,address,hash_val,hash_version,active_flag,
                    start_date,end_date,updated_at,etl_loaded_at,source_updated_at,source_sequence,run_id)
                   SELECT customer_id,name,email,phone,address,hash_val,2,1,
                          @etl_time,NULL,@etl_time,@etl_time,source_updated_at,source_sequence,run_id
                   FROM customer_work WHERE run_id=%s AND action IN ('INSERT','UPDATE')""", (run_id,)
            )
            cursor.execute(
                """UPDATE customer_master m JOIN customer_work w ON w.customer_id=m.customer_id
                   SET m.hash_val=w.hash_val,m.hash_version=2,
                       m.source_updated_at=w.source_updated_at,m.source_sequence=w.source_sequence
                   WHERE m.active_flag=1 AND w.run_id=%s AND w.action='UNCHANGED'""", (run_id,)
            )
            cursor.execute(
                """INSERT INTO customer_rejects
                   (source_row_id,batch_id,run_id,customer_id,name,email,phone,address,reason,rejected_at)
                   SELECT source_row_id,batch_id,run_id,customer_id,name,email,phone,address,reason,@etl_time
                   FROM customer_work WHERE run_id=%s AND reason IS NOT NULL""", (run_id,)
            )
            cursor.execute("UPDATE ingestion_batches SET status='PROCESSED',processed_at=@etl_time,run_id=%s WHERE batch_id=%s AND status='READY'", (run_id, batch_id))
            if cursor.rowcount != 1:
                raise RuntimeError("Batch changed during processing")
            cursor.execute("SELECT COUNT(*) FROM customer_work WHERE run_id=%s AND reason IS NOT NULL", (run_id,))
            metrics.rejects = cursor.fetchone()[0]
            metrics.valid_rows = metrics.extracted - metrics.rejects
            committed_metrics = RunMetrics(metrics.extracted, metrics.standardized, metrics.valid_rows, inserts, updates, metrics.rejects)
            RunLogRepository.finish_on_connection(conn, run_id, "SUCCESS", "Batch committed", committed_metrics)
            cursor.execute("DELETE FROM customer_work WHERE run_id=%s", (run_id,))
        metrics.inserts, metrics.updates = inserts, updates
