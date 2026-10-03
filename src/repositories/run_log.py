"""Audit rows; successful audit and checkpoint share the merge transaction."""

from dataclasses import dataclass

from config.settings import Settings
from src.infrastructure.db import get_connection


@dataclass
class RunMetrics:
    extracted: int = 0
    standardized: int = 0
    valid_rows: int = 0
    inserts: int = 0
    updates: int = 0
    rejects: int = 0


class RunLogRepository:
    def __init__(self, settings: Settings):
        self._settings = settings

    def start_run(self, batch_id: int) -> int:
        with get_connection(self._settings) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO etl_run_log (batch_id,status,started_at) VALUES (%s,'RUNNING',UTC_TIMESTAMP(6))", (batch_id,))
            return cursor.lastrowid

    @staticmethod
    def recover_abandoned(conn):
        """Called only under the global pipeline lock; no live run can own these."""
        with conn.cursor() as cursor:
            cursor.execute("UPDATE etl_run_log SET status='FAILED',finished_at=UTC_TIMESTAMP(6),message='Worker stopped before commit; safe to retry batch',inserts=0,updates=0 WHERE status='RUNNING'")
            cursor.execute("DELETE w FROM customer_work w JOIN etl_run_log r ON r.run_id=w.run_id WHERE r.status='FAILED'")
        # Commit recovery before Spark writes, creating a fresh merge snapshot.
        conn.commit()

    @staticmethod
    def finish_on_connection(conn, run_id, status, message, metrics):
        with conn.cursor() as cursor:
            cursor.execute(
                """UPDATE etl_run_log SET finished_at=UTC_TIMESTAMP(6),extracted=%s,standardized=%s,
                   valid_rows=%s,inserts=%s,updates=%s,rejects=%s,status=%s,message=%s WHERE run_id=%s AND status='RUNNING'""",
                (metrics.extracted, metrics.standardized, metrics.valid_rows, metrics.inserts,
                 metrics.updates, metrics.rejects, status, message, run_id),
            )

    def finish_run(self, run_id: int, *, status: str, message: str, metrics: RunMetrics | None = None):
        with get_connection(self._settings) as conn:
            self.finish_on_connection(conn, run_id, status, message, metrics or RunMetrics())
