"""Migration fixtures replace tables ONLY in the explicitly isolated test DB."""

import os
import unittest
from pathlib import Path

from config.settings import Settings
from scripts.migrate import migrate, statements
from src.infrastructure.db import get_connection


@unittest.skipUnless(os.getenv("ETL_TEST_DATABASE") == "1", "Requires disposable test database")
class MigrationTests(unittest.TestCase):
    def test_upgrade_preserves_history_and_is_restartable(self):
        settings = Settings.from_env()
        with get_connection(settings) as conn, conn.cursor() as cursor:
            # This module sorts after pipeline tests. These are test fixtures.
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            for table in ("customer_work", "customer_rejects", "customer_master", "customer_staging", "etl_run_log", "ingestion_batches"):
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            for sql in statements((Path(__file__).parent / "fixtures" / "legacy_schema.sql").read_text()):
                cursor.execute(sql)
            cursor.execute("INSERT INTO customer_master (customer_id,name,email,phone,address,hash_val,active_flag,start_date,end_date) VALUES ('legacy','alice',NULL,'123',NULL,REPEAT('0',64),1,'2026-01-01',NULL)")
            cursor.execute("INSERT INTO customer_staging (customer_id,name,phone) VALUES ('legacy','alice','123'),('ambiguous','a','123'),('ambiguous','b','123')")
            cursor.execute("INSERT INTO customer_rejects (customer_id,reason) VALUES ('old-reject','Missing phone')")
        migrate(settings)
        migrate(settings)
        with get_connection(settings) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT customer_id,name,active_flag,hash_version,start_date FROM customer_master")
            result = cursor.fetchone()
            self.assertEqual(result[:4], ("legacy", "alice", 1, 1))
            self.assertEqual(result[4].isoformat(), "2026-01-01T00:00:00")
            cursor.execute("SELECT COUNT(*) FROM customer_staging")
            self.assertEqual(cursor.fetchone()[0], 3)
            cursor.execute("SELECT status FROM ingestion_batches WHERE source_key='legacy-staging-migration-v2'")
            self.assertEqual(cursor.fetchone()[0], "READY")
            cursor.execute("SELECT reason FROM customer_rejects WHERE customer_id='old-reject'")
            self.assertEqual(cursor.fetchone()[0], "Missing phone")
        from src.pipeline import run
        metrics = run(settings)
        self.assertEqual((metrics.extracted, metrics.updates, metrics.rejects), (3, 0, 2))
        with get_connection(settings) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*),MAX(hash_version) FROM customer_master WHERE customer_id='legacy'")
            self.assertEqual(cursor.fetchone(), (1, 2))
