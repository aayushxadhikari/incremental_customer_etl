"""Run in the isolated compose.test.yaml project, never the working database."""

import csv
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import mysql.connector

from config.settings import Settings
from scripts.ingest import ingest
from src.infrastructure.db import get_connection
from src.infrastructure.spark import build_spark_session
from src.pipeline import run
from src.repositories.customer_master import CustomerMasterRepository


@unittest.skipUnless(os.getenv("ETL_TEST_DATABASE") == "1", "Use the isolated compose.test.yaml project")
class DatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = Settings.from_env()
        cls.spark = build_spark_session(cls.settings)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.key = "test-" + uuid.uuid4().hex
        self.customer = self.key

    def query(self, sql, args=()):
        with get_connection(self.settings) as conn, conn.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchall() if cursor.with_rows else cursor.rowcount

    def batch(self, rows=None, key=None):
        rows = rows if rows is not None else [{"customer_id": self.customer, "name": "Alice", "phone": "123"}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.csv"
            with path.open("w", newline="") as stream:
                fields = ["customer_id", "name", "email", "phone", "address", "source_updated_at", "source_sequence"]
                writer = csv.DictWriter(stream, fields)
                writer.writeheader()
                for r in rows:
                    writer.writerow({"source_updated_at": "2026-10-03T00:00:00Z", "source_sequence": 1, **r})
            return ingest(self.settings, path, key or self.key)

    def pipeline(self):
        with patch("src.pipeline.build_spark_session", return_value=self.spark), patch.object(self.spark, "stop"):
            return run(self.settings)

    def versions(self):
        return self.query("SELECT name,active_flag,start_date,end_date FROM customer_master WHERE customer_id=%s ORDER BY master_id", (self.customer,))

    def test_new_unchanged_changed_and_replay(self):
        batch = self.batch()
        metrics = self.pipeline()
        self.assertEqual(metrics.inserts, 1)
        self.assertEqual(self.pipeline().extracted, 0)
        self.assertEqual(self.batch(), batch)
        self.batch(key=self.key + "-unchanged")
        metrics = self.pipeline()
        self.assertEqual((metrics.inserts, metrics.updates), (0, 0))
        self.batch([{"customer_id": self.customer, "name": "Bob", "phone": "123", "source_sequence": 2}], self.key + "-changed")
        self.assertEqual(self.pipeline().updates, 1)
        versions = self.versions()
        self.assertEqual([(r[0], r[1]) for r in versions], [("alice", 0), ("bob", 1)])
        self.assertEqual(versions[0][3], versions[1][2])

    def test_failed_transaction_and_retry(self):
        self.batch()
        self.pipeline()
        batch = self.batch([
            {"customer_id": self.customer, "name": "Bob", "phone": "123", "source_sequence": 2},
            {"customer_id": "invalid", "name": "", "phone": "123"},
        ], self.key + "-failed")
        original = CustomerMasterRepository.merge

        def fail(repository, *args):
            original(repository, *args)
            raise RuntimeError("Injected failure after all merge SQL, before commit")

        with patch.object(CustomerMasterRepository, "merge", fail), self.assertRaises(RuntimeError):
            self.pipeline()
        self.assertEqual([(r[0], r[1]) for r in self.versions()], [("alice", 1)])
        self.assertEqual(self.query("SELECT status FROM ingestion_batches WHERE batch_id=%s", (batch,)), [("READY",)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM customer_rejects WHERE batch_id=%s", (batch,)), [(0,)])
        self.assertEqual(self.query("SELECT status,inserts,updates FROM etl_run_log WHERE batch_id=%s", (batch,)), [("FAILED", 0, 0)])
        metrics = self.pipeline()
        self.assertEqual((metrics.updates, metrics.rejects), (1, 1))
        self.assertEqual(self.pipeline().extracted, 0)
        self.assertEqual(self.query("SELECT COUNT(*) FROM customer_rejects WHERE batch_id=%s", (batch,)), [(1,)])

    def test_duplicate_and_invalid_accounting(self):
        batch = self.batch([
            {"customer_id": self.customer, "name": "Old", "phone": "123", "source_sequence": 1},
            {"customer_id": self.customer, "name": "New", "phone": "123", "source_sequence": 2},
            {"customer_id": "bad", "name": "", "phone": "123"},
            {"customer_id": "bad", "name": "", "phone": "123"},
        ])
        metrics = self.pipeline()
        self.assertEqual((metrics.extracted, metrics.valid_rows, metrics.rejects), (4, 1, 3))
        self.assertEqual(self.query("SELECT COUNT(*) FROM customer_rejects WHERE batch_id=%s", (batch,)), [(3,)])

    def test_conflicting_duplicates_never_insert(self):
        self.batch([
            {"customer_id": self.customer, "name": "A", "phone": "123"},
            {"customer_id": self.customer, "name": "B", "phone": "123"},
        ])
        self.assertEqual(self.pipeline().rejects, 2)
        self.assertEqual(self.versions(), [])

    def test_database_enforces_one_active_version(self):
        self.batch()
        self.pipeline()
        with self.assertRaises(mysql.connector.IntegrityError):
            self.query("INSERT INTO customer_master (customer_id,name,hash_val,start_date,updated_at,etl_loaded_at) SELECT customer_id,name,hash_val,start_date,updated_at,etl_loaded_at FROM customer_master WHERE customer_id=%s", (self.customer,))
        self.assertEqual(len(self.versions()), 1)

    def test_sealed_batches_cannot_be_modified(self):
        batch = self.batch()
        with self.assertRaises(mysql.connector.Error):
            self.query("INSERT INTO customer_staging (batch_id,customer_id) VALUES (%s,'late')", (batch,))
        with self.assertRaises(mysql.connector.Error):
            self.query("UPDATE customer_staging SET name='changed' WHERE batch_id=%s", (batch,))
        with self.assertRaises(mysql.connector.Error):
            self.query("UPDATE ingestion_batches SET status='OPEN' WHERE batch_id=%s", (batch,))
        self.pipeline()

    def test_concurrent_run_is_rejected(self):
        repository = CustomerMasterRepository(self.settings)
        with repository.locked_connection(), self.assertRaises(RuntimeError):
            self.pipeline()

    def test_late_source_version_does_not_overwrite(self):
        self.batch([{"customer_id": self.customer, "name": "New", "phone": "123", "source_sequence": 3}])
        self.pipeline()
        self.batch(key=self.key + "-late")
        self.assertEqual(self.pipeline().rejects, 1)
        self.assertEqual([(r[0], r[1]) for r in self.versions()], [("new", 1)])

    def test_legacy_hash_upgrade_preserves_history(self):
        self.batch()
        self.pipeline()
        self.query("UPDATE customer_master SET hash_version=1,hash_val=REPEAT('0',64),source_updated_at=NULL WHERE customer_id=%s", (self.customer,))
        self.batch(key=self.key + "-hash-upgrade")
        self.assertEqual(self.pipeline().updates, 0)
        self.assertEqual(len(self.versions()), 1)
        self.assertEqual(self.query("SELECT hash_version FROM customer_master WHERE customer_id=%s", (self.customer,)), [(2,)])

    def test_ingestion_failure_rolls_back_the_batch(self):
        with self.assertRaises(ValueError):
            self.batch([{"customer_id": self.customer, "name": "Alice", "phone": "123", "source_updated_at": "invalid"}])
        self.assertEqual(self.query("SELECT COUNT(*) FROM ingestion_batches WHERE source_key=%s", (self.key,)), [(0,)])
