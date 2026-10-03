import unittest
from datetime import datetime

from pyspark.sql import SparkSession

from src.etl.transform import add_hash, prepare

SCHEMA = "source_row_id long, batch_id long, customer_id string, name string, email string, phone string, address string, source_updated_at timestamp, source_sequence long"
TIME = datetime(2026, 10, 3)


def row(number, customer="c1", name="Alice", email=None, phone="123", address=None, sequence=1, timestamp=TIME):
    return (number, 1, customer, name, email, phone, address, timestamp, sequence)


class TransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("transform-tests")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def prepared(self, rows):
        return prepare(self.spark.createDataFrame(rows, SCHEMA)).orderBy("source_row_id").collect()

    def test_empty_required_fields_and_detailed_reasons(self):
        result = self.prepared([row(1, customer="  ", name="  ", phone=None)])
        for field in ("customer_id", "name", "phone"):
            self.assertIn(f"Missing {field}", result[0].reason)

    def test_normalization(self):
        result = self.prepared([row(1, customer=" c1 ", name=" Alice ", email=" A@EXAMPLE.COM ", phone=" 123 ")])[0]
        self.assertEqual((result.customer_id, result.name, result.email, result.phone), ("c1", "alice", "a@example.com", "123"))
        self.assertIsNone(result.reason)

    def test_oversize_optional_field_is_rejected_and_preserved(self):
        result = self.prepared([row(1, address="x" * 513)])[0]
        self.assertIn("address exceeds 512", result.reason)
        self.assertEqual(len(result.address), 513)

    def test_duplicate_invalid_rows_are_preserved(self):
        result = self.prepared([row(1, phone=None), row(2, phone=None)])
        self.assertEqual(len(result), 2)
        self.assertTrue(all(r.reason for r in result))

    def test_latest_sequence_wins(self):
        result = self.prepared([row(1, name="Old", sequence=1), row(2, name="New", sequence=2)])
        self.assertIn("Superseded", result[0].reason)
        self.assertIsNone(result[1].reason)

    def test_source_time_precedes_sequence(self):
        result = self.prepared([row(1, sequence=100, timestamp=datetime(2026, 10, 2)), row(2, sequence=1)])
        self.assertIsNone(result[1].reason)

    def test_conflicting_winning_ties_are_all_rejected(self):
        result = self.prepared([row(1, name="A"), row(2, name="B")])
        self.assertTrue(all("Ambiguous" in r.reason for r in result))

    def test_identical_ties_choose_one(self):
        result = self.prepared([row(1), row(2)])
        self.assertIsNone(result[0].reason)
        self.assertIn("Superseded", result[1].reason)

    def test_invalid_newer_row_does_not_supersede_valid(self):
        result = self.prepared([row(1), row(2, phone=None, sequence=2)])
        self.assertIsNone(result[0].reason)
        self.assertIn("Missing phone", result[1].reason)

    def test_hash_distinguishes_delimiters_and_nulls(self):
        frame = self.spark.createDataFrame([
            row(1, name="a|b", email="c"), row(2, name="a", email="b|c"),
            row(3, email=None), row(4, email=""),
        ], SCHEMA)
        hashes = [r.hash_val for r in add_hash(frame).orderBy("source_row_id").collect()]
        self.assertEqual(len(set(hashes)), 4)

    def test_missing_source_metadata_rejected(self):
        result = self.prepared([row(1, timestamp=None, sequence=None)])[0]
        self.assertIn("source_updated_at", result.reason)
        self.assertIn("source_sequence", result.reason)
