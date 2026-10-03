"""Atomically ingest a CSV and seal it. Source keys make ingestion replay-safe."""

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path

from config.settings import Settings
from src.infrastructure.db import get_connection

FIELDS = ("customer_id", "name", "email", "phone", "address")


def parse_time(value):
    if not value:
        return None
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("source_updated_at must include a timezone (e.g. 2026-10-03T00:00:00Z)")
    return timestamp.astimezone(timezone.utc).replace(tzinfo=None)


def ingest(settings: Settings, path: Path, source_key: str) -> int:
    if not source_key or len(source_key) > 255:
        raise ValueError("source_key must contain 1-255 characters")
    with get_connection(settings) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT batch_id FROM ingestion_batches WHERE source_key=%s", (source_key,))
        existing = cursor.fetchone()
        if existing:
            return existing[0]
        cursor.execute("INSERT INTO ingestion_batches (source_key) VALUES (%s)", (source_key,))
        batch_id = cursor.lastrowid
        with path.open(newline="", encoding="utf-8") as stream:
            rows = csv.DictReader(stream)
            required = {*FIELDS, "source_updated_at", "source_sequence"}
            if not required.issubset(rows.fieldnames or []):
                raise ValueError(f"CSV requires columns: {', '.join(sorted(required))}")
            chunk = []
            for row in rows:
                chunk.append((batch_id, *[row[f] or None for f in FIELDS],
                              parse_time(row["source_updated_at"]),
                              int(row["source_sequence"]) if row["source_sequence"] else None))
                if len(chunk) == 1000:
                    _insert(cursor, chunk)
                    chunk.clear()
            if chunk:
                _insert(cursor, chunk)
        cursor.execute("UPDATE ingestion_batches SET status='READY' WHERE batch_id=%s", (batch_id,))
        return batch_id


def _insert(cursor, rows):
    cursor.executemany(
        "INSERT INTO customer_staging (batch_id,customer_id,name,email,phone,address,source_updated_at,source_sequence) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", rows,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--source-key", required=True, help="Stable unique source delivery ID; reuse only for the same delivery")
    args = parser.parse_args()
    print(f"Batch {ingest(Settings.from_env(), args.csv, args.source_key)} ready (or already ingested)")


if __name__ == "__main__":
    main()
