"""Persist prepared rows; the repository commits SCD changes atomically."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from src.infrastructure.jdbc import JdbcClient

WORK_COLUMNS = [
    "run_id", "source_row_id", "batch_id", "customer_id", "name", "email",
    "phone", "address", "source_updated_at", "source_sequence", "hash_val", "reason",
]


def write_work(jdbc: JdbcClient, prepared: DataFrame, run_id: int) -> None:
    jdbc.write(prepared.withColumn("run_id", lit(run_id)).select(*WORK_COLUMNS), "customer_work")
