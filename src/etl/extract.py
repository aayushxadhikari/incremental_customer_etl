"""Read only a sealed batch, with bounded optional JDBC parallelism."""

from pyspark.sql import DataFrame

from src.infrastructure.jdbc import JdbcClient


def extract_staging(jdbc: JdbcClient, batch_id: int, lower: int, upper: int) -> DataFrame:
    query = f"(SELECT * FROM customer_staging WHERE batch_id={int(batch_id)}) AS batch_rows"
    return jdbc.read(query, lower=lower, upper=upper)
