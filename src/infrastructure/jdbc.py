"""Thin wrapper around Spark's JDBC reader/writer.

Centralises the connection options so individual ETL stages only deal with
table names and DataFrames instead of repeating url/driver/credentials.
"""

from pyspark.sql import DataFrame, SparkSession

from config.settings import Settings

DRIVER = "com.mysql.cj.jdbc.Driver"


class JdbcClient:
    def __init__(self, spark: SparkSession, settings: Settings):
        self._spark = spark
        self._settings = settings

    def _with_connection_options(self, reader_or_writer):
        return (
            reader_or_writer.option("url", self._settings.jdbc_url)
            .option("driver", DRIVER)
            .option("user", self._settings.mysql_user)
            .option("password", self._settings.mysql_password)
        )

    def read(self, dbtable: str, *, lower: int | None = None, upper: int | None = None) -> DataFrame:
        """Read a table, or a parenthesised sub-query aliased as a table."""
        reader = self._spark.read.format("jdbc").option("dbtable", dbtable)
        reader = reader.option("fetchsize", self._settings.jdbc_fetch_size)
        if lower is not None and upper is not None and upper > lower and self._settings.jdbc_partitions > 1:
            reader = (reader.option("partitionColumn", "source_row_id")
                      .option("lowerBound", lower).option("upperBound", upper + 1)
                      .option("numPartitions", self._settings.jdbc_partitions))
        return self._with_connection_options(reader).load()

    def write(self, df: DataFrame, table: str, mode: str = "append") -> None:
        writer = df.write.format("jdbc").option("dbtable", table).mode(mode)
        writer = writer.option("batchsize", self._settings.jdbc_batch_size)
        self._with_connection_options(writer).save()
