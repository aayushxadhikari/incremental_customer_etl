"""Spark session factory."""

from pyspark.sql import SparkSession

from config.settings import Settings

APP_NAME = "Customer_ETL"


def build_spark_session(settings: Settings) -> SparkSession:
    """Create (or reuse) the Spark session wired up with the MySQL JDBC jar."""
    return (
        SparkSession.builder.appName(APP_NAME)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.jars", settings.mysql_jar)
        .getOrCreate()
    )
