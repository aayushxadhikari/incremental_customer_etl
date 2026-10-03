"""Process sealed batches with serialized, atomic SCD2 commits."""

import logging

from pyspark.sql import functions as F

from config.settings import Settings
from src.etl import extract, load, transform
from src.infrastructure.jdbc import JdbcClient
from src.infrastructure.spark import build_spark_session
from src.repositories.customer_master import CustomerMasterRepository
from src.repositories.run_log import RunLogRepository, RunMetrics

logger = logging.getLogger(__name__)


def run(settings: Settings) -> RunMetrics:
    spark = None
    totals = RunMetrics()
    repository = CustomerMasterRepository(settings)
    audit = RunLogRepository(settings)
    try:
        while True:
            run_id = None
            metrics = RunMetrics()
            prepared = None
            try:
                with repository.locked_connection() as conn:
                    audit.recover_abandoned(conn)
                    batch = repository.next_batch(conn)
                    if batch is None:
                        break
                    run_id = audit.start_run(batch["batch_id"])
                    if spark is None:
                        spark = build_spark_session(settings)
                    jdbc = JdbcClient(spark, settings)
                    if batch["lower_id"] is not None:
                        prepared = transform.prepare(extract.extract_staging(
                            jdbc, batch["batch_id"], batch["lower_id"], batch["upper_id"]
                        )).persist()
                        counts = prepared.agg(
                            F.count("*").alias("total"),
                            F.count(F.when(F.col("reason").isNotNull(), 1)).alias("rejects"),
                        ).first()
                        metrics.extracted = metrics.standardized = counts["total"]
                        metrics.rejects = counts["rejects"]
                        metrics.valid_rows = metrics.extracted - metrics.rejects
                        if metrics.extracted:
                            load.write_work(jdbc, prepared, run_id)
                    repository.merge(conn, run_id, batch["batch_id"], metrics)
                logger.info("Committed batch %s, run %s: %s", batch["batch_id"], run_id, metrics)
                for field in totals.__dataclass_fields__:
                    setattr(totals, field, getattr(totals, field) + getattr(metrics, field))
            except Exception:
                logger.exception("Customer ETL failed (run %s)", run_id)
                if run_id is not None:
                    metrics.inserts = metrics.updates = 0
                    try:
                        audit.finish_run(run_id, status="FAILED", message="Batch rolled back; see worker logs", metrics=metrics)
                    except Exception:
                        logger.exception("Could not record failure for run %s", run_id)
                raise
            finally:
                if prepared is not None:
                    prepared.unpersist()
        logger.info("ETL finished: %s", totals)
        return totals
    finally:
        if spark is not None:
            spark.stop()
