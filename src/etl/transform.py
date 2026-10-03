"""Normalization, validation, structured hashing and deterministic deduplication."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HASH_COLUMNS = ("customer_id", "name", "email", "phone", "address")
LENGTHS = {"customer_id": 64, "name": 255, "email": 255, "phone": 64, "address": 512}
HASH_VERSION = 2


def standardize(df: DataFrame) -> DataFrame:
    for field in HASH_COLUMNS:
        df = df.withColumn(field, F.trim(F.col(field)))
    return df.withColumn("name", F.lower("name")).withColumn("email", F.lower("email"))


def add_hash(df: DataFrame) -> DataFrame:
    payload = F.to_json(F.struct(*[F.col(c) for c in HASH_COLUMNS]), {"ignoreNullFields": "false"})
    return df.withColumn("hash_val", F.sha2(payload, 256))


def validate(df: DataFrame) -> DataFrame:
    reasons = []
    for field in ("customer_id", "name", "phone"):
        reasons.append(F.when(F.col(field).isNull() | (F.length(field) == 0), F.lit(f"Missing {field}")))
    for field, limit in LENGTHS.items():
        reasons.append(F.when(F.length(field) > limit, F.lit(f"{field} exceeds {limit} characters")))
    reasons.append(F.when(F.col("source_updated_at").isNull(), F.lit("Missing source_updated_at")))
    reasons.append(F.when(F.col("source_sequence").isNull(), F.lit("Missing source_sequence")))
    reason = F.concat_ws("; ", *reasons)
    return df.withColumn("reason", F.when(F.length(reason) > 0, reason))


def prepare(df: DataFrame) -> DataFrame:
    """Newest source time then sequence wins; reject conflicting winning ties.

    Identical ties use source_row_id. Invalid input never supersedes valid rows.
    Every losing/invalid row is retained for auditing, including duplicates.
    """
    validated = validate(add_hash(standardize(df)))
    invalid = validated.filter(F.col("reason").isNotNull())
    valid = validated.filter(F.col("reason").isNull())
    precedence = Window.partitionBy("customer_id").orderBy(
        F.col("source_updated_at").desc(), F.col("source_sequence").desc()
    )
    ranked = valid.withColumn("_rank", F.dense_rank().over(precedence))
    conflicts = ranked.filter(F.col("_rank") == 1).groupBy("customer_id").agg(
        F.countDistinct("hash_val").alias("_variants")
    )
    tie = Window.partitionBy("customer_id").orderBy(
        F.col("source_updated_at").desc(), F.col("source_sequence").desc(), F.col("source_row_id").asc()
    )
    resolved = ranked.join(conflicts, "customer_id").withColumn("_winner", F.row_number().over(tie))
    resolved = resolved.withColumn(
        "reason",
        F.when((F.col("_rank") == 1) & (F.col("_variants") > 1), F.lit("Ambiguous duplicate customer_id"))
        .when(F.col("_winner") > 1, F.lit("Superseded duplicate customer_id")),
    ).drop("_rank", "_variants", "_winner")
    return resolved.select(*validated.columns).unionByName(invalid)
