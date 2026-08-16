"""Clean, validate, deduplicate, and publish the Silver retail-sales snapshot."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from config import Settings
from spark.extract import SAFE_BATCH_ID, _validate_batch_id
from spark.io_utils import publish_snapshot


LOGGER = logging.getLogger("project7.transform")
SILVER_COLUMNS = (
    "order_id",
    "customer_name",
    "product",
    "category",
    "price",
    "quantity",
    "city",
    "total_amount",
    "source_updated_at",
    "processed_at",
    "source_ingested_at",
    "source_ingestion_date",
    "source_batch_id",
)


@dataclass(frozen=True)
class TransformationResult:
    batch_id: str
    input_count: int
    valid_batch_count: int
    rejected_count: int
    duplicate_count: int
    silver_count: int
    silver_path: Path
    quarantine_path: Path | None


def read_bronze_batch(
    spark: SparkSession,
    settings: Settings,
    batch_id: str,
) -> DataFrame:
    _validate_batch_id(batch_id)
    bronze_root = settings.bronze_path / "retail_sales"
    if not bronze_root.exists():
        raise FileNotFoundError(f"Bronze dataset does not exist: {bronze_root}")

    # batch_id is a partition column, so Spark can prune unrelated batch paths.
    return spark.read.parquet(str(bronze_root)).filter(F.col("batch_id") == batch_id)


def clean_and_classify(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return valid and rejected rows after schema enforcement and cleaning."""
    processed_at = datetime.now(timezone.utc).isoformat()
    cleaned = bronze.select(
        F.col("order_id").cast("long").alias("order_id"),
        F.trim(F.col("customer_name").cast("string")).alias("customer_name"),
        F.trim(F.col("product").cast("string")).alias("product"),
        F.trim(F.col("category").cast("string")).alias("category"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("quantity").cast("integer").alias("quantity"),
        F.trim(F.col("city").cast("string")).alias("city"),
        F.col("updated_at").cast("timestamp").alias("source_updated_at"),
        F.lit(processed_at).cast("timestamp").alias("processed_at"),
        F.col("ingested_at").cast("timestamp").alias("source_ingested_at"),
        F.col("ingestion_date").cast("date").alias("source_ingestion_date"),
        F.col("batch_id").cast("string").alias("source_batch_id"),
    ).withColumn(
        "total_amount",
        (F.col("price") * F.col("quantity")).cast("decimal(18,2)"),
    )

    rejection_reasons = F.filter(
        F.array(
            F.when(F.col("order_id").isNull(), F.lit("order_id_is_null")),
            F.when(F.col("customer_name").isNull() | (F.col("customer_name") == ""), F.lit("customer_name_is_blank")),
            F.when(F.col("product").isNull() | (F.col("product") == ""), F.lit("product_is_blank")),
            F.when(F.col("category").isNull() | (F.col("category") == ""), F.lit("category_is_blank")),
            F.when(F.col("city").isNull() | (F.col("city") == ""), F.lit("city_is_blank")),
            F.when(F.col("price").isNull(), F.lit("price_is_null")),
            F.when(F.col("price") < 0, F.lit("price_is_negative")),
            F.when(F.col("quantity").isNull(), F.lit("quantity_is_null")),
            F.when(F.col("quantity") <= 0, F.lit("quantity_is_not_positive")),
            F.when(F.col("source_updated_at").isNull(), F.lit("updated_at_is_null")),
        ),
        lambda reason: reason.isNotNull(),
    )
    classified = cleaned.withColumn("rejection_reasons", rejection_reasons)
    fingerprint_columns = [
        "order_id", "customer_name", "product", "category", "price",
        "quantity", "city", "source_updated_at", "source_ingested_at",
        "source_ingestion_date", "source_batch_id",
    ]
    classified = classified.withColumn(
        "record_fingerprint",
        F.sha2(F.to_json(F.struct(*fingerprint_columns)), 256),
    )

    valid = classified.filter(F.size("rejection_reasons") == 0).drop(
        "rejection_reasons", "record_fingerprint"
    )
    rejected = (
        classified.filter(F.size("rejection_reasons") > 0)
        .withColumn("quarantined_at", F.current_timestamp())
    )
    return valid, rejected


def deduplicate_latest(dataframe: DataFrame) -> DataFrame:
    latest_record = Window.partitionBy("order_id").orderBy(
        F.col("source_updated_at").desc(),
        F.col("source_ingested_at").desc(),
        F.col("source_batch_id").desc(),
    )
    return (
        dataframe.withColumn("_record_rank", F.row_number().over(latest_record))
        .filter(F.col("_record_rank") == 1)
        .drop("_record_rank")
        .select(*SILVER_COLUMNS)
    )


def _write_quarantine(
    rejected: DataFrame,
    settings: Settings,
    batch_id: str,
    ingestion_date: str,
) -> Path:
    quarantine_root = settings.quarantine_path / "retail_sales"
    batch_path = (
        quarantine_root
        / f"ingestion_date={ingestion_date}"
        / f"batch_id={batch_id}"
    )
    if batch_path.exists():
        existing = rejected.sparkSession.read.parquet(str(batch_path))
        expected_fingerprints = rejected.select("record_fingerprint")
        existing_fingerprints = existing.select("record_fingerprint")
        mismatch_count = (
            expected_fingerprints.exceptAll(existing_fingerprints).count()
            + existing_fingerprints.exceptAll(expected_fingerprints).count()
        )
        if mismatch_count:
            raise FileExistsError(
                f"Quarantine batch exists with different records: {batch_path}"
            )
        LOGGER.info("Reusing identical immutable quarantine batch: %s", batch_path)
        return batch_path

    (
        rejected.withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("batch_id", F.lit(batch_id))
        .write.mode("append")
        .partitionBy("ingestion_date", "batch_id")
        .parquet(str(quarantine_root))
    )
    return batch_path


def transform_to_silver(
    spark: SparkSession,
    settings: Settings,
    *,
    batch_id: str,
) -> TransformationResult:
    if not SAFE_BATCH_ID.fullmatch(batch_id):
        raise ValueError("Invalid batch_id")

    LOGGER.info("Starting Silver transformation: batch_id=%s", batch_id)
    bronze = read_bronze_batch(spark, settings, batch_id).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    valid_batch: DataFrame | None = None
    rejected: DataFrame | None = None
    merged: DataFrame | None = None
    try:
        input_count = bronze.count()
        if input_count == 0:
            raise ValueError(f"Bronze batch contains no records: {batch_id}")

        ingestion_date_bounds = bronze.agg(
            F.min("ingestion_date").alias("minimum"),
            F.max("ingestion_date").alias("maximum"),
        ).first()
        if ingestion_date_bounds["minimum"] != ingestion_date_bounds["maximum"]:
            raise ValueError("A Bronze batch must have exactly one ingestion_date")
        ingestion_date = ingestion_date_bounds["minimum"].isoformat()

        valid_batch, rejected = clean_and_classify(bronze)
        valid_batch = valid_batch.persist(StorageLevel.MEMORY_AND_DISK)
        rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
        valid_batch_count = valid_batch.count()
        rejected_count = rejected.count()

        deduplicated_batch = deduplicate_latest(valid_batch)
        deduplicated_batch_count = deduplicated_batch.count()
        duplicate_count = valid_batch_count - deduplicated_batch_count

        silver_target = settings.silver_path / "retail_sales"
        if silver_target.exists():
            existing = spark.read.parquet(str(silver_target)).select(*SILVER_COLUMNS)
            merged = deduplicate_latest(existing.unionByName(deduplicated_batch))
        else:
            merged = deduplicated_batch
        merged = merged.persist(StorageLevel.MEMORY_AND_DISK)
        silver_count = merged.count()

        quarantine_path = None
        if rejected_count:
            quarantine_path = _write_quarantine(
                rejected, settings, batch_id, ingestion_date
            )

        # One file is appropriate for this tiny learning snapshot and avoids a
        # small-file problem. Large production snapshots need more partitions.
        publish_snapshot(merged, silver_target, batch_id)
        LOGGER.info(
            "Silver transformation completed: input=%s valid=%s rejected=%s duplicates=%s silver=%s",
            input_count,
            valid_batch_count,
            rejected_count,
            duplicate_count,
            silver_count,
        )
        return TransformationResult(
            batch_id=batch_id,
            input_count=input_count,
            valid_batch_count=valid_batch_count,
            rejected_count=rejected_count,
            duplicate_count=duplicate_count,
            silver_count=silver_count,
            silver_path=silver_target,
            quarantine_path=quarantine_path,
        )
    finally:
        bronze.unpersist()
        if valid_batch is not None:
            valid_batch.unpersist()
        if rejected is not None:
            rejected.unpersist()
        if merged is not None:
            merged.unpersist()
