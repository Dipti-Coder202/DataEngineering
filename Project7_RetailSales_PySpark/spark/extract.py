"""Incrementally extract PostgreSQL retail sales into immutable Bronze Parquet."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from config import Settings
from spark.watermark import Watermark, load_watermark


LOGGER = logging.getLogger("project7.extract")
SOURCE_COLUMNS = (
    "order_id",
    "customer_name",
    "product",
    "category",
    "price",
    "quantity",
    "city",
    "updated_at",
)
SAFE_BATCH_ID = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class ExtractionResult:
    batch_id: str
    ingestion_date: str
    row_count: int
    output_path: Path | None
    maximum_watermark: Watermark | None


def generate_batch_id(now: datetime | None = None) -> str:
    current_time = now or datetime.now(timezone.utc)
    return f"{current_time:%Y%m%dT%H%M%S}_{uuid4().hex[:8]}"


def _validate_batch_id(batch_id: str) -> None:
    if not SAFE_BATCH_ID.fullmatch(batch_id):
        raise ValueError("batch_id may contain only letters, numbers, '_' and '-'")


def build_source_query(table: str, watermark: Watermark | None) -> str:
    selected_columns = ", ".join(SOURCE_COLUMNS)
    query = f'SELECT {selected_columns} FROM public."{table}"'

    if watermark is not None:
        timestamp = watermark.utc_updated_at.isoformat(timespec="microseconds")
        query += (
            f" WHERE (updated_at > TIMESTAMPTZ '{timestamp}')"
            f" OR (updated_at = TIMESTAMPTZ '{timestamp}'"
            f" AND order_id > {watermark.order_id})"
        )

    return f"({query}) AS incremental_source"


def read_incremental_source(
    spark: SparkSession,
    settings: Settings,
    watermark: Watermark | None,
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", build_source_query(settings.db_source_table, watermark))
        .option("user", settings.db_user)
        .option("password", settings.db_password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 1000)
        .load()
    )


def get_maximum_watermark(dataframe: DataFrame) -> Watermark:
    maximum = dataframe.select(
        F.max(F.struct("updated_at", "order_id")).alias("maximum")
    ).select(
        F.unix_micros("maximum.updated_at").alias("updated_at_epoch_us"),
        F.col("maximum.order_id").alias("order_id"),
    ).first()
    if maximum is None or maximum["updated_at_epoch_us"] is None:
        raise ValueError("Cannot calculate a watermark from an empty DataFrame")

    return Watermark(
        # Collect epoch microseconds instead of a naive Python datetime. This
        # avoids the host timezone shifting a correct Spark instant.
        updated_at=datetime.fromtimestamp(
            maximum["updated_at_epoch_us"] / 1_000_000,
            tz=timezone.utc,
        ),
        order_id=maximum["order_id"],
    )


def extract_to_bronze(
    spark: SparkSession,
    settings: Settings,
    *,
    batch_id: str | None = None,
) -> ExtractionResult:
    extraction_time = datetime.now(timezone.utc)
    ingestion_date = extraction_time.date().isoformat()
    resolved_batch_id = batch_id or generate_batch_id(extraction_time)
    _validate_batch_id(resolved_batch_id)

    watermark_file = settings.state_path / "retail_sales_watermark.json"
    watermark = load_watermark(watermark_file)
    if watermark:
        LOGGER.info(
            "Starting incremental extraction: updated_at=%s order_id=%s batch_id=%s",
            watermark.utc_updated_at.isoformat(),
            watermark.order_id,
            resolved_batch_id,
        )
    else:
        LOGGER.info("Starting initial full extraction: batch_id=%s", resolved_batch_id)

    source = read_incremental_source(spark, settings, watermark)
    missing_columns = set(SOURCE_COLUMNS) - set(source.columns)
    if missing_columns:
        raise RuntimeError(f"JDBC source is missing columns: {sorted(missing_columns)}")

    bronze = (
        source.select(*SOURCE_COLUMNS)
        .withColumn("ingested_at", F.lit(extraction_time.isoformat()).cast("timestamp"))
        .withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("batch_id", F.lit(resolved_batch_id))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        row_count = bronze.count()
        if row_count == 0:
            LOGGER.info("No source changes found; no Bronze batch was written")
            return ExtractionResult(
                batch_id=resolved_batch_id,
                ingestion_date=ingestion_date,
                row_count=0,
                output_path=None,
                maximum_watermark=None,
            )

        maximum_watermark = get_maximum_watermark(bronze)

        bronze_root = settings.bronze_path / "retail_sales"
        batch_path = (
            bronze_root
            / f"ingestion_date={ingestion_date}"
            / f"batch_id={resolved_batch_id}"
        )
        if batch_path.exists():
            raise FileExistsError(f"Immutable Bronze batch already exists: {batch_path}")

        (
            bronze.write.mode("append")
            .partitionBy("ingestion_date", "batch_id")
            .parquet(str(bronze_root))
        )
        LOGGER.info(
            "Bronze extraction completed: rows=%s output=%s max_updated_at=%s max_order_id=%s",
            row_count,
            batch_path,
            maximum_watermark.utc_updated_at.isoformat(),
            maximum_watermark.order_id,
        )
        return ExtractionResult(
            batch_id=resolved_batch_id,
            ingestion_date=ingestion_date,
            row_count=row_count,
            output_path=batch_path,
            maximum_watermark=maximum_watermark,
        )
    finally:
        bronze.unpersist()
