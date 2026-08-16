"""Cross-layer data-quality checks and batch-disposition reporting."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg2
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from config import Settings
from spark.load_postgres import GOLD_TARGETS
from spark.transform import clean_and_classify, read_bronze_batch


LOGGER = logging.getLogger("project7.quality")


@dataclass(frozen=True)
class QualityResult:
    batch_id: str
    bronze_count: int
    valid_batch_count: int
    quarantined_count: int
    silver_count: int
    silver_revenue: str
    checks_passed: int
    status: str = "passed"


def _postgres_metrics(settings: Settings) -> tuple[dict[str, int], Decimal]:
    counts: dict[str, int] = {}
    with psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=10,
    ) as connection:
        with connection.cursor() as cursor:
            for target_table, _ in GOLD_TARGETS.values():
                cursor.execute(f'SELECT COUNT(*) FROM public."{target_table}"')
                counts[target_table] = cursor.fetchone()[0]
            cursor.execute(
                "SELECT total_revenue FROM public.analytics_overall_sales "
                "WHERE metric_scope = 'all_retail_sales'"
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("PostgreSQL overall analytics row is missing")
    return counts, row[0]


def _write_report(settings: Settings, result: QualityResult) -> Path:
    report_path = settings.project_root / "logs" / f"quality_{result.batch_id}.json"
    temporary = report_path.parent / f".{report_path.name}.tmp_{uuid4().hex}"
    payload = {
        **asdict(result),
        "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    try:
        with temporary.open("w", encoding="utf-8") as report_file:
            json.dump(payload, report_file, indent=2, sort_keys=True)
            report_file.write("\n")
            report_file.flush()
            os.fsync(report_file.fileno())
        os.replace(temporary, report_path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return report_path


def run_data_quality_checks(
    spark: SparkSession,
    settings: Settings,
    *,
    batch_id: str,
    write_report: bool = True,
) -> QualityResult:
    LOGGER.info("Starting cross-layer quality checks: batch_id=%s", batch_id)
    bronze = read_bronze_batch(spark, settings, batch_id).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    valid = None
    rejected = None
    try:
        bronze_count = bronze.count()
        if bronze_count == 0:
            raise ValueError("Quality checks require a non-empty Bronze batch")
        valid, rejected = clean_and_classify(bronze)
        valid = valid.persist(StorageLevel.MEMORY_AND_DISK)
        rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
        valid_count = valid.count()
        rejected_count = rejected.count()
        if valid_count + rejected_count != bronze_count:
            raise ValueError("Valid and rejected counts do not reconcile to Bronze")

        silver_path = settings.silver_path / "retail_sales"
        silver = spark.read.parquet(str(silver_path)).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        try:
            silver_count = silver.count()
            if silver_count != silver.select("order_id").distinct().count():
                raise ValueError("Silver contains duplicate order IDs")
            invalid_silver_count = silver.filter(
                F.col("order_id").isNull()
                | F.col("customer_name").isNull() | (F.trim("customer_name") == "")
                | F.col("product").isNull() | (F.trim("product") == "")
                | F.col("category").isNull() | (F.trim("category") == "")
                | F.col("city").isNull() | (F.trim("city") == "")
                | F.col("price").isNull() | (F.col("price") < 0)
                | F.col("quantity").isNull() | (F.col("quantity") <= 0)
                | F.col("source_updated_at").isNull()
                | (F.col("total_amount") != F.col("price") * F.col("quantity"))
            ).count()
            if invalid_silver_count:
                raise ValueError(f"Silver contains {invalid_silver_count} invalid rows")

            lineage_mismatches = (
                valid.select(
                    "order_id",
                    F.col("source_updated_at").alias("batch_updated_at"),
                )
                .join(
                    silver.select("order_id", "source_updated_at"),
                    on="order_id",
                    how="left",
                )
                .filter(
                    F.col("source_updated_at").isNull()
                    | (F.col("source_updated_at") != F.col("batch_updated_at"))
                )
                .count()
            )
            if lineage_mismatches:
                raise ValueError(f"Silver lineage mismatches: {lineage_mismatches}")

            quarantine_root = settings.quarantine_path / "retail_sales"
            if rejected_count:
                if not quarantine_root.exists():
                    raise FileNotFoundError("Rejected rows exist but quarantine is missing")
                quarantined = spark.read.parquet(str(quarantine_root)).filter(
                    F.col("batch_id") == batch_id
                )
                fingerprint_mismatches = (
                    rejected.select("record_fingerprint")
                    .exceptAll(quarantined.select("record_fingerprint"))
                    .count()
                    + quarantined.select("record_fingerprint")
                    .exceptAll(rejected.select("record_fingerprint"))
                    .count()
                )
                if fingerprint_mismatches:
                    raise ValueError(
                        f"Quarantine fingerprint mismatches: {fingerprint_mismatches}"
                    )
                actual_quarantined_count = quarantined.count()
            else:
                actual_quarantined_count = 0
            if actual_quarantined_count != rejected_count:
                raise ValueError("Rejected and quarantine counts do not match")

            silver_revenue = silver.agg(
                F.sum("total_amount").alias("revenue")
            ).first()["revenue"]
            gold_counts: dict[str, int] = {}
            gold_revenues: dict[str, Decimal | None] = {}
            for dataset_name, (target_table, _) in GOLD_TARGETS.items():
                gold = spark.read.parquet(str(settings.gold_path / dataset_name))
                # Produce both metrics in one Spark action. Previously the four
                # dimensional datasets were scanned again for their revenue.
                metrics = gold.agg(
                    F.count(F.lit(1)).alias("row_count"),
                    F.sum("total_revenue").alias("revenue"),
                ).first()
                gold_counts[target_table] = metrics["row_count"]
                gold_revenues[dataset_name] = metrics["revenue"]
            dimensional_names = (
                "category_sales", "city_sales", "product_sales", "customer_sales"
            )
            for name in dimensional_names:
                if gold_revenues[name] != silver_revenue:
                    raise ValueError(f"Gold revenue mismatch for {name}")

            postgres_counts, postgres_revenue = _postgres_metrics(settings)
            if postgres_counts != gold_counts:
                raise ValueError("Gold and PostgreSQL row counts do not match")
            if postgres_revenue != Decimal(silver_revenue):
                raise ValueError("Silver and PostgreSQL revenue do not match")

            result = QualityResult(
                batch_id=batch_id,
                bronze_count=bronze_count,
                valid_batch_count=valid_count,
                quarantined_count=actual_quarantined_count,
                silver_count=silver_count,
                silver_revenue=str(silver_revenue),
                checks_passed=10,
            )
            if write_report:
                report_path = _write_report(settings, result)
                LOGGER.info("Quality report written: %s", report_path)
            LOGGER.info("All cross-layer quality checks passed: %s", result)
            return result
        finally:
            silver.unpersist()
    finally:
        bronze.unpersist()
        if valid is not None:
            valid.unpersist()
        if rejected is not None:
            rejected.unpersist()
