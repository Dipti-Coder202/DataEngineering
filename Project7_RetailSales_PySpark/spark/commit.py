"""Quality-gated publication of the incremental extraction watermark."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pyspark.sql import SparkSession

from config import Settings
from spark.extract import get_maximum_watermark
from spark.quality import run_data_quality_checks
from spark.transform import read_bronze_batch
from spark.watermark import Watermark, save_watermark


LOGGER = logging.getLogger("project7.commit")


@dataclass(frozen=True)
class WatermarkCommitResult:
    batch_id: str
    watermark: Watermark
    silver_count: int
    silver_revenue: str


def commit_batch_watermark(
    spark: SparkSession,
    settings: Settings,
    *,
    batch_id: str,
) -> WatermarkCommitResult:
    LOGGER.info("Starting watermark quality gate: batch_id=%s", batch_id)
    bronze_batch = read_bronze_batch(spark, settings, batch_id)
    if bronze_batch.limit(1).count() == 0:
        raise ValueError(f"Cannot commit an empty or missing Bronze batch: {batch_id}")
    candidate = get_maximum_watermark(bronze_batch)

    quality = run_data_quality_checks(
        spark, settings, batch_id=batch_id, write_report=False
    )

    state_path = settings.state_path / "retail_sales_watermark.json"
    save_watermark(state_path, candidate, batch_id=batch_id)
    LOGGER.info(
        "Watermark committed: updated_at=%s order_id=%s batch_id=%s",
        candidate.utc_updated_at.isoformat(),
        candidate.order_id,
        batch_id,
    )
    return WatermarkCommitResult(
        batch_id=batch_id,
        watermark=candidate,
        silver_count=quality.silver_count,
        silver_revenue=quality.silver_revenue,
    )
