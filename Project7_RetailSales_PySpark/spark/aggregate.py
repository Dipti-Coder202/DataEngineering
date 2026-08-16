"""Create business-ready Gold aggregations from the trusted Silver snapshot."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from config import Settings
from spark.io_utils import publish_snapshots


LOGGER = logging.getLogger("project7.aggregate")


@dataclass(frozen=True)
class AggregationResult:
    publication_id: str
    silver_count: int
    silver_revenue: str
    dataset_counts: dict[str, int]
    output_paths: dict[str, Path]


def _with_common_metrics(
    silver: DataFrame,
    group_columns: list[str],
    refreshed_at: str,
) -> DataFrame:
    grouped = silver.groupBy(*group_columns) if group_columns else silver.groupBy()
    return grouped.agg(
        F.sum("total_amount").cast("decimal(20,2)").alias("total_revenue"),
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.sum("quantity").cast("long").alias("units_sold"),
        F.avg("total_amount").cast("decimal(20,2)").alias("average_order_value"),
        F.countDistinct("customer_name").cast("long").alias("distinct_customers"),
    ).withColumn("last_refreshed_at", F.lit(refreshed_at).cast("timestamp"))


def build_gold_datasets(silver: DataFrame) -> dict[str, DataFrame]:
    refreshed_at = datetime.now(timezone.utc).isoformat()

    overall = (
        _with_common_metrics(silver, [], refreshed_at)
        .withColumn("metric_scope", F.lit("all_retail_sales"))
        .select(
            "metric_scope",
            "total_revenue",
            "order_count",
            "units_sold",
            "average_order_value",
            "distinct_customers",
            "last_refreshed_at",
        )
    )
    category = _with_common_metrics(silver, ["category"], refreshed_at)
    city = _with_common_metrics(silver, ["city"], refreshed_at)
    product = _with_common_metrics(silver, ["product", "category"], refreshed_at)
    customer = (
        _with_common_metrics(silver, ["customer_name"], refreshed_at)
        .join(
            silver.groupBy("customer_name").agg(
                F.countDistinct("product").cast("long").alias("distinct_products")
            ),
            on="customer_name",
            how="inner",
        )
    )
    product_rank = Window.orderBy(
        F.col("total_revenue").desc(), F.col("product").asc()
    )
    top_products = product.withColumn(
        "sales_rank", F.dense_rank().over(product_rank)
    ).select(
        "sales_rank",
        "product",
        "category",
        "total_revenue",
        "order_count",
        "units_sold",
        "average_order_value",
        "last_refreshed_at",
    )

    return {
        "overall_sales": overall,
        "category_sales": category,
        "city_sales": city,
        "product_sales": product,
        "top_products": top_products,
        "customer_sales": customer,
    }


def create_gold(
    spark: SparkSession,
    settings: Settings,
) -> AggregationResult:
    silver_path = settings.silver_path / "retail_sales"
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver snapshot does not exist: {silver_path}")

    publication_id = f"gold_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid4().hex[:8]}"
    LOGGER.info("Starting Gold aggregation: publication_id=%s", publication_id)
    silver = spark.read.parquet(str(silver_path)).persist(StorageLevel.MEMORY_AND_DISK)
    gold_datasets: dict[str, DataFrame] = {}
    try:
        silver_count = silver.count()
        if silver_count == 0:
            raise ValueError("Silver snapshot is empty")
        silver_revenue = silver.agg(F.sum("total_amount").alias("revenue")).first()[
            "revenue"
        ]

        gold_datasets = {
            name: dataframe.persist(StorageLevel.MEMORY_AND_DISK)
            for name, dataframe in build_gold_datasets(silver).items()
        }
        dataset_counts = {
            name: dataframe.count() for name, dataframe in gold_datasets.items()
        }

        # Each dimensional aggregation must reconcile to the trusted Silver total.
        for name in ("category_sales", "city_sales", "product_sales", "customer_sales"):
            revenue = gold_datasets[name].agg(
                F.sum("total_revenue").alias("revenue")
            ).first()["revenue"]
            if revenue != silver_revenue:
                raise ValueError(
                    f"Gold revenue mismatch for {name}: {revenue} != {silver_revenue}"
                )

        output_paths = {
            name: settings.gold_path / name for name in gold_datasets
        }
        publish_snapshots(
            {output_paths[name]: dataframe for name, dataframe in gold_datasets.items()},
            publication_id,
        )
        LOGGER.info(
            "Gold aggregation completed: silver_rows=%s revenue=%s datasets=%s",
            silver_count,
            silver_revenue,
            dataset_counts,
        )
        return AggregationResult(
            publication_id=publication_id,
            silver_count=silver_count,
            silver_revenue=str(silver_revenue),
            dataset_counts=dataset_counts,
            output_paths=output_paths,
        )
    finally:
        silver.unpersist()
        for dataframe in gold_datasets.values():
            dataframe.unpersist()
