"""Reusable Spark transformations shared by Lakeflow and local tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = (
    "order_id",
    "customer_id",
    "product_id",
    "order_timestamp",
    "quantity",
    "unit_price",
)


def with_quality_columns(df: DataFrame) -> DataFrame:
    """Normalize retail events and attach deterministic quality metadata."""
    missing_required = F.lit(False)
    for column in REQUIRED_COLUMNS:
        missing_required = missing_required | F.col(column).isNull()

    return (
        df.withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .withColumn(
            "quality_reason",
            F.when(missing_required, F.lit("missing_required_field"))
            .when(F.col("quantity") <= 0, F.lit("non_positive_quantity"))
            .when(F.col("unit_price") < 0, F.lit("negative_unit_price"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("is_valid", F.col("quality_reason").isNull())
        .withColumn(
            "total_amount",
            F.round(F.col("quantity") * F.col("unit_price"), 2).cast("decimal(14,2)"),
        )
    )


def valid_orders(df: DataFrame) -> DataFrame:
    return with_quality_columns(df).filter(F.col("is_valid"))


def quarantined_orders(df: DataFrame) -> DataFrame:
    return with_quality_columns(df).filter(~F.col("is_valid"))


def daily_sales(df: DataFrame) -> DataFrame:
    """Create an interview-friendly daily aggregate from valid orders."""
    return (
        df.withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("order_date", "store_id", "category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("gross_sales"),
        )
    )
