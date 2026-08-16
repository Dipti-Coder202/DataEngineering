"""Reusable retail transformations independent of storage infrastructure."""

from pyspark.sql import DataFrame, Window, functions as F


def clean_retail_sales(source: DataFrame) -> tuple[DataFrame, DataFrame]:
    cleaned = (
        source.select(
            F.col("order_id").cast("long").alias("order_id"),
            F.trim("customer_name").alias("customer_name"),
            F.trim("product").alias("product"),
            F.trim("category").alias("category"),
            F.col("price").cast("decimal(12,2)").alias("price"),
            F.col("quantity").cast("integer").alias("quantity"),
            F.trim("city").alias("city"),
            F.col("order_date").cast("date").alias("order_date"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
        )
        .withColumn("total_amount", (F.col("price") * F.col("quantity")).cast("decimal(18,2)"))
    )
    reasons = F.filter(F.array(
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
        F.when(F.col("customer_name").isNull() | (F.col("customer_name") == ""), F.lit("blank_customer")),
        F.when(F.col("product").isNull() | (F.col("product") == ""), F.lit("blank_product")),
        F.when(F.col("price").isNull() | (F.col("price") < 0), F.lit("invalid_price")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
        F.when(F.col("order_date").isNull(), F.lit("missing_order_date")),
        F.when(F.col("updated_at").isNull(), F.lit("missing_updated_at")),
    ), lambda reason: reason.isNotNull())
    classified = cleaned.withColumn("rejection_reasons", reasons)
    valid = classified.filter(F.size("rejection_reasons") == 0).drop("rejection_reasons")
    rejected = classified.filter(F.size("rejection_reasons") > 0)
    return valid, rejected


def latest_by_order(dataframe: DataFrame) -> DataFrame:
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    return dataframe.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def daily_sales(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("order_date", "category").agg(
        F.sum("total_amount").cast("decimal(20,2)").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
    )
