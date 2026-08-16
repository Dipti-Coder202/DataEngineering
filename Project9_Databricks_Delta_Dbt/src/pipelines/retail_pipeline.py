"""Lakeflow Declarative Pipeline implementing Bronze, Silver, and Gold."""

import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Lakeflow executes source files as notebooks, where ``__file__`` is undefined.
# The bundle injects its uploaded root explicitly so shared modules are importable.
sys.path.insert(0, spark.conf.get("project9.bundle_root"))

from src.transformations import daily_sales, quarantined_orders, valid_orders

SOURCE_PATH = spark.conf.get("project9.source_path")

ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_name", T.StringType()),
        T.StructField("customer_email", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("store_id", T.StringType()),
        T.StructField("order_timestamp", T.StringType()),
        T.StructField("updated_at", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("operation", T.StringType()),
    ]
)


@dp.table(name="bronze_orders", comment="Raw retail order CDC events from Auto Loader")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(ORDER_SCHEMA)
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(name="silver_order_changes", comment="Validated and standardized order changes")
@dp.expect_or_drop("valid_operation", "operation IN ('INSERT', 'UPDATE', 'DELETE')")
def silver_order_changes():
    return valid_orders(spark.readStream.table("bronze_orders")).drop("is_valid", "quality_reason")


@dp.table(name="quarantined_orders", comment="Rejected records retained with a quality reason")
def quarantine():
    return quarantined_orders(spark.readStream.table("bronze_orders"))


dp.create_streaming_table(
    name="silver_orders_history",
    comment="Type 2 history maintained from the validated CDC stream",
)

dp.create_auto_cdc_flow(
    target="silver_orders_history",
    source="silver_order_changes",
    keys=["order_id"],
    sequence_by=F.col("updated_at"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "_rescued_data"],
    stored_as_scd_type=2,
)


@dp.materialized_view(name="gold_daily_sales", comment="Daily sales KPIs for BI consumption")
def gold_daily_sales():
    current_orders = spark.read.table("silver_orders_history").filter(F.col("__END_AT").isNull())
    return daily_sales(current_orders)
