"""Incremental PostgreSQL-to-Iceberg medallion pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from config import Settings
from lakehouse.transform import clean_retail_sales, daily_sales, latest_by_order


def _table_exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table)


def _watermark(spark: SparkSession) -> tuple[str, int] | None:
    if not _table_exists(spark, "local.control.pipeline_watermarks"):
        return None
    row = spark.sql("""
        SELECT updated_at, order_id FROM local.control.pipeline_watermarks
        WHERE pipeline_name = 'retail_sales' LIMIT 1
    """).first()
    return (row.updated_at.isoformat(), row.order_id) if row else None


def _source_query(watermark: tuple[str, int] | None) -> str:
    query = "SELECT * FROM retail_sales"
    if watermark:
        timestamp, order_id = watermark
        query += (
            f" WHERE updated_at > TIMESTAMPTZ '{timestamp}'"
            f" OR (updated_at = TIMESTAMPTZ '{timestamp}' AND order_id > {order_id})"
        )
    return f"({query}) AS incremental_retail_sales"


def run_pipeline(spark: SparkSession, settings: Settings) -> dict[str, object]:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.control")
    watermark = _watermark(spark)
    source = (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", _source_query(watermark))
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 1000)
        .load()
    )
    batch_id = f"batch_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid4().hex[:8]}"
    bronze = source.withColumn("batch_id", F.lit(batch_id)).withColumn("ingested_at", F.current_timestamp()).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        extracted = bronze.count()
        if extracted == 0:
            return {"batch_id": batch_id, "extracted": 0, "status": "no_changes"}
        bronze.writeTo("local.bronze.retail_sales").using("iceberg").partitionedBy("order_date").append() if _table_exists(spark, "local.bronze.retail_sales") else bronze.writeTo("local.bronze.retail_sales").using("iceberg").partitionedBy("order_date").create()
        valid_rows, rejected = clean_retail_sales(bronze)
        valid_count = valid_rows.count()
        rejected_count = rejected.count()
        if valid_count + rejected_count != extracted:
            raise ValueError("Valid and rejected rows do not reconcile to Bronze")
        valid = latest_by_order(valid_rows).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            if rejected.limit(1).count():
                rejected.withColumn("batch_id", F.lit(batch_id)).writeTo("local.control.quarantine").using("iceberg").append() if _table_exists(spark, "local.control.quarantine") else rejected.withColumn("batch_id", F.lit(batch_id)).writeTo("local.control.quarantine").using("iceberg").create()
            valid.createOrReplaceTempView("incoming_silver")
            if _table_exists(spark, "local.silver.retail_sales"):
                spark.sql("""
                    MERGE INTO local.silver.retail_sales target USING incoming_silver source
                    ON target.order_id = source.order_id
                    WHEN MATCHED AND source.updated_at >= target.updated_at THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            else:
                valid.writeTo("local.silver.retail_sales").using("iceberg").partitionedBy("category").create()
            silver = spark.table("local.silver.retail_sales").persist(StorageLevel.MEMORY_AND_DISK)
            try:
                if silver.count() != silver.select("order_id").distinct().count():
                    raise ValueError("Silver contains duplicate order IDs")
                invalid = silver.filter((F.col("quantity") <= 0) | (F.col("price") < 0) | F.col("order_id").isNull()).count()
                if invalid:
                    raise ValueError(f"Silver contains {invalid} invalid rows")
                gold = daily_sales(silver).persist(StorageLevel.MEMORY_AND_DISK)
                silver_revenue = silver.agg(F.sum("total_amount").alias("revenue")).first().revenue
                gold_revenue = gold.agg(F.sum("total_revenue").alias("revenue")).first().revenue
                if silver_revenue != gold_revenue:
                    raise ValueError(f"Gold revenue does not reconcile to Silver: {gold_revenue} != {silver_revenue}")
                gold.writeTo("local.gold.daily_category_sales").using("iceberg").partitionedBy("order_date").createOrReplace()
                maximum = bronze.orderBy(F.col("updated_at").desc(), F.col("order_id").desc()).first()
                spark.createDataFrame([("retail_sales", maximum.updated_at, maximum.order_id, batch_id)], "pipeline_name string, updated_at timestamp, order_id long, batch_id string").createOrReplaceTempView("new_watermark")
                if _table_exists(spark, "local.control.pipeline_watermarks"):
                    spark.sql("""
                        MERGE INTO local.control.pipeline_watermarks t USING new_watermark s
                        ON t.pipeline_name = s.pipeline_name
                        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
                    """)
                else:
                    spark.table("new_watermark").writeTo("local.control.pipeline_watermarks").using("iceberg").create()
                result = {
                    "batch_id": batch_id,
                    "extracted": extracted,
                    "valid": valid_count,
                    "rejected": rejected_count,
                    "silver": silver.count(),
                    "gold": gold.count(),
                    "silver_revenue": str(silver_revenue),
                    "status": "success",
                }
                gold.unpersist()
                return result
            finally:
                silver.unpersist()
        finally:
            valid.unpersist()
    finally:
        bronze.unpersist()
