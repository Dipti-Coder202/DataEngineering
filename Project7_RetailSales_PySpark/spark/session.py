"""Spark session construction shared by Project 7 stages."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import Settings


def create_spark_session(settings: Settings, app_name: str) -> SparkSession:
    if not settings.jdbc_jar_path.is_file():
        raise FileNotFoundError(f"JDBC driver not found: {settings.jdbc_jar_path}")

    spark = (
        SparkSession.builder.master(settings.spark_master)
        .appName(app_name)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.jars", str(settings.jdbc_jar_path))
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        # AQE adjusts the configured shuffle partition count to the amount of
        # data observed at runtime. This keeps tiny local batches lightweight
        # without removing the ability to scale the configured upper bound.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
