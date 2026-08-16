"""Validate Project 7 PostgreSQL access through psycopg2 and Spark JDBC."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from config import Settings  # noqa: E402


LOGGER = logging.getLogger("project7.source_check")
REQUIRED_COLUMNS = {
    "order_id",
    "customer_name",
    "product",
    "category",
    "price",
    "quantity",
    "city",
    "updated_at",
}


def check_postgres(settings: Settings) -> tuple[int, set[str]]:
    with psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=10,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (settings.db_source_table,),
            )
            columns = {row[0] for row in cursor.fetchall()}
            missing_columns = REQUIRED_COLUMNS - columns
            if missing_columns:
                raise RuntimeError(
                    f"Source table is missing columns: {sorted(missing_columns)}"
                )

            cursor.execute(
                f'SELECT COUNT(*) FROM public."{settings.db_source_table}"'
            )
            row_count = cursor.fetchone()[0]

    LOGGER.info("psycopg2 check passed: rows=%s columns=%s", row_count, len(columns))
    return row_count, columns


def check_spark_jdbc(settings: Settings) -> int:
    if not settings.jdbc_jar_path.is_file():
        raise FileNotFoundError(f"JDBC driver not found: {settings.jdbc_jar_path}")

    spark = (
        SparkSession.builder.master(settings.spark_master)
        .appName("Project7SourceConnectivityCheck")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.jars", str(settings.jdbc_jar_path))
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        source = (
            spark.read.format("jdbc")
            .option("url", settings.jdbc_url)
            .option("dbtable", f'public."{settings.db_source_table}"')
            .option("user", settings.db_user)
            .option("password", settings.db_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        row_count = source.count()
        missing_columns = REQUIRED_COLUMNS - set(source.columns)
        if missing_columns:
            raise RuntimeError(
                f"Spark JDBC result is missing columns: {sorted(missing_columns)}"
            )
    finally:
        spark.stop()

    LOGGER.info("Spark JDBC check passed: rows=%s", row_count)
    return row_count


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    settings = Settings.from_env(require_db_password=True)
    postgres_count, _ = check_postgres(settings)
    spark_count = check_spark_jdbc(settings)

    if postgres_count != spark_count:
        raise RuntimeError(
            f"Source count mismatch: psycopg2={postgres_count}, Spark={spark_count}"
        )

    LOGGER.info("All source connectivity checks passed without exposing credentials")


if __name__ == "__main__":
    main()
