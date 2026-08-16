"""Load Gold Parquet snapshots into PostgreSQL analytics tables atomically."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from config import Settings


LOGGER = logging.getLogger("project7.load_postgres")
GOLD_TARGETS: dict[str, tuple[str, tuple[str, ...]]] = {
    "overall_sales": (
        "analytics_overall_sales",
        (
            "metric_scope", "total_revenue", "order_count", "units_sold",
            "average_order_value", "distinct_customers", "last_refreshed_at",
        ),
    ),
    "category_sales": (
        "analytics_category_sales",
        (
            "category", "total_revenue", "order_count", "units_sold",
            "average_order_value", "distinct_customers", "last_refreshed_at",
        ),
    ),
    "city_sales": (
        "analytics_city_sales",
        (
            "city", "total_revenue", "order_count", "units_sold",
            "average_order_value", "distinct_customers", "last_refreshed_at",
        ),
    ),
    "product_sales": (
        "analytics_product_sales",
        (
            "product", "category", "total_revenue", "order_count", "units_sold",
            "average_order_value", "distinct_customers", "last_refreshed_at",
        ),
    ),
    "top_products": (
        "analytics_top_products",
        (
            "sales_rank", "product", "category", "total_revenue", "order_count",
            "units_sold", "average_order_value", "last_refreshed_at",
        ),
    ),
    "customer_sales": (
        "analytics_customer_sales",
        (
            "customer_name", "total_revenue", "order_count", "units_sold",
            "average_order_value", "distinct_customers", "last_refreshed_at",
            "distinct_products",
        ),
    ),
}


@dataclass(frozen=True)
class PostgresLoadResult:
    load_id: str
    table_counts: dict[str, int]


def _connect(settings: Settings):
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=10,
    )


def _load_staging_table(
    dataframe: DataFrame,
    settings: Settings,
    staging_table: str,
) -> None:
    (
        dataframe.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", f'public."{staging_table}"')
        .option("user", settings.db_user)
        .option("password", settings.db_password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 1000)
        .mode("overwrite")
        .save()
    )


def _drop_staging_tables(settings: Settings, staging_tables: list[str]) -> None:
    if not staging_tables:
        return
    try:
        with _connect(settings) as connection:
            with connection.cursor() as cursor:
                for table in staging_tables:
                    cursor.execute(
                        sql.SQL("DROP TABLE IF EXISTS public.{}")
                        .format(sql.Identifier(table))
                    )
    except Exception:
        LOGGER.exception("Failed to clean up PostgreSQL staging tables")


def load_gold_to_postgres(
    spark: SparkSession,
    settings: Settings,
) -> PostgresLoadResult:
    load_id = uuid4().hex[:12]
    LOGGER.info("Starting Gold PostgreSQL load: load_id=%s", load_id)
    gold_frames: dict[str, DataFrame] = {}
    staging_tables: list[str] = []

    try:
        for dataset_name, (_, expected_columns) in GOLD_TARGETS.items():
            gold_path = settings.gold_path / dataset_name
            if not gold_path.exists():
                raise FileNotFoundError(f"Gold dataset does not exist: {gold_path}")

            dataframe = spark.read.parquet(str(gold_path))
            actual_columns = set(dataframe.columns)
            if actual_columns != set(expected_columns):
                raise ValueError(
                    f"Unexpected columns for {dataset_name}: {sorted(actual_columns)}"
                )
            gold_frames[dataset_name] = dataframe.select(*expected_columns).persist(
                StorageLevel.MEMORY_AND_DISK
            )

        for dataset_name, dataframe in gold_frames.items():
            target_table = GOLD_TARGETS[dataset_name][0]
            staging_table = f"stg_{target_table}_{load_id}"
            _load_staging_table(dataframe, settings, staging_table)
            staging_tables.append(staging_table)
            LOGGER.info("Staged Gold dataset: %s -> %s", dataset_name, staging_table)

        ddl_path = settings.project_root / "sql" / "analytics_tables.sql"
        ddl = ddl_path.read_text(encoding="utf-8")
        table_counts: dict[str, int] = {}
        with _connect(settings) as connection:
            with connection.cursor() as cursor:
                # Prevent two publishers from replacing analytics simultaneously.
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (7007006,))
                cursor.execute(ddl)

                for dataset_name, (target_table, columns) in GOLD_TARGETS.items():
                    staging_table = f"stg_{target_table}_{load_id}"
                    column_identifiers = sql.SQL(", ").join(
                        sql.Identifier(column) for column in columns
                    )
                    cursor.execute(
                        sql.SQL("TRUNCATE TABLE public.{}").format(
                            sql.Identifier(target_table)
                        )
                    )
                    cursor.execute(
                        sql.SQL(
                            "INSERT INTO public.{} ({}) SELECT {} FROM public.{}"
                        ).format(
                            sql.Identifier(target_table),
                            column_identifiers,
                            column_identifiers,
                            sql.Identifier(staging_table),
                        )
                    )
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) FROM public.{}").format(
                            sql.Identifier(target_table)
                        )
                    )
                    table_counts[target_table] = cursor.fetchone()[0]

                for staging_table in staging_tables:
                    cursor.execute(
                        sql.SQL("DROP TABLE public.{}").format(
                            sql.Identifier(staging_table)
                        )
                    )

        staging_tables.clear()
        LOGGER.info(
            "Gold PostgreSQL load completed: load_id=%s counts=%s",
            load_id,
            table_counts,
        )
        return PostgresLoadResult(load_id=load_id, table_counts=table_counts)
    except Exception:
        _drop_staging_tables(settings, staging_tables)
        raise
    finally:
        for dataframe in gold_frames.values():
            dataframe.unpersist()
