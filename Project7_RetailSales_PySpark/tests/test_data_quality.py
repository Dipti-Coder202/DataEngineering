"""Unit tests for row-level validation and quarantine classification."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.transform import clean_and_classify


def test_invalid_record_has_all_applicable_rejection_reasons(spark):
    schema = StructType([
        StructField("order_id", LongType()),
        StructField("customer_name", StringType()),
        StructField("product", StringType()),
        StructField("category", StringType()),
        StructField("price", DecimalType(12, 2)),
        StructField("quantity", IntegerType()),
        StructField("city", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("ingestion_date", DateType()),
        StructField("batch_id", StringType()),
    ])
    invalid = (
        99, " ", "Mouse", "Accessories", Decimal("-1.00"), 0, " ", None,
        datetime(2026, 8, 16, 8, 1, tzinfo=timezone.utc),
        date(2026, 8, 16), "quality_test",
    )

    valid, rejected = clean_and_classify(spark.createDataFrame([invalid], schema))
    actual = rejected.first()

    assert valid.count() == 0
    assert set(actual.rejection_reasons) == {
        "customer_name_is_blank",
        "city_is_blank",
        "price_is_negative",
        "quantity_is_not_positive",
        "updated_at_is_null",
    }
    assert len(actual.record_fingerprint) == 64
