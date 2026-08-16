"""Unit tests for Bronze-to-Silver retail transformations."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from spark.transform import clean_and_classify, deduplicate_latest


BRONZE_COLUMNS = [
    "order_id",
    "customer_name",
    "product",
    "category",
    "price",
    "quantity",
    "city",
    "updated_at",
    "ingested_at",
    "ingestion_date",
    "batch_id",
]


def bronze_frame(spark, rows):
    return spark.createDataFrame(rows, BRONZE_COLUMNS)


def valid_row(**overrides):
    row = {
        "order_id": 1,
        "customer_name": "  Asha  ",
        "product": "  Laptop ",
        "category": " Electronics ",
        "price": Decimal("1250.50"),
        "quantity": 2,
        "city": " Pune ",
        "updated_at": datetime(2026, 8, 16, 8, 0, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 8, 16, 8, 1, tzinfo=timezone.utc),
        "ingestion_date": date(2026, 8, 16),
        "batch_id": "test_batch",
    }
    row.update(overrides)
    return tuple(row[column] for column in BRONZE_COLUMNS)


def test_transformation_trims_and_classifies_valid_record(spark):
    valid, rejected = clean_and_classify(bronze_frame(spark, [valid_row()]))

    actual = valid.first()
    assert rejected.count() == 0
    assert actual.customer_name == "Asha"
    assert actual.product == "Laptop"
    assert actual.category == "Electronics"
    assert actual.city == "Pune"


def test_total_amount_is_price_times_quantity(spark):
    valid, _ = clean_and_classify(bronze_frame(spark, [valid_row()]))

    assert valid.select("total_amount").first().total_amount == Decimal("2501.00")


def test_deduplication_keeps_latest_source_version(spark):
    older = valid_row(product="Old product")
    newer = valid_row(
        product="New product",
        updated_at=datetime(2026, 8, 16, 9, 0, tzinfo=timezone.utc),
    )
    valid, _ = clean_and_classify(bronze_frame(spark, [older, newer]))

    rows = deduplicate_latest(valid).collect()
    assert len(rows) == 1
    assert rows[0].product == "New product"
