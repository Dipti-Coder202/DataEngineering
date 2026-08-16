from decimal import Decimal

from src.transformations import daily_sales, quarantined_orders, valid_orders


def source_df(spark):
    rows = [
        ("O1", "C1", "P1", "2026-08-15T10:00:00", "2026-08-15T10:01:00", 2, 10.5, "S1", "Office", "INSERT"),
        ("O2", "C2", "P2", "2026-08-15T11:00:00", "2026-08-15T11:01:00", 0, 20.0, "S1", "Office", "INSERT"),
        ("O3", None, "P3", "2026-08-15T12:00:00", "2026-08-15T12:01:00", 1, 5.0, "S2", "Kitchen", "INSERT"),
    ]
    return spark.createDataFrame(
        rows,
        "order_id string, customer_id string, product_id string, order_timestamp string, "
        "updated_at string, quantity int, unit_price double, store_id string, category string, operation string",
    )


def test_quality_rules_split_valid_and_quarantined_records(spark):
    df = source_df(spark)
    assert valid_orders(df).count() == 1
    rejected = {row.quality_reason for row in quarantined_orders(df).collect()}
    assert rejected == {"non_positive_quantity", "missing_required_field"}


def test_daily_sales_calculates_business_metrics(spark):
    aggregate = daily_sales(valid_orders(source_df(spark))).collect()[0]
    assert aggregate.order_count == 1
    assert aggregate.units_sold == 2
    assert aggregate.gross_sales == Decimal("21.00")

