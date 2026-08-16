from datetime import date, datetime
from decimal import Decimal

from lakehouse.transform import clean_retail_sales, daily_sales, latest_by_order


def test_clean_deduplicate_and_aggregate(spark):
    columns = ["order_id", "customer_name", "product", "category", "price", "quantity", "city", "order_date", "updated_at"]
    rows = [
        (1, " Dipti ", "Laptop", "Electronics", Decimal("100.00"), 1, "Pune", date(2026, 8, 1), datetime(2026, 8, 1, 1)),
        (1, "Dipti", "Laptop", "Electronics", Decimal("120.00"), 2, "Pune", date(2026, 8, 1), datetime(2026, 8, 1, 2)),
    ]
    valid, rejected = clean_retail_sales(spark.createDataFrame(rows, columns))
    latest = latest_by_order(valid)
    gold = daily_sales(latest).first()
    assert rejected.count() == 0
    assert latest.count() == 1
    assert latest.first().customer_name == "Dipti"
    assert gold.total_revenue == Decimal("240.00")
    assert gold.order_count == 1
