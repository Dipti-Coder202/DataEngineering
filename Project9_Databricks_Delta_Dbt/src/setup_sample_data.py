# Databricks notebook source
"""Run once in Databricks to create the landing volume and demo CDC files."""

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "project9_retail_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_path = f"/Volumes/{catalog}/{schema}/landing/retail_orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`landing`")

records = [
    ("O1001", "C101", "Asha Rao", "asha@example.com", "P10", "Laptop Stand", "Office", "S01", "2026-08-15T10:10:00", "2026-08-15T10:10:01", 2, 35.50, "INSERT"),
    ("O1002", "C102", "Vikram Shah", "vikram@example.com", "P20", "Mechanical Keyboard", "Electronics", "S01", "2026-08-15T11:15:00", "2026-08-15T11:15:01", 1, 79.99, "INSERT"),
    ("O1003", "C103", "Neha Das", "neha@example.com", "P30", "Coffee Maker", "Kitchen", "S02", "2026-08-16T08:30:00", "2026-08-16T08:30:01", 3, 49.00, "INSERT"),
    ("O1002", "C102", "Vikram Shah", "vikram@example.com", "P20", "Mechanical Keyboard", "Electronics", "S01", "2026-08-15T11:15:00", "2026-08-16T09:00:00", 2, 79.99, "UPDATE"),
    ("O_BAD", None, "Unknown", None, "P99", "Invalid Item", "Other", "S03", "2026-08-16T09:30:00", "2026-08-16T09:30:01", 0, -5.00, "INSERT"),
]

columns = [
    "order_id", "customer_id", "customer_name", "customer_email", "product_id",
    "product_name", "category", "store_id", "order_timestamp", "updated_at",
    "quantity", "unit_price", "operation",
]

spark.createDataFrame(records, columns).coalesce(1).write.mode("append").json(landing_path)
print(f"Sample CDC data written to {landing_path}")

