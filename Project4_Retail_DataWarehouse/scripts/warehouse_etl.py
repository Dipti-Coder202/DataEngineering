from pyspark.sql import SparkSession

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import logger

# PostgreSQL Connection
url = "jdbc:postgresql://localhost:5432/retail_db"
properties = {
    "user": "retail_user",
    "password": "retail123",
    "driver": "org.postgresql.Driver"
}

spark = (
    SparkSession.builder
    .appName("Retail Data Warehouse")
    .config("spark.jars", "./jars/postgresql-42.7.7.jar")
    .getOrCreate()
)

test_df = spark.read.jdbc(
    url=url,
    table="""
    (
        SELECT
            current_database() AS db,
            current_user AS usr,
            inet_server_addr() AS ip,
            inet_server_port() AS port,
            version() AS version
    ) t
    """,
    properties=properties
)

test_df.show(truncate=False)

# Read CSV
df = spark.read.csv(
    "data/sales.csv",
    header=True,
    inferSchema=True
)

logger.info("Original Data Loaded")
df.show()

# Customer Dimension
dim_customer = df.select("customer_name").distinct()

logger.info("Customer Dimension Loaded")
dim_customer.show()

logger.info("Writing Customer Table...")



try:
    dim_customer.write.mode("append").jdbc(
        url=url,
        table="dim_customer",
        properties=properties
    )
    logger.info("Customer Dimension Loaded Successfully")

except Exception as e:
    logger.exception(f"Customer Dimension Load Failed: {e}")
    raise

logger.info("Customer Table Load Completed")

spark.read.jdbc(
    url=url,
    table="dim_customer",
    properties=properties
).show()

dim_product = df.select(
    "product",
    "category"
).distinct()

dim_product.write.mode("append").jdbc(
    url=url,
    table="dim_product",
    properties=properties
)

logger.info("Product Dimension Loaded")

dim_city = df.select("city").distinct()

dim_city.write.mode("append").jdbc(
    url=url,
    table="dim_city",
    properties=properties
)

logger.info("City Dimension Loaded")

# Read dimension tables from PostgreSQL
customer_df = spark.read.jdbc(
    url=url,
    table="dim_customer",
    properties=properties
)

product_df = spark.read.jdbc(
    url=url,
    table="dim_product",
    properties=properties
)

city_df = spark.read.jdbc(
    url=url,
    table="dim_city",
    properties=properties
)

fact_sales = (
    df
    .join(customer_df, on="customer_name")
    .join(product_df, on=["product", "category"])
    .join(city_df, on="city")
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "city_id",
        "price",
        "quantity"
    )
)

logger.info("Fact Table Created")
fact_sales.show()

fact_sales.write.mode("append").jdbc(
    url=url,
    table="fact_sales",
    properties=properties
)

logger.info("Fact Table Loaded")


spark.stop()