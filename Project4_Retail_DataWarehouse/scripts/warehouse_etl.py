from pyspark.sql import SparkSession

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

print("Original Data")
df.show()

# Customer Dimension
dim_customer = df.select("customer_name").distinct()

print("Customer rows:", dim_customer.count())
dim_customer.show()

print("Writing Customer Table...")

print("Writing Customer Table...")

try:
    dim_customer.write.mode("append").jdbc(
        url=url,
        table="dim_customer",
        properties=properties
    )
    print("✅ Customer write successful")

except Exception:
    import traceback
    print("❌ JDBC WRITE FAILED")
    traceback.print_exc()

print("Write Finished")

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

print("Product Dimension Loaded")

dim_city = df.select("city").distinct()

dim_city.write.mode("append").jdbc(
    url=url,
    table="dim_city",
    properties=properties
)

print("City Dimension Loaded")

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

print("Fact Table Preview")
fact_sales.show()

fact_sales.write.mode("append").jdbc(
    url=url,
    table="fact_sales",
    properties=properties
)

print("✅ Fact Table Loaded")


spark.stop()