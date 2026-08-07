from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("Retail To PostgreSQL")
#     .getOrCreate()
# )

spark = (
    SparkSession.builder
    .appName("Retail To PostgreSQL")
    .config(
    "spark.jars",
    "/Users/apple/Desktop/DataEngineering/Project2_RetailSales_Postgres/jars/postgresql-42.7.7.jar"
)
    .getOrCreate()
)

# url = "jdbc:postgresql://localhost:5432/retail_db"

# properties = {
#     "user": "apple",
#     "password": "",
#     "driver": "org.postgresql.Driver"
# }

url = "jdbc:postgresql://localhost:5432/retail_db"

properties = {
    "user": "retail_user",
    "password": "retail123",
    "driver": "org.postgresql.Driver"
}


spark = (
    SparkSession.builder
    .appName("Retail To PostgreSQL Docker")
    .config(
        "spark.jars",
        "../jars/postgresql-42.7.7.jar"
    )
    .getOrCreate()
)


import os

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_path, "data", "sales.csv")

print(csv_path)

df = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=True
)

df.show()

df.write.mode("append").jdbc(
        url=url,
        table="retail_sales",
        properties=properties
    )

print("Data Loaded Successfully!")

spark.stop()