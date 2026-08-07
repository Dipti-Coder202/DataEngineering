from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

from config import KAFKA_BROKER, TOPIC_NAME

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Retail Streaming ETL")
    .config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read stream from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("city", StringType())
])

json_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

query = (
    json_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "checkpoints")
    .start()
)

query.awaitTermination()