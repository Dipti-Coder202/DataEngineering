from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

from config import KAFKA_BROKER, TOPIC_NAME, CHECKPOINT_LOCATION

import logging
import os

# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/streaming_etl.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Starting Retail Streaming ETL")

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

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

logger.info("Spark Session created successfully")

# --------------------------------------------------
# Read Stream from Kafka
# --------------------------------------------------

logger.info("Connecting to Kafka")
logger.info(f"Kafka Broker: {KAFKA_BROKER}")
logger.info(f"Kafka Topic: {TOPIC_NAME}")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

logger.info("Kafka streaming source connected")

# --------------------------------------------------
# Schema
# --------------------------------------------------

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("city", StringType())
])

# --------------------------------------------------
# Parse JSON
# --------------------------------------------------

json_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

logger.info("Kafka JSON data parsed successfully")

# --------------------------------------------------
# Streaming Output
# --------------------------------------------------

query = (
    json_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "output/")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

logger.info("Streaming query started successfully")
logger.info("Output location: output/")

print("==========================================")
print("Retail Streaming ETL is running...")
print("Kafka Topic:", TOPIC_NAME)
print("Output: output/")
print("Checkpoint:", CHECKPOINT_LOCATION)
print("==========================================")

# --------------------------------------------------
# Wait for Streaming Query
# --------------------------------------------------

query.awaitTermination()