import sys
import os
import psycopg2

from dotenv import load_dotenv

load_dotenv()

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from utils.logger import logger

from scripts.config import (
    KAFKA_BROKER,
    TOPIC_NAME,
    CHECKPOINT_LOCATION
)

spark = (
    SparkSession.builder
    .appName("Retail Streaming to PostgreSQL")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config(
        "spark.jars",
        "jars/postgresql-42.7.7.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("city", StringType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

json_df = (
    df.selectExpr("CAST(value AS STRING) AS value")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

final_df = json_df.withColumn(
    "total_amount",
    col("price") * col("quantity")
)


def write_to_postgres(batch_df, batch_id):

    logger.info(f"Processing batch: {batch_id}")

    try:

        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty")
            return

        rows = batch_df.dropDuplicates(["order_id"]).collect()

        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

        cursor = connection.cursor()

        for row in rows:

            cursor.execute(
                """
                INSERT INTO streaming_sales
                (
                    order_id,
                    customer_name,
                    product,
                    category,
                    price,
                    quantity,
                    city,
                    total_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id)
                DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    product = EXCLUDED.product,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    city = EXCLUDED.city,
                    total_amount = EXCLUDED.total_amount;
                """,
                (
                    row.order_id,
                    row.customer_name,
                    row.product,
                    row.category,
                    row.price,
                    row.quantity,
                    row.city,
                    row.total_amount
                )
            )

        connection.commit()

        cursor.close()
        connection.close()

        logger.info(
            f"Batch {batch_id} loaded successfully. "
            f"Rows processed: {len(rows)}"
        )

    except Exception as e:

        logger.exception(
            f"Error processing batch {batch_id}: {e}"
        )
        raise

query = (
    final_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION + "postgres/"
    )
    .start()
)

logger.info("Streaming → PostgreSQL started...")

query.awaitTermination()
