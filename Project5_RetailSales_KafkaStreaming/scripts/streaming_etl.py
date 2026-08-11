import sys
import os

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

from config import (
    KAFKA_BROKER,
    TOPIC_NAME,
    CHECKPOINT_LOCATION
)

from utils.logger import logger


logger.info("Starting Retail Streaming ETL")


try:

    # --------------------------------------------------
    # Create Spark Session
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

    logger.info("Spark session created successfully")


    # --------------------------------------------------
    # Read Data From Kafka
    # --------------------------------------------------

    df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BROKER
        )
        .option(
            "subscribe",
            TOPIC_NAME
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
    )

    logger.info(
        f"Connected to Kafka topic: {TOPIC_NAME}"
    )


    # --------------------------------------------------
    # Define Schema
    # --------------------------------------------------

    schema = StructType([
        StructField(
            "order_id",
            IntegerType(),
            True
        ),
        StructField(
            "customer_name",
            StringType(),
            True
        ),
        StructField(
            "product",
            StringType(),
            True
        ),
        StructField(
            "category",
            StringType(),
            True
        ),
        StructField(
            "price",
            IntegerType(),
            True
        ),
        StructField(
            "quantity",
            IntegerType(),
            True
        ),
        StructField(
            "city",
            StringType(),
            True
        )
    ])


    # --------------------------------------------------
    # Convert Kafka JSON
    # --------------------------------------------------

    json_df = (
        df
        .selectExpr(
            "CAST(value AS STRING)"
        )
        .select(
            from_json(
                col("value"),
                schema
            ).alias("data")
        )
        .select("data.*")
    )

    logger.info(
        "Kafka JSON schema applied successfully"
    )


    # --------------------------------------------------
    # Transformation
    # --------------------------------------------------

    from pyspark.sql.functions import expr

    transformed_df = (
        json_df
        .withColumn(
            "total_amount",
            expr("price * quantity")
        )
    )

    logger.info(
        "Transformation applied: total_amount = price * quantity"
    )


    # --------------------------------------------------
    # Write Stream to Parquet
    # --------------------------------------------------

    query = (
        transformed_df
        .writeStream
        .format("parquet")
        .option(
            "path",
            "output/"
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION
        )
        .outputMode("append")
        .trigger(
            processingTime="5 seconds"
        )
        .start()
    )

    logger.info(
        "Streaming ETL started successfully"
    )

    print("Streaming ETL started...")
    print("Output location: output/")


    # --------------------------------------------------
    # Wait for Streaming Query
    # --------------------------------------------------

    query.awaitTermination()


except KeyboardInterrupt:

    logger.info(
        "Streaming ETL stopped by user"
    )

    print("\nStreaming ETL stopped.")


except Exception:

    logger.exception(
        "Streaming ETL failed"
    )

    raise


finally:

    try:
        spark.stop()

        logger.info(
            "Spark session stopped"
        )

    except:
        pass