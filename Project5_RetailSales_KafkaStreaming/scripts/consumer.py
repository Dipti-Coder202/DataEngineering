import json
import sys
import os

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from kafka import KafkaConsumer
from config import KAFKA_BROKER, TOPIC_NAME
from utils.logger import logger


logger.info("Starting Kafka Consumer")

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    logger.info(f"Connected to Kafka broker: {KAFKA_BROKER}")
    logger.info(f"Subscribed to topic: {TOPIC_NAME}")

    print("Waiting for messages...")

    for message in consumer:
        data = message.value

        print(data)

        logger.info(
            f"Received order: {data.get('order_id')}"
        )

except KeyboardInterrupt:
    logger.info("Kafka Consumer stopped by user")
    print("\nConsumer stopped.")

except Exception:
    logger.exception("Kafka Consumer failed")
    raise

finally:
    try:
        consumer.close()
    except:
        pass