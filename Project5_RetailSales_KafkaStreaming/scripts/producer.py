import json
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from config import KAFKA_BROKER, TOPIC_NAME, CSV_FILE
from utils.logger import logger


logger.info("Starting Kafka Producer")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    logger.info(f"Connected to Kafka broker: {KAFKA_BROKER}")

    import csv

    with open(CSV_FILE, "r") as file:
        reader = csv.DictReader(file)

        for row in reader:

            row["order_id"] = int(row["order_id"])
            row["price"] = int(row["price"])
            row["quantity"] = int(row["quantity"])

            producer.send(TOPIC_NAME, value=row)

            logger.info(f"Sent order: {row['order_id']}")

            print(f"Sent: {row['order_id']}")

            time.sleep(1)

    producer.flush()

    logger.info("Finished sending all records")
    print("Finished Sending Data")

except Exception as e:
    logger.exception("Kafka Producer failed")
    raise

finally:
    try:
        producer.close()
    except:
        pass
print("Finished Sending Data")