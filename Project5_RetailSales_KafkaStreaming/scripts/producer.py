from kafka import KafkaProducer
import json
import pandas as pd
import time

from config import KAFKA_BROKER, TOPIC_NAME, CSV_FILE

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

df = pd.read_csv(CSV_FILE)

print("Sending records to Kafka...")

for _, row in df.iterrows():
    producer.send(TOPIC_NAME, row.to_dict())
    print(row.to_dict())
    time.sleep(2)

producer.flush()

print("Finished Sending Data")