from kafka import KafkaConsumer
import json

from config import KAFKA_BROKER, TOPIC_NAME

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Waiting for messages...\n")

for message in consumer:
    print(message.value)