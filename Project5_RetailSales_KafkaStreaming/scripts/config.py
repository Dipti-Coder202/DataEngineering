import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "retail_sales"

CSV_FILE = "data/sales.csv"

CHECKPOINT_LOCATION = "checkpoints/"

DB_URL = (
    f"jdbc:postgresql://"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

DB_PROPERTIES = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}