import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import logger

logger.info("Project 5 logging started")
logger.info("Kafka connection successful")
logger.info("Spark Streaming started")
logger.info("Parquet output generated")

print("Logging test completed")