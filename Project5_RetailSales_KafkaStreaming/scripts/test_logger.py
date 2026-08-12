import sys
import os

sys.path.append(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)

from utils.logger import logger

logger.info("Project 5 logger started")
logger.info("Kafka streaming pipeline test successful")

print("Logger test completed")
