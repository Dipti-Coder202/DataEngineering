"""Project 8 command-line entry point."""

import json

from config import Settings
from lakehouse.pipeline import run_pipeline
from lakehouse.session import create_spark_session


def main() -> None:
    settings = Settings.from_env()
    spark = create_spark_session(settings)
    try:
        print(json.dumps(run_pipeline(spark, settings), default=str, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
