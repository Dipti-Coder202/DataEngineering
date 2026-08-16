import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = SparkSession.builder.master("local[1]").appName("project8-tests").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
