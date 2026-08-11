from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckOutput")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("output/")

df.show(truncate=False)

spark.stop()

