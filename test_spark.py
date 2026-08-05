from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

spark.range(5).show()

spark.stop()