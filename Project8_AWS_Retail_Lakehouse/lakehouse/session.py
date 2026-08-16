"""Spark session configured for Iceberg tables on MinIO."""

from pyspark.sql import SparkSession

from config import Settings


def create_spark_session(settings: Settings) -> SparkSession:
    packages = ",".join((
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{settings.iceberg_version}",
        f"org.apache.iceberg:iceberg-aws-bundle:{settings.iceberg_version}",
        f"org.postgresql:postgresql:{settings.postgres_jdbc_version}",
    ))
    spark = (
        SparkSession.builder.master(settings.spark_master)
        .appName("Project8LocalLakehouse")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.local.uri", settings.jdbc_url)
        .config("spark.sql.catalog.local.jdbc.user", settings.postgres_user)
        .config("spark.sql.catalog.local.jdbc.password", settings.postgres_password)
        .config("spark.sql.catalog.local.jdbc.schema-version", "V1")
        .config("spark.sql.catalog.local.warehouse", settings.warehouse_uri)
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", settings.minio_endpoint)
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.catalog.local.s3.access-key-id", settings.minio_access_key)
        .config("spark.sql.catalog.local.s3.secret-access-key", settings.minio_secret_key)
        .config("spark.sql.catalog.local.s3.region", settings.aws_region)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
