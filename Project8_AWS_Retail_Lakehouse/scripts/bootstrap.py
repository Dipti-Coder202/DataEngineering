"""Wait for local services and create the S3-compatible warehouse bucket."""

import time
import sys
from pathlib import Path

import boto3
import psycopg2
from botocore.config import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import Settings


def retry(operation, label: str) -> None:
    for attempt in range(1, 31):
        try:
            operation()
            return
        except Exception:
            if attempt == 30:
                raise
            time.sleep(2)
    raise RuntimeError(f"Unable to initialize {label}")


def main() -> None:
    settings = Settings.from_env()

    def check_postgres() -> None:
        with psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM retail_sales")
                print(f"PostgreSQL source rows: {cursor.fetchone()[0]}")

    client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name=settings.aws_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    def create_bucket() -> None:
        names = {item["Name"] for item in client.list_buckets()["Buckets"]}
        if settings.minio_bucket not in names:
            client.create_bucket(Bucket=settings.minio_bucket)
        print(f"MinIO bucket ready: {settings.minio_bucket}")

    retry(check_postgres, "PostgreSQL")
    retry(create_bucket, "MinIO")


if __name__ == "__main__":
    main()
