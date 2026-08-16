"""Validated local-AWS lakehouse configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


@dataclass(frozen=True)
class Settings:
    project_root: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str = field(repr=False)
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str = field(repr=False)
    minio_bucket: str
    aws_region: str
    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: int
    iceberg_version: str
    postgres_jdbc_version: str

    @classmethod
    def from_env(cls, *, require_secrets: bool = True) -> "Settings":
        load_dotenv(PROJECT_ROOT / ".env", override=False)
        postgres_password = os.getenv("POSTGRES_PASSWORD", "")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "")
        if require_secrets and (not postgres_password or not minio_secret_key):
            raise ValueError("POSTGRES_PASSWORD and MINIO_SECRET_KEY are required")
        return cls(
            project_root=PROJECT_ROOT,
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=_positive_int("POSTGRES_PORT", 5434),
            postgres_db=os.getenv("POSTGRES_DB", "retail_lakehouse"),
            postgres_user=os.getenv("POSTGRES_USER", "retail_user"),
            postgres_password=postgres_password,
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "lakehouse_admin"),
            minio_secret_key=minio_secret_key,
            minio_bucket=os.getenv("MINIO_BUCKET", "retail-lakehouse"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            spark_master=os.getenv("SPARK_MASTER", "local[2]"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1g"),
            spark_shuffle_partitions=_positive_int("SPARK_SHUFFLE_PARTITIONS", 4),
            iceberg_version=os.getenv("ICEBERG_VERSION", "1.11.0"),
            postgres_jdbc_version=os.getenv("POSTGRES_JDBC_VERSION", "42.7.7"),
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def warehouse_uri(self) -> str:
        return f"s3://{self.minio_bucket}/warehouse"
