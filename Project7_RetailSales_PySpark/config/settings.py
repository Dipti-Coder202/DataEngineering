"""Environment-backed settings and project paths for Project 7."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"


def _positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, received {raw_value!r}") from exc

    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _sql_identifier(name: str, default: str) -> str:
    value = os.getenv(name, default)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{name} must be a simple SQL identifier")
    return value


@dataclass(frozen=True)
class Settings:
    """Validated runtime settings without exposing secrets in representations."""

    project_root: Path
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str | None = field(repr=False)
    db_source_table: str
    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: int
    jdbc_jar_path: Path

    @classmethod
    def from_env(
        cls,
        env_file: Path = DEFAULT_ENV_FILE,
        *,
        require_db_password: bool = False,
    ) -> "Settings":
        load_dotenv(env_file, override=False)

        password = os.getenv("DB_PASSWORD")
        if require_db_password and not password:
            raise ValueError(
                f"DB_PASSWORD is required; add it to {env_file} or the environment"
            )

        jdbc_jar_path = Path(
            os.getenv(
                "JDBC_JAR_PATH",
                str(PROJECT_ROOT / "jars" / "postgresql-42.7.7.jar"),
            )
        ).expanduser()
        if not jdbc_jar_path.is_absolute():
            jdbc_jar_path = PROJECT_ROOT / jdbc_jar_path

        return cls(
            project_root=PROJECT_ROOT,
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=_positive_int("DB_PORT", 5432),
            db_name=os.getenv("DB_NAME", "retail_db"),
            db_user=os.getenv("DB_USER", "retail_user"),
            db_password=password,
            db_source_table=_sql_identifier("DB_SOURCE_TABLE", "retail_sales"),
            spark_master=os.getenv("SPARK_MASTER", "local[2]"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1g"),
            spark_shuffle_partitions=_positive_int("SPARK_SHUFFLE_PARTITIONS", 4),
            jdbc_jar_path=jdbc_jar_path.resolve(),
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def bronze_path(self) -> Path:
        return self.project_root / "data" / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.project_root / "data" / "silver"

    @property
    def gold_path(self) -> Path:
        return self.project_root / "data" / "gold"

    @property
    def quarantine_path(self) -> Path:
        return self.project_root / "data" / "quarantine"

    @property
    def state_path(self) -> Path:
        return self.project_root / "data" / "state"

    def safe_summary(self) -> dict[str, str | int | bool]:
        """Return diagnostic settings without the database password."""
        return {
            "project_root": str(self.project_root),
            "db_host": self.db_host,
            "db_port": self.db_port,
            "db_name": self.db_name,
            "db_user": self.db_user,
            "db_password_configured": bool(self.db_password),
            "db_source_table": self.db_source_table,
            "spark_master": self.spark_master,
            "spark_driver_memory": self.spark_driver_memory,
            "spark_shuffle_partitions": self.spark_shuffle_partitions,
            "jdbc_jar_path": str(self.jdbc_jar_path),
        }
