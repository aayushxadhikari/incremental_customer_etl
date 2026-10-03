"""Application configuration loaded from environment variables.

All runtime configuration lives here so the rest of the codebase never reads
environment variables directly. Call ``Settings.from_env()`` once at startup
and pass the resulting object down to the layers that need it.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = str(PROJECT_ROOT / "config" / "db.env")

_REQUIRED_VARS = (
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_DB",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_JAR",
)


@dataclass(frozen=True)
class Settings:
    """Immutable, validated view of the runtime configuration."""

    mysql_host: str
    mysql_port: int
    mysql_db: str
    mysql_user: str
    mysql_password: str = field(repr=False)
    mysql_jar: str
    spark_master: str = "local[2]"
    spark_shuffle_partitions: int = 4
    jdbc_partitions: int = 1
    jdbc_fetch_size: int = 1000
    jdbc_batch_size: int = 1000

    @property
    def jdbc_url(self) -> str:
        return (f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"
                "?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true&connectTimeout=10000")

    @classmethod
    def from_env(cls, env_file: str = DEFAULT_ENV_FILE) -> "Settings":
        load_dotenv(env_file)

        missing = [var for var in _REQUIRED_VARS if not os.getenv(var)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"Copy config/db.env.example to {env_file} and fill it in."
            )

        jar = str(Path(os.environ["MYSQL_JAR"]).expanduser())
        if not Path(jar).is_absolute():
            jar = str(PROJECT_ROOT / jar)
        if not os.path.isfile(jar):
            raise FileNotFoundError(
                f"MySQL JDBC jar not found at '{jar}'. "
                "Download the MySQL Connector/J jar and point MYSQL_JAR at it."
            )

        def positive_int(key, default):
            value = int(os.getenv(key, str(default)))
            if value < 1:
                raise ValueError(f"{key} must be positive")
            return value

        port = positive_int("MYSQL_PORT", 3307)
        if port > 65535:
            raise ValueError("MYSQL_PORT must be at most 65535")
        return cls(
            mysql_host=os.getenv("MYSQL_HOST"),
            mysql_port=port,
            mysql_db=os.getenv("MYSQL_DB"),
            mysql_user=os.getenv("MYSQL_USER"),
            mysql_password=os.getenv("MYSQL_PASSWORD"),
            mysql_jar=jar,
            spark_master=os.getenv("SPARK_MASTER", "local[2]"),
            spark_shuffle_partitions=positive_int("SPARK_SHUFFLE_PARTITIONS", 4),
            jdbc_partitions=positive_int("JDBC_PARTITIONS", 1),
            jdbc_fetch_size=positive_int("JDBC_FETCH_SIZE", 1000),
            jdbc_batch_size=positive_int("JDBC_BATCH_SIZE", 1000),
        )
