"""Pipeline configuration, read from environment variables.

`config/pipeline.env` (a copy of `config/pipeline.env.example`) is loaded first and never overrides
a variable already set in the shell, so a one-off change is `VAR=value make run`. Credentials are
only ever read from the environment and never written to logs.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = REPO_ROOT / "config" / "pipeline.env"

# Secrets, and the bundle path, which is local and points at credentials: logged only as set/unset.
SECRET_KEYS = {"astra_token", "astra_bundle", "cassandra_password", "pii_hash_salt"}


def load_env_file(path: Path = ENV_FILE) -> bool:
    if not path.exists():
        return False
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return True


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _path(name: str, default: str) -> Path:
    path = Path(_env(name, default)).expanduser()
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


@dataclass(frozen=True)
class Config:
    landing: Path
    datalake: Path
    logs: Path
    evidence: Path
    java_home: str
    spark_master: str
    driver_memory: str
    shuffle_partitions: int
    files_per_partition: int
    stream_max_files_per_trigger: int
    stream_trigger: str
    stream_processing_time: str
    stream_watermark: str
    serving_target: str
    cassandra_hosts: list = field(default_factory=list)
    cassandra_port: int = 9042
    cassandra_keyspace: str = "cloud_analytics"
    cassandra_username: str = ""
    cassandra_password: str = ""
    cassandra_consistency: str = "LOCAL_QUORUM"
    cassandra_local_dc: str = "dc1"
    astra_bundle: str = ""
    astra_token: str = ""
    anomaly_threshold: float = 3.5
    anomaly_window_days: int = 28
    anomaly_min_history: int = 14
    pii_hash_salt: str = ""

    @classmethod
    def from_env(cls) -> "Config":
        load_env_file()
        return cls(
            landing=_path("DATA_LANDING", "data/landing"),
            datalake=_path("DATALAKE_ROOT", "datalake"),
            logs=_path("LOG_DIR", "logs"),
            evidence=_path("EVIDENCE_DIR", "evidence/entrega-2"),
            java_home=_env("PIPELINE_JAVA_HOME") or _env("JAVA_HOME"),
            spark_master=_env("SPARK_MASTER", "local[4]"),
            driver_memory=_env("SPARK_DRIVER_MEMORY", "2g"),
            shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "8")),
            files_per_partition=int(_env("FILES_PER_PARTITION", "1")),
            stream_max_files_per_trigger=int(_env("STREAM_MAX_FILES_PER_TRIGGER", "10")),
            stream_trigger=_env("STREAM_TRIGGER", "available_now"),
            stream_processing_time=_env("STREAM_PROCESSING_TIME", "1 minute"),
            stream_watermark=_env("STREAM_WATERMARK", "1 hour"),
            serving_target=_env("SERVING_TARGET", "cassandra"),
            cassandra_hosts=[h.strip() for h in _env("CASSANDRA_HOSTS", "127.0.0.1").split(",") if h.strip()],
            cassandra_port=int(_env("CASSANDRA_PORT", "9042")),
            cassandra_keyspace=_env("CASSANDRA_KEYSPACE", "cloud_analytics"),
            cassandra_username=_env("CASSANDRA_USERNAME"),
            cassandra_password=_env("CASSANDRA_PASSWORD"),
            cassandra_consistency=_env("CASSANDRA_CONSISTENCY", "LOCAL_QUORUM"),
            cassandra_local_dc=_env("CASSANDRA_LOCAL_DC", "dc1"),
            astra_bundle=_env("ASTRA_SECURE_CONNECT_BUNDLE"),
            astra_token=_env("ASTRA_TOKEN"),
            anomaly_threshold=float(_env("ANOMALY_THRESHOLD", "3.5")),
            anomaly_window_days=int(_env("ANOMALY_WINDOW_DAYS", "28")),
            anomaly_min_history=int(_env("ANOMALY_MIN_HISTORY", "14")),
            pii_hash_salt=_env("PII_HASH_SALT"),
        )

    def table(self, zone: str, entity: str) -> Path:
        return self.datalake / zone / entity

    def checkpoint(self, query_name: str) -> Path:
        # One checkpoint directory per streaming query, never shared.
        return self.datalake / "_checkpoints" / query_name

    @property
    def registry(self) -> Path:
        return self.datalake / "_registry" / "ingested_files"

    def masked(self) -> dict:
        """The configuration as it goes into the logs: secrets replaced by whether they are set."""
        out = {}
        for key, value in self.__dict__.items():
            if key in SECRET_KEYS:
                out[key] = "<set>" if value else "<unset>"
            elif isinstance(value, Path):
                out[key] = str(value.relative_to(REPO_ROOT)) if value.is_relative_to(REPO_ROOT) else str(value)
            else:
                out[key] = value
        return out
