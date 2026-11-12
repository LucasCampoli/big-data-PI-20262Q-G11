"""Helpers over the Data Lake layout: datalake/<zone>/<entity>/<partition>=<value>/."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

ZONES = ("bronze", "silver", "gold", "quarantine")


def exists(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def read(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.parquet(str(path))


def tables(datalake: Path) -> list[tuple[str, str, Path]]:
    out = []
    for zone in ZONES:
        zone_dir = datalake / zone
        if zone_dir.exists():
            out += [(zone, d.name, d) for d in sorted(zone_dir.iterdir()) if d.is_dir()]
    return out


def write_partitioned(df: DataFrame, path: Path, partition_col: str, files_per_partition: int = 1):
    """Overwrite only the partitions present in df (dynamic mode), with a bounded file count.

    coalesce(n) before partitionBy gives at most n files per partition. At this scale one file per
    day is the right size; at the projected scale this becomes repartition(partition_col, ...).
    """
    (df.coalesce(files_per_partition)
       .write.mode("overwrite")
       .partitionBy(partition_col)
       .parquet(str(path)))


def write_table(df: DataFrame, path: Path):
    """Unpartitioned dimension, rebuilt in full on every run."""
    df.coalesce(1).write.mode("overwrite").parquet(str(path))
