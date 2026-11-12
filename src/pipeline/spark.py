"""SparkSession construction and the Java check that has to run before the JVM starts."""
import logging
import os
import re
import subprocess
import sys

from pyspark.sql import SparkSession

from .config import Config
from .errors import PipelineError

log = logging.getLogger("pipeline.spark")

# Spark 4.2 runs on 17, 21 and 25; the delivery evidence was produced on 21.
SUPPORTED_JAVA = {17, 21, 25}


def java_major(java_home: str) -> tuple[int, str]:
    java = os.path.join(java_home, "bin", "java") if java_home else "java"
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30).stderr
    except OSError as exc:
        raise PipelineError(f"cannot run {java}: {exc}. Set PIPELINE_JAVA_HOME to a JDK 17, 21 or 25") from exc
    first = out.splitlines()[0] if out else ""
    match = re.search(r'version "(\d+)', first)
    if not match:
        raise PipelineError(f"cannot read the Java version from: {first!r}")
    return int(match.group(1)), first


def check_java(cfg: Config) -> str:
    major, banner = java_major(cfg.java_home)
    if major not in SUPPORTED_JAVA:
        raise PipelineError(
            f"Java {major} found at {cfg.java_home or 'PATH'}; the pipeline supports JDK 17, 21 or 25. "
            "Set PIPELINE_JAVA_HOME in config/pipeline.env")
    if cfg.java_home:
        os.environ["JAVA_HOME"] = cfg.java_home
    return banner


def build_session(cfg: Config, app_name: str) -> SparkSession:
    check_java(cfg)
    # Python workers default to the python3 on PATH; pinning them to the driver's interpreter keeps
    # the venv's packages (the Cassandra driver among them) even when the venv is not activated.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark_master)
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        # The data is tiny: 200 default shuffle partitions would mean 200 near-empty tasks per
        # aggregation. AQE still coalesces what is left.
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        # Silver and Gold rewrite only the partitions a run produces (D8).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.warehouse.dir", str(cfg.datalake / "_warehouse"))
        .config("spark.ui.showConsoleProgress", "false")
        # WSL resolves the hostname to a loopback alias; local mode only needs localhost.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("spark %s, master %s, shuffle partitions %s, app id %s", spark.version,
             cfg.spark_master, cfg.shuffle_partitions, spark.sparkContext.applicationId)
    return spark
