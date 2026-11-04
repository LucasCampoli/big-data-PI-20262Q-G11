import pytest
from pyspark.sql import SparkSession

from pipeline.config import Config
from pipeline.spark import check_java


@pytest.fixture(scope="session")
def spark():
    check_java(Config.from_env())
    session = (
        SparkSession.builder.appName("tests").master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
