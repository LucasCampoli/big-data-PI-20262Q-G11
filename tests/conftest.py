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


@pytest.fixture
def make_ctx(spark, tmp_path):
    """A run context whose Landing, lake and logs live under tmp_path."""
    import dataclasses
    from pipeline.run import Context

    def _make(landing=None, run_id="test-run", **overrides):
        cfg = dataclasses.replace(
            Config.from_env(),
            landing=landing or tmp_path / "landing",
            datalake=tmp_path / "datalake",
            logs=tmp_path / "logs",
            evidence=tmp_path / "evidence",
            **overrides,
        )
        return Context(cfg, run_id, spark)
    return _make
