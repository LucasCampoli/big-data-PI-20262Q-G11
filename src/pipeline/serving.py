"""Serving: load the Gold marts into Cassandra (local Docker) or AstraDB with the Python driver.

The official Spark connector has no release for Spark 4, so each Spark partition opens its own
driver session and upserts its rows with a prepared INSERT (D16). An INSERT in Cassandra is an
upsert by primary key, so loading the same mart twice leaves the same rows: that is the
idempotence of this step (D8). Rows are repartitioned by org_id first, so each task writes whole
Cassandra partitions.

The target is chosen by SERVING_TARGET: cassandra (contact points) or astra (secure connect bundle
and token). Nothing else changes between the two.
"""
import logging
import re
from pathlib import Path

from pyspark.sql import DataFrame

from . import lake
from .config import REPO_ROOT, Config
from .errors import PipelineError

log = logging.getLogger("pipeline.serving")

CQL_DIR = REPO_ROOT / "cql"

# Gold table -> (Cassandra table, columns in load order). The key columns come first.
TABLES = {
    "org_daily_usage_by_service": ("org_daily_usage_by_service", [
        "org_id", "usage_date", "service", "daily_cost_usd", "requests", "cpu_hours",
        "storage_gb_hours", "genai_tokens", "carbon_kg", "events", "negative_cost_events",
        "unit_imputed_events", "computed_ts", "run_id"]),
    "cost_anomaly_mart": ("cost_anomalies_by_org", [
        "org_id", "usage_date", "service", "daily_cost_usd", "median_cost", "mad_cost",
        "history_days", "score_method", "anomaly_score", "is_anomaly", "anomaly_direction",
        "computed_ts", "run_id"]),
}
LOAD_PARTITIONS = 4
CONCURRENCY = 32


def connection_settings(cfg: Config) -> dict:
    """Plain values only: this dict is shipped to the Spark tasks."""
    if cfg.serving_target == "astra":
        if not (cfg.astra_bundle and cfg.astra_token):
            raise PipelineError("SERVING_TARGET=astra needs ASTRA_SECURE_CONNECT_BUNDLE and ASTRA_TOKEN")
        if not Path(cfg.astra_bundle).exists():
            raise PipelineError(f"secure connect bundle not found: {cfg.astra_bundle}")
        return {"target": "astra", "bundle": cfg.astra_bundle, "username": "token",
                "password": cfg.astra_token, "keyspace": cfg.cassandra_keyspace,
                "consistency": cfg.cassandra_consistency}
    if cfg.serving_target == "cassandra":
        return {"target": "cassandra", "hosts": cfg.cassandra_hosts, "port": cfg.cassandra_port,
                "username": cfg.cassandra_username, "password": cfg.cassandra_password,
                "keyspace": cfg.cassandra_keyspace, "consistency": cfg.cassandra_consistency,
                "local_dc": cfg.cassandra_local_dc}
    raise PipelineError(f"SERVING_TARGET must be cassandra, astra or none, not {cfg.serving_target!r}")


def connect(settings: dict):
    from cassandra import ConsistencyLevel
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    # LOCAL_QUORUM only means something against a named local DC. AstraDB takes its DC and policy
    # from the secure connect bundle.
    policy = ({"load_balancing_policy": TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=settings["local_dc"]))}
              if settings["target"] == "cassandra" else {})
    profile = ExecutionProfile(consistency_level=ConsistencyLevel.name_to_value[settings["consistency"]],
                               request_timeout=30, **policy)
    auth = (PlainTextAuthProvider(settings["username"], settings["password"])
            if settings.get("username") else None)
    kwargs = {"execution_profiles": {EXEC_PROFILE_DEFAULT: profile}, "auth_provider": auth,
              "protocol_version": 5 if settings["target"] == "cassandra" else 4}
    if settings["target"] == "astra":
        cluster = Cluster(cloud={"secure_connect_bundle": settings["bundle"]}, **kwargs)
    else:
        cluster = Cluster(settings["hosts"], port=settings["port"], **kwargs)
    return cluster, cluster.connect()


def cql_statements(path: Path, keyspace: str) -> list[str]:
    text = re.sub(r"--[^\n]*", "", path.read_text()).replace("cloud_analytics", keyspace)
    return [s.strip() for s in text.split(";") if s.strip()]


def ensure_schema(session, settings: dict):
    # AstraDB has no CREATE KEYSPACE: the keyspace comes from the console (README).
    if settings["target"] == "cassandra":
        for stmt in cql_statements(CQL_DIR / "00_keyspace.cql", settings["keyspace"]):
            session.execute(stmt)
    for stmt in cql_statements(CQL_DIR / "01_tables.cql", settings["keyspace"]):
        session.execute(stmt)


def write_partition(settings: dict, table: str, columns: list, rows, written):
    """Runs inside a Spark task: one session per partition, a prepared upsert, concurrent writes."""
    from cassandra.concurrent import execute_concurrent_with_args

    cluster, session = connect(settings)
    try:
        insert = session.prepare(
            f"INSERT INTO {settings['keyspace']}.{table} ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})")
        batch = [tuple(row[c] for c in columns) for row in rows]
        if not batch:
            return
        results = execute_concurrent_with_args(session, insert, batch, concurrency=CONCURRENCY,
                                               raise_on_first_error=True)
        written.add(sum(1 for ok, _ in results if ok))
    finally:
        cluster.shutdown()


def load(ctx, df: DataFrame, table: str, columns: list, settings: dict) -> int:
    written = ctx.spark.sparkContext.accumulator(0)
    (df.select(*columns)
       .repartition(LOAD_PARTITIONS, "org_id")
       .foreachPartition(lambda rows: write_partition(settings, table, columns, rows, written)))
    return written.value


def run(ctx, metrics):
    cfg = ctx.cfg
    if cfg.serving_target == "none":
        log.info("SERVING_TARGET=none: serving skipped")
        metrics["skipped"] = True
        return
    settings = connection_settings(cfg)
    try:
        cluster, session = connect(settings)
    except Exception as exc:
        raise PipelineError(f"cannot reach {settings['target']}: {exc}. "
                            "For local Cassandra run make cassandra-up first") from exc
    try:
        ensure_schema(session, settings)
        for gold_table, (table, columns) in TABLES.items():
            path = cfg.table("gold", gold_table)
            if not lake.exists(path):
                raise PipelineError(f"Gold {gold_table} is empty: run gold and anomalies first")
            gold = lake.read(ctx.spark, path)
            written = load(ctx, gold, table, columns, settings)
            expected = gold.count()
            metrics[table] = {"rows_written": written, "gold_rows": expected}
            log.info("%s.%s: %s rows upserted, Gold has %s", settings["keyspace"], table, written, expected)
            if written != expected:
                raise PipelineError(f"{table}: wrote {written} rows, Gold has {expected}")
        metrics["target"] = settings["target"]
    finally:
        cluster.shutdown()
