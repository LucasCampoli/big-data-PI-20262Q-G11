"""Serving without a running Cassandra: the CQL files parse into the statements the loader runs,
and a misconfigured target fails with a message instead of a driver traceback."""
import dataclasses

import pytest

from pipeline.config import Config
from pipeline.errors import PipelineError
from pipeline.serving import CQL_DIR, TABLES, connection_settings, cql_statements


def test_table_ddl_matches_the_loaded_columns():
    ddl = " ".join(cql_statements(CQL_DIR / "01_tables.cql", "ks"))
    for table, columns in TABLES.values():
        assert f"ks.{table}" in ddl
        for column in columns:
            assert f" {column} " in ddl, (table, column)
    assert "PRIMARY KEY ((org_id), usage_date, service)" in ddl
    assert "--" not in ddl


def test_queries_read_one_partition_without_filtering():
    for path in sorted(CQL_DIR.glob("q*.cql")):
        stmt = cql_statements(path, "cloud_analytics")[0]
        assert "WHERE org_id = " in stmt, path.name
        assert "ALLOW FILTERING" not in stmt.upper(), path.name


def test_astra_without_credentials_is_a_clear_error():
    cfg = dataclasses.replace(Config.from_env(), serving_target="astra", astra_bundle="", astra_token="")
    with pytest.raises(PipelineError, match="ASTRA_SECURE_CONNECT_BUNDLE"):
        connection_settings(cfg)


def test_unknown_target_is_rejected():
    with pytest.raises(PipelineError, match="SERVING_TARGET"):
        connection_settings(dataclasses.replace(Config.from_env(), serving_target="mongo"))
