"""Pin the event quality rules against the synthetic fixtures.

Real Landing data rejects nothing (0 of 43,200 events), so the fixtures in tests/fixtures/landing
are the only way to exercise the quarantine path. They never touch data/landing (D10).
"""
from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from pipeline.config import REPO_ROOT
from pipeline.quality_rules import PASSED, QUARANTINE, attach_references, classify, deduplicate
from pipeline.schemas import CORRUPT_RECORD, MASTER_SCHEMAS, USAGE_EVENT_SCHEMA, with_corrupt_column

FIXTURES = REPO_ROOT / "tests" / "fixtures" / "landing"


def read_master(spark, name):
    return spark.read.schema(MASTER_SCHEMAS[name]).options(header=True, escape='"').csv(str(FIXTURES / f"{name}.csv"))


@pytest.fixture(scope="module")
def classified(spark):
    raw = (spark.read.schema(with_corrupt_column(USAGE_EVENT_SCHEMA))
           .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
           .json(str(FIXTURES / "usage_events_stream")))
    # Lines that do not parse never reach Silver: Bronze sends them to Quarantine first.
    parsed = raw.cache().filter(raw[CORRUPT_RECORD].isNull()).drop(CORRUPT_RECORD)
    refs = attach_references(parsed, read_master(spark, "customers_orgs"), read_master(spark, "resources"))
    return classify(refs).cache()


def verdicts(classified):
    return {r.event_id: (r.dq_status, r.rule_name)
            for r in classified.select("event_id", "dq_status", "rule_name").collect()}


@pytest.mark.parametrize("event_id,expected_rule", [
    (None, "event_id_null"),
    ("evt_fixture_ts_null", "timestamp_unparseable"),
    ("evt_fixture_metric", "metric_unknown"),
    ("evt_fixture_unit", "unit_contradicts_metric"),
    ("evt_fixture_value", "value_not_castable"),
    ("evt_fixture_org", "org_id_unknown"),
    ("evt_fixture_res", "resource_id_unknown"),
    ("evt_fixture_owner", "resource_org_mismatch"),
])
def test_defective_records_are_quarantined(classified, event_id, expected_rule):
    assert verdicts(classified)[event_id] == (QUARANTINE, expected_rule)


def test_repairable_record_is_kept_and_flagged(classified):
    """The control case: looks defective, is unambiguous, so it must not be quarantined."""
    row = classified.filter(classified.event_id == "evt_fixture_ok").first()
    assert row.dq_status == PASSED
    assert row.unit_imputed is True
    assert row.unit == "hours", "unit must be derived from metric=cpu_hours"
    assert row.value_double == pytest.approx(4.25), "numeric text must cast"


def test_negative_cost_is_flagged_not_dropped(classified):
    row = classified.filter(classified.event_id == "evt_fixture_neg").first()
    assert row.dq_status == PASSED
    assert row.cost_anomaly_flag is True
    assert row.cost_usd_increment == -5.0


def test_quarantined_rows_are_not_silently_repaired(classified):
    """A rejected row keeps its original unit, so Quarantine shows what actually arrived."""
    row = classified.filter(classified.event_id == "evt_fixture_unit").first()
    assert row.unit == "count"
    assert row.unit_imputed is False


def test_references_enrich_the_event(classified):
    row = classified.filter(classified.event_id == "evt_fixture_ok").first()
    assert (row.org_industry, row.resource_state) == ("Energy", "running")


def test_null_value_does_not_become_zero(spark):
    """D6: a missing measurement contributes null, not zero, to a usage sum."""
    schema = StructType(USAGE_EVENT_SCHEMA.fields + [
        StructField("_org_found", BooleanType()), StructField("_resource_found", BooleanType()),
        StructField("resource_org_id", StringType())])
    df = spark.createDataFrame(
        [Row(event_id="a", timestamp=datetime(2025, 7, 20, 10, 0), org_id="o", resource_id="r",
             service="compute", region="us-east", metric="cpu_hours", value=None, unit="hours",
             cost_usd_increment=5.0, schema_version=2, carbon_kg=None, genai_tokens=None,
             _org_found=True, _resource_found=True, resource_org_id="o")],
        schema=schema,
    )
    row = classify(df).first()
    assert row.value_double is None
    assert row.dq_status == PASSED


def test_deduplicate_keeps_the_first_row_by_order(spark):
    df = spark.createDataFrame([("e1", 2, "b"), ("e1", 1, "a"), ("e2", 5, "c")], "event_id string, ts int, tag string")
    kept = {r.event_id: r.tag for r in deduplicate(df, ["event_id"], ["ts"]).collect()}
    assert kept == {"e1": "a", "e2": "c"}
