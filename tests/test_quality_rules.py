"""Pin the quality rules against synthetic fixtures.

Real Landing data rejects nothing (0 of 43,200 events), so these fixtures are the only way to
exercise the quarantine path. They live in tests/fixtures/quarantine/ and never touch Landing.
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from quality_rules import (  # noqa: E402
    PASSED, QUARANTINE, USAGE_EVENT_SCHEMA, classify, deduplicate,
)

FIXTURE = REPO_ROOT / "tests" / "fixtures" / "quarantine" / "usage_events_invalid.jsonl"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.appName("test-quality-rules").master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def classified(spark):
    raw = spark.read.schema(USAGE_EVENT_SCHEMA).json(str(FIXTURE))
    return classify(raw).cache()


def verdicts(classified):
    """event_id -> (dq_status, dq_rule). The null event_id is keyed as None."""
    return {
        r["event_id"]: (r["dq_status"], r["dq_rule"])
        for r in classified.select("event_id", "dq_status", "dq_rule").collect()
    }


def test_fixture_loads_completely(classified):
    assert classified.count() == 8, "fixture should have 8 records, including the duplicate"


@pytest.mark.parametrize(
    "event_id,expected_rule",
    [
        (None, "event_id_null"),
        ("evt_fixture_metric", "metric_unknown"),
        ("evt_fixture_ts", "timestamp_unparseable"),
        ("evt_fixture_unit", "unit_contradicts_metric"),
        ("evt_fixture_value", "value_not_castable"),
    ],
)
def test_defective_records_are_quarantined(classified, event_id, expected_rule):
    status, rule = verdicts(classified)[event_id]
    assert status == QUARANTINE
    assert rule == expected_rule


def test_repairable_record_is_kept_and_flagged(classified):
    """The control case: looks defective, is unambiguous, so it must NOT be quarantined."""
    row = classified.filter(classified.event_id == "evt_fixture_ok").first()
    assert row["dq_status"] == PASSED
    assert row["unit_imputed"] is True
    assert row["unit"] == "hours", "unit must be derived from metric=cpu_hours"
    assert row["value_double"] == pytest.approx(4.25), "numeric text must cast"


def test_duplicate_event_id_collapses_to_one_row(classified):
    dups = classified.filter(classified.event_id == "evt_fixture_dup")
    assert dups.count() == 2, "both copies survive classification"
    assert deduplicate(dups).count() == 1, "dedupe keeps exactly one"


def test_null_value_does_not_become_zero(spark):
    """D9: a missing measurement contributes null, not zero, to a usage sum."""
    from pyspark.sql import Row
    df = spark.createDataFrame(
        [Row(event_id="a", timestamp=datetime(2025, 7, 20, 10, 0), org_id="o", resource_id="r",
             service="compute",
             region="us-east", metric="cpu_hours", value=None, unit="hours",
             cost_usd_increment=5.0, schema_version=2, carbon_kg=None, genai_tokens=None)],
        schema=USAGE_EVENT_SCHEMA,
    )
    row = classify(df).first()
    assert row["value_double"] is None, "null value must stay null, never 0.0"


def test_quarantined_rows_are_not_silently_repaired(classified):
    """A rejected row keeps its original unit, so Quarantine shows what actually arrived."""
    row = classified.filter(classified.event_id == "evt_fixture_unit").first()
    assert row["unit"] == "count", "the contradicting unit must be preserved for review"
    assert row["unit_imputed"] is False
