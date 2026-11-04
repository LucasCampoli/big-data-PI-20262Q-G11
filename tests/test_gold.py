"""Gold: null semantics of the daily mart and the reconciliation gate."""
from datetime import date

import pytest

from pipeline.gold import daily_usage, reconcile

COLS = ("org_id string, event_date date, service string, cost_usd_increment double, requests double, "
        "cpu_hours double, storage_gb_hours double, genai_tokens bigint, carbon_kg double, "
        "cost_anomaly_flag boolean, unit_imputed boolean")
D = date(2025, 7, 20)


@pytest.fixture
def silver(spark):
    rows = [
        # two requests events, one with a null value: the sum ignores it, it does not count as 0
        ("o1", D, "compute", 1.0, 10.0, None, None, None, 0.1, False, False),
        ("o1", D, "compute", 2.0, None, None, None, None, None, False, True),
        # only cpu events: requests must stay null for this key, never 0
        ("o1", D, "storage", -3.0, None, 2.5, None, None, None, True, False),
        ("o2", D, "genai", 4.0, 5.0, None, None, 900, 0.2, False, False),
    ]
    return spark.createDataFrame(rows, COLS)


def test_daily_mart_keeps_nulls_as_null(silver):
    rows = {(r.org_id, r.service): r for r in daily_usage(silver, "r", None).collect()}
    compute = rows[("o1", "compute")]
    assert compute.daily_cost_usd == 3.0 and compute.requests == 10.0 and compute.events == 2
    assert compute.cpu_hours is None and compute.genai_tokens is None
    assert compute.unit_imputed_events == 1
    storage = rows[("o1", "storage")]
    assert storage.requests is None and storage.carbon_kg is None
    assert storage.daily_cost_usd == -3.0 and storage.negative_cost_events == 1
    assert rows[("o2", "genai")].genai_tokens == 900


def test_reconciliation_passes_on_the_real_aggregate(silver):
    report = reconcile(daily_usage(silver, "r", None), silver)
    assert report["passed"], report
    assert report["gold_rows"] == 3 and report["events_in_gold"] == 4


def test_reconciliation_blocks_a_mart_that_lost_cost(silver):
    from pyspark.sql import functions as F
    broken = daily_usage(silver, "r", None).withColumn(
        "daily_cost_usd", F.when(F.col("service") == "genai", F.col("daily_cost_usd") + 0.05)
                           .otherwise(F.col("daily_cost_usd")))
    report = reconcile(broken, silver)
    assert not report["passed"]
    assert report["org_days_over_tolerance"] == 1
