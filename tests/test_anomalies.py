"""cost_anomaly_mart: modified z-score with MAD, its fallback when MAD is 0, and the history rule."""
from datetime import date, timedelta

import pytest

from pipeline.anomalies import score

START = date(2025, 7, 1)


def series(spark, costs, org="o1", service="compute"):
    rows = [(org, START + timedelta(days=i), service, c) for i, c in enumerate(costs)]
    return spark.createDataFrame(rows, "org_id string, usage_date date, service string, daily_cost_usd double")


def by_day(mart):
    return {(r.usage_date - START).days: r for r in mart.collect()}


def test_spike_is_flagged_and_normal_days_are_not(spark):
    costs = [10.0, 11.0, 9.0, 10.5, 9.5] * 4 + [60.0]
    days = by_day(score(series(spark, costs), window_days=28, min_history=14, threshold=3.5))
    spike = days[20]
    assert spike.score_method == "mad" and spike.is_anomaly and spike.anomaly_direction == "spike"
    # median 10, MAD 0.5: 0.6745 * 50 / 0.5
    assert spike.anomaly_score == pytest.approx(67.45)
    assert not any(r.is_anomaly for d, r in days.items() if d < 20)


def test_days_without_enough_history_are_not_scored(spark):
    days = by_day(score(series(spark, [10.0] * 5), window_days=28, min_history=14, threshold=3.5))
    assert all(r.score_method == "insufficient_history" and r.anomaly_score is None for r in days.values())
    assert days[0].history_days == 0


def test_mad_zero_falls_back_to_mean_absolute_deviation(spark):
    # 15 days of history, 10 at exactly 10.0: the median deviation is 0.
    costs = [10.0] * 10 + [12.0, 8.0, 12.0, 8.0, 12.0] + [30.0]
    last = by_day(score(series(spark, costs), window_days=28, min_history=14, threshold=3.5))[15]
    assert last.mad_cost == 0.0 and last.score_method == "mean_ad"
    # mean absolute deviation is 10/15; score = 20 / (1.253314 * 0.6667)
    assert last.anomaly_score == pytest.approx(20 / (1.253314 * 10 / 15), rel=1e-3)
    assert last.is_anomaly


def test_window_only_looks_back(spark):
    costs = [10.0, 11.0, 9.0, 10.5, 9.5] * 4
    days = by_day(score(series(spark, costs + [500.0]), window_days=28, min_history=14, threshold=3.5))
    # Day 15 must score the same whether or not day 20 exists.
    alone = by_day(score(series(spark, costs), window_days=28, min_history=14, threshold=3.5))
    assert days[15].anomaly_score == alone[15].anomaly_score
