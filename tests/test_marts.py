"""Gold marts for Q2 to Q5: windows, ranks, null semantics and the gates that block a bad mart."""
from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from pipeline import marts
from pipeline.errors import PipelineError

DAILY = "org_id string, usage_date date, service string, daily_cost_usd double, events long, genai_tokens long"
D0 = date(2025, 8, 1)


def day(n):
    return D0 + timedelta(days=n)


@pytest.fixture
def daily(spark):
    rows = [("o1", day(n), "compute", 1.0, 1, None) for n in range(20)]   # 1 USD a day, 20 days
    rows += [("o1", day(19), "genai", 5.0, 2, 300),                       # only on the last day
             ("o1", day(19), "storage", 5.0, 1, None),                    # tie with genai
             ("o1", day(0), "database", 50.0, 1, None)]                   # outside the last window
    return spark.createDataFrame(rows, DAILY)


def test_top_services_sums_the_14_days_ending_on_each_cutoff(daily):
    top = {(r.as_of_date, r.service): r for r in marts.top_services_14d(daily).collect()}
    last = top[(day(19), "compute")]
    assert last.cost_14d_usd == 14.0 and last.days_with_usage == 14
    assert last.window_start == day(6) and last.window_complete
    # database cost 50 on day 0: it ranks first on early cutoffs and is gone 14 days later.
    assert top[(day(13), "database")].rank == 1
    assert (day(14), "database") not in top
    assert not top[(day(5), "compute")].window_complete


def test_top_services_ranks_ties_by_service_name(daily):
    ranks = {r.service: r.rank for r in marts.top_services_14d(daily).filter(F.col("as_of_date") == day(19)).collect()}
    assert ranks == {"compute": 1, "genai": 2, "storage": 3}
    report = marts.check_top(marts.top_services_14d(daily), daily)
    assert report["passed"] and report["rank_gaps"] == 0


TICKETS = "ticket_id string, org_id string, severity string, created_at date, sla_breached boolean, is_open boolean, csat double"


@pytest.fixture
def tickets(spark):
    return spark.createDataFrame([
        ("t1", "o1", "critical", D0, True, False, 4.0),
        ("t2", "o1", "low", D0, False, True, None),       # open, no answer
        ("t3", "o1", "critical", D0, False, False, 2.0),
        ("t4", "o2", "high", D0, None, False, None),      # breach unknown: left out of the rate
    ], TICKETS)


def test_tickets_daily_counts_critical_and_breach_rate(tickets):
    rows = {r.org_id: r for r in marts.tickets_daily(tickets).collect()}
    o1 = rows["o1"]
    assert (o1.tickets, o1.critical_tickets, o1.sla_breached_tickets, o1.critical_sla_breached) == (3, 2, 1, 1)
    assert o1.sla_breach_rate == pytest.approx(1 / 3, abs=1e-4)
    assert o1.csat_avg == 3.0 and o1.csat_responses == 2 and o1.open_tickets == 1
    o2 = rows["o2"]
    assert o2.sla_breach_rate is None and o2.csat_avg is None


def test_tickets_gate_blocks_a_mart_that_lost_tickets(tickets):
    mart = marts.tickets_by_org_date(tickets)
    assert marts.check_tickets("tickets_by_org_date", mart, tickets)["passed"]
    with pytest.raises(PipelineError, match="ticket count differs"):
        marts.check_tickets("tickets_by_org_date", mart.filter(F.col("severity") != "high"), tickets)


BILLING = ("invoice_id string, org_id string, month date, currency string, exchange_rate_applied double, "
           "subtotal_usd double, credits_usd double, taxes_usd double, revenue_usd double, "
           "fx_overridden boolean, credits_imputed boolean, subtotal_negative boolean")


def test_revenue_sums_converted_invoices_and_marks_mixed_currency(spark):
    billing = spark.createDataFrame([
        ("i1", "o1", D0, "USD", 1.0, 100.0, 10.0, 21.0, 111.0, True, False, False),
        ("i2", "o1", D0, "EUR", 1.1, 110.0, 0.0, 0.0, 110.0, False, True, False),
        ("i3", "o2", D0, "BRL", 0.2, 20.0, 0.0, 4.0, 24.0, False, False, False),
    ], BILLING)
    rows = {r.org_id: r for r in marts.revenue_by_org_month(billing).collect()}
    assert rows["o1"].revenue_usd == 221.0 and rows["o1"].invoices == 2
    assert rows["o1"].currency == "MIXED" and rows["o1"].exchange_rate_applied is None
    assert rows["o1"].fx_overridden_invoices == 1 and rows["o1"].credits_imputed_invoices == 1
    assert rows["o2"].currency == "BRL" and rows["o2"].exchange_rate_applied == 0.2
    assert marts.check_revenue(marts.revenue_by_org_month(billing), billing)["passed"]


EVENTS = "org_id string, event_date date, service string, cost_usd_increment double, genai_tokens long"


def test_genai_day_without_tokens_keeps_null_tokens(spark):
    events = spark.createDataFrame([
        ("o1", D0, "genai", 2.0, 1000), ("o1", D0, "genai", 1.0, None),   # v2 with tokens, v1 without
        ("o1", day(1), "genai", 3.0, None),                              # only v1 that day
        ("o1", D0, "compute", 9.0, None),                                # not GenAI
    ], EVENTS)
    rows = {r.usage_date: r for r in marts.genai_tokens_by_org_date(events).collect()}
    first = rows[D0]
    assert (first.genai_tokens, first.genai_cost_usd, first.token_cost_usd) == (1000, 3.0, 2.0)
    assert first.usd_per_1k_tokens == 2.0 and first.events_with_tokens == 1
    second = rows[day(1)]
    assert second.genai_tokens is None and second.usd_per_1k_tokens is None and second.genai_cost_usd == 3.0


def test_genai_gate_compares_with_the_daily_mart(spark, daily):
    events = spark.createDataFrame([("o1", day(19), "genai", 5.0, 300)], EVENTS)
    mart = marts.genai_tokens_by_org_date(events)
    assert marts.check_genai(mart, daily)["passed"]
    with pytest.raises(PipelineError, match="tokens or cost differ"):
        marts.check_genai(mart.withColumn("genai_tokens", F.lit(301).cast("long")), daily)
