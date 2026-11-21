"""Gold marts for Q2 to Q5, the queries the daily mart does not answer by itself.

| mart                       | grain                              | query |
| top_services_14d_by_org    | org_id, as_of_date, rank           | Q2    |
| tickets_by_org_date        | org_id, ticket_date, severity      | -     |
| tickets_daily_by_org       | org_id, ticket_date                | Q3    |
| revenue_by_org_month       | org_id, month                      | Q4    |
| genai_tokens_by_org_date   | org_id, usage_date                 | Q5    |

Every serving table is precomputed here with exactly the rows its query reads, so Cassandra only
slices one partition and the client neither sums nor sorts (D11). tickets_by_org_date keeps the
grain of the brief (§7.3); Q3 asks for one line per day, which is tickets_daily_by_org.

Each mart is rebuilt in full on every run, like Silver (D8), and checked against its source before
it is written: keys not null, one row per grain key, and its totals equal to the source's.
"""
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import lake
from .errors import PipelineError

log = logging.getLogger("pipeline.marts")

TOP_WINDOW_DAYS = 14
TOLERANCE_USD = 0.01

GRAINS = {
    "top_services_14d_by_org": ["org_id", "as_of_date", "rank"],
    "tickets_by_org_date": ["org_id", "ticket_date", "severity"],
    "tickets_daily_by_org": ["org_id", "ticket_date"],
    "revenue_by_org_month": ["org_id", "month"],
    "genai_tokens_by_org_date": ["org_id", "usage_date"],
}


def top_services_14d(daily: DataFrame) -> DataFrame:
    """Q2 precomputed: for every cutoff date, each service's cost over the 14 days ending on it,
    ranked within the organization. Reading the top N is then one partition slice with LIMIT N."""
    first_day = daily.agg(F.min("usage_date")).first()[0]
    cutoffs = daily.select(F.col("usage_date").alias("as_of_date")).distinct()
    in_window = (F.col("d.usage_date") <= F.col("c.as_of_date")) & \
                (F.col("d.usage_date") > F.date_sub(F.col("c.as_of_date"), TOP_WINDOW_DAYS))
    # 60 cutoff dates: Spark broadcasts them and the range join stays a nested loop over 11.050 rows.
    windowed = daily.alias("d").join(F.broadcast(cutoffs.alias("c")), in_window)
    totals = (windowed.groupBy("org_id", "as_of_date", "service")
                      .agg(F.round(F.sum("daily_cost_usd"), 4).alias("cost_14d_usd"),
                           F.count("*").alias("days_with_usage"),
                           F.sum("events").alias("events")))
    # Ties are broken by service name so a rerun ranks the same way.
    order = Window.partitionBy("org_id", "as_of_date").orderBy(F.col("cost_14d_usd").desc_nulls_last(), "service")
    return (totals.withColumn("rank", F.row_number().over(order))
                  .withColumn("window_start", F.date_sub("as_of_date", TOP_WINDOW_DAYS - 1))
                  # The first 13 cutoffs have less than 14 days of data behind them.
                  .withColumn("window_complete", F.col("window_start") >= F.lit(first_day)))


def tickets_by_org_date(tickets: DataFrame) -> DataFrame:
    return (tickets.groupBy("org_id", F.col("created_at").alias("ticket_date"), "severity")
                   .agg(*ticket_measures())
                   .withColumn("sla_breach_rate", breach_rate()))


def tickets_daily(tickets: DataFrame) -> DataFrame:
    critical = F.col("severity") == "critical"
    return (tickets.groupBy("org_id", F.col("created_at").alias("ticket_date"))
                   .agg(*ticket_measures(),
                        F.sum(critical.cast("int")).alias("critical_tickets"),
                        F.sum((critical & F.col("sla_breached")).cast("int")).alias("critical_sla_breached"))
                   .withColumn("sla_breach_rate", breach_rate()))


def ticket_measures():
    # A ticket counts on the day it was opened, the only date every ticket has (D18).
    return [F.count("*").alias("tickets"),
            F.sum(F.col("sla_breached").cast("int")).alias("sla_breached_tickets"),
            F.count("sla_breached").alias("sla_known_tickets"),
            F.sum(F.col("is_open").cast("int")).alias("open_tickets"),
            # csat outside 1-5 was nulled in Silver (D7): the average covers valid answers only.
            F.count("csat").alias("csat_responses"),
            F.round(F.avg("csat"), 2).alias("csat_avg")]


def breach_rate():
    return F.when(F.col("sla_known_tickets") > 0,
                  F.round(F.col("sla_breached_tickets") / F.col("sla_known_tickets"), 4))


def revenue_by_org_month(billing: DataFrame) -> DataFrame:
    """Q4: invoices were converted one by one with their own rate in Silver (D9); here they are only
    summed. currency and exchange_rate_applied are filled when the month has a single value."""
    one = lambda c: F.when(F.countDistinct(c) == 1, F.min(c))
    return (billing.groupBy("org_id", "month")
                   .agg(F.count("*").alias("invoices"),
                        F.round(F.sum("subtotal_usd"), 2).alias("subtotal_usd"),
                        F.round(F.sum("credits_usd"), 2).alias("credits_usd"),
                        F.round(F.sum("taxes_usd"), 2).alias("taxes_usd"),
                        F.round(F.sum("revenue_usd"), 2).alias("revenue_usd"),
                        F.coalesce(one("currency"), F.lit("MIXED")).alias("currency"),
                        one("exchange_rate_applied").alias("exchange_rate_applied"),
                        F.sum(F.col("fx_overridden").cast("int")).alias("fx_overridden_invoices"),
                        F.sum(F.col("credits_imputed").cast("int")).alias("credits_imputed_invoices"),
                        F.sum(F.col("subtotal_negative").cast("int")).alias("negative_subtotal_invoices")))


def genai_tokens_by_org_date(events: DataFrame) -> DataFrame:
    """Q5: one row per organization and day with GenAI usage. Only schema v2 GenAI events carry
    tokens, so a day with only v1 events has cost and null tokens, never 0 tokens (D18)."""
    with_tokens = F.col("genai_tokens").isNotNull()
    return (events.filter(F.col("service") == "genai")
                  .groupBy("org_id", F.col("event_date").alias("usage_date"))
                  .agg(F.sum("genai_tokens").alias("genai_tokens"),
                       F.round(F.sum("cost_usd_increment"), 4).alias("genai_cost_usd"),
                       F.round(F.sum(F.when(with_tokens, F.col("cost_usd_increment"))), 4).alias("token_cost_usd"),
                       F.count("*").alias("events"),
                       F.sum(with_tokens.cast("int")).alias("events_with_tokens"))
                  .withColumn("usd_per_1k_tokens",
                              F.when(F.col("genai_tokens") > 0,
                                     F.round(1000 * F.col("token_cost_usd") / F.col("genai_tokens"), 6))))


# --- checks ---------------------------------------------------------------------------------------

def key_checks(df: DataFrame, grain: list) -> dict:
    return {"rows": df.count(),
            "distinct_keys": df.select(*grain).distinct().count(),
            "null_keys": df.filter(" OR ".join(f"{c} IS NULL" for c in grain)).count()}


def max_diff(a: DataFrame, b: DataFrame, keys: list, col_a: str, col_b: str) -> float:
    joined = a.join(b, keys, "full_outer")
    diff = F.abs(F.coalesce(F.col(col_a), F.lit(0.0)) - F.coalesce(F.col(col_b), F.lit(0.0)))
    return round(joined.agg(F.max(diff)).first()[0] or 0.0, 6)


def check(name: str, report: dict, totals_ok: bool, detail: str) -> dict:
    failures = []
    if report["null_keys"]:
        failures.append("null grain keys")
    if report["rows"] != report["distinct_keys"]:
        failures.append("more than one row per grain key")
    if not totals_ok:
        failures.append(detail)
    report.update(passed=not failures, failures=failures)
    if failures:
        raise PipelineError(f"{name} not published: {'; '.join(failures)}")
    return report


def check_top(top: DataFrame, daily: DataFrame) -> dict:
    report = key_checks(top, GRAINS["top_services_14d_by_org"])
    last = daily.agg(F.max("usage_date")).first()[0]
    expected = (daily.filter(F.col("usage_date") > F.date_sub(F.lit(last), TOP_WINDOW_DAYS))
                     .groupBy("org_id").agg(F.sum("daily_cost_usd").alias("daily_14d")))
    served = top.filter(F.col("as_of_date") == F.lit(last)).groupBy("org_id").agg(F.sum("cost_14d_usd").alias("top_14d"))
    # Ranks must be 1..n without gaps inside each (org_id, as_of_date).
    gaps = (top.groupBy("org_id", "as_of_date").agg(F.max("rank").alias("m"), F.count("*").alias("n"))
               .filter("m != n").count())
    report.update(last_cutoff=str(last), max_abs_diff_usd=max_diff(expected, served, ["org_id"], "daily_14d", "top_14d"),
                  rank_gaps=gaps)
    ok = report["max_abs_diff_usd"] <= TOLERANCE_USD and gaps == 0
    return check("top_services_14d_by_org", report, ok, "14-day totals differ from the daily mart or ranks have gaps")


def check_tickets(name: str, mart: DataFrame, tickets: DataFrame) -> dict:
    report = key_checks(mart, GRAINS[name])
    report.update(tickets_in_mart=mart.agg(F.sum("tickets")).first()[0] or 0, silver_tickets=tickets.count())
    return check(name, report, report["tickets_in_mart"] == report["silver_tickets"], "ticket count differs from Silver")


def check_revenue(mart: DataFrame, billing: DataFrame) -> dict:
    report = key_checks(mart, GRAINS["revenue_by_org_month"])
    silver = billing.groupBy("org_id", "month").agg(F.sum("revenue_usd").alias("silver_revenue"))
    report.update(invoices_in_mart=mart.agg(F.sum("invoices")).first()[0] or 0, silver_invoices=billing.count(),
                  max_abs_diff_usd=max_diff(silver, mart, ["org_id", "month"], "silver_revenue", "revenue_usd"))
    ok = report["invoices_in_mart"] == report["silver_invoices"] and report["max_abs_diff_usd"] <= TOLERANCE_USD
    return check("revenue_by_org_month", report, ok, "invoices or revenue differ from Silver")


def check_genai(mart: DataFrame, daily: DataFrame) -> dict:
    """Built from Silver, compared with the GenAI rows of the daily mart: two paths, one answer."""
    report = key_checks(mart, GRAINS["genai_tokens_by_org_date"])
    genai = daily.filter(F.col("service") == "genai").select(
        "org_id", "usage_date", F.col("daily_cost_usd").alias("daily_cost"), F.col("genai_tokens").alias("daily_tokens"))
    joined = genai.join(mart, ["org_id", "usage_date"], "full_outer")
    report.update(
        daily_mart_rows=genai.count(),
        max_abs_diff_usd=max_diff(genai, mart, ["org_id", "usage_date"], "daily_cost", "genai_cost_usd"),
        token_mismatches=joined.filter(~F.col("daily_tokens").eqNullSafe(F.col("genai_tokens"))).count())
    ok = (report["rows"] == report["daily_mart_rows"] and report["token_mismatches"] == 0
          and report["max_abs_diff_usd"] <= TOLERANCE_USD)
    return check("genai_tokens_by_org_date", report, ok, "tokens or cost differ from the daily mart")


def stamp(df: DataFrame, ctx) -> DataFrame:
    return (df.withColumn("computed_ts", F.lit(ctx.run_ts).cast("timestamp"))
              .withColumn("run_id", F.lit(ctx.run_id)))


def source(ctx, zone: str, entity: str) -> DataFrame:
    path = ctx.cfg.table(zone, entity)
    if not lake.exists(path):
        raise PipelineError(f"{zone}/{entity} is empty: run silver and gold first")
    return lake.read(ctx.spark, path)


def run(ctx, metrics):
    cfg = ctx.cfg
    daily = source(ctx, "gold", "org_daily_usage_by_service").cache()
    tickets = source(ctx, "silver", "support_tickets").cache()
    billing = source(ctx, "silver", "billing_monthly")
    events = source(ctx, "silver", "usage_events")

    # (name, mart, check, partition column or None). The small marts are a few KB each: one file
    # per table instead of a hundred files of a few rows (design §5.1).
    builds = [
        ("top_services_14d_by_org", top_services_14d(daily), lambda m: check_top(m, daily), "as_of_date"),
        ("tickets_by_org_date", tickets_by_org_date(tickets),
         lambda m: check_tickets("tickets_by_org_date", m, tickets), None),
        ("tickets_daily_by_org", tickets_daily(tickets),
         lambda m: check_tickets("tickets_daily_by_org", m, tickets), None),
        ("revenue_by_org_month", revenue_by_org_month(billing), lambda m: check_revenue(m, billing), "month"),
        ("genai_tokens_by_org_date", genai_tokens_by_org_date(events), lambda m: check_genai(m, daily), None),
    ]
    for name, mart, checker, partition in builds:
        mart = stamp(mart, ctx).cache()
        report = checker(mart)
        metrics[name] = report
        log.info("%s %s", name, report)
        target = cfg.table("gold", name)
        if partition:
            lake.write_partitioned(mart, target, partition, cfg.files_per_partition)
        else:
            lake.write_table(mart, target)
        mart.unpersist()
    daily.unpersist()
    tickets.unpersist()
