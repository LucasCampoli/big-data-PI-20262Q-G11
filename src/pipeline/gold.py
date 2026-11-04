"""Gold: the FinOps mart org_daily_usage_by_service, one row per (org_id, usage_date, service).

This is the job design §7 writes as MapReduce. The key is the mart grain; Spark's sum ignores nulls
and returns null when every input is null, which is the (sum, count) pair of §7.1 for free, so a day
with no usable requests reports null and not 0. The mart is reconciled against Silver before it is
written, and the plan Spark chose is kept for the evidence.
"""
import io
import json
import logging
from contextlib import redirect_stdout

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import lake
from .errors import PipelineError, QualityGateError

log = logging.getLogger("pipeline.gold")

GRAIN = ["org_id", "usage_date", "service"]
COST_TOLERANCE_USD = 0.01  # per organization and day (design §5, D9)


def daily_usage(silver_events: DataFrame, run_id: str, run_ts) -> DataFrame:
    return (silver_events
            .groupBy("org_id", F.col("event_date").alias("usage_date"), "service")
            .agg(F.round(F.sum("cost_usd_increment"), 4).alias("daily_cost_usd"),
                 F.sum("requests").alias("requests"),
                 F.sum("cpu_hours").alias("cpu_hours"),
                 F.sum("storage_gb_hours").alias("storage_gb_hours"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.sum("carbon_kg").alias("carbon_kg"),
                 F.count("*").alias("events"),
                 # Negative increments stay in the cost sum as real corrections; the count shows how
                 # many a day relies on (design §7.5).
                 F.sum(F.col("cost_anomaly_flag").cast("int")).alias("negative_cost_events"),
                 F.sum(F.col("unit_imputed").cast("int")).alias("unit_imputed_events"))
            .withColumn("computed_ts", F.lit(run_ts).cast("timestamp"))
            .withColumn("run_id", F.lit(run_id)))


def reconcile(gold: DataFrame, silver_events: DataFrame) -> dict:
    """The promotion gate of Gold (design §5): keys not null, one row per grain key, and cost per
    organization and day within 0.01 USD of Silver."""
    silver_day = silver_events.groupBy("org_id", F.col("event_date").alias("usage_date")) \
                              .agg(F.sum("cost_usd_increment").alias("silver_cost"))
    gold_day = gold.groupBy("org_id", "usage_date").agg(F.sum("daily_cost_usd").alias("gold_cost"))
    diff = silver_day.join(gold_day, ["org_id", "usage_date"], "full_outer") \
                     .withColumn("abs_diff", F.abs(F.coalesce("silver_cost", F.lit(0.0)) - F.coalesce("gold_cost", F.lit(0.0))))
    d = diff.agg(F.count("*").alias("org_days"),
                 F.max("abs_diff").alias("max_abs_diff_usd"),
                 F.sum((F.col("abs_diff") > COST_TOLERANCE_USD).cast("int")).alias("org_days_over_tolerance")).first()
    report = {
        "gold_rows": gold.count(),
        "silver_grain_keys": silver_events.select("org_id", "event_date", "service").distinct().count(),
        "null_keys": gold.filter(" OR ".join(f"{c} IS NULL" for c in GRAIN)).count(),
        "events_in_gold": gold.agg(F.sum("events")).first()[0],
        "silver_rows": silver_events.count(),
        "org_days": d.org_days,
        "max_abs_diff_usd": round(d.max_abs_diff_usd or 0.0, 6),
        "org_days_over_tolerance": d.org_days_over_tolerance,
        "tolerance_usd": COST_TOLERANCE_USD,
    }
    failures = []
    if report["null_keys"]:
        failures.append("null grain keys")
    if report["gold_rows"] != report["silver_grain_keys"]:
        failures.append("row count differs from the Silver grain")
    if report["events_in_gold"] != report["silver_rows"]:
        failures.append("events in Gold differ from Silver rows")
    if report["org_days_over_tolerance"]:
        failures.append(f"{report['org_days_over_tolerance']} org-days over {COST_TOLERANCE_USD} USD")
    report["passed"] = not failures
    report["failures"] = failures
    return report


def plan_text(df: DataFrame, mode: str = "formatted") -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def run(ctx, metrics):
    cfg, spark = ctx.cfg, ctx.spark
    silver_path = cfg.table("silver", "usage_events")
    if not lake.exists(silver_path):
        raise PipelineError("Silver usage_events is empty: run silver first")
    silver_events = lake.read(spark, silver_path)
    daily = daily_usage(silver_events, ctx.run_id, ctx.run_ts)

    cfg.logs.mkdir(parents=True, exist_ok=True)
    (cfg.logs / f"{ctx.run_id}_gold_explain.txt").write_text(plan_text(daily))

    daily = daily.cache()
    report = reconcile(daily, silver_events)
    (cfg.logs / f"{ctx.run_id}_gold_reconciliation.json").write_text(json.dumps(report, indent=2) + "\n")
    metrics["org_daily_usage_by_service"] = report
    log.info("gold reconciliation %s", report)
    if not report["passed"]:
        raise QualityGateError(f"org_daily_usage_by_service not published: {'; '.join(report['failures'])}")

    lake.write_partitioned(daily, cfg.table("gold", "org_daily_usage_by_service"), "usage_date",
                           cfg.files_per_partition)
    daily.unpersist()
