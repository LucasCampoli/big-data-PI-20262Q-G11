"""cost_anomaly_mart: a robust score of each day's cost against the recent history of the same
organization and service (D12).

score = 0.6745 * (x - median) / MAD over the previous `window_days` days on which the service was
used, the modified z-score of Iglewicz and Hoaglin. Median and MAD barely move with the long tail of
this data (p99 16.69 against a max of 317.43), where a mean and a standard deviation would be pulled
by the very spikes they are meant to find. When MAD is 0, more than half of the history is the same
value, and the score falls back to the mean absolute deviation (scaled by 1.2533 so both estimate the
same sigma). With less than `min_history` days of history the day is not scored.

Only past days count, so the score of a day never depends on later data and the same job can run
every morning on D+1.
"""
import json
import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from . import lake
from .errors import PipelineError

log = logging.getLogger("pipeline.anomalies")

MAD_SCALE = 0.6745        # makes MAD comparable with a standard deviation under normality
MEAN_AD_SCALE = 1.253314  # same for the mean absolute deviation, used when MAD is 0


def array_median(values: Column) -> Column:
    s = F.array_sort(values)
    n = F.size(s)
    half = F.floor(n / 2).cast("int")
    odd = F.element_at(s, half + 1)
    even = (F.element_at(s, F.greatest(half, F.lit(1))) + F.element_at(s, half + 1)) / 2
    return F.when(n == 0, F.lit(None)).when(n % 2 == 1, odd).otherwise(even)


def score(daily: DataFrame, window_days: int, min_history: int, threshold: float) -> DataFrame:
    cur = daily.select("org_id", "service", "usage_date", "daily_cost_usd").alias("c")
    past = daily.select("org_id", "service", "usage_date", "daily_cost_usd").alias("h")
    # Each day joins the days of its trailing window, about 18 rows per day at this density. The
    # window is part of the left join condition so a day without history is kept, unscored.
    in_window = ((F.col("c.org_id") == F.col("h.org_id")) & (F.col("c.service") == F.col("h.service"))
                 & (F.col("h.usage_date") < F.col("c.usage_date"))
                 & (F.col("h.usage_date") >= F.date_sub(F.col("c.usage_date"), window_days)))
    history = cur.join(past, in_window, "left").select(
        "c.org_id", "c.service", "c.usage_date", "c.daily_cost_usd", F.col("h.daily_cost_usd").alias("h_cost"))
    stats = (history.groupBy("org_id", "service", "usage_date", "daily_cost_usd")
             .agg(F.collect_list("h_cost").alias("hist"))
             .withColumn("history_days", F.size("hist"))
             .withColumn("median_cost", array_median(F.col("hist")))
             .withColumn("deviations", F.transform("hist", lambda v: F.abs(v - F.col("median_cost"))))
             .withColumn("mad_cost", array_median(F.col("deviations")))
             .withColumn("mean_ad_cost", F.when(F.col("history_days") > 0,
                         F.aggregate("deviations", F.lit(0.0), lambda acc, v: acc + v) / F.col("history_days"))))

    x, med = F.col("daily_cost_usd"), F.col("median_cost")
    scored = F.col("history_days") >= min_history
    method = (F.when(~scored, F.lit("insufficient_history"))
               .when(F.col("mad_cost") > 0, F.lit("mad"))
               .when(F.col("mean_ad_cost") > 0, F.lit("mean_ad"))
               .otherwise(F.lit("constant_history")))
    value = (F.when(method == "mad", MAD_SCALE * (x - med) / F.col("mad_cost"))
              .when(method == "mean_ad", (x - med) / (MEAN_AD_SCALE * F.col("mean_ad_cost"))))
    return (stats
            .withColumn("score_method", method)
            .withColumn("anomaly_score", F.round(value, 4))
            .withColumn("is_anomaly", F.coalesce(F.abs(F.col("anomaly_score")) > threshold, F.lit(False)))
            .withColumn("anomaly_direction", F.when(~F.col("is_anomaly"), F.lit(None))
                                              .when(F.col("anomaly_score") > 0, F.lit("spike"))
                                              .otherwise(F.lit("drop")))
            .select("org_id", "usage_date", "service", "daily_cost_usd", "history_days",
                    F.round("median_cost", 4).alias("median_cost"), F.round("mad_cost", 4).alias("mad_cost"),
                    "score_method", "anomaly_score", "is_anomaly", "anomaly_direction"))


def summarize(mart: DataFrame, threshold: float) -> dict:
    total = mart.count()
    by_method = {r.score_method: r["count"] for r in mart.groupBy("score_method").count().collect()}
    flagged = mart.filter("is_anomaly")
    by_direction = {r.anomaly_direction: r["count"] for r in flagged.groupBy("anomaly_direction").count().collect()}
    scored = total - by_method.get("insufficient_history", 0)
    top = [r.asDict() for r in flagged.orderBy(F.abs("anomaly_score").desc()).limit(10)
           .select("org_id", "usage_date", "service", "daily_cost_usd", "median_cost", "mad_cost",
                   "anomaly_score", "anomaly_direction").collect()]
    return {"rows": total, "scored_rows": scored, "by_method": by_method, "threshold": threshold,
            "flagged": flagged.count(), "flagged_by_direction": by_direction,
            "flagged_pct_of_scored": round(100 * flagged.count() / scored, 2) if scored else 0.0,
            "orgs_with_flags": flagged.select("org_id").distinct().count(),
            "top": top}


def run(ctx, metrics):
    cfg = ctx.cfg
    gold_path = cfg.table("gold", "org_daily_usage_by_service")
    if not lake.exists(gold_path):
        raise PipelineError("Gold org_daily_usage_by_service is empty: run gold first")
    daily = lake.read(ctx.spark, gold_path)
    mart = (score(daily, cfg.anomaly_window_days, cfg.anomaly_min_history, cfg.anomaly_threshold)
            .withColumn("computed_ts", F.lit(ctx.run_ts).cast("timestamp"))
            .withColumn("run_id", F.lit(ctx.run_id)))
    lake.write_partitioned(mart, cfg.table("gold", "cost_anomaly_mart"), "usage_date", cfg.files_per_partition)

    written = lake.read(ctx.spark, cfg.table("gold", "cost_anomaly_mart"))
    summary = summarize(written, cfg.anomaly_threshold)
    summary.update(window_days=cfg.anomaly_window_days, min_history=cfg.anomaly_min_history)
    (cfg.logs / f"{ctx.run_id}_anomaly_summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    metrics["cost_anomaly_mart"] = {k: v for k, v in summary.items() if k != "top"}
    log.info("cost_anomaly_mart %s", metrics["cost_anomaly_mart"])
