"""Silver: conformed masters and events, rebuilt in full from Bronze on every run (D8).

Masters go first because the events join against customers_orgs and resources. A master keeps the
latest row per natural key (D13), gets the D7 repairs and flags, and only a contradiction goes to
Quarantine. Events get the D6 rules, the reference joins, the exact dedupe by event_id and the
per-metric feature columns that Gold sums.
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import lake, quarantine
from .errors import PipelineError
from .quality_rules import (
    CSAT_RANGE, NPS_RANGE, PASSED, QUARANTINE, attach_references, classify,
    deduplicate, out_of_range,
)
from .schemas import MASTER_KEYS, MASTER_SCHEMAS, USAGE_EVENT_SCHEMA

log = logging.getLogger("pipeline.silver")

EVENT_SOURCE_COLUMNS = [f.name for f in USAGE_EVENT_SCHEMA.fields]

O3_LIMIT_PCT = 1.0  # design §1.3

# Repair and flag columns the masters carry; their counts go to the run summary.
MASTER_FLAGS = ["nps_score_invalid", "csat_invalid", "fx_overridden", "credits_imputed",
                "subtotal_negative", "last_login_before_created", "created_before_org_signup",
                "converted_without_click"]


def bronze(ctx, entity: str) -> DataFrame:
    path = ctx.cfg.table("bronze", entity)
    if not lake.exists(path):
        raise PipelineError(f"Bronze {entity} is empty: run bronze-batch and bronze-stream first")
    return lake.read(ctx.spark, path)


def latest(df: DataFrame, entity: str) -> DataFrame:
    return deduplicate(df, MASTER_KEYS[entity], [F.col("ingest_ts").desc(), F.col("source_file").desc()])


def lower_trim(df: DataFrame, *cols) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    return df


def null_and_flag(df: DataFrame, column: str, bounds: tuple) -> DataFrame:
    """Out-of-range value: the field is nulled and flagged, the row stays (D7)."""
    flag = f"{column}_invalid"
    return (df.withColumn(flag, out_of_range(column, bounds))
              .withColumn(column, F.when(F.col(flag), F.lit(None)).otherwise(F.col(column))))


# --- masters -------------------------------------------------------------------------------------

def customers_orgs(df):
    df = lower_trim(df, "hq_region", "plan_tier", "lifecycle_stage", "marketing_source")
    return null_and_flag(df, "nps_score", NPS_RANGE), None


def users(df, orgs, salt: str):
    email = F.lower(F.trim(F.col("email")))
    df = (df.join(orgs.select("org_id", "signup_date"), "org_id", "left")
            # The address is personal data: Silver keeps a salted hash to count and join users and
            # the domain for aggregates. Only Bronze and Landing hold the raw value.
            .withColumn("email_hash", F.sha2(F.concat(F.lit(salt), email), 256))
            .withColumn("email_domain", F.substring_index(email, "@", -1))
            # Both rules fire on about a third of the rows: the timestamps are low-trust and only
            # flagged, the identity fields stay usable (D7).
            .withColumn("last_login_before_created", F.coalesce(F.col("last_login") < F.col("created_at"), F.lit(False)))
            .withColumn("created_before_org_signup", F.coalesce(F.col("created_at") < F.col("signup_date"), F.lit(False)))
            .drop("email", "signup_date"))
    return lower_trim(df, "role"), None


def resources(df):
    df = lower_trim(df, "service", "region", "state")
    return df.withColumn("tags", F.from_json("tags_json", "array<string>")), None


def support_tickets(df):
    contradiction = F.coalesce(F.col("resolved_at") < F.col("created_at"), F.lit(False))
    rejected = df.filter(contradiction).withColumn("rule_name", F.lit("resolved_before_created"))
    kept = lower_trim(df.filter(~contradiction), "category", "severity")
    kept = (null_and_flag(kept, "csat", CSAT_RANGE)
            .withColumn("is_open", F.col("resolved_at").isNull())
            .withColumn("resolution_days", F.datediff("resolved_at", "created_at")))
    return kept, rejected


def marketing_touches(df):
    df = lower_trim(df, "campaign", "channel").withColumnRenamed("timestamp", "touch_date")
    # A view-through conversion is plausible, so this is a flag and not a contradiction (D7).
    return df.withColumn("converted_without_click", F.col("converted") & ~F.col("clicked")), None


def nps_surveys(df):
    return null_and_flag(df, "nps_score", NPS_RANGE), None


def billing_monthly(df):
    rate = F.when(F.col("currency") == "USD", F.lit(1.0)).otherwise(F.col("exchange_rate_to_usd"))
    df = (df.withColumn("currency", F.upper(F.trim("currency")))
            # One USD is one USD: the rate that came with a USD invoice is replaced and flagged (D9).
            .withColumn("fx_overridden", (F.col("currency") == "USD") & (F.col("exchange_rate_to_usd") != 1.0))
            .withColumn("exchange_rate_applied", rate)
            .withColumn("credits_imputed", F.col("credits").isNull())
            .withColumn("credits", F.coalesce("credits", F.lit(0.0)))
            .withColumn("subtotal_negative", F.col("subtotal") < 0))
    # Each invoice is converted with its own rate before anything is summed (D9).
    for col in ("subtotal", "credits", "taxes"):
        df = df.withColumn(f"{col}_usd", F.col(col) * F.col("exchange_rate_applied"))
    return df.withColumn("revenue_usd", F.col("subtotal_usd") - F.col("credits_usd") + F.col("taxes_usd")), None


def build_masters(ctx, metrics) -> dict:
    cfg = ctx.cfg
    # Without a salt the email hash is reversible with a list of candidate addresses (D17), so
    # Silver refuses to write users instead of publishing a weak pseudonym.
    if not cfg.pii_hash_salt:
        raise PipelineError("PII_HASH_SALT is not set: run make setup or set it in config/pipeline.env")
    out, rejected_frames = {}, []
    # customers_orgs first: users are checked against the organization's signup_date.
    transforms = {
        "customers_orgs": customers_orgs,
        "users": lambda df: users(df, out["customers_orgs"], cfg.pii_hash_salt),
        "resources": resources,
        "support_tickets": support_tickets,
        "marketing_touches": marketing_touches,
        "nps_surveys": nps_surveys,
        "billing_monthly": billing_monthly,
    }
    for entity, transform in transforms.items():
        kept, rejected = transform(latest(bronze(ctx, entity), entity))
        kept = (kept.withColumn("processed_ts", F.lit(ctx.run_ts).cast("timestamp"))
                    .withColumn("run_id", F.lit(ctx.run_id)))
        target = cfg.table("silver", entity)
        if entity == "billing_monthly":
            lake.write_partitioned(kept, target, "month")
        else:
            lake.write_table(kept, target)
        out[entity] = lake.read(ctx.spark, target)
        n_rejected = rejected.count() if rejected is not None else 0
        flags = [c for c in MASTER_FLAGS if c in out[entity].columns]
        counts = out[entity].agg(F.count("*").alias("rows"),
                                 *[F.sum(F.col(c).cast("int")).alias(c) for c in flags]).first().asDict()
        metrics[entity] = {**counts, "quarantined": n_rejected}
        if rejected is not None:
            raw = quarantine.raw_json(list(MASTER_SCHEMAS[entity].fieldNames()))
            rejected_frames.append(quarantine.shape(rejected, entity, F.col("rule_name"), raw, ctx.run_id, ctx.run_ts))

    write_quarantine(ctx, rejected_frames, "masters_rules")
    return out


# --- events --------------------------------------------------------------------------------------

def feature(metric: str):
    return F.when(F.col("metric") == metric, F.col("value_double"))


def build_events(ctx, orgs: DataFrame, resources_df: DataFrame, metrics):
    cfg = ctx.cfg
    events = (bronze(ctx, "usage_events")
              # The row as Bronze holds it, kept for Quarantine before anything is conformed.
              .withColumn("raw_record", quarantine.raw_json(EVENT_SOURCE_COLUMNS)))
    events = lower_trim(events, "service", "region", "metric", "unit")
    classified = classify(attach_references(events, orgs, resources_df)).cache()

    rejected = classified.filter(F.col("dq_status") == QUARANTINE)
    passed = classified.filter(F.col("dq_status") == PASSED)
    deduped = deduplicate(passed, ["event_id"], [F.col("ingest_ts"), F.col("source_file")])

    silver = deduped.select(
        "event_id", "timestamp", F.to_date("timestamp").alias("event_date"),
        "org_id", "resource_id", "service", "region", "metric", "value_double", "unit",
        "cost_usd_increment", "schema_version",
        # Features: one column per metric, so a day's sum of requests never mixes in cpu hours.
        feature("requests").alias("requests"),
        feature("cpu_hours").alias("cpu_hours"),
        feature("storage_gb_hours").alias("storage_gb_hours"),
        "genai_tokens", "carbon_kg",
        "org_industry", "org_plan_tier", "org_is_enterprise", "resource_state",
        "unit_imputed", "cost_anomaly_flag", "dq_status",
        "source_file", "ingest_ts",
        F.lit(ctx.run_ts).cast("timestamp").alias("processed_ts"),
        F.lit(ctx.run_id).alias("run_id"),
    )
    lake.write_partitioned(silver, cfg.table("silver", "usage_events"), "event_date", cfg.files_per_partition)
    write_quarantine(ctx, [quarantine.shape(rejected, "usage_events", F.col("rule_name"),
                                            F.col("raw_record"), ctx.run_id, ctx.run_ts)],
                     "usage_events_rules")

    written = lake.read(ctx.spark, cfg.table("silver", "usage_events"))
    m = classified.agg(
        F.count("*").alias("bronze_rows"),
        F.sum(F.when(F.col("dq_status") == QUARANTINE, 1).otherwise(0)).alias("quarantined_rows"),
        F.sum("cost_usd_increment").alias("bronze_cost"),
        F.sum(F.when(F.col("dq_status") == QUARANTINE, F.col("cost_usd_increment"))).alias("quarantined_cost"),
    ).first().asDict()
    s = written.agg(
        F.count("*").alias("silver_rows"),
        F.sum(F.col("unit_imputed").cast("int")).alias("unit_imputed"),
        F.sum(F.col("value_double").isNull().cast("int")).alias("value_null"),
        F.sum(F.col("cost_anomaly_flag").cast("int")).alias("cost_anomaly_flag"),
        # The consigna rule "unit not null when value is present", after imputation.
        F.sum((F.col("value_double").isNotNull() & F.col("unit").isNull()).cast("int")).alias("value_without_unit"),
        F.countDistinct("event_id").alias("distinct_event_id"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("event_id_null"),
        F.countDistinct("event_date").alias("event_dates"),
    ).first().asDict()
    by_rule = {r.rule_name: r["count"] for r in rejected.groupBy("rule_name").count().collect()}
    classified.unpersist()
    m.update(s)
    m["duplicates_removed"] = m["bronze_rows"] - m["quarantined_rows"] - m["silver_rows"]
    m["quarantined_by_rule"] = by_rule
    m["quarantined_cost"] = m["quarantined_cost"] or 0.0
    m["quarantined_cost_pct"] = round(100 * m["quarantined_cost"] / m["bronze_cost"], 4) if m["bronze_cost"] else 0.0
    m["quarantined_rows_pct"] = round(100 * m["quarantined_rows"] / m["bronze_rows"], 4) if m["bronze_rows"] else 0.0
    # O3: Quarantine stays an exception, measured in rows and in cost. A breach does not stop the
    # run (the rows are safe in Quarantine) but it is logged and recorded for whoever reviews it.
    m["o3_breached"] = m["quarantined_rows_pct"] > O3_LIMIT_PCT or m["quarantined_cost_pct"] > O3_LIMIT_PCT
    if m["o3_breached"]:
        log.warning("O3 breached: %.2f%% of events and %.2f%% of cost quarantined (limit %s%%)",
                    m["quarantined_rows_pct"], m["quarantined_cost_pct"], O3_LIMIT_PCT)
    metrics["usage_events"] = m
    log.info("silver usage_events %s", m)


def write_quarantine(ctx, frames: list, entity: str):
    """Rule violations are derived from Bronze like Silver itself, so they are rebuilt in full:
    static overwrite, or a rerun on a later day would keep the previous day's copy."""
    frames = [f for f in frames if f is not None]
    if not frames:
        return
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    (df.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static")
       .partitionBy("quarantine_date").parquet(str(ctx.cfg.table("quarantine", entity))))


def run(ctx, metrics):
    masters = build_masters(ctx, metrics)
    build_events(ctx, masters["customers_orgs"], masters["resources"], metrics)
