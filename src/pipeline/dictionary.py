"""Data dictionary, read back from the Parquet files of the lake (brief §7.5).

python -m pipeline.dictionary [--out FILE]      (make dictionary, default docs/data_dictionary.md)

Every table of every zone with its columns, types and partition column, as stored. The schema comes
from the files, so the dictionary cannot drift from the code; the notes say what a column means.
"""
import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import lake
from .config import REPO_ROOT, Config
from .spark import build_session

log = logging.getLogger("pipeline.dictionary")

TABLE_NOTES = {
    "bronze/usage_events": "Parsed events as they arrived, deduplicated within the watermark (D14). One row per event.",
    "silver/usage_events": "Conformed events: rules, imputation, reference joins, exact dedupe by event_id (D6). One row per event.",
    "gold/org_daily_usage_by_service": "FinOps daily mart, Q1. Grain org_id, usage_date, service.",
    "gold/cost_anomaly_mart": "Cost anomaly score per day, Q6 (D12). Grain org_id, usage_date, service.",
    "gold/top_services_14d_by_org": "Q2 precomputed: 14-day cost per service, ranked, for every cutoff date (D11). Grain org_id, as_of_date, rank.",
    "gold/tickets_by_org_date": "Support mart of the brief (§7.3). Grain org_id, ticket_date, severity.",
    "gold/tickets_daily_by_org": "Q3: one row per organization and day with tickets (D18). Grain org_id, ticket_date.",
    "gold/revenue_by_org_month": "Q4: revenue in USD after credits and taxes (D9). Grain org_id, month.",
    "gold/genai_tokens_by_org_date": "Q5: GenAI tokens and cost per day with GenAI usage (D18). Grain org_id, usage_date.",
}

COLUMN_NOTES = {
    # source columns
    "org_id": "organization (tenant) identifier; partition key of every serving table",
    "org_name": "organization name", "industry": "industry of the organization",
    "hq_region": "region of the organization's headquarters", "plan_tier": "commercial plan",
    "is_enterprise": "enterprise customer", "signup_date": "date the organization signed up",
    "sales_rep": "assigned sales representative", "lifecycle_stage": "customer lifecycle stage",
    "marketing_source": "acquisition channel", "nps_score": "Net Promoter Score, -100 to 100",
    "user_id": "user identifier", "role": "role of the user in the organization", "active": "active user",
    "email": "personal data: Bronze only, read by the ingestion and Silver jobs (D17)",
    "created_at": "creation date of the record", "last_login": "date of the last login",
    "resource_id": "cloud resource identifier", "service": "cloud service (compute, storage, database, ...)",
    "region": "cloud region", "state": "resource state", "tags_json": "resource tags as a JSON array",
    "tags": "tags_json parsed into an array",
    "ticket_id": "support ticket identifier", "category": "ticket category",
    "severity": "low, medium, high or critical", "resolved_at": "resolution date; null while open",
    "csat": "customer satisfaction 1 to 5; out-of-range values nulled in Silver (D7)",
    "sla_breached": "the ticket missed its SLA",
    "touch_id": "marketing touch identifier", "campaign": "campaign name", "channel": "marketing channel",
    "clicked": "the touch was clicked", "converted": "the touch converted", "touch_date": "date of the touch",
    "survey_date": "date of the NPS survey", "comment": "free-text survey comment",
    "invoice_id": "invoice identifier", "subtotal": "invoice subtotal in its currency",
    "credits": "credits in the invoice currency", "taxes": "taxes in the invoice currency",
    "exchange_rate_to_usd": "rate sent by the source",
    # technical columns, the same in every zone
    "ingest_ts": "when the row entered Bronze (one value per run or micro-batch)",
    "ingest_date": "date of ingest_ts; Bronze partition of events and snapshot masters",
    "source_file": "Landing file the row came from",
    "processed_ts": "when Silver rebuilt the row",
    "computed_ts": "when Gold computed the row",
    "run_id": "pipeline run that wrote the row, key of logs/<run_id>.log",
    "dq_status": "passed for every Silver row; rejected rows live in the quarantine zone",
    "rule_name": "first rule the row broke",
    "raw_record": "the line or Bronze row as it arrived",
    "quarantined_ts": "when the row was sent to Quarantine",
    "quarantine_date": "date of quarantined_ts, partition of the quarantine tables",
    "entity": "source the rejected row belongs to",
    # events
    "event_id": "event identifier from the source, unique in Silver",
    "timestamp": "event time as sent by the source (UTC)",
    "event_date": "date of timestamp; Silver partition",
    "metric": "requests, cpu_hours or storage_gb_hours",
    "value": "measurement as text, as it arrived (D2)",
    "value_double": "value cast with try_cast; null when missing (D2)",
    "unit": "unit of value; imputed from metric when missing (D6)",
    "unit_imputed": "unit derived from metric (D6)",
    "cost_usd_increment": "cost of the event in USD; negative values are corrections",
    "cost_anomaly_flag": "cost_usd_increment below -0.01 (D6)",
    "schema_version": "1 or 2; v2 adds carbon_kg and, for GenAI, genai_tokens (D1)",
    "carbon_kg": "estimated emissions of the event (v2 only)",
    "genai_tokens": "tokens processed (GenAI v2 events only); null, never 0, when not measured",
    "requests": "value of the requests events; null when the key has none",
    "cpu_hours": "value of the cpu_hours events; null when the key has none",
    "storage_gb_hours": "value of the storage_gb_hours events; null when the key has none",
    "org_industry": "industry of the organization, from customers_orgs",
    "org_plan_tier": "plan of the organization, from customers_orgs",
    "org_is_enterprise": "enterprise flag of the organization, from customers_orgs",
    "resource_state": "state of the resource, from resources",
    # masters
    "email_hash": "sha256 of salt + normalized email; the raw email stays in Bronze (D17)",
    "email_domain": "domain part of the email",
    "nps_score_invalid": "nps_score outside -100..100, nulled (D7)",
    "csat_invalid": "csat outside 1..5, nulled (D7)",
    "is_open": "ticket without resolved_at",
    "resolution_days": "days from created_at to resolved_at",
    "fx_overridden": "USD invoice whose rate was forced to 1.0 (D9)",
    "credits_imputed": "credits missing in the source, set to 0 (D7)",
    "subtotal_negative": "invoice with a negative subtotal, flagged and kept (D7)",
    "exchange_rate_applied": "rate used for the USD amounts; in the monthly mart, null when the month mixes rates",
    "subtotal_usd": "subtotal x exchange_rate_applied",
    "credits_usd": "credits x exchange_rate_applied",
    "taxes_usd": "taxes x exchange_rate_applied",
    "revenue_usd": "subtotal_usd - credits_usd + taxes_usd",
    "last_login_before_created": "low-trust user timestamps, flagged (D7)",
    "created_before_org_signup": "user created before the organization signed up, flagged (D7)",
    "converted_without_click": "conversion without a click, flagged (D7)",
    # marts
    "usage_date": "event date, named after the mart grain",
    "daily_cost_usd": "sum of cost_usd_increment; corrections included",
    "events": "Silver events summed into the row",
    "negative_cost_events": "events with cost_anomaly_flag",
    "unit_imputed_events": "events whose unit was imputed",
    "median_cost": "median daily cost of the trailing window (D12)",
    "mad_cost": "median absolute deviation of the trailing window (D12)",
    "history_days": "days of history in the trailing window",
    "score_method": "mad, mean_ad when MAD is 0, or insufficient_history",
    "anomaly_score": "modified z-score against the trailing 28 days (D12)",
    "is_anomaly": "abs(anomaly_score) >= 3.5",
    "anomaly_direction": "spike or drop",
    "as_of_date": "cutoff date: the 14-day window ends on it",
    "rank": "1 = most expensive service of the window; ties broken by service name",
    "cost_14d_usd": "cost of the service over the window",
    "days_with_usage": "days of the window with usage of the service",
    "window_start": "first day of the 14-day window",
    "window_complete": "false for cutoffs with less than 14 days of data behind them",
    "ticket_date": "date the ticket was opened (created_at) (D18)",
    "tickets": "tickets opened that day",
    "critical_tickets": "tickets with severity critical",
    "sla_breached_tickets": "tickets with sla_breached true",
    "critical_sla_breached": "critical tickets with sla_breached true",
    "sla_known_tickets": "tickets with a known sla_breached value",
    "sla_breach_rate": "sla_breached_tickets / sla_known_tickets",
    "open_tickets": "tickets still open",
    "csat_responses": "tickets with a valid csat",
    "csat_avg": "average of the valid csat answers; null without answers",
    "month": "first day of the billing month",
    "invoices": "invoices summed into the row",
    "currency": "invoice currency; MIXED when the month has more than one",
    "fx_overridden_invoices": "invoices with fx_overridden",
    "credits_imputed_invoices": "invoices with credits_imputed",
    "negative_subtotal_invoices": "invoices with subtotal_negative",
    "genai_cost_usd": "cost of all GenAI events of the day",
    "token_cost_usd": "cost of the GenAI events that carry tokens",
    "usd_per_1k_tokens": "token_cost_usd per 1000 tokens; null without tokens",
    "events_with_tokens": "GenAI events that carry genai_tokens",
}


def build(spark, cfg: Config) -> str:
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")
    text = ("# Data dictionary\n\nGenerated by `python -m pipeline.dictionary` on " + when + " from the Parquet "
            "files under `datalake/`. Do not edit by hand. The profile of each source is in "
            "`evidence/landing_profile.md`.\n\n")
    for zone, entity, path in lake.tables(cfg.datalake):
        if not lake.exists(path):
            continue
        df = lake.read(spark, path)
        part_cols = {d.name.split("=")[0] for d in path.iterdir() if d.is_dir() and "=" in d.name}
        key = f"{zone}/{entity}"
        text += f"## {key}\n\n"
        if key in TABLE_NOTES:
            text += TABLE_NOTES[key] + " "
        text += f"{df.count():,} rows.\n\n| column | type | note |\n| --- | --- | --- |\n"
        for f in df.schema.fields:
            name = f.name + (" (partition)" if f.name in part_cols else "")
            text += f"| {name} | {f.dataType.simpleString()} | {COLUMN_NOTES.get(f.name, '')} |\n"
        text += "\n"
    return text


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default=str(REPO_ROOT / "docs" / "data_dictionary.md"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config.from_env()
    spark = build_session(cfg, "dictionary")
    try:
        Path(args.out).write_text(build(spark, cfg))
        log.info("wrote %s", args.out)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
