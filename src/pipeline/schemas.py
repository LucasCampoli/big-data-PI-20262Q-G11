"""Explicit schemas for every Landing source. Nothing in the pipeline infers a schema (D1)."""
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

CORRUPT_RECORD = "_corrupt_record"

# One schema for both event layouts (D1). carbon_kg and genai_tokens are nullable so v1 rows load
# with them null, and value is a string so a malformed measurement survives the read and can be
# quarantined in Silver instead of silently becoming null (D2).
USAGE_EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType(),    nullable=False),
    StructField("timestamp",          TimestampType(), nullable=False),
    StructField("org_id",             StringType(),    nullable=False),
    StructField("resource_id",        StringType(),    nullable=False),
    StructField("service",            StringType(),    nullable=False),
    StructField("region",             StringType(),    nullable=False),
    StructField("metric",             StringType(),    nullable=False),
    StructField("value",              StringType(),    nullable=True),
    StructField("unit",               StringType(),    nullable=True),
    StructField("cost_usd_increment", DoubleType(),    nullable=True),
    StructField("schema_version",     IntegerType(),   nullable=False),
    StructField("carbon_kg",          DoubleType(),    nullable=True),
    StructField("genai_tokens",       LongType(),      nullable=True),
])


def _fields(*cols):
    return StructType([StructField(name, dtype, True) for name, dtype in cols])


S, D, B, F = StringType(), DateType(), BooleanType(), DoubleType()

# Column order matches the CSV headers; the reader checks the header against these names.
MASTER_SCHEMAS = {
    "customers_orgs": _fields(
        ("org_id", S), ("org_name", S), ("industry", S), ("hq_region", S), ("plan_tier", S),
        ("is_enterprise", B), ("signup_date", D), ("sales_rep", S), ("lifecycle_stage", S),
        ("marketing_source", S), ("nps_score", F)),
    "users": _fields(
        ("user_id", S), ("org_id", S), ("email", S), ("role", S), ("active", B),
        ("created_at", D), ("last_login", D)),
    "resources": _fields(
        ("resource_id", S), ("org_id", S), ("service", S), ("region", S), ("created_at", D),
        ("state", S), ("tags_json", S)),
    "support_tickets": _fields(
        ("ticket_id", S), ("org_id", S), ("category", S), ("severity", S), ("created_at", D),
        ("resolved_at", D), ("csat", F), ("sla_breached", B)),
    "marketing_touches": _fields(
        ("touch_id", S), ("org_id", S), ("campaign", S), ("channel", S), ("timestamp", D),
        ("clicked", B), ("converted", B)),
    "nps_surveys": _fields(
        ("org_id", S), ("survey_date", D), ("nps_score", F), ("comment", S)),
    "billing_monthly": _fields(
        ("invoice_id", S), ("org_id", S), ("month", D), ("subtotal", F), ("credits", F),
        ("taxes", F), ("currency", S), ("exchange_rate_to_usd", F)),
}

# Natural key of each master, used for the duplicate-key check in Bronze and the dedupe in Silver.
MASTER_KEYS = {
    "customers_orgs": ["org_id"],
    "users": ["user_id"],
    "resources": ["resource_id"],
    "support_tickets": ["ticket_id"],
    "marketing_touches": ["touch_id"],
    "nps_surveys": ["org_id", "survey_date"],
    "billing_monthly": ["invoice_id"],
}

# Bronze partition column per master (D5): billing by month, the snapshots unpartitioned.
MASTER_PARTITION = {"billing_monthly": "month"}


def with_corrupt_column(schema: StructType) -> StructType:
    return StructType(schema.fields + [StructField(CORRUPT_RECORD, StringType(), True)])
