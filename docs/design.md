# Cloud Provider Analytics — Design notes

Group 11. First partial: problem framing, why this is a Big Data case, and a first look at the sources. Architecture comes later.

## 1. Problem, users, questions, and goals

We act as the data team of a cloud provider. Customer data arrives raw and messy: nulls, noisy values, numbers that sometimes come as text, occasional negative costs, and a schema change mid-history (`schema_version=2` adds `carbon_kg` and, for GenAI, `genai_tokens`). Usage events also show up as small JSONL fragments, not as one clean file.

The job is to take those sources and turn them into something FinOps, Support, and Product can actually query. Masters and billing can wait for a daily or monthly batch. Usage needs to be closer to real time.

### Users

| Who | What they need |
| :--- | :--- |
| FinOps | Costs, consumption, revenue, credits, taxes, odd spikes, and efficiency by organization and service. |
| Support | Ticket volume, severity, SLA compliance, and CSAT by organization and date. |
| Product / Usage | Service usage, requests, operational metrics, and GenAI tokens / carbon when those fields exist. |

### Main questions

- FinOps: what did each organization spend per service on a given day, and which days look abnormal?
- FinOps: for a month, what is revenue in USD after credits and taxes?
- Support: how many critical tickets opened, and what share missed the SLA?
- Product: how many requests (and GenAI tokens, when present) did an organization generate?

### Measurable objectives

1. Produce daily cost and request totals by organization and service.
2. Produce ticket counts and an SLA-breach rate by organization, date, and severity.
3. Keep the original Landing files unchanged, and still load records that have nulls or mixed types without dropping the whole file.

## 2. Why Big Data (5V)

| V | Why it shows up here |
| :--- | :--- |
| Volume | Usage is split across many JSONL files, not a single spreadsheet. Masters (orgs, users, resources, tickets, billing) add more tables that have to be joined later. |
| Velocity | Usage arrives as micro-batches meant to be read as a stream. Billing and CRM can stay on a slower batch schedule. |
| Variety | Mix of CSV masters and JSONL events. Event schema changes from v1 to v2 (`carbon_kg`, `genai_tokens`). Some numeric fields arrive as text. |
| Veracity | Nulls, noisy org attributes, negative costs, and large cost spikes. We cannot treat every row as clean. |
| Value | FinOps, Support, and Product need the same customer data for different questions (spend, SLA, usage). A shared pipeline is what makes those answers possible. |

## 3. Source inventory

Files are in `data/landing/`. We did not change them. Masters are small CSVs. Usage is a folder of JSONL parts, one event per line.

| Source | Grain | How often | Notes |
| :--- | :--- | :--- | :--- |
| `customers_orgs.csv` | One org (`org_id`) | Snapshot | Industry, region, plan, NPS. Some NPS values are empty or look off. |
| `users.csv` | One user (`user_id`) | Snapshot | Role and activity. `last_login` is often empty. |
| `resources.csv` | One resource (`resource_id`) | Snapshot | Service, region, state. `tags_json` is sometimes empty. |
| `support_tickets.csv` | One ticket (`ticket_id`) | As tickets are opened | Severity, SLA, CSAT. Open tickets have no `resolved_at`; CSAT is often empty. |
| `marketing_touches.csv` | One touch (`touch_id`) | As campaigns run | Channel and whether it converted. |
| `nps_surveys.csv` | One answer per org and date | Over time | Separate from the NPS column on the org file. Some scores and comments are empty. |
| `billing_monthly.csv` | One invoice (`invoice_id`) | Monthly (three months in this dump) | Credits, taxes, currency. Some credits are empty, some subtotals are negative, and not everything is USD. |
| `usage_events_stream/*.jsonl` | One event (`event_id`) | Micro-batches | `value` is sometimes null or text. Costs can be negative. `schema_version=2` adds `carbon_kg` and, for genai, `genai_tokens`. |

`org_id` is on every file, so that is the join key. Traceability for now is the file name. We should keep that name when we load Bronze, and not edit Landing.

### Main risks

- **Schema change.** Events mix v1 and v2. v2 adds `carbon_kg` and, for genai, `genai_tokens`. A strict per-version schema would fail or drop the older rows. Mitigated by the superset schema below.
- **Ambiguous types.** `value` arrives as a number, as text, or as null. A hard cast at read time turns the bad rows into nulls without telling us. Mitigated by reading `value` as `string` in Bronze and casting in Silver, where a failed cast is quarantined instead of silently lost.
- **Bad amounts.** Costs and invoice subtotals can be negative, and billing is not all USD. Summing them raw will distort FinOps numbers.
- **Nulls on facts we need.** Open tickets have no resolution time, CSAT and NPS are often empty. Averages that ignore that will look better than they are.

### Event superset schema

Both versions are read with one explicit `StructType`, never with schema inference: inference samples the files it is given, so a micro-batch holding only v1 events would produce a narrower schema than a v2 one and the column set would drift between runs. Declaring the union of all fields once keeps every part file readable by the same reader.

| Field | Bronze type | Nullable | Present in | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | string | no | v1, v2 | Event key. Dedupe key for re-runs. |
| `timestamp` | timestamp | no | v1, v2 | Uniform ISO-8601 UTC (`2025-08-17T01:55:00Z`) across all 43,200 events. Source of the `event_date` partition. |
| `org_id` | string | no | v1, v2 | Join key to the masters. |
| `resource_id` | string | no | v1, v2 | Join key to `resources.csv`. |
| `service` | string | no | v1, v2 | 6 values, consistent with the masters. |
| `region` | string | no | v1, v2 | 7 values, consistent with the masters. |
| `metric` | string | no | v1, v2 | `requests`, `cpu_hours`, `storage_gb_hours`. |
| `value` | string | yes | v1, v2 | Read as text on purpose: 41,014 arrive numeric, 1,309 as text, 877 null. Cast to double in Silver. |
| `unit` | string | yes | v1, v2 | 2,075 null, and 2,038 of those have a `value` — a quality rule, not a schema problem. |
| `cost_usd_increment` | double | yes | v1, v2 | Always numeric in this dump, but can be negative. |
| `schema_version` | int | no | v1, v2 | `1` or `2`. Kept so a row can be traced back to its layout. |
| `carbon_kg` | double | **yes** | v2 only | Null for every v1 row. Mixed int/float in the source, so double. |
| `genai_tokens` | long | **yes** | v2, `service = genai` only | Null for v1 and for every non-GenAI v2 row. |

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, TimestampType,
)

# One schema for v1 and v2. carbon_kg and genai_tokens are nullable,
# so v1 rows load with those two columns as null instead of being rejected.
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
```

Field counts observed in Landing: 10,800 v1 events carry 11 fields, 29,268 v2 events carry 12, and 3,132 GenAI v2 events carry 13. All three shapes load through the schema above.

On top of these 13 fields, Bronze adds `ingest_ts` and `source_file` so a row can always be traced back to the part file it came from.

Architecture and the rest of the first-partial design are still to be written.
