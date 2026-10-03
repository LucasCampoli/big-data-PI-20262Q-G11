# Cloud Provider Analytics — Design notes

Group 11. First partial: problem framing, why this is a Big Data case, and a first look at the sources. Architecture comes later.

## 1. Problem, users, questions and objectives

### 1.1 The problem

We are the data team of a cloud provider. Three domains — FinOps, Support and Product — need to
answer questions about the same customers, from data that lands raw: nulls, numbers that arrive as
text, negative costs, spikes two orders of magnitude above the median, and a schema change
mid-history. Usage arrives as a stream of small JSONL fragments; masters and billing arrive as flat
CSV snapshots.

The job is to turn that into a small set of query-ready tables, with the mess handled explicitly
rather than averaged away. Two speeds are required: usage needs near-real-time ingestion, while
masters and billing are fine on a daily or monthly batch.

### 1.2 Users and the decisions they make

| Domain | Decision they make | What they need from us |
| :--- | :--- | :--- |
| **FinOps** | Where spend is going, which charges to investigate, what to invoice. | Daily cost and consumption by org and service, anomaly flags, revenue in USD after credits and taxes. |
| **Support** | Where to staff, which accounts are at risk. | Ticket volume by severity, SLA breach rate and CSAT by org and date. |
| **Product / Usage** | Which services to invest in, how GenAI adoption is growing. | Requests and operational metrics by service, GenAI tokens and carbon where the fields exist. |

### 1.3 Business questions mapped to the serving queries

Every question below has to be answerable from Cassandra, from a single partition read. The five
queries are the ones the brief fixes in §7.4; the marts are the Gold tables that back them.

| # | Business question | Domain | Serving query (§7.4) | Gold mart | Partition key → clustering |
| :-- | :--- | :--- | :--- | :--- | :--- |
| Q1 | What did each org spend, per service, day by day over a date range? | FinOps | Daily cost and requests by org and service | `org_daily_usage_by_service` | `(org_id)` → `usage_date, service` |
| Q2 | Which services cost this org the most over the last 14 days? | FinOps | Top-N services by accumulated cost, 14 days | `org_daily_usage_by_service` | same table, range-scanned then ranked |
| Q3 | How are critical tickets and SLA breaches trending over the last 30 days? | Support | Critical tickets and SLA breach rate per day | `tickets_by_org_date` | `(org_id)` → `ticket_date, severity` |
| Q4 | What is this org's monthly revenue in USD after credits and taxes? | FinOps | Monthly revenue normalized to USD | `revenue_by_org_month` | `(org_id)` → `month` |
| Q5 | How many GenAI tokens did this org consume per day, and at what cost? | Product | GenAI tokens and estimated cost per day | `genai_tokens_by_org_date` | `(org_id)` → `usage_date` |
| Q1b | Which days look abnormal and should be investigated? | FinOps | — *additional FinOps question, outside the five §7.4 queries* | `cost_anomaly_mart` | `(org_id)` → `usage_date, service` |

Q1 and Q2 share one mart on purpose: both are a date-range scan inside one org's partition, so a
second table would duplicate the data for no gain. Q2 ranks the scan result, which is cheap at this
grain (one org × 14 days × 6 services = 84 rows at most).

### 1.4 Measurable objectives

These are the success criteria. Each threshold is either derived from the serving requirement or
anchored to the baseline measured in `evidence/landing_profile.md`, so it can be checked rather
than asserted.

**Freshness and latency**

| # | Objective | Threshold | Measured as |
| :-- | :--- | :--- | :--- |
| O1 | A usage event is queryable in Bronze shortly after its file lands | ≤ 5 min | `ingest_ts` − file arrival time, p95 |
| O2 | Streaming micro-batches keep up with arrivals | p95 batch duration < 30 s, trigger 1 min | Spark `StreamingQueryProgress` |
| O3 | Daily Gold marts for day D are published early on D+1 | by 06:00 UTC | mart write timestamp |
| O4 | The five §7.4 queries answer fast enough for a dashboard | p95 < 1 s | single-partition read, timed from the client |

**Quality** — the policy is **impute-first, quarantine-last**: a row is rejected only when the data
contradicts itself or cannot be placed, never because it is merely incomplete. Baselines are
today's Landing, so a regression is visible.

| # | Objective | Threshold | Baseline today |
| :-- | :--- | :--- | :--- |
| O5 | `event_id` present and unique in Silver | 100% / 0 duplicates | 0 null, 0 duplicate |
| O6 | Quarantine stays an exception, measured in cost as well as in rows | ≤ 1% of events **and** ≤ 1% of cost | **0% / 0%** — every rejecting rule returns zero on 43,200 events |
| O7 | A repaired field is always flagged, never silently changed | 100% of imputed rows carry `unit_imputed` | 2,075 events (4.80% of events, 4.92% of cost) |
| O8 | A missing `value` never becomes a zero | contributes `null` to usage sums; its cost is still counted | 877 events (2.03% of events, 2.01% of cost) |
| O9 | Negative-cost events flagged, never dropped | 100% flagged | 211 events (0.49% of events, −0.62% of cost) |
| O10 | Every Bronze row traceable to its source file | 100% carry `ingest_ts` + `source_file` | by construction |
| O11 | Referential integrity to the masters | 100% | 80 of 80 orgs, 400 of 400 resources resolve |

O6 is the objective that changed most once cost was measured alongside row counts. A
quarantine-first reading of the `unit` rule would have rejected 4.80% of events carrying **4.92% of
all cost**, understating every FinOps mart by about 5%. Because `metric` determines `unit` 1:1 in
the data, those rows are repaired instead — which is what drops the real quarantine baseline to
zero.

**Correctness**

| # | Objective | Threshold | Measured as |
| :-- | :--- | :--- | :--- |
| O12 | Gold daily cost reconciles with Silver | ±0.01 USD per org-day | sum comparison after each run |
| O13 | Re-running any step does not duplicate rows | row counts unchanged | re-run on the same input, compare counts |
| O14 | Revenue converted per invoice, never at one blended rate | 100% of invoices use their own `exchange_rate_to_usd` | 240 invoices across USD/ARS/EUR |

## 2. Why this is a Big Data problem (5V)

Three Vs dominate this case and they are the ones the architecture is actually built around:
**velocity, variety and veracity**. Volume is the weakest V in the data we were handed — 13 MB — so
it is the one that has to be argued by projection rather than by what is on disk. Every figure in
the Evidence column is measured in `notebooks/01_landing_exploration.ipynb`.

| V | Weight here | Evidence measured in the notebook | What it forces in the design |
| :--- | :--- | :--- | :--- |
| **Velocity** | **Dominant** | Usage arrives as 120 JSONL parts meant to be read as micro-batches. Replayed in order, a 1-day watermark would treat **97.5%** of events as late, 7 days **87.6%**, 30 days **49.6%**. | Structured Streaming for ingestion, but **no stateful windowed aggregation**. Daily marts are recomputed in batch, so a late event lands in the day it belongs to. This is what makes the pattern Lambda-style. |
| **Variety** | **Dominant** | Two formats (7 CSV masters + JSONL events). Two event layouts: v1 carries 11 fields, v2 carries 12, GenAI v2 carries 13. The same `value` field is written as a JSON **number 41,014** times and as a **quoted string 1,309** times. `unit` is absent on 2,075 events. Billing spans **3 currencies**. | One explicit superset schema instead of inference; `value` read as string and cast in Silver; conformance of services and regions in Silver; per-invoice currency conversion before any revenue sum. |
| **Veracity** | **Dominant** | **211** events below −0.01 USD. Cost p99 is **16.69** while the max is **317.43** — a 19× tail. `nps_score` is outside 0–100 on **17 of 80** orgs (range −38 to 101). **877** events have a null `value`; **240** tickets have no `resolved_at`. | Impute-first quality rules with a Quarantine zone for genuine contradictions, rather than dropping files; anomaly detection by relative measure (z-score / MAD / percentile), never a fixed threshold; NPS treated as a validity flag, not averaged. |
| **Volume** | Secondary today | 13 MB total: 43,200 events (299 B each) over 60 days, ~720 events/day. Projected to real provider scale below. | Partition events by `event_date` only, Parquet + Snappy, and keep file sizes under control. The design must hold when the projection is real, not just at 13 MB. |
| **Value** | The payoff | One pipeline feeds **5 mandatory queries** across **3 domains** from the same conformed data. | Shared Silver layer, domain-specific Gold marts, query-first modelling in Cassandra. |

### 2.1 Projecting volume to a real provider

The dataset is a scale model. To check that the architecture is sized for the real thing, we
project it with assumptions stated explicitly — the five below are inputs, not measurements.

| | Assumption | This dataset | Projected |
| :-- | :--- | :--- | :--- |
| A1 | Billable organizations | 80 | 50,000 |
| A2 | Metered resources per org | 5 (measured: 400 / 80) | 200 |
| A3 | Metering samples per resource per day | 1.8 (measured: 43,200 / 400 / 60) | 4,320 — 3 metrics sampled every 60 s |
| A4 | Raw event payload | 299 B (measured) | 300 B |
| A5 | Parquet + Snappy compression vs raw JSON | — | 8× |

Applying A1–A3, `events/day = orgs × resources × samples`:

| Metric | This dataset | Projected | Factor |
| :--- | ---: | ---: | ---: |
| Events per day | 720 | 43,200,000,000 | 6 × 10⁷ |
| Sustained ingest rate | 0.008 events/s | **500,000 events/s** | — |
| Raw JSON per day | 210 KiB | **~13 TB** | — |
| Parquet per day (A5) | — | ~1.6 TB | — |
| Parquet per year | — | **~0.6 PB** | — |

At 500k events/s and 0.6 PB/year, single-node tooling is out and the choices in this document stop
being academic: a partitioned object-store lake, a distributed engine, and a wide-column store for
serving are the minimum. At 13 MB none of it is necessary — which is exactly why the justification
rests on velocity, variety and veracity, with volume as the argument for why the design must
survive growth.

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

Row counts, null shares and duplicate-key checks for every source are measured in
`notebooks/01_landing_exploration.ipynb` and recorded in `evidence/landing_profile.md`.

### Main risks

- **Schema change.** Events mix v1 and v2. v2 adds `carbon_kg` and, for genai, `genai_tokens`. A strict per-version schema would fail or drop the older rows. Mitigated by the superset schema below.
- **Ambiguous types.** `value` arrives as a number, as text, or as null. A hard cast at read time turns the bad rows into nulls without telling us. Mitigated by reading `value` as `string` in Bronze and casting in Silver, where a failed cast is quarantined instead of silently lost.
- **Bad amounts.** Costs and invoice subtotals can be negative, and billing is not all USD. Summing them raw will distort FinOps numbers.
- **Nulls on facts we need.** Open tickets have no resolution time, CSAT and NPS are often empty, and 2,075 events arrive without a `unit`. Averages that ignore that will look better than they are. Mitigated by imputing only what another field already determines (`unit` from `metric`, flagged `unit_imputed`) and otherwise keeping the null as a null: a missing `value` contributes nothing to a usage sum rather than a zero.

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
