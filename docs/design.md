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

O6 covers events; the master sources are tracked the same way and their baseline is also **0 of
4,112 rows**. Because real data rejects nothing, the quarantine path is exercised by synthetic
fixtures under `tests/fixtures/quarantine/` (§5.7.1), not by Landing.

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

### 3.1 Main risks

- **Schema change.** Events mix v1 and v2. v2 adds `carbon_kg` and, for genai, `genai_tokens`. A strict per-version schema would fail or drop the older rows. Mitigated by the superset schema below.
- **Ambiguous types.** `value` arrives as a number, as text, or as null. A hard cast at read time turns the bad rows into nulls without telling us. Mitigated by reading `value` as `string` in Bronze and casting in Silver with `try_cast`, where a failed cast is quarantined instead of silently lost. `try_cast` is not optional: Spark 4 enables ANSI mode by default, so a plain cast *raises* and would abort the job instead of quarantining the row.
- **Bad amounts.** Costs and invoice subtotals can be negative, and billing is not all USD. Summing them raw will distort FinOps numbers. Worse, all 160 USD invoices carry an `exchange_rate_to_usd` that is not 1.0 (0.855 to 1.118), which is meaningless for an invoice already denominated in USD and skews per-org revenue by up to 12% while barely moving the total. Mitigated by §5.7 and D12.
- **Nulls on facts we need.** Open tickets have no resolution time, CSAT and NPS are often empty, and 2,075 events arrive without a `unit`. Averages that ignore that will look better than they are. Mitigated by imputing only what another field already determines (`unit` from `metric`, flagged `unit_imputed`) and otherwise keeping the null as a null: a missing `value` contributes nothing to a usage sum rather than a zero.

### 3.2 Event superset schema

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

## 4. Architectural pattern

### 4.1 Decision: a Lambda-style hybrid

**Streaming ingests, batch aggregates.** Structured Streaming reads the JSONL directory and appends
events to Bronze and Silver with no stateful aggregation. Every daily mart in Gold is recomputed in
batch from Silver. Masters and billing are batch-only.

The deciding evidence is the late-data measurement in
[`evidence/landing_profile.md`](../evidence/landing_profile.md). Each of the 120 part files spans 59
of the 60 days in the dataset, so the files are not time-ordered slices. Replaying them in order as
micro-batches, the share of events a watermark would treat as late is:

| Watermark | Late events | Share |
| :--- | ---: | ---: |
| 1 day | 42,118 | **97.5%** |
| 7 days | 37,831 | **87.6%** |
| 30 days | 21,421 | **49.6%** |

A windowed streaming aggregation closes a window once the watermark passes it. On this data that
would discard most of the input. Recomputing the day in batch has no such problem: a late event
simply lands in the partition for the day it belongs to, and the next run produces the corrected
total.

### 4.2 Alternatives considered

| Option | What it would mean here | Why rejected |
| :--- | :--- | :--- |
| **Pure batch** | One scheduled job reads the whole JSONL directory and rebuilds everything. Simplest possible design, and correct. | Fails the near-real-time requirement of §2.1 and the mandatory Structured Streaming capability of §4.4 (watermark, dedupe by `event_id`, checkpointing). Operational usage metrics would be hours stale. |
| **Pure Kappa** | One streaming job is the only path; every mart is a stateful streaming aggregation. | Two reasons. First, half the sources do not belong in a stream at all: the masters are periodic snapshots and billing is monthly, with no latency requirement — modelling a monthly invoice file as a stream adds machinery and buys nothing. Second, computing the daily Gold marts in streaming leaves only bad options: a realistic 1-day watermark discards 97.5% of events, and the watermark that would not discard them is ~60 days, which means holding window state for the entire period. That is the batch recomputation, just more expensive and harder to reason about. |
| **Lambda-style hybrid** *(chosen)* | Streaming path for low-latency append-only ingestion; batch path for all aggregation and all masters. | Meets both speed requirements, keeps the streaming job stateless and cheap, and makes correctness a property of the batch recomputation rather than of watermark tuning. |

The usual objection to Lambda is having to maintain the same business logic twice. That does not
apply here, because the two paths do different jobs rather than the same job at two speeds: the
streaming path only parses, flags and appends; **all** aggregation lives in the batch path. There is
one implementation of every metric.

### 4.3 What runs where

| Path | Sources | Trigger | Writes | Stateful? |
| :--- | :--- | :--- | :--- | :--- |
| **Streaming** | `usage_events_stream/*.jsonl` | micro-batch, 1 min | Bronze events, Silver events (append) | No — only `dropDuplicates` on `event_id` within a watermark for in-flight re-delivery |
| **Batch daily** | Silver events, Silver dimensions | daily, after 00:00 UTC | Gold daily marts (Q1, Q2, Q3, Q5, anomalies) | n/a |
| **Batch snapshot** | 7 CSV masters | daily | Bronze + Silver dimensions | n/a |
| **Batch monthly** | `billing_monthly.csv` | monthly | Gold `revenue_by_org_month` (Q4) | n/a |

## 5. Data Lake design

Five zones. Each one has a single kind of writer, a declared format, and an explicit rule for when
data may move to the next zone. A zone is not a folder — it is a contract about who may write, who
may read, and what must be true before data arrives.

| Zone | Purpose | Format | Partitioning | Retention |
| :--- | :--- | :--- | :--- | :--- |
| **Landing** | Immutable raw arrival | CSV + JSONL, as delivered | none (directory per source) | full history — the only replay source |
| **Bronze** | Faithful typed copy, same grain | Parquet + Snappy | **`ingest_date`** (events), `month` (billing), none (masters) | 90 days rolling on `ingest_date` (events); 2 snapshots (masters) |
| **Silver** | Conformed, repaired, joinable | Parquet + Snappy | `event_date` | full history — the recomputation base for Gold |
| **Gold** | Query-shaped marts | Parquet + Snappy, then Cassandra | mart grain (`usage_date` / `month`) | 13 months rolling |
| **Quarantine** | Rows that failed a rejecting rule | Parquet + Snappy | `quarantine_date` | 180 days |

### 5.1 Landing

| | |
| :--- | :--- |
| **Who writes** | The source systems only (here: the course dataset drop). We never write to Landing. |
| **Who reads** | Ingestion jobs. Read-only exploration, like `notebooks/01_landing_exploration.ipynb`. No mart and no analyst reads Landing directly. |
| **Format** | As delivered: 7 CSV masters, 120 JSONL event parts. Never converted in place. |
| **Partitioning** | None. One directory per source, file names as received. |
| **Naming** | `datalake/landing/<source>.csv`, `datalake/landing/usage_events_stream/events_part_NNNN.jsonl` |
| **Retention** | Full history for the project. It is the only thing we can replay from, so deleting it removes our ability to rebuild. |
| **Technical columns** | None. The bytes are untouched. |
| **Quality controls** | None applied. Quality is *observed* here and *enforced* later — profiling reads Landing without writing to it. |
| **Promotion rule** | A file is promoted to Bronze when it is complete (not still being written) **and** its name is not already in the ingested-files log. That log is what makes re-running safe. |

### 5.2 Bronze

| | |
| :--- | :--- |
| **Who writes** | The streaming job (events) and the batch snapshot job (masters, billing). Append-only. |
| **Who reads** | Silver jobs. Engineers debugging an ingestion. |
| **Format** | Parquet + Snappy. Columnar and splittable, so Silver reads only the columns it needs. |
| **Partitioning** | Events by **`ingest_date`**, not `event_date` — see §5.6, this is the single most consequential partitioning decision in the design. Billing by `month`; masters unpartitioned (80–1,500 rows each). |
| **Naming** | `datalake/bronze/usage_events/ingest_date=YYYY-MM-DD/part-*.parquet` |
| **Retention** | 90 days rolling for events, **measured on `ingest_date`** — that is, 90 days since we read the file, not since the event happened. The distinction is not academic: measured on `event_date`, a 90-day window would already have expired this entire 2025 dataset. Silver holds the long history and Landing can replay anything older. Masters keep the current and previous snapshot. |
| **Technical columns** | `ingest_ts` (when we read it), `ingest_date` (its partition), `source_file` (which part file it came from). `schema_version` already exists in the source and is preserved. |
| **Quality controls** | Schema conformance only, no business rules. The explicit superset schema of §3.2 is applied; a row that cannot be parsed under it goes to Quarantine with its raw text. Grain is identical to the source — no filtering, no aggregation, no deduplication. |
| **Promotion rule** | A row reaches Bronze if and only if it parsed under the declared schema. Everything else is a Silver concern. |

### 5.3 Silver

| | |
| :--- | :--- |
| **Who writes** | Batch Spark jobs (conform, repair, join) and the streaming job's append path. Writes are **overwrite-by-partition**, which is what makes a re-run idempotent. |
| **Who reads** | Gold jobs. Analysts and notebooks for ad-hoc work. |
| **Format** | Parquet + Snappy. |
| **Partitioning** | `event_date` for the usage fact — deliberately *different* from Bronze's `ingest_date` (§5.6). Dimensions unpartitioned. |
| **Naming** | `datalake/silver/usage_events/event_date=YYYY-MM-DD/`, `datalake/silver/dim_org/` |
| **Retention** | Full history. Gold is always rebuildable from Silver, so this is the layer we do not expire. |
| **Technical columns** | `ingest_ts`, `source_file`, `processed_ts`, `unit_imputed`, `cost_anomaly_flag`, `dq_status`. |
| **Quality controls** | The **impute-first, quarantine-last** rule set (§1.4, measured in the notebook): `value` cast to double; `unit` imputed from `metric` and flagged `unit_imputed = true`; a null `value` kept as null so it contributes nothing — not zero — to usage sums, while its cost is still counted; `cost_usd_increment < −0.01` flagged `cost_anomaly_flag`; `dropDuplicates` on `event_id`; services and regions conformed; referential checks against the dimensions. A row is sent to Quarantine only on a contradiction: unknown `metric`, a `unit` that disagrees with its `metric`, a `value` that will not cast, a null `event_id`, or an unjoinable `org_id` / `resource_id`. |
| **Promotion rule** | A Bronze row is promoted when it passes every *rejecting* rule. Repairs are applied and flagged, never blocking. Re-running a date overwrites that date's partition, so the result does not depend on how many times the job ran. |

### 5.4 Gold

| | |
| :--- | :--- |
| **Who writes** | Batch aggregation jobs, then the Cassandra loader. |
| **Who reads** | The five §7.4 queries through Cassandra; BI tools; nothing upstream. |
| **Format** | Parquet + Snappy in the lake, mirrored into Cassandra tables for serving. The lake copy is what lets us re-publish without recomputing. |
| **Partitioning** | Mart grain: `usage_date` for the daily marts, `month` for revenue. |
| **Naming** | `datalake/gold/org_daily_usage_by_service/usage_date=YYYY-MM-DD/` |
| **Retention** | 13 months rolling, so year-over-year comparisons work with one month of margin. |
| **Technical columns** | `computed_ts`, `run_id`, plus the flags carried through from Silver (`unit_imputed`, `cost_anomaly_flag`) so a consumer can see which figures rest on a repair. |
| **Quality controls** | Reconciliation against Silver before publishing (objective O12: ±0.01 USD per org-day); partition keys non-null; row count checked against the expected grain. |
| **Promotion rule** | A mart partition is published **only after it reconciles**. Publishing overwrites that partition in the lake and upserts into Cassandra on the mart's primary key, so a repeated publish is a no-op rather than a duplicate. |

### 5.5 Quarantine

| | |
| :--- | :--- |
| **Who writes** | The Bronze loader (parse failures) and the Silver jobs (rejecting-rule violations), for events **and** for the master sources (§5.7). |
| **Who reads** | Data engineers, as a review queue. **No mart ever reads Quarantine** — that is the whole point of separating it. |
| **Format** | Parquet + Snappy, holding the full original row plus why it was rejected. |
| **Partitioning** | `quarantine_date` (the run date, not the event date) so a bad run is easy to isolate. `rule_name` is a column, not a partition, to avoid shattering a small zone into tiny files. |
| **Naming** | `datalake/quarantine/usage_events/quarantine_date=YYYY-MM-DD/` |
| **Retention** | 180 days — long enough to notice a problem, fix the rule or the source, and replay. |
| **Technical columns** | `rule_name`, `quarantined_ts`, `source_file`, `raw_record`, plus the parsed columns when parsing succeeded. |
| **Quality controls** | This zone *is* the control output, and its size is the monitored signal: objective O6 holds events at ≤ 1% of events and ≤ 1% of cost. Both baselines are **zero** today — 0 of 43,200 events and 0 of 4,112 master rows — so the path is exercised by synthetic fixtures instead (§5.7.1). |
| **Promotion rule** | Nothing is promoted automatically, ever. A quarantined row re-enters only by fixing the rule or the source and **replaying the original Landing file** — which is exactly why Landing must stay immutable. |

### 5.6 Partitioning and the small-files problem

There are two separate small-files problems here, and they pull in opposite directions. The first is
about *how many dimensions* to partition on; the second is about *which* column, and it is the reason
Bronze and Silver do not share one.

#### Too many partition columns

Events are partitioned on date and nothing else. The temptation is to add `service` and `region`,
since every query filters on them, but at this volume that backfires:

| Partitioning | Partitions | Rows per partition | Approx. file size |
| :--- | ---: | ---: | ---: |
| date only *(chosen)* | 60 | 720 | ~30 KB |
| date + `service` | 360 | 120 | ~6 KB |
| date + `service` + `region` | 2,520 | 17 | **~1 KB** |

A distributed file system pays a fixed cost per file — a metadata entry on the NameNode in HDFS, a
listing round-trip on object storage — and Spark schedules at least one task per file. Thousands of
1 KB files means the job spends its time on metadata and task setup rather than on data, and the
columnar layout of Parquet stops paying for itself because each file is smaller than a single row
group. The target is files in the tens-to-hundreds of MB, reached with `coalesce` / `repartition`
before writing.

`service` and `region` stay as **columns**, not partitions. Parquet's row-group statistics let the
reader skip row groups that cannot match a predicate, which recovers most of the benefit of
partitioning on them without creating any files.

This choice is tied to current volume, and the §2.1 projection says when to revisit it: at ~1.6 TB
of Parquet per day, a single `event_date` partition becomes too large, and the second partition
level should then be time (`hour`) or a hash bucket of `org_id` — something uniformly distributed —
rather than `service`, which is skewed.

#### The wrong partition column: `ingest_date` in Bronze, `event_date` in Silver

A streaming write produces at least one file per *(micro-batch × partition touched)*. The part files
are not time-ordered slices — each one spans **59 to 60 distinct event dates** (mean 59.8) — so a
micro-batch partitioned by `event_date` fans out across almost the whole calendar:

| Bronze partitioned by | Files written | Avg rows per file |
| :--- | ---: | ---: |
| `event_date` | **7,180** | **6.0** |
| `ingest_date` *(chosen)* | 120 | 360 |

Partitioning Bronze by `event_date` would write 7,180 files of 6 rows each for 43,200 events — the
small-files pathology, created by the streaming layer itself and not fixable by `coalesce`, because
the fan-out happens per micro-batch and each batch legitimately holds all those dates.

So the zones partition differently, by design:

| Zone | Partition | Rationale |
| :--- | :--- | :--- |
| **Bronze** | `ingest_date` | How the data *arrived*. One file per micro-batch, append-only, no fan-out. |
| **Silver / Gold** | `event_date` | What the data *means*. Written by the batch job, which sees a whole day at once and can `coalesce` each partition into a few right-sized files. |

This is the mechanical reason the batch path is not optional. The streaming layer cannot produce
event-time partitions at a sane file size; the batch rebuild is what converts arrival-ordered Bronze
into event-ordered Silver. It is the same conclusion the late-data measurement reached in §4.1,
arrived at from file layout instead of watermarks.

It also means **Bronze retention must be measured on `ingest_date`**, the column it is partitioned
by. Measured on `event_date`, a 90-day window would have expired every row of this 2025 dataset
before we ever queried it.

### 5.7 Quality rules per source

Events are clean enough that nothing is rejected (§1.4, O6: 0 of 43,200). The master sources are
where Quarantine actually gets used, which is also what gives delivery 2 its required quarantine
samples. Each rule's action depends on whether the **row** is untrustworthy or only a **field**:

| Source | Rule | Rows | Action |
| :--- | :--- | ---: | :--- |
| `users` | `last_login` earlier than `created_at` | 232 | **flag** — timestamps are low-trust (below) |
| `users` | `created_at` earlier than the org's `signup_date` | 249 | **flag** — same root cause |
| `support_tickets` | `resolved_at` earlier than `created_at` | 0 | **quarantine** — a contradiction with no determinable repair |
| `billing_monthly` | `currency = USD` but `exchange_rate_to_usd ≠ 1.0` | **160** | **repair + flag** — force `fx = 1.0`, set `fx_overridden` |
| `support_tickets` | `csat` outside 1–5 | 40 | **null + flag** — null `csat`, set `csat_invalid` |
| `customers_orgs` | `nps_score` outside [−100, 100] | 1 | **null + flag** — null the field, set `nps_score_invalid` |
| `nps_surveys` | `nps_score` outside [−100, 100] | 0 | **null + flag** — same scale |
| `billing_monthly` | `subtotal < 0` | 13 | **flag** — a real credit or adjustment, kept in revenue |
| `billing_monthly` | `credits` is null | 137 | **impute** — zero credit, not an unknown amount |
| `customers_orgs` | `nps_score` is null | 11 | **keep** — no survey taken is not an error |
| `marketing_touches` | `converted` while `clicked = false` | 96 | **flag** — view-through conversion is plausible |

Master quarantine baseline: **0 of 4,112 rows**. Combined with the event baseline of 0 of 43,200,
**real data rejects nothing** — see §5.7.1 for how the quarantine path is exercised anyway.

Three points behind the table:

- **Only a self-contradiction rejects a row, and only when the row is worthless without the
  contradicting field.** An out-of-range `csat` or `nps_score` is one bad field on a row downstream
  joins still need, so the field is nulled and flagged. The `users` timestamps fail an even more
  basic test: **462 of 800 rows (57.8%)** break at least one of the two rules, so rejecting them would
  discard more than half a dimension whose `user_id`, `org_id`, `role` and `active` are perfectly
  sound, and would halve any per-org user count. The two rules fire on 29.0% and 31.1% of rows
  respectively — a causally generated dataset would show ~0% — so the three timestamps were evidently
  drawn independently of each other and of the org. Nothing identifies *which* one is wrong, so there
  is no field to repair either. Every row is kept, both rules are flagged, and the `users` timestamps
  are marked **low-trust**: excluded from any tenure or recency metric (account age,
  days-since-last-login, activation time). See D16.
- **Two scale assumptions are ours, not the brief's.** `csat` is treated as 1–5 (the data holds 0, 6
  and 7, which is what the rule catches) and both NPS columns as the aggregate [−100, 100] scale.
  `nps_surveys.nps_score` ranges −16 to 68, which rules out a 0–10 per-respondent scale and is what
  makes [−100, 100] the consistent reading. If the course intends different scales, these two rules
  change and nothing else does.
- **The USD rate is the one that would have slipped through.** See D12 in
  [`../DECISIONS.md`](../DECISIONS.md): forcing `fx = 1.0` moves *total* revenue by only +0.07%,
  because the rates scatter symmetrically around 1.0 and cancel across 160 invoices. Per invoice the
  error runs from **−14.5% to +11.8%**, and Q4 serves one row per org and month. An aggregate
  reconciliation check would have passed.

#### 5.7.1 Exercising Quarantine with fixtures

Real Landing data rejects nothing, so the quarantine path would never run — and a path that never
runs is a path that does not work. It is exercised instead by a small synthetic fixture set in
[`../tests/fixtures/quarantine/`](../tests/fixtures/quarantine/), one record per rejecting rule:
null `event_id`, duplicate `event_id`, unknown `metric`, unparseable `timestamp`, `unit`
contradicting `metric`, and an uncastable `value`. An eighth record is the **control**: it looks
defective (`unit` null, `value` as the text `"4.25"`) but is unambiguous, so a rule set that
quarantines it is too strict.

The fixtures are not Landing data. They are never copied into `data/landing/`, never read by the
exploration notebook, and never counted in any profile or mart, so Landing stays immutable (D10).
`tests/test_quality_rules.py` asserts all eight outcomes against
[`../src/quality_rules.py`](../src/quality_rules.py), which is the canonical implementation of §5.7.

**Delivery 2's required quarantine samples come from these fixtures plus whatever real data
produces** — today that is the fixtures alone.

This already paid for itself. The fixtures caught a defect the real data cannot reveal: **Spark 4
enables ANSI SQL mode by default, so a plain `cast` raises instead of returning null.** The rule
"quarantine a `value` that will not cast" was written with a plain cast, which means the first
malformed value would have aborted the entire job instead of quarantining one row — precisely
inverting the policy. Every numeric cast in the project now uses `try_cast`. On this dataset, where
everything casts cleanly, the bug was invisible.

### 5.8 Promotion at a glance

```text
Landing ──parse under explicit schema──▶ Bronze ──pass rejecting rules──▶ Silver ──reconcile──▶ Gold ──upsert──▶ Cassandra
   │                 │                                    │
   │                 └─ parse failure ──┐                 └─ contradiction ──┐
   │                                    ▼                                    ▼
   └──────── replay (manual, after a fix) ◀──────────── Quarantine ◀─────────┘
```

Each arrow is a gate with a stated condition, and the only way out of Quarantine is a fix plus a
replay from immutable Landing. Decisions behind these zones are recorded in
[`../DECISIONS.md`](../DECISIONS.md).
