# Decisions

Architecture and data decisions for the Cloud Provider Analytics project, with the alternatives
considered and what each choice costs us. Format is one short record per decision.

Status: **Accepted** — decided and reflected in the design. **Open** — deliberately deferred, with
the deadline for deciding it. Records are listed accepted-first rather than by number, so the two
open items stay at the end where they are easy to find.

All records dated 2026-10-03 unless noted. Design document: [`docs/design.md`](docs/design.md).
Measurements referenced below are in [`evidence/landing_profile.md`](evidence/landing_profile.md).

| # | Decision | Status |
| :-- | :--- | :--- |
| [D1](#d1-one-explicit-superset-schema-for-usage-events) | One explicit superset schema for usage events | Accepted |
| [D2](#d2-read-value-as-string-in-bronze-cast-in-silver) | Read `value` as string in Bronze, cast in Silver | Accepted |
| [D3](#d3-lambda-style-hybrid-pattern) | Lambda-style hybrid pattern | Accepted |
| [D4](#d4-streaming-is-stateless-all-aggregation-is-batch) | Streaming is stateless; all aggregation is batch | Accepted |
| [D5](#d5-five-zones-with-an-explicit-quarantine) | Five zones, with an explicit Quarantine | Accepted |
| [D6](#d6-parquet--snappy-for-every-managed-zone) | Parquet + Snappy for every managed zone | Accepted |
| [D7](#d7-partition-events-on-one-date-column-only) | Partition events on one date column only | Accepted |
| [D8](#d8-impute-first-quarantine-last-quality-policy) | Impute-first, quarantine-last quality policy | Accepted |
| [D9](#d9-a-null-value-contributes-null-not-zero) | A null `value` contributes null, not zero | Accepted |
| [D10](#d10-landing-is-immutable-replay-is-the-only-way-back) | Landing is immutable; replay is the only way back | Accepted |
| [D11](#d11-idempotency-by-partition-overwrite-plus-upsert-on-natural-keys) | Idempotency by partition overwrite plus upsert on natural keys | Accepted |
| [D12](#d12-convert-currency-per-invoice-never-at-a-blended-rate) | Convert currency per invoice, never at a blended rate | Accepted |
| [D15](#d15-bronze-partitions-by-ingest_date-silver-by-event_date) | Bronze partitions by `ingest_date`, Silver by `event_date` | Accepted |
| [D16](#d16-per-source-quality-rule-actions-and-two-scale-assumptions) | Per-source quality rule actions, and two scale assumptions | Accepted |
| [D17](#d17-exercise-quarantine-with-synthetic-fixtures-not-with-landing) | Exercise Quarantine with synthetic fixtures, not with Landing | Accepted |
| [D13](#d13-cassandra-query-first-table-layout) | Cassandra query-first table layout | **Open** |
| [D14](#d14-anomaly-detection-method-and-threshold) | Anomaly detection method and threshold | **Open** |

---

## D1 · One explicit superset schema for usage events

**Context.** `schema_version=2` adds `carbon_kg`, and GenAI v2 events add `genai_tokens`. Landing
holds all three shapes: 10,800 rows with 11 fields, 29,268 with 12, 3,132 with 13.

**Decision.** Declare one `StructType` covering the union of all 13 fields, with `carbon_kg` and
`genai_tokens` nullable. Use it for every read, batch and streaming.

**Alternatives.** *Schema inference* — rejected: inference samples the files it is given, so a
micro-batch holding only v1 events would infer a narrower column set and the schema would drift
between runs. *A schema per version with a union afterwards* — rejected: two readers and a merge
step to maintain, for no gain over nullable columns.

**Consequences.** v1 rows carry two null columns, which is accurate rather than lossy. Verified on
all 120 part files: 43,200 rows, 0 corrupt records. A new v3 field means editing one declaration.

## D2 · Read `value` as string in Bronze, cast in Silver

**Context.** The same `value` field is written as a JSON number 41,014 times and as a quoted string
1,309 times, and is null 877 times.

**Decision.** Bronze types `value` as `string`. Silver casts to double; a cast failure is a
rejecting rule.

**Alternatives.** *Cast at read time* — rejected: Spark turns an unparseable value into a silent
null, which would land inside a cost sum with nothing to show it happened.

**Consequences.** Bronze is not directly summable on `value`, which is acceptable because Bronze is
not a query layer. Every non-null value in today's data casts cleanly, so the rule costs nothing now
and catches the first genuinely malformed value later. The Silver cast must use `try_cast`: Spark 4
enables ANSI mode by default, so a plain cast raises rather than returning null, which would abort
the job instead of quarantining the row (found via the D17 fixtures).

## D3 · Lambda-style hybrid pattern

**Context.** §2.1 of the brief requires near-real-time usage metrics *and* batch masters. Each of
the 120 part files spans 59 of the 60 days in the dataset, so arrival order has almost no relation
to event time.

**Decision.** Streaming for event ingestion, batch for all aggregation and all masters.

**Alternatives.** *Pure batch* — rejected: fails the near-real-time requirement and the mandatory
Structured Streaming capability of §4.4. *Pure Kappa* — rejected on measured evidence: as windowed
streaming aggregations, the daily marts would treat 97.5% of events as late at a 1-day watermark and
49.6% at 30 days. Kappa also assumes a replayable durable log, and Landing is a directory of files.

**Consequences.** Two execution paths to operate. The usual Lambda complaint — duplicated business
logic — does not apply here, because the paths do different jobs (see D4): there is exactly one
implementation of every metric.

## D4 · Streaming is stateless; all aggregation is batch

**Context.** The same late-data measurement as D3.

**Decision.** The streaming job parses, flags and appends only. It keeps no windows and no running
aggregates; its only state is a checkpoint and a short-horizon `dropDuplicates` on `event_id` for
in-flight re-delivery. Every daily mart is recomputed in batch from Silver.

**Alternatives.** *Windowed streaming aggregation with a long watermark* — rejected: holding 60 days
of window state to avoid dropping late events is the batch job, only more expensive and harder to
reason about.

**Consequences.** Correctness becomes a property of batch recomputation instead of watermark tuning,
and a late event is simply picked up by the next run of the day it belongs to. Marts are as fresh as
the batch schedule (objective O3: by 06:00 UTC for D−1), not as fresh as the stream.

## D5 · Five zones, with an explicit Quarantine

**Context.** The brief mandates Landing/Bronze/Silver/Gold plus quarantine in Parquet. Bad rows need
somewhere to go that is not "dropped" and not "in the marts".

**Decision.** Five zones. Each has one kind of writer, a declared format, and a stated condition for
promotion. Quarantine is a peer zone, never read by a mart.

**Alternatives.** *A boolean `is_valid` column in Silver* — rejected: every downstream query then has
to remember to filter on it, and the one that forgets silently corrupts a mart.

**Consequences.** One more path to write and monitor. In exchange, "valid" is a property of the zone
a row is in, not of a predicate each consumer must repeat.

## D6 · Parquet + Snappy for every managed zone

**Context.** Bronze onward is ours to choose; Landing's format is given.

**Decision.** Parquet with Snappy compression for Bronze, Silver, Gold and Quarantine.

**Alternatives.** *CSV/JSON* — rejected: no column pruning, no predicate pushdown, no typed schema.
*Gzip* — rejected: not splittable, so one file becomes one task. *Delta/Iceberg* — attractive for
ACID partition overwrites, but out of scope for this project's required stack.

**Consequences.** Readers fetch only the columns they need. Snappy trades some compression ratio for
speed, which is the right side of that trade when the files are read repeatedly.

## D7 · Partition events on one date column only

**Context.** 60 days of events, ~720 per day. Queries filter on `org_id`, `service` and date range.

**Decision.** Partition on a single date column. Keep `service` and `region` as columns. *Which* date
column differs by zone — see D15.

**Alternatives.** *Add `service` and `region`* — rejected on arithmetic: it produces 2,520 partitions
of ~17 rows (~1 KB files) for 43,200 events. A distributed file system pays a fixed cost per file and
Spark schedules a task per file, so the job would spend its time on metadata, and Parquet row groups
smaller than one block stop paying for themselves. *No partitioning* — rejected: every date-range
query would scan the whole history.

**Consequences.** Date-range queries prune partitions; service filters rely on row-group statistics
instead, which recovers most of the benefit without creating files. Which date column is a separate
decision — see D15. Revisit at the §2.1 projected
scale (~1.6 TB of Parquet per day): the second level should then be `hour` or a hash bucket of
`org_id`, not `service`, which is skewed.

## D8 · Impute-first, quarantine-last quality policy

**Context.** 2,075 events (4.80%) arrive with a null `unit`. They carry **4.92% of all cost**. In the
data, `metric` determines `unit` 1:1 — `requests`→`count`, `cpu_hours`→`hours`,
`storage_gb_hours`→`gb_hours` — with zero contradictions.

**Decision.** Quarantine only contradictions: unknown `metric`, a `unit` that disagrees with its
`metric`, a `value` that will not cast, a null `event_id`, an unjoinable `org_id` / `resource_id`.
A missing-but-unambiguous field is repaired instead: `unit` is imputed from `metric` and the row is
flagged `unit_imputed = true` for lineage.

**Alternatives.** *Quarantine every row with a null `unit`* — rejected because the cost share was
measured, not assumed: it would reject 4.92% of all cost and understate every FinOps mart by about
5%. *Impute silently* — rejected: a repair nobody can see is indistinguishable from source data.

**Consequences.** The measured quarantine baseline drops to **0 of 43,200 events**, which is what
objective O6 is set against (≤ 1% of events and ≤ 1% of cost). Consumers can always separate repaired
from original rows. Imputation is valid only while `metric` → `unit` stays 1:1, so the notebook
asserts that relationship before relying on it, and a contradiction becomes a rejecting rule rather
than a silent overwrite.

## D9 · A null `value` contributes null, not zero

**Context.** 877 events (2.03%) have a null `value` but a real `cost_usd_increment`, together 2.01%
of all cost.

**Decision.** Keep the row. Its cost counts in the cost marts; its usage metric contributes `null`
to usage sums and averages.

**Alternatives.** *Treat null as 0* — rejected: it biases every average downward and invents a
measurement that was never taken. *Drop the row* — rejected: it would discard 2.01% of real cost.

**Consequences.** Usage aggregates and cost aggregates can have different denominators for the same
org-day, so any usage metric must be reported with its row count.

## D10 · Landing is immutable; replay is the only way back

**Context.** The brief requires raw data to stay untouched, and a quarantined row has to be able to
re-enter the pipeline after a fix.

**Decision.** Nothing ever writes to Landing. Reprocessing means re-reading the original files.
Quarantine holds no promotion path of its own: fix the rule or the source, then replay.

**Alternatives.** *Correct rows in place in Landing* — rejected: it destroys the only reproducible
baseline and makes any past result unverifiable.

**Consequences.** Landing is never expired, which costs storage. Bronze can then be retained for
only 90 days, since anything older is rebuildable.

## D11 · Idempotency by partition overwrite plus upsert on natural keys

**Context.** `event_id` has no duplicates in the data, but a re-run re-reads the same part files, so
duplicates are a property of *execution*, not of the source.

**Decision.** Silver and Gold writes overwrite the affected partition rather than appending. The
Cassandra load upserts on each mart's primary key. The streaming job uses checkpoints plus
`dropDuplicates` on `event_id`. An ingested-files log gates Landing → Bronze.

**Alternatives.** *Append plus a dedupe pass afterwards* — rejected: correctness then depends on a
cleanup job having run, and the window between the two is visible to consumers.

**Consequences.** Re-running any date is safe and produces the same result (objective O13). Partition
overwrite is not atomic on plain object storage, so a reader can briefly see a partition mid-write —
acceptable at a daily cadence, and the reason D6 notes Delta/Iceberg as the thing to adopt if that
stops being acceptable.

## D12 · Convert currency per invoice, never at a blended rate

**Context.** `billing_monthly.csv` holds 240 invoices in USD (160), ARS (51) and EUR (29), each with
its own `exchange_rate_to_usd`. 13 invoices have a negative subtotal and 137 have a null `credits`.

**Decision.** Normalize every invoice with its own `exchange_rate_to_usd` before any sum. Treat a
null `credits` as zero credit — the absence of a credit line, not an unknown amount. Keep negative
subtotals as real adjustments and flag them.

**Decision on USD.** When `currency = USD`, force `exchange_rate_to_usd = 1.0` and set
`fx_overridden = true`. All **160** USD invoices carry a rate that is not 1.0, spanning **0.855 to
1.118**, which is meaningless for an invoice already denominated in USD.

**Alternatives.** *One rate per month* — rejected: it misstates revenue for every invoice whose rate
differs from the month's average. *Drop negative subtotals* — rejected: credits and adjustments are
legitimately negative, and dropping them overstates revenue. *Trust the USD rate as given* —
rejected: it makes a USD invoice's USD revenue depend on a number that cannot mean anything.
*Quarantine the 160 USD invoices* — rejected: that is 67% of billing and all of USD revenue, for a
defect confined to one field that has a known correct value.

**Why this one is easy to miss.** Forcing the rate moves *total* revenue by only **+0.07%**
(164,184.90 → 164,293.06 USD), because the rates scatter symmetrically around 1.0 and cancel out
across 160 invoices. **Per invoice** the error runs from **−14.5% to +11.8%**, and Q4 is served one
row per org and month, not in aggregate. A reconciliation check on the grand total would have passed
while two thirds of the served rows were wrong — which is the argument for reconciling at the grain
that is queried, not at the top.

**Consequences.** Revenue for non-USD invoices is only as good as the per-invoice rate, which we
accept as authoritative. The override is visible via `fx_overridden`, so the untouched figure can be
recovered. Objective O14 is checked per invoice, not on the total.

## D15 · Bronze partitions by `ingest_date`, Silver by `event_date`

**Context.** A streaming write produces at least one file per *(micro-batch × partition touched)*.
The 120 part files are not time-ordered slices: each spans 59–60 distinct event dates (mean 59.8).

**Decision.** Partition Bronze events by `ingest_date` — the date we read the file — and Silver and
Gold by `event_date`. Silver is rebuilt in batch with `coalesce` per partition. Bronze retention is
measured on `ingest_date`.

**Alternatives.** *`event_date` everywhere, for consistency* — rejected on measurement: every
micro-batch would fan out across ~59.8 partitions, writing **7,180 files of 6 rows** for 43,200
events. `coalesce` cannot fix it, because the fan-out happens inside each micro-batch and each batch
genuinely holds all those dates. On `ingest_date` the same data lands as **120 files of 360 rows**.
*`event_date` in Bronze with a later compaction job* — rejected: it adds a job whose only purpose is
to undo a partitioning choice we are free not to make.

**Consequences.** The two zones answer different questions — "when did this arrive" versus "when did
this happen" — and the batch rebuild is what converts one into the other. This is a second,
independent argument for D3/D4: even setting watermarks aside, the streaming layer cannot produce
event-time partitions at a sane file size. It also means Bronze's 90-day retention is counted on
`ingest_date`; counted on `event_date`, a 90-day window would already have expired every row of this
2025 dataset. Querying Bronze by event time requires a full scan, which is acceptable because Bronze
is not a query layer (D5).

## D16 · Per-source quality rule actions, and two scale assumptions

**Context.** Events quarantine nothing (D8, baseline 0 of 43,200). The master sources hold defects of
genuinely different kinds, and treating them uniformly would be wrong in both directions.

**Decision.** Three actions, chosen by whether the **row** or only a **field** is untrustworthy:

| Action | Applies when | Rules |
| :--- | :--- | :--- |
| **quarantine** | the row contradicts itself, with no determinable repair *and* no value without the broken field | `support_tickets.resolved_at < created_at` (0 rows today) |
| **repair / null + flag** | one field is wrong but has a known or irrelevant value | USD `fx` → 1.0 (160, D12), `csat` outside 1–5 → null (40), `nps_score` outside [−100, 100] → null (1), null `credits` → 0 (137) |
| **flag only** | the value is plausible and usable, or the row is useful despite it | `users` timestamp rules (232 and 249), `subtotal < 0` (13), `converted` without `clicked` (96) |

**The `users` timestamps.** Two rules fire: `last_login < created_at` on **232** rows (29.0%) and
`created_at` earlier than the org's `signup_date` on **249** rows (31.1%). **462 of 800 rows (57.8%)**
break at least one. A causally generated dataset would show ~0% on both, so the three timestamps were
evidently drawn independently of each other and of the org.

Every row is kept and both rules are flagged. The `users` timestamps are marked **low-trust** and
excluded from any tenure or recency metric — account age, days-since-last-login, activation time. The
identity fields (`user_id`, `org_id`, `role`, `active`) are unaffected and stay fully usable.

**Alternatives.** *Quarantine the contradicting rows* — rejected, and this reverses an earlier draft
of this decision. It would discard 29% of `users` on the first rule alone and 57.8% across both, from
a dimension whose identity fields are sound, and would silently halve any per-org user count. The
row-versus-field principle that keeps an out-of-range `csat` also keeps these rows: the defect is
confined to fields we can refuse to use. *Null the timestamps* — rejected: nothing identifies which
of the three is wrong, so nulling any one would be arbitrary, and nulling all three destroys
information a careful consumer could still interpret. *Flag with no low-trust marking* — rejected:
the flag alone does not stop someone computing average account age from a column that cannot support
it.

**Assumptions.** The brief does not state either scale, so these are ours: `csat` is **1–5** (the data
holds 0, 6 and 7), and both NPS columns use the aggregate **[−100, 100]** scale.
`nps_surveys.nps_score` measures −16 to 68, which rules out a 0–10 per-respondent scale and makes
[−100, 100] the only consistent reading across the two columns. If the course intends different
scales, exactly two rules change.

**Consequences.** Both quarantine baselines are now **zero** — 0 of 43,200 events and 0 of 4,112
master rows. Real data rejects nothing, which is a good result for the data and a problem for the
pipeline: an untested path is an unproven one. Hence D17.

## D17 · Exercise Quarantine with synthetic fixtures, not with Landing

**Context.** After D8 and D16, no real record is rejected. The quarantine write path, the `dq_rule`
column and the split between passed and rejected rows would therefore never execute.

**Decision.** Keep a small fixture set under `tests/fixtures/quarantine/` — one record per rejecting
rule, plus a control record that *looks* defective but must pass. `src/quality_rules.py` is the
canonical implementation of the rules and `tests/test_quality_rules.py` asserts every outcome
against it. Delivery 2's required quarantine samples come from these fixtures plus whatever real data
produces.

**Alternatives.** *Inject bad rows into Landing* — rejected outright: it breaks D10 and would corrupt
every profile and baseline in `evidence/`. *Wait for real bad data* — rejected: the path would ship
unexecuted, and delivery 2 requires quarantine samples. *Lower the bar so real rows get rejected* —
rejected: that is tuning the rules to the test rather than to the data, and D8 measured what it would
cost (4.92% of all cost).

**Consequences.** The fixtures are test data and are never read by the notebook, never copied into
`data/landing/`, and never counted in any profile or mart. The notebook currently inlines the same
rules so it stays Colab-runnable without the repo on the path; delivery 2 unifies the two once the
package is installable, and until then the test is what pins the module.

**This already found a real defect.** The rule "quarantine a `value` that will not cast" was written
with a plain `cast`. **Spark 4 enables ANSI SQL mode by default, so `cast` raises instead of
returning null** — meaning the first malformed value would have aborted the whole job instead of
quarantining one row, exactly inverting the policy. Every numeric cast in the project now uses
`try_cast`. On real Landing data, where every value casts cleanly, this bug was undetectable; the
fixture found it on the first run. That is the argument for D17 in one paragraph.

## D13 · Cassandra query-first table layout — **Open**

**Context.** The five §7.4 queries each need a single-partition read. §1.3 of the design maps each
question to a mart and a proposed partition key, but the table definitions are not final.

**What is decided.** The approach: one table per query shape, partition key chosen so each query hits
exactly one partition, no secondary indexes, no `ALLOW FILTERING`. Q1 and Q2 share
`org_daily_usage_by_service` because both are a date-range scan within one org partition.

**What is open.** Whether `(org_id)` alone is a safe partition key or whether it needs bucketing by
month to bound partition growth; the clustering order for Q2's top-N; whether `cost_anomaly_mart`
is its own table or a column set on the daily mart.

**Why deferred.** It depends on the mart row counts, which only exist once Gold runs. Deciding now
would be guessing. **Decide by:** start of delivery 2, before the Cassandra schema is written.

## D14 · Anomaly detection method and threshold — **Open**

**Context.** `cost_usd_increment` has a long tail: p50 is 1.00, p99 is 16.69, the maximum is 317.43 —
a 19× gap between p99 and the max. 211 events are below −0.01.

**What is decided.** The method must be relative, not a fixed cost threshold, because the same
absolute amount is normal for a large org and an anomaly for a small one. Negative costs are flagged
rather than dropped.

**What is open.** Which of z-score, MAD or percentile to use, at which grain (org-day, or
org-service-day), over which trailing window, and at what cut-off.

**Why deferred.** The choice needs the distribution of the *aggregated* daily series, not of raw
increments. MAD is the current favourite because it is robust to exactly the kind of tail measured
here, but that should be checked against Gold data rather than asserted. **Decide by:** delivery 2,
when `cost_anomaly_mart` is implemented.
