# Decisions

Design decisions, with the alternatives we considered and what each one costs. Accepted decisions
first, then the two we deliberately left open. Dated 2026-10-03.

Measurements come from [`evidence/landing_profile.md`](evidence/landing_profile.md). Design document:
[`docs/design.md`](docs/design.md).

| # | Decision | Status |
| :-- | :--- | :--- |
| D1 | One explicit superset schema for usage events | Accepted |
| D2 | Read `value` as string in Bronze, cast in Silver with `try_cast` | Accepted |
| D3 | Lambda-style hybrid, with a windowless speed layer | Accepted |
| D4 | Five zones, Parquet with Snappy | Accepted |
| D5 | One date column, `ingest_date` in Bronze and `event_date` in Silver | Accepted |
| D6 | Impute-first quality policy | Accepted |
| D7 | Per-source rule actions, and two scale assumptions | Accepted |
| D8 | Landing immutable, idempotency by overwrite and upsert | Accepted |
| D9 | Convert currency per invoice, and force USD to 1.0 | Accepted |
| D10 | Quarantine samples come from fixtures in delivery 2 | Accepted |
| D11 | Cassandra query-first table layout | Open |
| D12 | Anomaly detection method and threshold | Open |

## D1. One explicit superset schema for usage events

Context. `schema_version=2` adds `carbon_kg`, and GenAI v2 events add `genai_tokens`. Landing holds
all three shapes: 10,800 rows with 11 fields, 29,268 with 12, 3,132 with 13.

Decision. One `StructType` covering the union of all 13 fields, with `carbon_kg` and `genai_tokens`
nullable. Used for every read, batch and streaming.

Alternatives. Schema inference samples the files it is given, so a micro-batch holding only v1 events
would infer a narrower column set and the schema would drift between runs. A schema per version plus
a union means two readers and a merge step, for no gain over nullable columns.

Consequences. v1 rows carry two null columns, which is accurate rather than lossy. Verified on all
120 part files: 43,200 rows, no corrupt records. A v3 field means editing one declaration.

## D2. Read `value` as string in Bronze, cast in Silver with `try_cast`

Context. `value` is written as a JSON number 41,014 times, as a quoted string 1,309 times, and is
null 877 times.

Decision. Bronze types `value` as string. Silver casts with `try_cast`, and a cast failure is a
rejecting rule.

Alternatives. Casting at read time lets Spark turn an unparseable value into a silent null, which
would land inside a cost sum with nothing to show it happened.

Consequences. Bronze is not directly summable on `value`, which is fine because Bronze is not a
query layer. `try_cast` rather than `cast` is required: Spark 4 enables ANSI SQL mode by default, so
a plain cast raises and would abort the job instead of quarantining the row.

## D3. Lambda-style hybrid, with a windowless speed layer

Context. §2.1 of the brief requires near-real-time usage, consumption and incremental cost metrics as
well as batch masters. Each of the 120 part files spans 59 of the 60 days in the dataset, so arrival
order barely relates to event time.

Decision. A speed layer and a batch layer over the same events. The streaming job appends to Bronze
and, in `foreachBatch`, maintains a provisional intraday view of cost and requests by org, service
and event date. It holds no windows: it aggregates within one micro-batch, and idempotency comes from
including the Spark `batchId` in the Cassandra key, so replaying a batch overwrites its own row. The
batch layer recomputes every Gold mart from Silver and replaces the provisional figures with final
ones at D+1. Masters and billing are batch only.

Alternatives. Pure batch fails the near-real-time requirement and the mandatory Structured Streaming
capability of §4.4. Pure Kappa fails twice over: masters are periodic snapshots and billing is
monthly, with no latency need, and computing daily Gold in streaming either discards 97.5% of events
at a 1-day watermark or needs a 60-day watermark holding state for the whole period. Streaming that
stops at Bronze with no serving output was the earlier draft of this decision, and it was wrong: with
no speed-layer output nothing answers §2.1, and the design is batch with a streaming loader rather
than Lambda. A stateful streaming aggregation instead of `foreachBatch` would hold 60 days of windows
to avoid dropping late events, which is the batch job at a higher price.

Consequences. Two execution paths to operate, and cost and requests are now computed in both, so the
two can disagree between the last micro-batch and the next batch run. The containment is that only
those two measures are duplicated, the provisional figure is labelled provisional wherever it is
served, and the batch value wins at D+1. Keeping the two definitions in step is manual work, which is
why the speed layer is limited to two measures. Final marts are as fresh as the batch schedule, O2.
The provisional view is design only for delivery 1 and sits in the backlog as post-delivery-2 work,
since delivery 2 requires the daily mart.

## D4. Five zones, Parquet with Snappy

Context. The brief mandates Landing, Bronze, Silver and Gold plus quarantine in Parquet. Bad rows
need somewhere to go that is neither dropped nor in the marts.

Decision. Five zones, each with one kind of writer, a declared format and a stated promotion
condition. Quarantine is a peer zone that no mart reads. Parquet with Snappy for every managed zone.

Alternatives. A boolean `is_valid` column in Silver means every downstream query has to remember to
filter on it, and the one that forgets corrupts a mart. CSV and JSON give no column pruning or
pushdown. Gzip is not splittable. Delta and Iceberg would give atomic partition overwrites but are
outside the required stack.

Consequences. One more path to write and monitor. In exchange, validity is a property of the zone a
row is in rather than a predicate each consumer repeats.

## D5. One date column, `ingest_date` in Bronze and `event_date` in Silver

Context. 60 days of events, about 720 a day. A streaming write produces at least one file per
micro-batch and partition touched, and each part file spans 59 to 60 distinct event dates.

Decision. Partition on a single date column, keeping `service` and `region` as columns. Bronze uses
`ingest_date`, Silver and Gold use `event_date`. Silver is rebuilt in batch with `coalesce` per
partition. Bronze retention is measured on `ingest_date`.

Alternatives. Adding `service` and `region` gives 2,520 partitions of about 17 rows, so roughly 1 KB
files, where per-file metadata and per-file task scheduling dominate. Partitioning Bronze by
`event_date` writes 7,180 files of 6 rows, and `coalesce` cannot fix it because the fan-out happens
inside each micro-batch. Partitioning Bronze by `event_date` with a later compaction job adds a job
whose only purpose is to undo a choice we are free not to make.

Consequences. Date-range queries prune partitions and service filters rely on row-group statistics.
Querying Bronze by event time needs a full scan, which is acceptable because Bronze is not a query
layer. Bronze retention counted on `event_date` would already have expired every row of this 2025
dataset. This is also a second argument for D3: the streaming layer cannot produce event-time
partitions at a sane file size. Revisit at the projected 1.6 TB of Parquet a day, where the second
level should be `hour` or a hash bucket of `org_id`.

## D6. Impute-first quality policy

Context. 2,075 events, 4.80%, arrive with a null `unit` and carry 4.92% of all cost. In the data,
`metric` determines `unit` 1:1 with no contradictions. A further 877 events have a null `value` and
2.01% of cost.

Decision. Quarantine only contradictions: unknown `metric`, a `unit` that disagrees with its
`metric`, a `value` that will not cast, a null `event_id`, an unjoinable `org_id` or `resource_id`. A
missing but unambiguous field is repaired and flagged, so `unit` is imputed from `metric` with
`unit_imputed = true`. A null `value` keeps its cost and contributes null, not zero, to usage sums.

Alternatives. Quarantining every null `unit` rejects 4.92% of all cost and understates every FinOps
mart by about 5%. Imputing silently makes a repair indistinguishable from source data. Treating a
null `value` as zero biases every average downward and invents a measurement never taken. Dropping
those rows discards 2.01% of real cost.

Consequences. The event quarantine baseline is 0 of 43,200, which is what O5 is set against.
Imputation holds only while `metric` determines `unit`, so the notebook checks that before relying on
it. Usage and cost aggregates can have different denominators for the same org-day, so a usage metric
has to be reported with its row count.

## D7. Per-source rule actions, and two scale assumptions

Context. The master sources hold defects of different kinds and treating them uniformly would be
wrong in both directions.

Decision. Three actions, chosen by whether the row or only a field is untrustworthy.

| Action | Applies when | Rules |
| :--- | :--- | :--- |
| quarantine | the row contradicts itself and is worth nothing without the broken field | `support_tickets.resolved_at` before `created_at`, 0 rows today |
| repair or null, plus a flag | one field is wrong but has a known or irrelevant value | USD rate to 1.0 (160), `csat` outside 1 to 5 (40), `nps_score` outside -100 to 100 (1), null `credits` to zero (137) |
| flag only | the value is plausible, or the row is useful despite it | `users` timestamp rules (232 and 249), negative `subtotal` (13), `converted` without `clicked` (96) |

The `users` timestamps are kept. The two rules fire on 29.0% and 31.1% of rows and 462 of 800 rows
break at least one, where a causally generated dataset would show about 0%, so the three timestamps
were drawn independently. The timestamps are marked low-trust and excluded from any tenure or
recency metric. The identity fields stay usable.

Assumptions. The brief states neither scale, so these are ours: `csat` is 1 to 5, the data holding 0,
6 and 7; both NPS columns use the aggregate -100 to 100 scale, since `nps_surveys.nps_score` ranges
-16 to 68 and rules out a 0 to 10 per-respondent scale. If the course intends other scales, two rules
change.

Alternatives. Quarantining the contradicting `users` rows discards 57.8% of a dimension whose
identity fields are sound and halves any per-org user count. Nulling the timestamps is arbitrary,
because nothing says which of the three is wrong. Flagging without the low-trust marking does not
stop someone computing average account age from a column that cannot support it.

Consequences. The master quarantine baseline is 0 of 4,112 rows. Together with D6, real data rejects
nothing, which is good for the data and a problem for the pipeline, hence D10.

## D8. Landing immutable, idempotency by overwrite and upsert

Context. The brief requires raw data to stay untouched. `event_id` has no duplicates in the data, but
a re-run re-reads the same part files, so duplicates are a property of execution rather than of the
source.

Decision. Nothing writes to Landing, and reprocessing re-reads the original files. Silver and Gold
overwrite the affected partition rather than appending. The Cassandra load upserts on each mart key,
and the provisional intraday view keys on `batchId` (D3). An ingested-files log gates Landing to
Bronze.

Streaming dedupe uses a watermark on `ingest_ts`, never on `timestamp`. Both dedupe APIs discard
data beyond the watermark: PySpark 4.2 documents that `dropDuplicates` drops "data older than
watermark to avoid any possibility of duplicates", and that `dropDuplicatesWithinWatermark`, added
in Spark 3.5, drops "too late data older than watermark". Since `ingest_ts` is stamped at read time
it never lags behind arrival, so nothing is late against it and Bronze keeps every event. The
watermark then bounds only how long a duplicate `event_id` is remembered, in arrival time, which is
the horizon a retry lives on. Exact deduplication is the batch rebuild's job, which dedupes a whole
`event_date` partition out of Bronze with no watermark.

Alternatives. Correcting rows in place in Landing destroys the only reproducible baseline. Appending
plus a later dedupe pass makes correctness depend on a cleanup job having run, and the window between
the two is visible to consumers. On the watermark column, an event-time watermark of 1 to 7 days
would delete 87.6% to 97.5% of events from Bronze, the zone whose job is to be a faithful copy, and
an event-time watermark of 60 days would keep them but hold 60 days of `event_id`s in state: trivial
at today's 43,200 keys, and 2.6 trillion at the §2.1 projection.

Consequences. Landing is never expired, which costs storage, and Bronze can then be kept for only 90
days. Re-running any date is safe, O9. Partition overwrite is not atomic on plain object storage, so
a reader can briefly see a partition mid-write, which is acceptable at a daily cadence and is the
reason D4 names Delta and Iceberg as the thing to adopt if it stops being.

## D9. Convert currency per invoice, and force USD to 1.0

Context. `billing_monthly.csv` holds 240 invoices in USD (160), ARS (51) and EUR (29), each with its
own `exchange_rate_to_usd`. All 160 USD invoices carry a rate that is not 1.0, from 0.855 to 1.118.
13 invoices have a negative subtotal and 137 a null `credits`.

Decision. Normalize every invoice at its own rate before any sum. When `currency` is USD, force the
rate to 1.0 and set `fx_overridden = true`. Treat a null `credits` as zero credit. Keep negative
subtotals as real adjustments and flag them.

Alternatives. One rate per month misstates revenue for every invoice whose rate differs from the
average. Trusting the USD rate makes a USD invoice's USD revenue depend on a meaningless number.
Quarantining the 160 USD invoices drops 67% of billing and all USD revenue over one field with a
known correct value. Dropping negative subtotals overstates revenue.

Consequences. Forcing the rate moves total revenue by only 0.07%, from 164,184.90 to 164,293.06 USD,
because the rates scatter symmetrically around 1.0 and cancel across 160 invoices. Per invoice the
error runs from -14.5% to +11.8%, and Q4 serves one row per org and month. A reconciliation on the
grand total would have passed while two thirds of served rows were wrong, so O9 is checked per
org-day rather than on the total. The override is visible through `fx_overridden`.

## D10. Quarantine samples come from fixtures in delivery 2

Context. After D6 and D7 no real record is rejected, so the quarantine write path, the `dq_rule`
column and the split between passed and rejected rows have no input.

Decision. Delivery 2 adds a small set of synthetic fixtures: one record per rejecting rule, plus a
control record that looks defective but has to pass, being `unit` null with `value` as the text
`"4.25"`. The control record is there because a rule set can fail in both directions, and D6
measured what being too strict costs. Delivery 2's required quarantine samples come from those
fixtures plus whatever real data produces by then.

Alternatives. Injecting bad rows into Landing breaks D8 and corrupts every baseline in `evidence/`.
Waiting for real bad data ships the path unexecuted. Lowering the rules so real rows get rejected
tunes the rules to the test, and D6 measured what that costs.

Consequences. The fixtures will be test data. They never enter `data/landing/` and are never counted
in a profile or a mart. The rule hardest to exercise without them is the uncastable `value`, which
is also the one that depends on `try_cast` rather than `cast`: see D2.

## D11. Cassandra query-first table layout. Open

What is decided. One table per query shape, partition key chosen so each query hits exactly one
partition, no secondary indexes and no `ALLOW FILTERING`. Q1 and Q2 share
`org_daily_usage_by_service`.

What is open. Whether `(org_id)` alone is a safe partition key or needs bucketing by month to bound
partition growth; the clustering order for Q2's top-N; whether `cost_anomaly_mart` is its own table
or a column set on the daily mart.

Why deferred. It depends on the mart row counts, which exist only once Gold runs. Decide before the
Cassandra schema is written in delivery 2.

## D12. Anomaly detection method and threshold. Open

What is decided. The method has to be relative rather than a fixed cost threshold, because the same
amount is normal for a large org and an anomaly for a small one. Negative costs are flagged, not
dropped.

What is open. Which of z-score, MAD or percentile, at which grain, over which trailing window, and at
what cut-off.

Why deferred. The choice needs the distribution of the aggregated daily series, not of raw
increments. MAD is the current favourite because it is robust to the tail measured here, cost p99
16.69 against a maximum of 317.43, but that should be checked against Gold data. Decide in delivery 2
with `cost_anomaly_mart`.
