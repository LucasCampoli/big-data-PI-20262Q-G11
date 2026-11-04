-- Query-first tables (D11). One partition per organization, days newest first.

-- Q1: daily cost and requests per service for one organization in a date range.
-- Q2: top-N services by cost in the last 14 days for one organization: the same partition, a
--     14-day slice (at most 84 rows) summed and ranked by the client, since CQL cannot order by a
--     non-clustering column.
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_by_service (
  org_id               text,
  usage_date           date,
  service              text,
  daily_cost_usd       double,
  requests             double,
  cpu_hours            double,
  storage_gb_hours     double,
  genai_tokens         bigint,
  carbon_kg            double,
  events               int,
  negative_cost_events int,
  unit_imputed_events  int,
  computed_ts          timestamp,
  run_id               text,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Q6: which days look unusual for one organization. Every scored day is stored, so a rerun with a
-- different threshold overwrites the same rows instead of leaving stale ones.
CREATE TABLE IF NOT EXISTS cloud_analytics.cost_anomalies_by_org (
  org_id            text,
  usage_date        date,
  service           text,
  daily_cost_usd    double,
  median_cost       double,
  mad_cost          double,
  history_days      int,
  score_method      text,
  anomaly_score     double,
  is_anomaly        boolean,
  anomaly_direction text,
  computed_ts       timestamp,
  run_id            text,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
