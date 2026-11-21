-- Query-first tables (D11). One partition per organization, days newest first.

-- Every table holds exactly the rows its query returns: Cassandra reads one slice of one partition
-- and the client neither sums nor sorts.

-- Q1: daily cost and requests per service for one organization in a date range.
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

-- Q2: top-N services by cost over the 14 days ending on a cutoff date, for one organization.
-- Precomputed and ranked in Gold for every cutoff: the query is one (as_of_date) slice of the
-- partition with LIMIT N. At most 6 services per cutoff, so about 2.190 rows per organization a year.
CREATE TABLE IF NOT EXISTS cloud_analytics.top_services_14d_by_org (
  org_id           text,
  as_of_date       date,
  rank             int,
  service          text,
  cost_14d_usd     double,
  days_with_usage  int,
  events           bigint,
  window_start     date,
  window_complete  boolean,
  computed_ts      timestamp,
  run_id           text,
  PRIMARY KEY ((org_id), as_of_date, rank)
) WITH CLUSTERING ORDER BY (as_of_date DESC, rank ASC);

-- Q3: critical tickets and SLA breach rate per day over the last 30 days, for one organization.
-- One row per day with tickets; a day without a row had no tickets.
CREATE TABLE IF NOT EXISTS cloud_analytics.tickets_daily_by_org (
  org_id                 text,
  ticket_date            date,
  tickets                int,
  critical_tickets       int,
  sla_breached_tickets   int,
  critical_sla_breached  int,
  sla_known_tickets      int,
  sla_breach_rate        double,
  open_tickets           int,
  csat_responses         int,
  csat_avg               double,
  computed_ts            timestamp,
  run_id                 text,
  PRIMARY KEY ((org_id), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- Q4: monthly revenue in USD with credits and taxes applied, for one organization.
CREATE TABLE IF NOT EXISTS cloud_analytics.revenue_by_org_month (
  org_id                      text,
  month                       date,
  invoices                    int,
  subtotal_usd                double,
  credits_usd                 double,
  taxes_usd                   double,
  revenue_usd                 double,
  currency                    text,
  exchange_rate_applied       double,
  fx_overridden_invoices      int,
  credits_imputed_invoices    int,
  negative_subtotal_invoices  int,
  computed_ts                 timestamp,
  run_id                      text,
  PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- Q5: GenAI tokens and estimated cost per day, for one organization. Days with GenAI usage only;
-- tokens stay null on a day whose events carry none.
CREATE TABLE IF NOT EXISTS cloud_analytics.genai_tokens_by_org_date (
  org_id              text,
  usage_date          date,
  genai_tokens        bigint,
  genai_cost_usd      double,
  token_cost_usd      double,
  usd_per_1k_tokens   double,
  events              int,
  events_with_tokens  int,
  computed_ts         timestamp,
  run_id              text,
  PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
