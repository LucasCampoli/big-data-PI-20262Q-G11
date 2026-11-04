-- Q2. Top services by cost over the last 14 days for one organization.
-- The data ends on 2025-08-31, so the 14 days are 2025-08-18 to 2025-08-31. The read is one slice
-- of one partition (at most 14 days x 6 services = 84 rows); the sum per service and the ranking
-- happen in the client (python -m pipeline.queries), because CQL can only GROUP BY the primary key
-- prefix and cannot ORDER BY a regular column.
SELECT usage_date, service, daily_cost_usd
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-18' AND usage_date <= '2025-08-31';
