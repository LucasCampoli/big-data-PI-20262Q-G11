-- Q1. Daily cost and requests per service for one organization in a date range.
-- One partition (org_id), one contiguous slice of the clustering order.
SELECT usage_date, service, daily_cost_usd, requests, events
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-07';
