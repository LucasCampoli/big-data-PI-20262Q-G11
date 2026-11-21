-- Q2. Top 3 services by cost over the last 14 days for one organization.
-- The data ends on 2025-08-31, so the cutoff is that day and the window 2025-08-18 to 2025-08-31.
-- One partition, one as_of_date slice already in rank order: LIMIT is the N of the top-N.
SELECT rank, service, cost_14d_usd, days_with_usage, window_start
FROM cloud_analytics.top_services_14d_by_org
WHERE org_id = 'org_okep7y6w' AND as_of_date = '2025-08-31'
LIMIT 3;
