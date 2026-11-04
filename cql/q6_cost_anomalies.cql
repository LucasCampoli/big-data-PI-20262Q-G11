-- Q6. Scored days of one organization in August, newest first; the client keeps is_anomaly = true.
SELECT usage_date, service, daily_cost_usd, median_cost, anomaly_score, is_anomaly, anomaly_direction
FROM cloud_analytics.cost_anomalies_by_org
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-31';
