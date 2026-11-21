-- Q5. GenAI tokens and estimated cost per day for one organization in August.
-- One partition, one contiguous slice of days with GenAI usage.
SELECT usage_date, genai_tokens, genai_cost_usd, usd_per_1k_tokens, events, events_with_tokens
FROM cloud_analytics.genai_tokens_by_org_date
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-31';
