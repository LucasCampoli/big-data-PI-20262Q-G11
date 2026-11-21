-- Q4. Monthly revenue in USD after credits and taxes for one organization, newest month first.
-- One partition: one row per month, about 12 a year.
SELECT month, invoices, subtotal_usd, credits_usd, taxes_usd, revenue_usd, currency, exchange_rate_applied
FROM cloud_analytics.revenue_by_org_month
WHERE org_id = 'org_okep7y6w';
