-- Q3. Critical tickets and SLA breach rate per day over the last 30 days for one organization.
-- The data ends on 2025-08-31, so the 30 days are 2025-08-02 to 2025-08-31. One partition, one
-- contiguous slice; a day without a row had no tickets.
SELECT ticket_date, tickets, critical_tickets, sla_breached_tickets, sla_breach_rate, csat_avg
FROM cloud_analytics.tickets_daily_by_org
WHERE org_id = 'org_kdgigatj'
  AND ticket_date >= '2025-08-02' AND ticket_date <= '2025-08-31';
