# Cloud Provider Analytics — Design notes

Group 11. First partial: problem framing and why this is a Big Data case. Architecture comes later.

## 1. Problem, users, questions, and goals

We act as the data team of a cloud provider. Customer data arrives raw and messy: nulls, noisy values, numbers that sometimes come as text, occasional negative costs, and a schema change mid-history (`schema_version=2` adds `carbon_kg` and, for GenAI, `genai_tokens`). Usage events also show up as small JSONL fragments, not as one clean file.

The job is to take those sources and turn them into something FinOps, Support, and Product can actually query. Masters and billing can wait for a daily or monthly batch. Usage needs to be closer to real time.

### Users

| Who | What they need |
| :--- | :--- |
| FinOps | Costs, consumption, revenue, credits, taxes, odd spikes, and efficiency by organization and service. |
| Support | Ticket volume, severity, SLA compliance, and CSAT by organization and date. |
| Product / Usage | Service usage, requests, operational metrics, and GenAI tokens / carbon when those fields exist. |

### Main questions

- FinOps: what did each organization spend per service on a given day, and which days look abnormal?
- FinOps: for a month, what is revenue in USD after credits and taxes?
- Support: how many critical tickets opened, and what share missed the SLA?
- Product: how many requests (and GenAI tokens, when present) did an organization generate?

### Measurable objectives

1. Produce daily cost and request totals by organization and service.
2. Produce ticket counts and an SLA-breach rate by organization, date, and severity.
3. Keep the original Landing files unchanged, and still load records that have nulls or mixed types without dropping the whole file.

## 2. Why Big Data (5V)

| V | Why it shows up here |
| :--- | :--- |
| Volume | Usage is split across many JSONL files, not a single spreadsheet. Masters (orgs, users, resources, tickets, billing) add more tables that have to be joined later. |
| Velocity | Usage arrives as micro-batches meant to be read as a stream. Billing and CRM can stay on a slower batch schedule. |
| Variety | Mix of CSV masters and JSONL events. Event schema changes from v1 to v2 (`carbon_kg`, `genai_tokens`). Some numeric fields arrive as text. |
| Veracity | Nulls, noisy org attributes, negative costs, and large cost spikes. We cannot treat every row as clean. |
| Value | FinOps, Support, and Product need the same customer data for different questions (spend, SLA, usage). A shared pipeline is what makes those answers possible. |

Architecture and the rest of the first-partial design are still to be written.
