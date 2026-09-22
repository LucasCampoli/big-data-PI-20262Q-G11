# Cloud Provider Analytics — Design notes

Group 11. First partial: problem framing, why this is a Big Data case, and a first look at the sources. Architecture comes later.

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

## 3. Source inventory

Files are in `data/landing/`. We did not change them. Masters are small CSVs. Usage is a folder of JSONL parts, one event per line.

| Source | Grain | How often | Notes |
| :--- | :--- | :--- | :--- |
| `customers_orgs.csv` | One org (`org_id`) | Snapshot | Industry, region, plan, NPS. Some NPS values are empty or look off. |
| `users.csv` | One user (`user_id`) | Snapshot | Role and activity. `last_login` is often empty. |
| `resources.csv` | One resource (`resource_id`) | Snapshot | Service, region, state. `tags_json` is sometimes empty. |
| `support_tickets.csv` | One ticket (`ticket_id`) | As tickets are opened | Severity, SLA, CSAT. Open tickets have no `resolved_at`; CSAT is often empty. |
| `marketing_touches.csv` | One touch (`touch_id`) | As campaigns run | Channel and whether it converted. |
| `nps_surveys.csv` | One answer per org and date | Over time | Separate from the NPS column on the org file. Some scores and comments are empty. |
| `billing_monthly.csv` | One invoice (`invoice_id`) | Monthly (three months in this dump) | Credits, taxes, currency. Some credits are empty, some subtotals are negative, and not everything is USD. |
| `usage_events_stream/*.jsonl` | One event (`event_id`) | Micro-batches | `value` is sometimes null or text. Costs can be negative. `schema_version=2` adds `carbon_kg` and, for genai, `genai_tokens`. |

`org_id` is on every file, so that is the join key. Traceability for now is the file name. We should keep that name when we load Bronze, and not edit Landing.

### Main risks

- **Schema change.** Events mix v1 and v2. v2 adds `carbon_kg` and, for genai, `genai_tokens`. A strict schema will fail or drop the older rows.
- **Ambiguous types.** Some numeric fields arrive as text or null (`value`, and similar). A hard cast will break the load.
- **Bad amounts.** Costs and invoice subtotals can be negative, and billing is not all USD. Summing them raw will distort FinOps numbers.
- **Nulls on facts we need.** Open tickets have no resolution time, CSAT and NPS are often empty. Averages that ignore that will look better than they are.

Architecture and the rest of the first-partial design are still to be written.
