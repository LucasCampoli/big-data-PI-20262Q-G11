# Fixture Landing

A synthetic Landing with the same layout as `data/landing/`: the seven CSV masters plus
`usage_events_stream/`. Each defective record breaks one quality rule from `docs/design.md` §5.2,
because real Landing data rejects nothing (0 of 43,200 events and 0 of 4,112 master rows) and the
quarantine path would otherwise never run.

These files are not Landing data. They are never copied into `data/landing/` and never counted in a
profile or a mart (D10). `make fixtures` runs the pipeline over them with `DATA_LANDING` pointing
here and its own lake in `datalake/_fixtures/`.

## Events, `usage_events_stream/events_fixture_0000.jsonl`

| `event_id` | Defect | Expected outcome |
| :--- | :--- | :--- |
| `null` | null `event_id` | Silver quarantine, `event_id_null` |
| `evt_fixture_dup` x2 | same event twice in one file | one row in Bronze, the streaming dedupe drops the copy |
| `evt_fixture_metric` | `metric = quantum_flops` | Silver quarantine, `metric_unknown` |
| `evt_fixture_ts` | `timestamp = "not-a-timestamp"` | Bronze quarantine, `parse_type_mismatch` |
| `evt_fixture_ts_null` | `timestamp = null` | Silver quarantine, `timestamp_unparseable` |
| `evt_fixture_unit` | `metric = cpu_hours` with `unit = count` | Silver quarantine, `unit_contradicts_metric` |
| `evt_fixture_value` | `value = "n/a"` | Silver quarantine, `value_not_castable` |
| `evt_fixture_org` | `org_id` not in `customers_orgs` | Silver quarantine, `org_id_unknown` |
| `evt_fixture_res` | `resource_id` not in `resources` | Silver quarantine, `resource_id_unknown` |
| `evt_fixture_owner` | resource of another organization | Silver quarantine, `resource_org_mismatch` |
| `evt_fixture_broken` | truncated JSON line | Bronze quarantine, `parse_malformed_json` |
| `evt_fixture_ok` | `unit` null, `value` as text `"4.25"` | kept: `unit` imputed to `hours`, `unit_imputed = true`, value 4.25 |
| `evt_fixture_neg` | `cost_usd_increment = -5.0` | kept, `cost_anomaly_flag = true` |
| `evt_fixture_v1` | `schema_version = 1`, `value` null | kept, null in the usage sums, cost counted |
| `evt_fixture_genai` | v2 GenAI event with tokens | kept, feeds `genai_tokens` |

`evt_fixture_ok` is the control: it looks defective but is repairable, so a rule set that
quarantines it is too strict.

## Masters

| File | Record | Expected outcome |
| :--- | :--- | :--- |
| `customers_orgs.csv` | `org_fixture01` with `nps_score = 150` | nulled, `nps_score_invalid` |
| `users.csv` | `user_fixture01`, `last_login` before `created_at` | flagged; email replaced by hash and domain |
| `users.csv` | `user_fixture02`, created before the org signed up | flagged |
| `support_tickets.csv` | `tkt_fixture02`, resolved before created | Silver quarantine, `resolved_before_created` |
| `support_tickets.csv` | `tkt_fixture03`, `csat = 7` | nulled, `csat_invalid` |
| `support_tickets.csv` | `tkt_fixture04`, `created_at = yesterday` | Bronze quarantine, `parse_error` |
| `marketing_touches.csv` | `mkt_fixture01`, converted without a click | flagged |
| `nps_surveys.csv` | `nps_score = -120` | nulled, `nps_score_invalid` |
| `billing_monthly.csv` | USD invoice with rate 0.97 and null credits | rate forced to 1.0, `fx_overridden`; credits 0, `credits_imputed` |
| `billing_monthly.csv` | negative subtotal | flagged `subtotal_negative`, stays in revenue |

`tests/test_quality_rules.py` and `tests/test_silver.py` assert every row of both tables.
