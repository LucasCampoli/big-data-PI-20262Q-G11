# Quarantine fixtures

Synthetic records that deliberately violate the quality rules in
[`../../../docs/design.md`](../../../docs/design.md) §5.7, one rule per record. They exist because
**real Landing data rejects almost nothing** — the measured quarantine baseline is 0 of 43,200
events and 0 of 4,112 master rows — so without fixtures the quarantine path would never be
exercised by a test.

These files are **not** Landing data. They are never copied into `data/landing/`, never read by the
exploration notebook, and never counted in any profile or mart. Landing stays immutable (D10).

`usage_events_invalid.jsonl` — 8 records covering the event rules:

| `event_id` | Defect | Expected outcome |
| :--- | :--- | :--- |
| `null` | null `event_id` | quarantine — cannot dedupe or trace |
| `evt_fixture_dup` ×2 | duplicate `event_id` | one row survives, `dropDuplicates` removes the other |
| `evt_fixture_metric` | `metric = quantum_flops` | quarantine — unknown metric, cannot map to a feature |
| `evt_fixture_ts` | `timestamp = "not-a-timestamp"` | quarantine — unparseable, so the row cannot be placed on a date |
| `evt_fixture_unit` | `metric = cpu_hours` with `unit = count` | quarantine — two fields contradict |
| `evt_fixture_value` | `value = "n/a"` | quarantine — present but will not cast to double |
| `evt_fixture_ok` | `unit` null, `value` as text `"4.25"` | **kept** — unit imputed to `hours`, `unit_imputed = true`, value cast to 4.25 |

The last record is the control: it looks defective but is repairable, so a rule set that quarantines
it is too strict. `tests/test_quality_rules.py` asserts all eight outcomes.

Delivery 2's required quarantine samples come from these fixtures plus whatever real data produces.
