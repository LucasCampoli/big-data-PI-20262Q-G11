# Evidence

Generated artifacts that back the claims in `docs/design.md`. Do not edit by hand, re-run the
notebook that produces them.

| File | Content |
| :--- | :--- |
| `landing_profile.md` | Full Landing profile: per-source grain and row counts, nulls, event schema versions, the `value` type inconsistency, cost distribution, the late-data measurement, Bronze and Silver partitioning, and the baseline for every quality rule. |
| `landing_source_profile.csv` | One row per source: format, files, rows, columns, grain key, duplicate keys, date range. |
| `landing_column_profile.csv` | One row per column: null count and null share. |

All three come from `notebooks/01_landing_exploration.ipynb`. Screenshots and run logs for the
deliveries also go here.
