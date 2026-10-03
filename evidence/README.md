# Evidence

Generated artifacts that back the claims in `docs/design.md`. Do not edit by hand — re-run the
notebook that produces them.

| File | Produced by | Content |
| :--- | :--- | :--- |
| `landing_profile.md` | `notebooks/01_landing_exploration.ipynb` | Full Landing profile: per-source grain and row counts, nulls, event schema versions, `value` type inconsistency, cost distribution, the late-data measurement, and the baseline count for every quality rule. |
| `landing_source_profile.csv` | same | One row per source: format, files, rows, columns, grain key, duplicate keys, date range. |
| `landing_column_profile.csv` | same | One row per column: null count and null share. |

Screenshots and run logs for the deliveries also go here.
