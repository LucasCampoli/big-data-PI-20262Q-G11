# Pipeline health

Checked at 2026-11-21T19:14:39+00:00 over 8 run summaries. Overall: **OK**.

| level | check | detail |
| --- | --- | --- |
| OK | last run | 20261121T191249Z ok |
| OK | freshness | Gold built by 20261121T191249Z, 0.0 h ago (limit 26 h) |
| OK | stream watermark | 0 rows dropped by the watermark (D8 expects 0) |
| OK | O3 quarantine share | 0.0% of events, 0.0% of cost (limit 1%) |
| OK | Gold reconciliation | 1 mart(s) passed |
| OK | marts reconciliation | 5 mart(s) passed |
| OK | serving row check | 6 tables on cassandra, rows equal to Gold |
| OK | Quarantine growth | 0 rows in 20261121T191249Z, 0 in 20261121T191156Z |

## Last 8 runs

| run_id | status | stages | silver_events | quarantined | gold_rows | serving | seconds |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 20261115T182355Z | ok | 7 | 43200 | 0 | 11050 | cassandra | 23.6 |
| 20261121T184650Z | ok | 1 |  |  |  |  | 14.4 |
| 20261121T184934Z | failed | 1 |  |  |  |  | 0.9 |
| 20261121T190729Z | ok | 1 |  |  |  | cassandra | 11.0 |
| 20261121T190818Z | ok | 8 | 43200 | 0 | 11050 | cassandra | 41.7 |
| 20261121T190919Z | ok | 8 | 43200 | 0 | 11050 | cassandra | 35.6 |
| 20261121T191156Z | ok | 8 | 43200 | 0 | 11050 | cassandra | 47.3 |
| 20261121T191249Z | ok | 8 | 43200 | 0 | 11050 | cassandra | 35.9 |
