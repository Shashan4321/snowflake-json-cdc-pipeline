# Cost & performance notes

Design choices that keep a Snowflake CDC pipeline cheap and fast, and how to measure them.

## Cost controls built into the scripts

| Control | Where | Why |
|---|---|---|
| X-Small warehouse, `AUTO_SUSPEND = 60` | `00_setup.sql` | Pay only while queries run; suspends after 1 idle minute |
| `WHEN SYSTEM$STREAM_HAS_DATA(...)` on the task | `04_streams_tasks.sql` | The warehouse does not even start when there is nothing new, so an idle pipeline costs 0 credits |
| Stream-based incremental processing | `04_streams_tasks.sql` | Each run touches only new events, not the whole history |
| `APPEND_ONLY = TRUE` stream | `04_streams_tasks.sql` | Cheaper than a standard stream for an insert-only landing table |
| COPY load metadata (64 days) | `01_raw_landing.sql` | Re-running `COPY INTO` never double-loads a file |
| Optional resource monitor | `00_setup.sql` | Hard stop before the trial budget is used up |

## Query performance levers

| Lever | Applied to | Effect to look for in the Query Profile |
|---|---|---|
| Flatten once into typed tables (`ORDERS`, `ORDER_ITEMS`) instead of querying `VARIANT` in every report | marts read typed columns | Less work per query; fewer bytes scanned |
| `CLUSTER BY (TO_DATE(LAST_EVENT_TS))` | `ORDERS` | Date filters prune micro-partitions (`PARTITIONS_SCANNED` much lower than `PARTITIONS_TOTAL`) |
| `QUALIFY ROW_NUMBER()` rather than self-joins for dedupe and latest-record | views + procedure | One pass over the data, no join spill |
| Exact decimal arithmetic (`* 0.01` instead of `/ 100`) | net amount | Avoids float rounding. The local tests caught an 8-paise drift over INR 18.7 crore before this change |

## How to capture before/after evidence (to do on a trial account)

1. Run a report query (e.g. `SELECT * FROM MARTS.DAILY_SALES WHERE ORDER_DATE >= '2025-03-01'`) against a version that reads `RAW_ORDER_EVENTS` directly (flattening at query time). Record `QUERY_ID`.
2. Run the same query against the typed, clustered `CORE` tables. Record `QUERY_ID`.
3. Pull both from `06_monitoring.sql` (`TOTAL_ELAPSED_TIME`, `BYTES_SCANNED`, `PARTITIONS_SCANNED/TOTAL`) and screenshot the Query Profile.
4. Add the numbers and screenshots to the README table "Measured on Snowflake". Only measured numbers go in, nothing estimated.
