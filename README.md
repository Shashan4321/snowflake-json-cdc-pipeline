# Snowflake JSON CDC Pipeline

**Nested JSON order events go into Snowflake as raw `VARIANT`, get flattened with `LATERAL FLATTEN`, and are applied incrementally with Streams + Tasks and idempotent `MERGE`. The pipeline handles schema drift, duplicate deliveries and late events.**

[![CI](https://github.com/Shashan4321/snowflake-json-cdc-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Shashan4321/snowflake-json-cdc-pipeline/actions/workflows/ci.yml)
![Snowflake](https://img.shields.io/badge/Snowflake-Streams%20%7C%20Tasks%20%7C%20FLATTEN-29B5E8?logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-MERGE%20%7C%20QUALIFY-4479A1)
![DuckDB](https://img.shields.io/badge/CI%20mirror-DuckDB-FFF000?logo=duckdb&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green)

> **Business problem.** The order app emits JSON events: created, updated, cancelled. Finance needs a *current, correct* order table and daily sales without reloading the full history every time. The app team also keeps changing the JSON, and the queue sometimes delivers the same event twice.

| | |
|---|---|
| **Stack** | Snowflake (VARIANT, FLATTEN, Streams, Tasks, Snowflake Scripting, MERGE, clustering) · DuckDB (local CI mirror) · Python · pytest · sqlfluff |
| **Skills shown** | Semi-structured data modelling · CDC / incremental loads · idempotency · schema-drift handling · data-quality testing · cost-aware warehouse design |
| **Data** | 14 days of synthetic order events (2,058 events, 28 duplicates, schema v1 → v2 on 1-Mar-2025) in [`sample_data/`](sample_data) |

## Architecture

```mermaid
flowchart LR
    A[Order app<br/>JSON events] -->|daily NDJSON files| S[(Stage<br/>@STG_ORDERS)]
    S -->|COPY INTO<br/>idempotent| R[(RAW.RAW_ORDER_EVENTS<br/>VARIANT, append-only)]
    R --> ST{{STREAM<br/>new rows only}}
    ST -->|TASK every 5 min<br/>WHEN STREAM_HAS_DATA| P[SP_APPLY_ORDER_EVENTS<br/>dedupe · drift · FLATTEN]
    P -->|MERGE latest-wins| O[(CORE.ORDERS)]
    P -->|MERGE| I[(CORE.ORDER_ITEMS)]
    P -->|insert-if-new| H[(CORE.ORDER_STATUS_HISTORY)]
    O --> M[MARTS views<br/>DAILY_SALES · CANCELLATION_RATE]
    I --> M
    M --> BI[Power BI / Tableau]
```

## The hard parts, and how they are handled

| Problem in the data | Handling | Where |
|---|---|---|
| Nested arrays: `order.items[]`, `items[].promos[]` | `LATERAL FLATTEN(items)` then `LATERAL FLATTEN(promos, OUTER => TRUE)`, so lines without promos are kept | `03_flatten_views.sql`, `04_streams_tasks.sql` |
| **Schema drift**: v2 renames `payment.method` → `payment.wallet` and adds `order.channel` | `COALESCE(payment.wallet, payment.method)`; `channel` is nullable and kept once known | same |
| Duplicate deliveries (at-least-once queue) | `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ...) = 1` | procedure |
| Several events for one order in a batch | latest `event_ts` wins, `event_id` breaks ties | procedure |
| **Late events** (older than the stored state) | `WHEN MATCHED AND s.EVENT_TS > t.LAST_EVENT_TS` | procedure |
| Re-runs / failures | stream consumed via `INSERT` into a work table that is cleared only after all MERGEs succeed; all MERGEs are idempotent | procedure |
| Idle cost | task runs only `WHEN SYSTEM$STREAM_HAS_DATA`, XS warehouse, 60 s auto-suspend | `00_setup.sql`, `04_...sql` |

```sql
-- Order lines from nested JSON, promos summed per line, drift-safe payment field
SELECT e.ORDER_ID,
       i.value:line::NUMBER  AS LINE_NO,
       i.value:sku::STRING   AS SKU,
       i.value:qty::NUMBER   AS QTY,
       i.value:price::NUMBER(12,2) AS PRICE,
       COALESCE(SUM(p.value:pct::NUMBER(5,2)), 0) AS PROMO_PCT
FROM V_EVENTS e,
     LATERAL FLATTEN(INPUT => e.ITEMS) i,
     LATERAL FLATTEN(INPUT => i.value:promos, OUTER => TRUE) p
GROUP BY ALL;
```

## In this project

Snowflake is the target platform. So that every push is tested without spending credits, [`local_sql/`](local_sql) mirrors the same logic in DuckDB (`UNNEST` for `FLATTEN`, `MERGE INTO`, `QUALIFY`) and replays the 14 daily files one batch at a time, like a stream. Results from `make local`:

| Metric | Value |
|---|---:|
| Raw events landed (incl. duplicates) | 2,058 |
| Unique events after dedupe | 2,030 |
| Orders (current state) / order lines | 1,680 / 4,182 |
| Cancelled orders | 84 |
| Orders with schema-v2 `channel` | 1,080 |
| Orders with missing payment method after drift handling | **0** |
| Net sales excl. cancelled | ₹187,339,788.49 |

The 6 tests compare the pipeline to an **independent pure-Python recomputation** from the raw JSON: order count, cancellations and net sales must match to the paisa. They also check that replaying an old batch changes nothing (idempotency) and that a late event cannot overwrite newer state.

> A real bug this caught: dividing `DECIMAL` by 100 gave a `DOUBLE` in DuckDB, and net sales drifted by 8 paise over ₹18.7 crore. Both SQL versions now use exact decimal arithmetic (`* 0.01`). Details in [`docs/cost_and_performance.md`](docs/cost_and_performance.md).

### Measured on Snowflake

To be filled from a trial account run using [`06_monitoring.sql`](snowflake/06_monitoring.sql) (query IDs, elapsed time, bytes and partitions scanned, credits). The method is in the [cost & performance notes](docs/cost_and_performance.md). Only measured numbers will be added.

## Run it

**Locally (no account needed):**

```bash
git clone https://github.com/Shashan4321/snowflake-json-cdc-pipeline.git
cd snowflake-json-cdc-pipeline
pip install -r requirements-dev.txt
make local     # replay 14 daily batches through the CDC logic
make test      # 6 tests vs independent recomputation
```

**On Snowflake (free 30-day trial):** run `snowflake/00` → `05` in order in a Snowsight worksheet, `PUT` the files from `sample_data/` into `@STG_ORDERS`, then `EXECUTE TASK TSK_APPLY_ORDER_EVENTS` (or wait 5 minutes). Load the files a few days at a time to watch the stream pick up only the new ones.

## Project structure

```text
├── snowflake/            # 00 setup · 01 raw landing · 02 core tables · 03 flatten views
│                         # 04 stream + task + MERGE procedure · 05 marts · 06 monitoring
├── local_sql/            # DuckDB mirror of the CDC logic (runs in CI)
├── src/sfcdc/
│   ├── events.py         # seeded event generator (drift, duplicates, updates, cancels)
│   └── local_pipeline.py # batch-by-batch replay
├── sample_data/          # 14 daily NDJSON files
├── tests/test_cdc.py
└── docs/cost_and_performance.md
```

## Data & license

* **Data:** 100% synthetic, generated by [`events.py`](src/sfcdc/events.py) (seed 11). No employer or client data, schema or code is used.
* **Code:** MIT License.

## Author

**Shashank Singh**, Senior Data Analyst · [Portfolio](https://shashan4321.github.io) · [LinkedIn](https://www.linkedin.com/in/shashank-moon)

*Professional impact:* built Snowflake JSON pipelines with FLATTEN and LATERAL joins, Streams (CDC) and Replication, and cut query time by 35%. This repo shows the patterns on open, synthetic data.
