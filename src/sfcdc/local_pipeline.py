"""Run the CDC logic locally on DuckDB, one daily file per batch.

Snowflake is the target platform (see ``snowflake/``). This mirror lets CI prove the
transformation logic (flattening, drift handling, de-duplication, idempotent MERGE,
late-event handling) on every push, without spending credits.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
SQL = ROOT / "local_sql"


def connect(db: str = ":memory:") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db)
    con.execute((SQL / "01_tables.sql").read_text())
    return con


def land(con: duckdb.DuckDBPyConnection, file: Path, batch_id: int) -> int:
    """COPY INTO equivalent: append each NDJSON line as a JSON payload."""
    before = con.execute("SELECT COUNT(*) FROM raw_order_events").fetchone()[0]
    con.execute(
        "INSERT INTO raw_order_events (payload, src_file, batch_id) "
        "SELECT json, ?, ? FROM read_ndjson_objects(?)",
        [file.name, batch_id, str(file)],
    )
    return con.execute("SELECT COUNT(*) FROM raw_order_events").fetchone()[0] - before


def apply_batch(con: duckdb.DuckDBPyConnection, batch_id: int) -> None:
    con.execute("SET VARIABLE batch = ?", [batch_id])
    con.execute((SQL / "02_apply_batch.sql").read_text())


def run(data_dir: Path = ROOT / "sample_data", db: str = ":memory:") -> duckdb.DuckDBPyConnection:
    con = connect(db)
    for batch_id, f in enumerate(sorted(data_dir.glob("orders_*.json")), start=1):
        land(con, f, batch_id)
        apply_batch(con, batch_id)
    return con


def summary(con: duckdb.DuckDBPyConnection) -> dict:
    q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    return {
        "raw_events": q("SELECT COUNT(*) FROM raw_order_events"),
        "unique_events": q("SELECT COUNT(*) FROM order_status_history"),
        "orders": q("SELECT COUNT(*) FROM orders"),
        "order_lines": q("SELECT COUNT(*) FROM order_items"),
        "cancelled_orders": q("SELECT COUNT(*) FROM orders WHERE order_status = 'cancelled'"),
        "v2_orders_with_channel": q("SELECT COUNT(*) FROM orders WHERE channel IS NOT NULL"),
        "null_payment_method": q("SELECT COUNT(*) FROM orders WHERE payment_method IS NULL"),
        "net_sales_excl_cancelled": float(
            q(
                "SELECT SUM(i.net_amount) FROM order_items i JOIN orders o USING (order_id) "
                "WHERE o.order_status <> 'cancelled'"
            )
        ),
    }


if __name__ == "__main__":
    for k, v in summary(run()).items():
        print(f"{k:28s} {v:,}")
