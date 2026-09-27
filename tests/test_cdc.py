"""Prove the CDC logic against an independent pure-Python recomputation."""

import json
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

from sfcdc import events, local_pipeline

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "sample_data"


@pytest.fixture(scope="module")
def con():
    if not list(DATA.glob("orders_*.json")):
        events.generate(DATA)
    return local_pipeline.run(DATA)


def expected_state():
    """Latest event per order wins; duplicates ignored. Written without SQL on purpose."""
    latest = {}
    seen = set()
    for f in sorted(DATA.glob("orders_*.json")):
        for line in f.read_text().splitlines():
            e = json.loads(line)
            if e["event_id"] in seen:
                continue
            seen.add(e["event_id"])
            oid = e["order"]["order_id"]
            key = (e["event_ts"], e["event_id"])
            if oid not in latest or key > latest[oid][0]:
                latest[oid] = (key, e)
    return latest, seen


def test_counts_match_independent_recomputation(con):
    latest, seen = expected_state()
    s = local_pipeline.summary(con)
    assert s["unique_events"] == len(seen)
    assert s["orders"] == len(latest)
    cancelled = sum(1 for _, e in latest.values() if e["event_type"] == "order_cancelled")
    assert s["cancelled_orders"] == cancelled


def test_net_sales_match_to_the_paisa(con):
    latest, _ = expected_state()
    total = Decimal("0")
    for _, e in latest.values():
        if e["event_type"] == "order_cancelled":
            continue
        for it in e["order"]["items"]:
            pct = sum((Decimal(str(p["pct"])) for p in it["promos"]), Decimal(0))
            amt = it["qty"] * Decimal(str(it["price"])) * (1 - pct / 100)
            total += amt.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    assert Decimal(str(local_pipeline.summary(con)["net_sales_excl_cancelled"])) == total


def test_schema_drift_is_handled(con):
    s = local_pipeline.summary(con)
    assert s["null_payment_method"] == 0  # v1 method + v2 wallet coalesced
    assert s["v2_orders_with_channel"] > 0


def test_duplicates_are_removed(con):
    s = local_pipeline.summary(con)
    assert s["raw_events"] > s["unique_events"]  # raw keeps everything (replayable)


def test_reapplying_a_batch_is_idempotent(con):
    before = con.execute("SELECT * FROM orders ORDER BY order_id").fetchall()
    items_before = con.execute("SELECT * FROM order_items ORDER BY 1, 2").fetchall()
    local_pipeline.apply_batch(con, 3)  # replay an old batch
    assert con.execute("SELECT * FROM orders ORDER BY order_id").fetchall() == before
    assert con.execute("SELECT * FROM order_items ORDER BY 1, 2").fetchall() == items_before


def test_late_event_does_not_overwrite_newer_state(tmp_path):
    c = local_pipeline.connect()
    new = {
        "event_id": "E2",
        "event_type": "order_cancelled",
        "event_ts": "2025-03-02T10:00:00",
        "schema_version": 2,
        "order": {
            "order_id": "O1",
            "customer": {"id": "C", "tier": "gold", "address": {"city": "Pune", "country": "IN"}},
            "items": [],
            "payment": {"wallet": "upi", "status": "voided"},
        },
    }
    late = json.loads(json.dumps(new))
    late.update(event_id="E1", event_type="order_updated", event_ts="2025-03-01T09:00:00")
    for i, ev in enumerate([new, late], start=1):
        f = tmp_path / f"orders_{i}.json"
        f.write_text(json.dumps(ev) + "\n")
        local_pipeline.land(c, f, i)
        local_pipeline.apply_batch(c, i)
    assert c.execute("SELECT order_status FROM orders").fetchone()[0] == "cancelled"
    assert c.execute("SELECT COUNT(*) FROM order_status_history").fetchone()[0] == 2
