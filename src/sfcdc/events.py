"""Generate nested JSON order events (NDJSON), one file per day, like an app's event export.

Realistic semi-structured features on purpose:
* nested objects (order.customer.address) and arrays (order.items[], items[].promos[])
* change events for the same order: created -> updated (qty/price change) -> cancelled
* **schema drift**: from 2025-03-01 events are ``schema_version: 2`` and carry a new
  ``order.channel`` field and ``payment.wallet`` instead of ``payment.method``
* late-arriving and duplicate events (same event_id delivered twice)
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 11
CITIES = [
    ("Gurugram", "IN"),
    ("Bengaluru", "IN"),
    ("Mumbai", "IN"),
    ("Hyderabad", "IN"),
    ("Dubai", "AE"),
    ("Singapore", "SG"),
]
SKUS = [(f"SKU-{i:04d}", round(random.Random(i).uniform(99, 49999), 2)) for i in range(1, 121)]
DRIFT_DATE = datetime(2025, 3, 1)


def _order(rng: random.Random, order_id: str, ts: datetime) -> dict:
    city, country = rng.choice(CITIES)
    items = []
    for line in range(1, rng.randint(1, 4) + 1):
        sku, price = rng.choice(SKUS)
        promos = (
            [{"code": rng.choice(["FEST10", "NEW5", "BANK15"]), "pct": rng.choice([5, 10, 15])}]
            if rng.random() < 0.3
            else []
        )
        items.append({"line": line, "sku": sku, "qty": rng.randint(1, 3), "price": price, "promos": promos})
    order = {
        "order_id": order_id,
        "customer": {
            "id": f"CUST-{rng.randint(1, 1500):05d}",
            "tier": rng.choice(["gold", "silver", "standard"]),
            "address": {"city": city, "country": country},
        },
        "items": items,
    }
    if ts >= DRIFT_DATE:
        order["channel"] = rng.choice(["app", "web", "store"])
        order["payment"] = {"wallet": rng.choice(["upi", "card", "cod"]), "status": "authorised"}
    else:
        order["payment"] = {"method": rng.choice(["upi", "card", "cod"]), "status": "authorised"}
    return order


def generate(
    out_dir: str | Path = "sample_data", start: str = "2025-02-24", days: int = 14, orders_per_day: int = 120
) -> dict[str, int]:
    """Write one NDJSON file per day; returns counts."""
    rng = random.Random(SEED)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    d0 = datetime.fromisoformat(start)
    open_orders: list[tuple[str, dict]] = []
    stats = {"events": 0, "duplicates": 0, "files": 0}
    n = 0
    for day in range(days):
        date = d0 + timedelta(days=day)
        events = []
        for _ in range(orders_per_day):
            n += 1
            ts = date + timedelta(seconds=rng.randint(0, 86399))
            order = _order(rng, f"ORD-{n:06d}", ts)
            events.append({"event_type": "order_created", "event_ts": ts, "order": order})
            open_orders.append((order["order_id"], order))
        # updates and cancellations for earlier orders
        for _oid, order in rng.sample(open_orders, k=min(25, len(open_orders))):
            ts = date + timedelta(seconds=rng.randint(0, 86399))
            upd = json.loads(json.dumps(order))
            upd["items"][0]["qty"] += 1
            kind = "order_cancelled" if rng.random() < 0.3 else "order_updated"
            upd.setdefault("payment", {})["status"] = "voided" if kind == "order_cancelled" else "captured"
            events.append({"event_type": kind, "event_ts": ts, "order": upd})
        rng.shuffle(events)
        lines = []
        for i, e in enumerate(events):
            ts = e["event_ts"]
            rec = {
                "event_id": f"EVT-{date:%Y%m%d}-{i:05d}",
                "event_type": e["event_type"],
                "event_ts": ts.isoformat(timespec="seconds"),
                "schema_version": 2 if ts >= DRIFT_DATE else 1,
                "order": e["order"],
            }
            lines.append(json.dumps(rec))
            if rng.random() < 0.01:  # at-least-once delivery: duplicates
                lines.append(json.dumps(rec))
                stats["duplicates"] += 1
        (out / f"orders_{date:%Y%m%d}.json").write_text("\n".join(lines) + "\n")
        stats["events"] += len(lines)
        stats["files"] += 1
    return stats


if __name__ == "__main__":
    print(generate())
