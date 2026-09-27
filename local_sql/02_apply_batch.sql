-- DuckDB mirror of SP_APPLY_ORDER_EVENTS. The "stream" is emulated as the rows
-- of one batch_id (getvariable('batch')); the logic after that is identical.
CREATE OR REPLACE TEMP TABLE tmp_new_events AS
SELECT
    payload ->> '$.event_id' AS event_id,
    payload ->> '$.event_type' AS event_type,
    CAST(payload ->> '$.event_ts' AS TIMESTAMP) AS event_ts,
    CAST(payload ->> '$.schema_version' AS INTEGER) AS schema_version,
    payload ->> '$.order.order_id' AS order_id,
    payload ->> '$.order.customer.id' AS customer_id,
    payload ->> '$.order.customer.tier' AS customer_tier,
    payload ->> '$.order.customer.address.city' AS city,
    payload ->> '$.order.customer.address.country' AS country,
    payload ->> '$.order.channel' AS channel,
    COALESCE(
        payload ->> '$.order.payment.wallet', payload ->> '$.order.payment.method'
    ) AS payment_method,
    payload ->> '$.order.payment.status' AS payment_status,
    payload -> '$.order.items' AS items,
    loaded_at
FROM raw_order_events
WHERE batch_id = GETVARIABLE('batch')
QUALIFY ROW_NUMBER() OVER (PARTITION BY payload ->> '$.event_id' ORDER BY loaded_at) = 1;

INSERT INTO order_status_history
SELECT
    s.event_id,
    s.order_id,
    s.event_type,
    s.event_ts
FROM tmp_new_events AS s
WHERE s.event_id NOT IN (SELECT h.event_id FROM order_status_history AS h);

CREATE OR REPLACE TEMP TABLE tmp_latest AS
SELECT *
FROM tmp_new_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC, event_id DESC) = 1;

MERGE INTO orders AS t
USING tmp_latest AS s
    ON t.order_id = s.order_id
WHEN MATCHED AND s.event_ts > t.last_event_ts THEN
    UPDATE SET
        customer_tier = s.customer_tier,
        channel = COALESCE(s.channel, t.channel),
        payment_method = s.payment_method,
        payment_status = s.payment_status,
        order_status = REPLACE(s.event_type, 'order_', ''),
        last_event_ts = s.event_ts,
        last_event_id = s.event_id,
        schema_version = s.schema_version
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.customer_id, s.customer_tier, s.city, s.country, s.channel,
    s.payment_method, s.payment_status, REPLACE(s.event_type, 'order_', ''),
    s.event_ts, s.event_ts, s.event_id, s.schema_version
);

-- LATERAL FLATTEN equivalent: UNNEST items[], then promos[] (LEFT JOIN = OUTER => TRUE)
CREATE OR REPLACE TEMP TABLE tmp_items AS
WITH items AS (
    SELECT
        e.order_id,
        e.event_ts,
        UNNEST(CAST(e.items AS JSON[])) AS item
    FROM tmp_latest AS e
),

promos AS (
    SELECT
        i.order_id,
        CAST(i.item ->> '$.line' AS INTEGER) AS line_no,
        UNNEST(CAST(i.item -> '$.promos' AS JSON[])) AS promo
    FROM items AS i
)

SELECT
    i.order_id,
    i.event_ts,
    CAST(i.item ->> '$.line' AS INTEGER) AS line_no,
    i.item ->> '$.sku' AS sku,
    CAST(i.item ->> '$.qty' AS INTEGER) AS qty,
    CAST(i.item ->> '$.price' AS DECIMAL(12, 2)) AS price,
    COALESCE(SUM(CAST(p.promo ->> '$.pct' AS DECIMAL(5, 2))), 0) AS promo_pct
FROM items AS i
LEFT JOIN promos AS p
    ON i.order_id = p.order_id AND CAST(i.item ->> '$.line' AS INTEGER) = p.line_no
GROUP BY ALL;

MERGE INTO order_items AS t
USING tmp_items AS s
    ON t.order_id = s.order_id AND t.line_no = s.line_no
WHEN MATCHED AND s.event_ts > t.last_event_ts THEN
    UPDATE SET
        sku = s.sku,
        qty = s.qty,
        price = s.price,
        promo_pct = s.promo_pct,
        net_amount = ROUND(s.qty * s.price * (100 - s.promo_pct) * 0.01, 2),
        last_event_ts = s.event_ts
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.line_no, s.sku, s.qty, s.price, s.promo_pct,
    ROUND(s.qty * s.price * (100 - s.promo_pct) * 0.01, 2), s.event_ts
);
