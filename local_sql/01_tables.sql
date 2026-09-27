-- DuckDB mirror of snowflake/01 + 02: same tables, same columns.
CREATE TABLE IF NOT EXISTS raw_order_events (
    payload JSON,
    src_file VARCHAR,
    batch_id INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    customer_tier VARCHAR,
    city VARCHAR,
    country VARCHAR,
    channel VARCHAR,
    payment_method VARCHAR,
    payment_status VARCHAR,
    order_status VARCHAR,
    first_event_ts TIMESTAMP,
    last_event_ts TIMESTAMP,
    last_event_id VARCHAR,
    schema_version INTEGER
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR,
    line_no INTEGER,
    sku VARCHAR,
    qty INTEGER,
    price DECIMAL(12, 2),
    promo_pct DECIMAL(5, 2),
    net_amount DECIMAL(14, 2),
    last_event_ts TIMESTAMP,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS order_status_history (
    event_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    event_type VARCHAR,
    event_ts TIMESTAMP
);
