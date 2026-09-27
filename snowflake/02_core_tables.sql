-- =====================================================================
-- 02_core_tables.sql  |  Typed, flattened target tables.
-- =====================================================================
USE SCHEMA ORDERS_DB.CORE;

-- Current state of each order (SCD type 1: latest event wins)
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID          STRING PRIMARY KEY,
    CUSTOMER_ID       STRING,
    CUSTOMER_TIER     STRING,
    CITY              STRING,
    COUNTRY           STRING,
    CHANNEL           STRING,          -- only present from schema v2
    PAYMENT_METHOD    STRING,          -- v1: payment.method, v2: payment.wallet
    PAYMENT_STATUS    STRING,
    ORDER_STATUS      STRING,          -- created / updated / cancelled
    FIRST_EVENT_TS    TIMESTAMP_NTZ,
    LAST_EVENT_TS     TIMESTAMP_NTZ,
    LAST_EVENT_ID     STRING,
    SCHEMA_VERSION    NUMBER
) CLUSTER BY (TO_DATE(LAST_EVENT_TS));

-- One row per order line, current state
CREATE OR REPLACE TABLE ORDER_ITEMS (
    ORDER_ID       STRING,
    LINE_NO        NUMBER,
    SKU            STRING,
    QTY            NUMBER,
    PRICE          NUMBER(12, 2),
    PROMO_PCT      NUMBER(5, 2),       -- sum of promo percentages on the line
    NET_AMOUNT     NUMBER(14, 2),
    LAST_EVENT_TS  TIMESTAMP_NTZ,
    PRIMARY KEY (ORDER_ID, LINE_NO)
);

-- Full, append-only history of status changes (audit trail)
CREATE OR REPLACE TABLE ORDER_STATUS_HISTORY (
    EVENT_ID     STRING PRIMARY KEY,
    ORDER_ID     STRING,
    EVENT_TYPE   STRING,
    EVENT_TS     TIMESTAMP_NTZ,
    LOADED_AT    TIMESTAMP_LTZ
);

-- Work table the task fills from the stream. It is cleared only after all MERGEs
-- succeed, so a failed run keeps its events and the next run retries them
-- (all MERGEs are idempotent).
CREATE OR REPLACE TABLE STG_NEW_EVENTS (
    EVENT_ID        STRING,
    EVENT_TYPE      STRING,
    EVENT_TS        TIMESTAMP_NTZ,
    SCHEMA_VERSION  NUMBER,
    ORDER_ID        STRING,
    CUSTOMER_ID     STRING,
    CUSTOMER_TIER   STRING,
    CITY            STRING,
    COUNTRY         STRING,
    CHANNEL         STRING,
    PAYMENT_METHOD  STRING,
    PAYMENT_STATUS  STRING,
    ITEMS           VARIANT,
    LOADED_AT       TIMESTAMP_LTZ
);
