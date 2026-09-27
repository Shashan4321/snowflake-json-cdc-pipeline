-- =====================================================================
-- 03_flatten_views.sql  |  Semi-structured -> relational with FLATTEN.
-- Handles schema drift with COALESCE over old/new paths and removes
-- duplicate deliveries with QUALIFY ROW_NUMBER().
-- =====================================================================
USE SCHEMA ORDERS_DB.CORE;

CREATE OR REPLACE VIEW V_EVENTS AS
SELECT
    PAYLOAD:event_id::STRING                        AS EVENT_ID,
    PAYLOAD:event_type::STRING                      AS EVENT_TYPE,
    PAYLOAD:event_ts::TIMESTAMP_NTZ                 AS EVENT_TS,
    PAYLOAD:schema_version::NUMBER                  AS SCHEMA_VERSION,
    PAYLOAD:"order":order_id::STRING                AS ORDER_ID,
    PAYLOAD:"order":customer.id::STRING             AS CUSTOMER_ID,
    PAYLOAD:"order":customer.tier::STRING           AS CUSTOMER_TIER,
    PAYLOAD:"order":customer.address.city::STRING   AS CITY,
    PAYLOAD:"order":customer.address.country::STRING AS COUNTRY,
    PAYLOAD:"order":channel::STRING                 AS CHANNEL,
    COALESCE(PAYLOAD:"order":payment.wallet::STRING,
             PAYLOAD:"order":payment.method::STRING) AS PAYMENT_METHOD,   -- schema drift
    PAYLOAD:"order":payment.status::STRING          AS PAYMENT_STATUS,
    PAYLOAD:"order":items                           AS ITEMS,
    LOADED_AT
FROM ORDERS_DB.RAW.RAW_ORDER_EVENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY PAYLOAD:event_id ORDER BY LOADED_AT) = 1;  -- dedupe

-- Order lines: LATERAL FLATTEN over items[], then an OUTER flatten over promos[]
CREATE OR REPLACE VIEW V_EVENT_ITEMS AS
SELECT
    e.EVENT_ID,
    e.ORDER_ID,
    e.EVENT_TS,
    i.value:line::NUMBER              AS LINE_NO,
    i.value:sku::STRING               AS SKU,
    i.value:qty::NUMBER               AS QTY,
    i.value:price::NUMBER(12, 2)      AS PRICE,
    COALESCE(SUM(p.value:pct::NUMBER(5, 2)), 0) AS PROMO_PCT
FROM V_EVENTS e,
    LATERAL FLATTEN(INPUT => e.ITEMS) i,
    LATERAL FLATTEN(INPUT => i.value:promos, OUTER => TRUE) p
GROUP BY ALL;
