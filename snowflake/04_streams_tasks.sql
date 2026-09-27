-- =====================================================================
-- 04_streams_tasks.sql  |  Change data capture with a STREAM and a TASK.
-- The stream records only rows added to RAW since the last consumption, so
-- each task run processes new events only (incremental, not full reload).
-- =====================================================================
USE SCHEMA ORDERS_DB.CORE;

CREATE OR REPLACE STREAM STRM_RAW_ORDER_EVENTS
    ON TABLE ORDERS_DB.RAW.RAW_ORDER_EVENTS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE SP_APPLY_ORDER_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    n INTEGER DEFAULT 0;
BEGIN
    -- Consuming the stream in a DML statement (INSERT) is what advances its offset.
    INSERT INTO STG_NEW_EVENTS
    SELECT
        PAYLOAD:event_id::STRING,
        PAYLOAD:event_type::STRING,
        PAYLOAD:event_ts::TIMESTAMP_NTZ,
        PAYLOAD:schema_version::NUMBER,
        PAYLOAD:"order":order_id::STRING,
        PAYLOAD:"order":customer.id::STRING,
        PAYLOAD:"order":customer.tier::STRING,
        PAYLOAD:"order":customer.address.city::STRING,
        PAYLOAD:"order":customer.address.country::STRING,
        PAYLOAD:"order":channel::STRING,
        COALESCE(PAYLOAD:"order":payment.wallet::STRING,
                 PAYLOAD:"order":payment.method::STRING),        -- schema drift v1 -> v2
        PAYLOAD:"order":payment.status::STRING,
        PAYLOAD:"order":items,
        LOADED_AT
    FROM STRM_RAW_ORDER_EVENTS;

    -- De-duplicate at-least-once deliveries
    CREATE OR REPLACE TEMPORARY TABLE TMP_NEW_EVENTS AS
    SELECT * FROM STG_NEW_EVENTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY LOADED_AT) = 1;

    -- 1) audit trail: insert events we have not seen (idempotent on EVENT_ID)
    MERGE INTO ORDER_STATUS_HISTORY h
    USING TMP_NEW_EVENTS s ON h.EVENT_ID = s.EVENT_ID
    WHEN NOT MATCHED THEN INSERT (EVENT_ID, ORDER_ID, EVENT_TYPE, EVENT_TS, LOADED_AT)
        VALUES (s.EVENT_ID, s.ORDER_ID, s.EVENT_TYPE, s.EVENT_TS, s.LOADED_AT);

    -- 2) current order state: latest event per order wins; late events are ignored
    MERGE INTO ORDERS t
    USING (
        SELECT * FROM TMP_NEW_EVENTS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY EVENT_TS DESC, EVENT_ID DESC) = 1
    ) s
    ON t.ORDER_ID = s.ORDER_ID
    WHEN MATCHED AND s.EVENT_TS > t.LAST_EVENT_TS THEN UPDATE SET
        CUSTOMER_TIER = s.CUSTOMER_TIER,
        CHANNEL = COALESCE(s.CHANNEL, t.CHANNEL),
        PAYMENT_METHOD = s.PAYMENT_METHOD,
        PAYMENT_STATUS = s.PAYMENT_STATUS,
        ORDER_STATUS = REPLACE(s.EVENT_TYPE, 'order_', ''),
        LAST_EVENT_TS = s.EVENT_TS,
        LAST_EVENT_ID = s.EVENT_ID,
        SCHEMA_VERSION = s.SCHEMA_VERSION
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, CUSTOMER_ID, CUSTOMER_TIER, CITY, COUNTRY, CHANNEL, PAYMENT_METHOD,
        PAYMENT_STATUS, ORDER_STATUS, FIRST_EVENT_TS, LAST_EVENT_TS, LAST_EVENT_ID, SCHEMA_VERSION)
    VALUES (
        s.ORDER_ID, s.CUSTOMER_ID, s.CUSTOMER_TIER, s.CITY, s.COUNTRY, s.CHANNEL, s.PAYMENT_METHOD,
        s.PAYMENT_STATUS, REPLACE(s.EVENT_TYPE, 'order_', ''), s.EVENT_TS, s.EVENT_TS,
        s.EVENT_ID, s.SCHEMA_VERSION);

    -- 3) order lines from the latest event of each order (FLATTEN items + promos)
    MERGE INTO ORDER_ITEMS t
    USING (
        SELECT
            e.ORDER_ID,
            i.value:line::NUMBER AS LINE_NO,
            i.value:sku::STRING AS SKU,
            i.value:qty::NUMBER AS QTY,
            i.value:price::NUMBER(12, 2) AS PRICE,
            COALESCE(SUM(p.value:pct::NUMBER(5, 2)), 0) AS PROMO_PCT,
            e.EVENT_TS
        FROM (
            SELECT * FROM TMP_NEW_EVENTS
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY EVENT_TS DESC, EVENT_ID DESC) = 1
        ) e,
            LATERAL FLATTEN(INPUT => e.ITEMS) i,
            LATERAL FLATTEN(INPUT => i.value:promos, OUTER => TRUE) p
        GROUP BY ALL
    ) s
    ON t.ORDER_ID = s.ORDER_ID AND t.LINE_NO = s.LINE_NO
    WHEN MATCHED AND s.EVENT_TS > t.LAST_EVENT_TS THEN UPDATE SET
        SKU = s.SKU, QTY = s.QTY, PRICE = s.PRICE, PROMO_PCT = s.PROMO_PCT,
        NET_AMOUNT = ROUND(s.QTY * s.PRICE * (100 - s.PROMO_PCT) * 0.01, 2),
        LAST_EVENT_TS = s.EVENT_TS
    WHEN NOT MATCHED THEN INSERT (ORDER_ID, LINE_NO, SKU, QTY, PRICE, PROMO_PCT, NET_AMOUNT, LAST_EVENT_TS)
    VALUES (s.ORDER_ID, s.LINE_NO, s.SKU, s.QTY, s.PRICE, s.PROMO_PCT,
            ROUND(s.QTY * s.PRICE * (100 - s.PROMO_PCT) * 0.01, 2), s.EVENT_TS);

    SELECT COUNT(*) INTO :n FROM TMP_NEW_EVENTS;
    TRUNCATE TABLE STG_NEW_EVENTS;     -- only after every MERGE succeeded
    RETURN 'applied ' || n || ' events';
END;
$$;

-- Serverless-free option: run on CDC_WH every 5 minutes, but ONLY if the stream
-- has data, so an idle pipeline costs zero credits.
CREATE OR REPLACE TASK TSK_APPLY_ORDER_EVENTS
    WAREHOUSE = CDC_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ORDERS_DB.CORE.STRM_RAW_ORDER_EVENTS')
AS
    CALL SP_APPLY_ORDER_EVENTS();

ALTER TASK TSK_APPLY_ORDER_EVENTS RESUME;

-- Manual run while testing:  EXECUTE TASK TSK_APPLY_ORDER_EVENTS;
