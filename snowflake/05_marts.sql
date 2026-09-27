-- =====================================================================
-- 05_marts.sql  |  Business-facing views for Power BI / Tableau.
-- =====================================================================
USE SCHEMA ORDERS_DB.MARTS;

CREATE OR REPLACE VIEW DAILY_SALES AS
SELECT
    TO_DATE(o.FIRST_EVENT_TS)             AS ORDER_DATE,
    o.COUNTRY,
    COALESCE(o.CHANNEL, 'unknown (v1)')   AS CHANNEL,
    COUNT(DISTINCT o.ORDER_ID)            AS ORDERS,
    SUM(i.NET_AMOUNT)                     AS NET_SALES
FROM ORDERS_DB.CORE.ORDERS o
JOIN ORDERS_DB.CORE.ORDER_ITEMS i ON i.ORDER_ID = o.ORDER_ID
WHERE o.ORDER_STATUS <> 'cancelled'
GROUP BY ALL;

CREATE OR REPLACE VIEW CANCELLATION_RATE AS
SELECT
    COUNTRY,
    COUNT_IF(ORDER_STATUS = 'cancelled') / COUNT(*) AS CANCEL_RATE
FROM ORDERS_DB.CORE.ORDERS
GROUP BY COUNTRY;
