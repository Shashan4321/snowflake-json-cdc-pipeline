-- =====================================================================
-- 06_monitoring.sql  |  Is the pipeline healthy, and what does it cost?
-- =====================================================================

-- Task runs in the last 24 h (failures first)
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(ORDERS_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())))
ORDER BY STATE <> 'FAILED', SCHEDULED_TIME DESC;

-- Is the stream going stale? (must be consumed within the retention period)
SHOW STREAMS IN SCHEMA ORDERS_DB.CORE;

-- Credits used by the warehouse (ACCOUNT_USAGE has up to ~3 h latency)
SELECT TO_DATE(START_TIME) AS DAY, SUM(CREDITS_USED) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'CDC_WH'
GROUP BY 1 ORDER BY 1 DESC;

-- Before/after tuning: capture these for the same query to compare
SELECT QUERY_ID, TOTAL_ELAPSED_TIME, BYTES_SCANNED, PARTITIONS_SCANNED, PARTITIONS_TOTAL,
       COMPILATION_TIME, EXECUTION_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(WAREHOUSE_NAME => 'CDC_WH'))
WHERE QUERY_TEXT ILIKE '%DAILY_SALES%'
ORDER BY START_TIME DESC
LIMIT 10;
