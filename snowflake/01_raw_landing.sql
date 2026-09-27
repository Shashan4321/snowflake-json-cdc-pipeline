-- =====================================================================
-- 01_raw_landing.sql  |  Land every JSON event as a VARIANT, untouched.
-- Keep the raw payload forever: it is the replayable source of truth when
-- downstream logic changes (e.g. schema drift in v2 events).
-- =====================================================================
USE SCHEMA ORDERS_DB.RAW;

CREATE OR REPLACE TABLE RAW_ORDER_EVENTS (
    PAYLOAD      VARIANT,
    SRC_FILE     STRING,
    SRC_ROW      NUMBER,
    LOADED_AT    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Append-only landing table for order events (NDJSON)';

-- Idempotent load: Snowflake remembers loaded files for 64 days, so re-running
-- COPY does not duplicate data. METADATA$ columns give lineage per row.
COPY INTO RAW_ORDER_EVENTS (PAYLOAD, SRC_FILE, SRC_ROW)
FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @STG_ORDERS
)
PATTERN = '.*orders_.*[.]json.*'
ON_ERROR = 'CONTINUE';

-- What did the last COPY do?
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_ORDER_EVENTS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())));
