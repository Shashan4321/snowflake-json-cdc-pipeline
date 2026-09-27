-- =====================================================================
-- 00_setup.sql  |  Run once as SYSADMIN on a Snowflake trial account.
-- Creates an X-Small warehouse with aggressive auto-suspend (cost control),
-- a database with RAW / CORE / MARTS schemas, a JSON file format and a stage.
-- =====================================================================
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS CDC_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60            -- seconds; trial credits are precious
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Demo: JSON CDC pipeline';

CREATE DATABASE IF NOT EXISTS ORDERS_DB;
CREATE SCHEMA IF NOT EXISTS ORDERS_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ORDERS_DB.CORE;
CREATE SCHEMA IF NOT EXISTS ORDERS_DB.MARTS;

USE WAREHOUSE CDC_WH;
USE SCHEMA ORDERS_DB.RAW;

CREATE OR REPLACE FILE FORMAT FF_NDJSON
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    COMPRESSION = 'AUTO';

-- Internal stage; upload with SnowSQL / Snowsight:
--   PUT file://sample_data/orders_*.json @ORDERS_DB.RAW.STG_ORDERS AUTO_COMPRESS=TRUE;
CREATE OR REPLACE STAGE STG_ORDERS
    FILE_FORMAT = FF_NDJSON
    COMMENT = 'Landing zone for daily order event files';

-- Resource monitor: hard stop before the trial budget is burned (needs ACCOUNTADMIN)
-- USE ROLE ACCOUNTADMIN;
-- CREATE RESOURCE MONITOR RM_CDC WITH CREDIT_QUOTA = 10
--   TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
-- ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = RM_CDC;
