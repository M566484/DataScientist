-- =====================================================
-- Veteran Evaluation Services - Database Setup
-- =====================================================
-- Purpose: Create database and schemas for veteran evaluation dimensional model
-- Target: Snowflake Data Warehouse
-- Standards: VES Snowflake Naming Conventions v1.0

-- =====================================================
-- STEP 0: Create Environment Configuration
-- =====================================================
-- This configuration table must exist BEFORE creating the main data warehouse
-- It stores environment-specific configuration values

-- Create a temporary schema for configuration (if using shared database)
-- OR create in your organization's configuration database
-- Example: Use PLAYGROUND.CHAPPEM or your org's config schema

CREATE TABLE IF NOT EXISTS PLAYGROUND.CHAPPEM.environment_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(500) NOT NULL,
    config_description VARCHAR(1000),
    environment VARCHAR(50) DEFAULT 'PRODUCTION',
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Environment-specific configuration values for VES Data Warehouse';

-- Populate configuration values (CUSTOMIZE FOR YOUR ENVIRONMENT)
MERGE INTO PLAYGROUND.CHAPPEM.environment_config AS target
USING (
    SELECT 'DW_DATABASE' AS config_key,
           'VESDW_PRD' AS config_value,
           'Data warehouse database name' AS config_description,
           'PRODUCTION' AS environment
    UNION ALL
    SELECT 'ODS_DATABASE' AS config_key,
           'VESODS_PRDDATA_PRD' AS config_value,
           'Operational data store database name' AS config_description,
           'PRODUCTION' AS environment
    UNION ALL
    SELECT 'ETL_WAREHOUSE' AS config_key,
           'ETL_WH' AS config_value,
           'Warehouse for ETL processing' AS config_description,
           'PRODUCTION' AS environment
    UNION ALL
    SELECT 'ANALYTICS_WAREHOUSE' AS config_key,
           'ANALYTICS_WH' AS config_value,
           'Warehouse for analytics queries' AS config_description,
           'PRODUCTION' AS environment
) AS source
ON target.config_key = source.config_key
WHEN MATCHED THEN
    UPDATE SET
        config_value = source.config_value,
        config_description = source.config_description,
        updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, config_description, environment)
    VALUES (source.config_key, source.config_value, source.config_description, source.environment);

-- =====================================================
-- STEP 1: Create Utility Functions
-- =====================================================

-- Function: get_dw_database()
-- Purpose: Returns the data warehouse database name from configuration
CREATE OR REPLACE FUNCTION PLAYGROUND.CHAPPEM.get_dw_database()
RETURNS VARCHAR
LANGUAGE SQL
AS '
    SELECT config_value
    FROM PLAYGROUND.CHAPPEM.environment_config
    WHERE config_key = ''DW_DATABASE''
';

-- Function: get_ods_database()
-- Purpose: Returns the ODS database name from configuration
CREATE OR REPLACE FUNCTION PLAYGROUND.CHAPPEM.get_ods_database()
RETURNS VARCHAR
LANGUAGE SQL
AS '
    SELECT config_value
    FROM PLAYGROUND.CHAPPEM.environment_config
    WHERE config_key = ''ODS_DATABASE''
';

-- Function: get_etl_warehouse()
-- Purpose: Returns the ETL warehouse name from configuration
CREATE OR REPLACE FUNCTION PLAYGROUND.CHAPPEM.get_etl_warehouse()
RETURNS VARCHAR
LANGUAGE SQL
AS '
    SELECT config_value
    FROM PLAYGROUND.CHAPPEM.environment_config
    WHERE config_key = ''ETL_WAREHOUSE''
';

-- Function: get_analytics_warehouse()
-- Purpose: Returns the analytics warehouse name from configuration
CREATE OR REPLACE FUNCTION PLAYGROUND.CHAPPEM.get_analytics_warehouse()
RETURNS VARCHAR
LANGUAGE SQL
AS '
    SELECT config_value
    FROM PLAYGROUND.CHAPPEM.environment_config
    WHERE config_key = ''ANALYTICS_WAREHOUSE''
';

-- =====================================================
-- STEP 2: Create Data Warehouse Database
-- =====================================================

-- Create database using dynamic database name
SET dw_database = (SELECT PLAYGROUND.CHAPPEM.get_dw_database());

CREATE DATABASE IF NOT EXISTS IDENTIFIER($dw_database)
    COMMENT = 'Data warehouse for veteran evaluation services and reporting';

-- Use the database
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================
-- Create schemas following VES naming conventions
-- =====================================================

-- REFERENCE schema: EDW metadata, manually maintained reference data
CREATE SCHEMA IF NOT EXISTS REFERENCE
    COMMENT = 'Reference data and metadata tables for the enterprise data warehouse';

-- STAGING schema: 1:1 views with source systems, light transformations
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging layer with views to source systems (VEMS, OMS) - materialized as views only';

-- WAREHOUSE schema: Star schema (dimensions + facts together)
CREATE SCHEMA IF NOT EXISTS WAREHOUSE
    COMMENT = 'Core star schema warehouse layer containing dimension and fact tables';

-- MARTS schemas: Department-specific data marts
CREATE SCHEMA IF NOT EXISTS MARTS_CLINICAL
    COMMENT = 'Clinical operations data mart - exam quality, medical outcomes, evaluator performance';

CREATE SCHEMA IF NOT EXISTS MARTS_OPERATIONS
    COMMENT = 'Operational metrics data mart - scheduling, capacity, throughput, wait times';

CREATE SCHEMA IF NOT EXISTS MARTS_FINANCE
    COMMENT = 'Financial analytics data mart - revenue, claims processing, contractor payments';

CREATE SCHEMA IF NOT EXISTS MARTS_QUALITY
    COMMENT = 'Quality metrics data mart - HEDIS measures, compliance, sufficiency rates';

CREATE SCHEMA IF NOT EXISTS MARTS_PROVIDER
    COMMENT = 'Provider analytics data mart - examiner productivity, performance, utilization';

CREATE SCHEMA IF NOT EXISTS MARTS_EXECUTIVE
    COMMENT = 'Executive dashboard data mart - leadership KPIs, strategic metrics';

-- UTIL schema: Utility objects (sequences, procedures, functions)
CREATE SCHEMA IF NOT EXISTS UTIL
    COMMENT = 'Utility objects including file formats, procedures, functions, and sequences';

-- =====================================================
-- Create file formats for data loading
-- =====================================================

CREATE OR REPLACE FILE FORMAT IDENTIFIER($dw_database || '.UTIL.csv_format')
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = AUTO
    COMMENT = 'Standard CSV file format for data ingestion';

CREATE OR REPLACE FILE FORMAT IDENTIFIER($dw_database || '.UTIL.json_format')
    TYPE = 'JSON'
    COMPRESSION = AUTO
    COMMENT = 'Standard JSON file format for data ingestion';

CREATE OR REPLACE FILE FORMAT IDENTIFIER($dw_database || '.UTIL.parquet_format')
    TYPE = 'PARQUET'
    COMPRESSION = AUTO
    COMMENT = 'Standard Parquet file format for data ingestion';

-- =====================================================
-- Grant permissions on schemas
-- =====================================================

GRANT USAGE ON DATABASE IDENTIFIER($dw_database) TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.REFERENCE') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.STAGING') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.WAREHOUSE') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.MARTS_CLINICAL') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.MARTS_OPERATIONS') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.MARTS_FINANCE') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.MARTS_QUALITY') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.MARTS_PROVIDER') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.MARTS_EXECUTIVE') TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA IDENTIFIER($dw_database || '.UTIL') TO ROLE SYSADMIN;
