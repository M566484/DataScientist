-- =====================================================
-- Veteran Evaluation Services - Database Setup
-- =====================================================
-- Purpose: Create database and schemas for veteran evaluation dimensional model
-- Target: Snowflake Data Warehouse

-- Create database
CREATE DATABASE IF NOT EXISTS VETERAN_EVALUATION_DW
    COMMENT = 'Data warehouse for veteran evaluation services and reporting';

-- Use the database
USE DATABASE VETERAN_EVALUATION_DW;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS DIM
    COMMENT = 'Dimension tables for veteran evaluation data';

CREATE SCHEMA IF NOT EXISTS FACT
    COMMENT = 'Fact tables for veteran evaluation metrics and events';

CREATE SCHEMA IF NOT EXISTS STG
    COMMENT = 'Staging area for data loads';

CREATE SCHEMA IF NOT EXISTS UTIL
    COMMENT = 'Utility objects (sequences, procedures, functions)';

-- Create file formats for data loading
CREATE OR REPLACE FILE FORMAT VETERAN_EVALUATION_DW.UTIL.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = AUTO;

CREATE OR REPLACE FILE FORMAT VETERAN_EVALUATION_DW.UTIL.JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = AUTO;

-- Grant usage on schemas
GRANT USAGE ON DATABASE VETERAN_EVALUATION_DW TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.DIM TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.FACT TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.STG TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.UTIL TO ROLE SYSADMIN;
