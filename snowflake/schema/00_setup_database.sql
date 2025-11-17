-- =====================================================
-- Veteran Evaluation Services - Database Setup
-- =====================================================
-- Purpose: Create database and schemas for veteran evaluation dimensional model
-- Target: Snowflake Data Warehouse
-- Standards: VES Snowflake Naming Conventions v1.0

-- Create database
CREATE DATABASE IF NOT EXISTS VETERAN_EVALUATION_DW
    COMMENT = 'Data warehouse for veteran evaluation services and reporting';

-- Use the database
USE DATABASE VETERAN_EVALUATION_DW;

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

CREATE OR REPLACE FILE FORMAT VETERAN_EVALUATION_DW.UTIL.csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = AUTO
    COMMENT = 'Standard CSV file format for data ingestion';

CREATE OR REPLACE FILE FORMAT VETERAN_EVALUATION_DW.UTIL.json_format
    TYPE = 'JSON'
    COMPRESSION = AUTO
    COMMENT = 'Standard JSON file format for data ingestion';

CREATE OR REPLACE FILE FORMAT VETERAN_EVALUATION_DW.UTIL.parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = AUTO
    COMMENT = 'Standard Parquet file format for data ingestion';

-- =====================================================
-- Grant permissions on schemas
-- =====================================================

GRANT USAGE ON DATABASE VETERAN_EVALUATION_DW TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.REFERENCE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.STAGING TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.MARTS_CLINICAL TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.MARTS_OPERATIONS TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.MARTS_FINANCE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.MARTS_QUALITY TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.MARTS_PROVIDER TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.MARTS_EXECUTIVE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA VETERAN_EVALUATION_DW.UTIL TO ROLE SYSADMIN;
