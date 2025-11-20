-- =====================================================
-- Master Deployment Script
-- =====================================================
-- Purpose: Execute all DDL scripts in correct order
-- Usage: Run this script to deploy the entire dimensional model
-- Standards: VES Snowflake Naming Conventions v1.0

-- Step 1: Setup Database and Schemas
!source 00_setup_database.sql

-- Step 2: Create ODS Layer Tables (Raw Data from OMS and VEMS)
!source ../ods/01_create_ods_tables.sql

-- Step 3: Create Reference Tables (Multi-Source Mappings)
!source ../reference/01_create_reference_tables.sql

-- Step 4: Create Staging Layer Tables
!source ../staging/01_create_staging_tables.sql

-- Step 5: Create Dimension Tables
!source ../dimensions/01_dim_date.sql
!source ../dimensions/02_dim_veteran.sql
!source ../dimensions/03_dim_evaluator.sql
!source ../dimensions/04_dim_facility.sql
!source ../dimensions/05_dim_evaluation_type.sql
!source ../dimensions/06_dim_medical_condition.sql
!source ../dimensions/07_dim_claim.sql
!source ../dimensions/08_dim_appointment.sql
!source ../dimensions/09_dim_exam_request_types.sql

-- Step 6: Create Fact Tables
!source ../facts/01_fact_evaluation.sql
!source ../facts/02_fact_claim_status.sql
!source ../facts/03_fact_appointment.sql
!source ../facts/04_fact_daily_snapshot.sql
!source ../facts/05_fact_appointment_events.sql
!source ../facts/06_fact_evaluation_qa_events.sql
!source ../facts/07_fact_exam_requests.sql
!source ../facts/08_fact_examiner_assignments.sql

-- Step 7: Populate Date Dimension
!source 01_populate_date_dimension.sql

-- Step 8: Create ETL Procedures
!source ../etl/01_etl_procedures_dimensions.sql
!source ../etl/02_etl_procedures_facts.sql
!source ../etl/03_etl_procedures_multi_source.sql

-- Step 9: Create Marts Layer Views
!source ../marts/01_create_marts_clinical.sql

-- =====================================================
-- Verify Deployment
-- =====================================================

SELECT 'Deployment completed successfully' AS status;

-- Show ODS tables
SELECT 'ODS Tables (Raw Data):' AS info;
SELECT table_name, row_count, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'ODS_RAW'
  AND table_name LIKE 'ods_%'
ORDER BY table_name;

-- Show reference tables
SELECT 'Reference Tables (Multi-Source Mappings):' AS info;
SELECT table_name, row_count, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'REFERENCE'
  AND table_name LIKE 'ref_%'
ORDER BY table_name;

-- Show staging tables
SELECT 'Staging Tables:' AS info;
SELECT table_name, row_count, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'STAGING'
  AND table_name LIKE 'stg_%'
ORDER BY table_name;

-- Show dimension tables
SELECT 'Dimension Tables:' AS info;
SELECT table_name, row_count, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'dim_%'
ORDER BY table_name;

-- Show fact tables
SELECT 'Fact Tables:' AS info;
SELECT table_name, row_count, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'fact_%'
ORDER BY table_name;

-- Show marts views
SELECT 'Marts Views:' AS info;
SELECT table_name, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.VIEWS')
WHERE table_schema = 'MARTS_CLINICAL'
  AND table_name LIKE 'vw_%'
ORDER BY table_name;

-- Show stored procedures
SELECT 'ETL Stored Procedures:' AS info;
SELECT procedure_name, created
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.PROCEDURES')
WHERE procedure_schema = 'WAREHOUSE'
ORDER BY procedure_name;
