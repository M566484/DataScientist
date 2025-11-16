-- =====================================================
-- Master Deployment Script
-- =====================================================
-- Purpose: Execute all DDL scripts in correct order
-- Usage: Run this script to deploy the entire dimensional model
-- Standards: VES Snowflake Naming Conventions v1.0

-- Step 1: Setup Database and Schemas
!source 00_setup_database.sql

-- Step 2: Create Dimension Tables
!source ../dimensions/01_dim_date.sql
!source ../dimensions/02_dim_veteran.sql
!source ../dimensions/03_dim_evaluator.sql
!source ../dimensions/04_dim_facility.sql
!source ../dimensions/05_dim_evaluation_type.sql
!source ../dimensions/06_dim_medical_condition.sql
!source ../dimensions/07_dim_claim.sql
!source ../dimensions/08_dim_appointment.sql

-- Step 3: Create Fact Tables
!source ../facts/01_fact_evaluation.sql
!source ../facts/02_fact_claim_status.sql
!source ../facts/03_fact_appointment.sql
!source ../facts/04_fact_daily_snapshot.sql
!source ../facts/05_fact_appointment_events.sql

-- Step 4: Populate Date Dimension
!source 01_populate_date_dimension.sql

-- Verify deployment
USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

SELECT 'Deployment completed successfully' AS status;

-- Show all tables created
SELECT
    table_schema,
    table_name,
    table_type,
    row_count,
    bytes,
    created,
    comment
FROM VETERAN_EVALUATION_DW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
ORDER BY table_name;

-- Show dimension tables
SELECT 'Dimension Tables:' AS info;
SELECT table_name
FROM VETERAN_EVALUATION_DW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'dim_%'
ORDER BY table_name;

-- Show fact tables
SELECT 'Fact Tables:' AS info;
SELECT table_name
FROM VETERAN_EVALUATION_DW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'fct_%'
ORDER BY table_name;
