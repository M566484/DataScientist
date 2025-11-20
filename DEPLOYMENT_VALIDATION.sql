-- =============================================================================
-- VES Data Warehouse - Deployment Validation & Health Check Suite
-- =============================================================================
-- Author: Mark Chappell
-- Purpose: Comprehensive validation of VES data warehouse deployment
-- Run this script after deploying the solution to verify all components
-- Expected runtime: 2-3 minutes
-- =============================================================================

-- Usage:
--   snowsql -a <account> -u <username> -f DEPLOYMENT_VALIDATION.sql

-- Expected Output:
--   All tests should show 'PASS' status
--   Any 'FAIL' status indicates deployment issues that need investigation

-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Create validation results table to store test outcomes
CREATE OR REPLACE TABLE IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id INT,
    test_category VARCHAR(50),
    test_name VARCHAR(200),
    expected_value VARCHAR(100),
    actual_value VARCHAR(100),
    status VARCHAR(10),
    error_message VARCHAR(500),
    test_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- TEST CATEGORY 1: DATABASE & SCHEMA STRUCTURE
-- =============================================================================

-- Test 1.1: Verify VESDW_PRD database exists
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    101 AS test_id,
    'Database Structure' AS test_category,
    CONCAT(get_dw_database(), ' database exists') AS test_name,
    'EXISTS' AS expected_value,
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END AS actual_value,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = get_dw_database();

-- Test 1.2: Verify VESODS_PRDDATA_PRD database exists
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    102 AS test_id,
    'Database Structure' AS test_category,
    CONCAT(get_ods_database(), ' database exists') AS test_name,
    'EXISTS' AS expected_value,
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END AS actual_value,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = get_ods_database();

-- Test 1.3: Verify all required schemas exist in VESDW_PRD
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    103 AS test_id,
    'Database Structure' AS test_category,
    'All required schemas exist (staging, warehouse, marts, metadata)' AS test_name,
    '4' AS expected_value,
    COUNT(*)::VARCHAR AS actual_value,
    CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME IN ('staging', 'warehouse', 'marts', 'metadata')
  AND CATALOG_NAME = get_dw_database();

-- =============================================================================
-- TEST CATEGORY 2: DIMENSION TABLES
-- =============================================================================

-- Test 2.1: Verify all 9 dimension tables exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    201 AS test_id,
    'Dimension Tables' AS test_category,
    'All 9 dimension tables exist in warehouse schema' AS test_name,
    '9' AS expected_value,
    COUNT(*)::VARCHAR AS actual_value,
    CASE WHEN COUNT(*) = 9 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'warehouse'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'dim_%';

-- Test 2.2: Verify dim_date is populated (should have ~3,650 rows for 10 years)
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    202 AS test_id,
    'Dimension Tables' AS test_category,
    'dim_date table is populated (>3000 rows)' AS test_name,
    '>3000' AS expected_value,
    COUNT(*)::VARCHAR AS actual_value,
    CASE WHEN COUNT(*) > 3000 THEN 'PASS' ELSE 'FAIL' END AS status
FROM IDENTIFIER(get_dw_database() || '.warehouse.dim_date');

-- Test 2.3: Check dim_date date range (should span at least 5 years)
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    203 AS test_id,
    'Dimension Tables' AS test_category,
    'dim_date spans at least 5 years' AS test_name,
    '>= 5 years' AS expected_value,
    DATEDIFF(year, MIN(full_date), MAX(full_date))::VARCHAR || ' years' AS actual_value,
    CASE WHEN DATEDIFF(year, MIN(full_date), MAX(full_date)) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM IDENTIFIER(get_dw_database() || '.warehouse.dim_date');

-- Test 2.4: Verify SCD Type 2 columns exist in applicable dimensions
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    204 AS test_id,
    'Dimension Tables' AS test_category,
    'SCD Type 2 dimensions have required columns (is_current, valid_from, valid_to)' AS test_name,
    '5 tables' AS expected_value,
    COUNT(DISTINCT TABLE_NAME)::VARCHAR || ' tables' AS actual_value,
    CASE WHEN COUNT(DISTINCT TABLE_NAME) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'warehouse'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'dim_%'
  AND COLUMN_NAME IN ('is_current', 'valid_from', 'valid_to')
GROUP BY TABLE_NAME
HAVING COUNT(DISTINCT COLUMN_NAME) = 3;

-- =============================================================================
-- TEST CATEGORY 3: FACT TABLES
-- =============================================================================

-- Test 3.1: Verify all 9 fact tables exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    301 AS test_id,
    'Fact Tables' AS test_category,
    'All 9 fact tables exist in warehouse schema' AS test_name,
    '9' AS expected_value,
    COUNT(*)::VARCHAR AS actual_value,
    CASE WHEN COUNT(*) = 9 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'warehouse'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'fact_%';

-- Test 3.2: Verify fact tables have foreign keys to date dimension
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    302 AS test_id,
    'Fact Tables' AS test_category,
    'Fact tables have date dimension foreign keys (date_sk columns)' AS test_name,
    '>= 5 tables' AS expected_value,
    COUNT(DISTINCT TABLE_NAME)::VARCHAR || ' tables' AS actual_value,
    CASE WHEN COUNT(DISTINCT TABLE_NAME) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'warehouse'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'fact_%'
  AND COLUMN_NAME LIKE '%date_sk';

-- =============================================================================
-- TEST CATEGORY 4: STAGING & ODS LAYERS
-- =============================================================================

-- Test 4.1: Verify staging tables exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    401 AS test_id,
    'Staging Layer' AS test_category,
    'Staging tables exist in staging schema' AS test_name,
    '>= 5 tables' AS expected_value,
    COUNT(*)::VARCHAR || ' tables' AS actual_value,
    CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'staging'
  AND TABLE_CATALOG = get_dw_database();

-- =============================================================================
-- TEST CATEGORY 5: STORED PROCEDURES & FUNCTIONS
-- =============================================================================

-- Test 5.1: Verify ETL stored procedures exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    501 AS test_id,
    'Stored Procedures' AS test_category,
    'ETL stored procedures exist' AS test_name,
    '>= 10 procedures' AS expected_value,
    COUNT(*)::VARCHAR || ' procedures' AS actual_value,
    CASE WHEN COUNT(*) >= 10 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_CATALOG = get_dw_database();

-- Test 5.2: Verify monitoring procedures exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    502 AS test_id,
    'Stored Procedures' AS test_category,
    'Monitoring procedures exist (sp_record_pipeline_health, sp_run_data_quality_checks)' AS test_name,
    '>= 2 procedures' AS expected_value,
    COUNT(*)::VARCHAR || ' procedures' AS actual_value,
    CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_CATALOG = get_dw_database()
  AND PROCEDURE_NAME IN ('sp_record_pipeline_health', 'sp_run_data_quality_checks');

-- =============================================================================
-- TEST CATEGORY 6: MONITORING & QUALITY FRAMEWORK
-- =============================================================================

-- Test 6.1: Verify monitoring dashboard views exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    601 AS test_id,
    'Monitoring Framework' AS test_category,
    'Monitoring dashboard views exist in metadata schema' AS test_name,
    '>= 5 views' AS expected_value,
    COUNT(*)::VARCHAR || ' views' AS actual_value,
    CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'metadata'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'vw_%';

-- Test 6.2: Verify data quality framework tables exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    602 AS test_id,
    'Quality Framework' AS test_category,
    'Data quality tables exist (dq_rule_catalog, dq_rule_execution_history)' AS test_name,
    '>= 2 tables' AS expected_value,
    COUNT(*)::VARCHAR || ' tables' AS actual_value,
    CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'metadata'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'dq_%';

-- Test 6.3: Verify data quality rules are pre-loaded
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    603 AS test_id,
    'Quality Framework' AS test_category,
    'Data quality rules are pre-loaded (>= 40 rules)' AS test_name,
    '>= 40 rules' AS expected_value,
    COUNT(*)::VARCHAR || ' rules' AS actual_value,
    CASE WHEN COUNT(*) >= 40 THEN 'PASS' ELSE 'FAIL' END AS status
FROM IDENTIFIER(get_dw_database() || '.metadata.dq_rule_catalog')
WHERE is_active = TRUE;

-- Test 6.4: Verify pipeline health metrics table exists
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    604 AS test_id,
    'Monitoring Framework' AS test_category,
    'Pipeline health metrics table exists' AS test_name,
    'EXISTS' AS expected_value,
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END AS actual_value,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'pipeline_health_metrics'
  AND TABLE_SCHEMA = 'metadata'
  AND TABLE_CATALOG = get_dw_database();

-- =============================================================================
-- TEST CATEGORY 7: ORCHESTRATION (TASKS & STREAMS)
-- =============================================================================

-- Test 7.1: Verify Snowflake Tasks exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    701 AS test_id,
    'Orchestration' AS test_category,
    'Snowflake Tasks exist (>= 10 tasks)' AS test_name,
    '>= 10 tasks' AS expected_value,
    COUNT(*)::VARCHAR || ' tasks' AS actual_value,
    CASE WHEN COUNT(*) >= 10 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TASKS
WHERE TASK_CATALOG = get_dw_database();

-- Test 7.2: Verify Snowflake Streams exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    702 AS test_id,
    'Orchestration' AS test_category,
    'Snowflake Streams exist for CDC (>= 5 streams)' AS test_name,
    '>= 5 streams' AS expected_value,
    COUNT(*)::VARCHAR || ' streams' AS actual_value,
    CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'STREAM'
  AND TABLE_CATALOG = get_dw_database();

-- Test 7.3: Check if tasks are in correct state (should be suspended initially)
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    703 AS test_id,
    'Orchestration' AS test_category,
    'Tasks are configured (state is started or suspended)' AS test_name,
    'All valid' AS expected_value,
    COUNT(*)::VARCHAR || ' tasks configured' AS actual_value,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TASKS
WHERE TASK_CATALOG = get_dw_database()
  AND STATE IN ('started', 'suspended');

-- =============================================================================
-- TEST CATEGORY 8: MARTS & ANALYTICS
-- =============================================================================

-- Test 8.1: Verify marts schema views exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    801 AS test_id,
    'Analytics Marts' AS test_category,
    'Mart views exist in marts schema' AS test_name,
    '>= 5 views' AS expected_value,
    COUNT(*)::VARCHAR || ' views' AS actual_value,
    CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'marts'
  AND TABLE_CATALOG = get_dw_database();

-- Test 8.2: Verify executive dashboard views exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    802 AS test_id,
    'Analytics Marts' AS test_category,
    'Executive dashboard views exist (vw_exec_*)' AS test_name,
    '>= 5 views' AS expected_value,
    COUNT(*)::VARCHAR || ' views' AS actual_value,
    CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'marts'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'vw_exec_%';

-- Test 8.3: Verify materialized views exist for performance
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    803 AS test_id,
    'Analytics Marts' AS test_category,
    'Materialized views exist for performance (mv_*)' AS test_name,
    '>= 2 views' AS expected_value,
    COUNT(*)::VARCHAR || ' materialized views' AS actual_value,
    CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'marts'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_TYPE = 'MATERIALIZED VIEW';

-- =============================================================================
-- TEST CATEGORY 9: PERFORMANCE OPTIMIZATION
-- =============================================================================

-- Test 9.1: Verify clustering keys are defined on large tables
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    901 AS test_id,
    'Performance' AS test_category,
    'Clustering keys defined on fact tables' AS test_name,
    '>= 3 tables' AS expected_value,
    COUNT(DISTINCT TABLE_NAME)::VARCHAR || ' tables' AS actual_value,
    CASE WHEN COUNT(DISTINCT TABLE_NAME) >= 3 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'warehouse'
  AND TABLE_CATALOG = get_dw_database()
  AND TABLE_NAME LIKE 'fact_%'
  AND CLUSTERING_KEY IS NOT NULL;

-- =============================================================================
-- TEST CATEGORY 10: SECURITY & ACCESS CONTROL
-- =============================================================================

-- Test 10.1: Verify warehouse resource monitors exist
INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    1001 AS test_id,
    'Security & Access' AS test_category,
    'Virtual warehouses exist (ETL_WH, ANALYTICS_WH)' AS test_name,
    '>= 2 warehouses' AS expected_value,
    COUNT(*)::VARCHAR || ' warehouses' AS actual_value,
    CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.WAREHOUSES
WHERE WAREHOUSE_NAME IN ('ETL_WH', 'ANALYTICS_WH');

-- =============================================================================
-- VALIDATION SUMMARY REPORT
-- =============================================================================

-- Display validation summary
SELECT
    '=' || REPEAT('=', 80) || '=' AS separator
UNION ALL
SELECT '  VES DATA WAREHOUSE - DEPLOYMENT VALIDATION SUMMARY'
UNION ALL
SELECT '  Run Date: ' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')
UNION ALL
SELECT '=' || REPEAT('=', 80) || '='
UNION ALL
SELECT ''
UNION ALL
SELECT '  Total Tests: ' || COUNT(*)::VARCHAR
FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results')
WHERE test_timestamp = (SELECT MAX(test_timestamp) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results'))
UNION ALL
SELECT '  Tests Passed: ' || COUNT(*)::VARCHAR
FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results')
WHERE test_timestamp = (SELECT MAX(test_timestamp) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results'))
  AND status = 'PASS'
UNION ALL
SELECT '  Tests Failed: ' || COUNT(*)::VARCHAR
FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results')
WHERE test_timestamp = (SELECT MAX(test_timestamp) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results'))
  AND status = 'FAIL'
UNION ALL
SELECT ''
UNION ALL
SELECT '=' || REPEAT('=', 80) || '=';

-- Display detailed results by category
SELECT
    test_category,
    test_name,
    expected_value,
    actual_value,
    status,
    CASE
        WHEN status = 'PASS' THEN '✓'
        WHEN status = 'FAIL' THEN '✗'
        ELSE '?'
    END AS result_icon
FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results')
WHERE test_timestamp = (SELECT MAX(test_timestamp) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results'))
ORDER BY test_id;

-- Display only failed tests (if any)
SELECT
    '=' || REPEAT('=', 80) || '=' AS separator
UNION ALL
SELECT '  FAILED TESTS (Action Required)'
UNION ALL
SELECT '=' || REPEAT('=', 80) || '=';

SELECT
    test_id,
    test_category,
    test_name,
    expected_value,
    actual_value,
    error_message
FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results')
WHERE test_timestamp = (SELECT MAX(test_timestamp) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results'))
  AND status = 'FAIL'
ORDER BY test_id;

-- Overall deployment status
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results')
              WHERE test_timestamp = (SELECT MAX(test_timestamp) FROM IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results'))
                AND status = 'FAIL') = 0
        THEN '✓ DEPLOYMENT VALIDATION PASSED - All systems operational!'
        ELSE '✗ DEPLOYMENT VALIDATION FAILED - Please review failed tests above'
    END AS overall_status;

-- =============================================================================
-- PERFORMANCE TESTS (Optional - runs sample queries to verify performance)
-- =============================================================================

-- Test sample query on dim_date (should be <100ms)
SET start_time = (SELECT CURRENT_TIMESTAMP());

SELECT COUNT(*) FROM IDENTIFIER(get_dw_database() || '.warehouse.dim_date')
WHERE fiscal_year = 2024;

SET end_time = (SELECT CURRENT_TIMESTAMP());

INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results') (
    test_id, test_category, test_name, expected_value, actual_value, status
)
SELECT
    9001 AS test_id,
    'Performance Validation' AS test_category,
    'Sample query on dim_date executes quickly' AS test_name,
    '<1 second' AS expected_value,
    DATEDIFF(millisecond, $start_time, $end_time)::VARCHAR || 'ms' AS actual_value,
    CASE WHEN DATEDIFF(millisecond, $start_time, $end_time) < 1000 THEN 'PASS' ELSE 'FAIL' END AS status;

-- =============================================================================
-- END OF VALIDATION
-- =============================================================================

SELECT
    '=' || REPEAT('=', 80) || '=' AS separator
UNION ALL
SELECT '  Deployment validation complete!'
UNION ALL
SELECT '  Review results above. If any tests failed, consult TROUBLESHOOTING_PLAYBOOK.md'
UNION ALL
SELECT '=' || REPEAT('=', 80) || '=';

-- Cleanup (optional - comment out if you want to keep validation history)
-- DROP TABLE IDENTIFIER(get_dw_database() || '.metadata.deployment_validation_results');
