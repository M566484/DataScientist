-- =====================================================================================================================
-- TEST SUITE: PHASE 1-3 IMPROVEMENTS
-- =====================================================================================================================
-- Purpose: Comprehensive tests for all improvements from code review phases 1-3
-- Author: Code Review Remediation Testing
-- Date: 2025-11-22
--
-- Test Coverage:
-- 1. Phase 1 (Critical): Function naming, error handling, SQL injection protection
-- 2. Phase 2 (High Priority): Configuration, clustering, documentation
-- 3. Phase 3 (Medium Priority): Column comments, masking, RLS, validation, audit
--
-- Usage:
-- Run this entire script in a TEST environment ONLY
-- Review results to ensure all tests pass
-- =====================================================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA WAREHOUSE;

-- =====================================================================================================================
-- SECTION 1: PHASE 1 TESTS - SQL INJECTION PROTECTION
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 1: SQL INJECTION PROTECTION TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 1.1: Valid inputs should succeed
SELECT 'Test 1.1: Valid table name and batch ID' AS test_name;
CALL sp_load_scd_type2_generic_secure('dim_veterans', 'BATCH_20251122_001');
-- Expected: SUCCESS message or configuration not found (both acceptable in test environment)

-- Test 1.2: SQL injection in table name should fail
SELECT 'Test 1.2: SQL injection attempt in table_name' AS test_name;
CALL sp_load_scd_type2_generic_secure('dim_veterans; DROP TABLE dim_veterans;--', 'BATCH_001');
-- Expected: 'ERROR: Invalid table_name format. Only alphanumeric characters and underscores allowed'

-- Test 1.3: SQL injection in batch ID should fail
SELECT 'Test 1.3: SQL injection attempt in batch_id' AS test_name;
CALL sp_load_scd_type2_generic_secure('dim_veterans', 'BATCH''; DROP TABLE dim_veterans;--');
-- Expected: 'ERROR: Invalid batch_id format. Only alphanumeric characters, hyphens, and underscores allowed'

-- Test 1.4: Empty table name should fail
SELECT 'Test 1.4: Empty table_name validation' AS test_name;
CALL sp_load_scd_type2_generic_secure('', 'BATCH_001');
-- Expected: 'ERROR: table_name parameter is required and cannot be empty'

-- Test 1.5: NULL batch ID should fail
SELECT 'Test 1.5: NULL batch_id validation' AS test_name;
CALL sp_load_scd_type2_generic_secure('dim_veterans', NULL);
-- Expected: 'ERROR: batch_id parameter is required and cannot be empty'

-- Test 1.6: Special characters in table name should fail
SELECT 'Test 1.6: Special characters in table_name' AS test_name;
CALL sp_load_scd_type2_generic_secure('dim-veterans!@#', 'BATCH_001');
-- Expected: 'ERROR: Invalid table_name format'

-- =====================================================================================================================
-- SECTION 2: PHASE 1 TESTS - ERROR HANDLING
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 1: ERROR HANDLING TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 2.1: NULL batch_id should be rejected with clear error
SELECT 'Test 2.1: ETL procedure rejects NULL batch_id' AS test_name;
CALL sp_transform_ods_to_staging_veterans(NULL);
-- Expected: 'ERROR: batch_id parameter is required and cannot be empty'

-- Test 2.2: Empty batch_id should be rejected
SELECT 'Test 2.2: ETL procedure rejects empty batch_id' AS test_name;
CALL sp_transform_ods_to_staging_veterans('');
-- Expected: 'ERROR: batch_id parameter is required and cannot be empty'

-- Test 2.3: Valid batch_id should proceed (may fail on missing data, but should handle gracefully)
SELECT 'Test 2.3: ETL procedure handles valid batch_id' AS test_name;
CALL sp_transform_ods_to_staging_veterans('TEST_BATCH_001');
-- Expected: Either SUCCESS or detailed error message with logging (not a crash)

-- =====================================================================================================================
-- SECTION 3: PHASE 2 TESTS - CONFIGURATION MANAGEMENT
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 2: CONFIGURATION MANAGEMENT TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 3.1: Verify configuration values exist
SELECT 'Test 3.1: Check configuration table has expected categories' AS test_name;
SELECT
    config_category,
    COUNT(*) AS config_count
FROM metadata.system_configuration
WHERE config_category IN ('date_dimension', 'performance', 'defaults', 'testing')
GROUP BY config_category
ORDER BY config_category;
-- Expected: At least 4 categories with multiple configs each

-- Test 3.2: Test fn_get_config function
SELECT 'Test 3.2: Test configuration retrieval function' AS test_name;
SELECT fn_get_config('date_dimension', 'start_date') AS start_date;
SELECT fn_get_config('date_dimension', 'end_date') AS end_date;
-- Expected: Valid date strings (e.g., '2020-01-01', '2030-12-31')

-- Test 3.3: Test config-driven date dimension population
SELECT 'Test 3.3: Test populate_dim_dates_from_config procedure' AS test_name;
-- This test would populate dim_dates, so only run if safe to do so
-- CALL populate_dim_dates_from_config();
-- Expected: SUCCESS message with row count

-- =====================================================================================================================
-- SECTION 4: PHASE 2 TESTS - CLUSTERING VERIFICATION
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 2: CLUSTERING VERIFICATION' AS test_section;
SELECT '========================================' AS test_section;

-- Test 4.1: Verify clustering keys on dimension tables
SELECT 'Test 4.1: Check clustering on dimension tables' AS test_name;
SELECT
    table_name,
    clustering_key
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'dim_%'
  AND clustering_key IS NOT NULL
ORDER BY table_name;
-- Expected: At least 5 dimension tables with clustering keys

-- Test 4.2: Verify clustering keys on fact tables
SELECT 'Test 4.2: Check clustering on fact tables' AS test_name;
SELECT
    table_name,
    clustering_key
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'fact_%'
  AND clustering_key IS NOT NULL
ORDER BY table_name;
-- Expected: At least 9 fact tables with clustering keys

-- =====================================================================================================================
-- SECTION 5: PHASE 3 TESTS - COLUMN COMMENTS
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 3: COLUMN COMMENTS VERIFICATION' AS test_section;
SELECT '========================================' AS test_section;

-- Test 5.1: Verify column comments on dimension tables
SELECT 'Test 5.1: Count columns with comments in dimension tables' AS test_name;
SELECT
    table_name,
    COUNT(*) AS columns_with_comments
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'dim_%'
  AND comment IS NOT NULL
GROUP BY table_name
ORDER BY table_name;
-- Expected: At least 4 dimension tables with 15+ comments each

-- Test 5.2: Sample comments from dim_veterans
SELECT 'Test 5.2: Sample comments from dim_veterans' AS test_name;
SELECT
    column_name,
    comment
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'WAREHOUSE'
  AND table_name = 'DIM_VETERANS'
  AND column_name IN ('VETERAN_SK', 'FIRST_NAME', 'DISABILITY_RATING_CATEGORY')
ORDER BY column_name;
-- Expected: Detailed, meaningful comments for each column

-- =====================================================================================================================
-- SECTION 6: PHASE 3 TESTS - DATA MASKING POLICIES
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 3: DATA MASKING TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 6.1: Verify masking policies exist
SELECT 'Test 6.1: Check masking policies created' AS test_name;
SHOW MASKING POLICIES IN SCHEMA WAREHOUSE;
-- Expected: At least 7 masking policies (name, SSN, email, phone, address, ZIP, DOB)

-- Test 6.2: Test masking as SYSADMIN (should see unmasked data)
SELECT 'Test 6.2: Data unmasked for SYSADMIN role' AS test_name;
USE ROLE SYSADMIN;
SELECT
    first_name,
    last_name,
    email,
    phone
FROM dim_veterans
LIMIT 5;
-- Expected: Full, unmasked values

-- Test 6.3: Test masking as lower-privileged role (if available)
SELECT 'Test 6.3: Data masked for non-privileged roles' AS test_name;
-- Note: This test requires a non-privileged role to exist
-- USE ROLE ANALYST;
-- SELECT first_name, last_name, email, phone FROM dim_veterans LIMIT 5;
-- Expected: Masked values like 'J***', '***@domain.com', '***-***-1234'

-- =====================================================================================================================
-- SECTION 7: PHASE 3 TESTS - ROW-LEVEL SECURITY
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 3: ROW-LEVEL SECURITY TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 7.1: Verify row access policies exist
SELECT 'Test 7.1: Check row access policies created' AS test_name;
SHOW ROW ACCESS POLICIES IN SCHEMA WAREHOUSE;
-- Expected: At least 1 row access policy (veteran_data_access_policy)

-- Test 7.2: Verify policy definition
SELECT 'Test 7.2: Review row access policy details' AS test_name;
-- Manual inspection of policy via SHOW command above
-- Expected: Policy should restrict access based on facility and role

-- =====================================================================================================================
-- SECTION 8: PHASE 3 TESTS - VALIDATION RULES
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 3: DATA VALIDATION TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 8.1: Verify validation rules exist
SELECT 'Test 8.1: Check validation rules created' AS test_name;
SELECT
    validation_type,
    severity,
    COUNT(*) AS rule_count
FROM metadata.enhanced_validation_rules
WHERE is_active = TRUE
GROUP BY validation_type, severity
ORDER BY validation_type, severity;
-- Expected: Multiple validation rules across different types and severities

-- Test 8.2: Review specific validation rules
SELECT 'Test 8.2: Sample validation rules' AS test_name;
SELECT
    rule_code,
    rule_name,
    table_name,
    validation_type,
    severity
FROM metadata.enhanced_validation_rules
WHERE is_active = TRUE
ORDER BY rule_code
LIMIT 10;
-- Expected: Well-defined validation rules with clear purposes

-- =====================================================================================================================
-- SECTION 9: PHASE 3 TESTS - AUDIT LOGGING
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'PHASE 3: AUDIT LOGGING TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 9.1: Verify audit log table exists
SELECT 'Test 9.1: Check audit_log table structure' AS test_name;
DESCRIBE TABLE metadata.audit_log;
-- Expected: Table with columns for user, action, timestamp, PII flag, etc.

-- Test 9.2: Test logging an audit event
SELECT 'Test 9.2: Log a test audit event' AS test_name;
CALL metadata.sp_log_audit_event(
    'SELECT',
    'TABLE',
    'dim_veterans',
    'SUCCESS',
    100,
    TRUE,  -- PII accessed
    PARSE_JSON('{"test": "Phase 3 testing", "purpose": "verification"}')
);
-- Expected: SUCCESS return value

-- Test 9.3: Verify audit event was logged
SELECT 'Test 9.3: Retrieve logged audit event' AS test_name;
SELECT
    audit_id,
    user_name,
    action_type,
    object_type,
    object_name,
    operation_status,
    pii_accessed,
    audit_timestamp
FROM metadata.audit_log
ORDER BY audit_timestamp DESC
LIMIT 5;
-- Expected: Recent audit event from Test 9.2

-- Test 9.4: Test PII access audit view
SELECT 'Test 9.4: Check PII access audit view' AS test_name;
SELECT * FROM metadata.vw_pii_access_audit
ORDER BY audit_timestamp DESC
LIMIT 5;
-- Expected: Recent PII access events

-- =====================================================================================================================
-- SECTION 10: INTEGRATION TESTS
-- =====================================================================================================================

SELECT '========================================' AS test_section;
SELECT 'INTEGRATION TESTS' AS test_section;
SELECT '========================================' AS test_section;

-- Test 10.1: Verify data type standards table
SELECT 'Test 10.1: Check data type standards' AS test_name;
SELECT
    data_type_category,
    COUNT(*) AS standard_count
FROM metadata.data_type_standards
GROUP BY data_type_category
ORDER BY data_type_category;
-- Expected: Standards across multiple categories

-- Test 10.2: Verify pipeline execution logging works
SELECT 'Test 10.2: Check recent pipeline executions' AS test_name;
SELECT
    execution_id,
    procedure_name,
    execution_status,
    batch_id,
    execution_start_time
FROM metadata.pipeline_execution_history
ORDER BY execution_start_time DESC
LIMIT 10;
-- Expected: Recent pipeline executions with proper logging

-- =====================================================================================================================
-- TEST SUMMARY
-- =====================================================================================================================

SELECT '========================================' AS summary;
SELECT 'TEST SUITE COMPLETE' AS summary;
SELECT '========================================' AS summary;

SELECT
    'Total Test Sections' AS metric,
    '10' AS value
UNION ALL
SELECT
    'Phase 1 Tests',
    'SQL Injection Protection + Error Handling'
UNION ALL
SELECT
    'Phase 2 Tests',
    'Configuration Management + Clustering'
UNION ALL
SELECT
    'Phase 3 Tests',
    'Comments + Masking + RLS + Validation + Audit';

SELECT '========================================' AS notes;
SELECT 'NOTES' AS notes;
SELECT '========================================' AS notes;

SELECT
    '1. Review all test results for errors' AS note
UNION ALL
SELECT '2. Some tests may fail in non-production environments (expected)'
UNION ALL
SELECT '3. SQL injection tests should ALL show ERROR messages (not SUCCESS)'
UNION ALL
SELECT '4. Masking tests require appropriate role setup'
UNION ALL
SELECT '5. Document any unexpected failures for investigation';

-- =====================================================================================================================
-- END OF TEST SUITE
-- =====================================================================================================================
