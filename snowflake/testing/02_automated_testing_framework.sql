-- =====================================================================================
-- AUTOMATED TESTING FRAMEWORK
-- =====================================================================================
-- Purpose: Test-driven development for ETL pipelines
-- Prevents regressions and ensures data quality
--
-- Test Categories:
-- 1. UNIT - Test individual procedures/functions
-- 2. INTEGRATION - Test data flow between layers
-- 3. E2E - Test complete pipeline execution
-- 4. DATA_QUALITY - Test business rules and constraints
--
-- Benefits:
-- - Catch bugs before production
-- - Confidence in deployments
-- - Living documentation of expected behavior
-- - Faster debugging when tests fail
--
-- Author: Data Team
-- Date: 2025-11-21
-- =====================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA metadata;

-- =====================================================================================
-- TEST CASE DEFINITIONS
-- =====================================================================================

CREATE TABLE IF NOT EXISTS etl_test_cases (
    test_id INTEGER AUTOINCREMENT PRIMARY KEY,
    test_name VARCHAR(200) NOT NULL,
    test_category VARCHAR(50) NOT NULL, -- UNIT, INTEGRATION, E2E, DATA_QUALITY
    test_priority VARCHAR(20) DEFAULT 'MEDIUM', -- CRITICAL, HIGH, MEDIUM, LOW
    test_sql VARCHAR(5000) NOT NULL,    -- SQL query to execute
    expected_result_type VARCHAR(50) NOT NULL, -- COUNT_ZERO, COUNT_POSITIVE, VALUE_EQUALS, BOOLEAN_TRUE
    expected_result_value VARCHAR(100), -- Expected value (for VALUE_EQUALS)
    is_active BOOLEAN DEFAULT TRUE,
    failure_severity VARCHAR(20) DEFAULT 'HIGH', -- CRITICAL, HIGH, MEDIUM, LOW
    failure_action VARCHAR(50) DEFAULT 'WARN', -- BLOCK, WARN, LOG
    description VARCHAR(1000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================================
-- TEST EXECUTION HISTORY
-- =====================================================================================

CREATE TABLE IF NOT EXISTS test_execution_history (
    execution_id INTEGER AUTOINCREMENT PRIMARY KEY,
    test_id INTEGER,
    test_name VARCHAR(200),
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    test_status VARCHAR(20), -- PASSED, FAILED, ERROR, SKIPPED
    actual_result VARCHAR(1000),
    expected_result VARCHAR(1000),
    execution_duration_ms INTEGER,
    error_message VARCHAR(5000),
    batch_id VARCHAR(50)
);

-- =====================================================================================
-- SAMPLE TEST CASES - DATA INTEGRITY
-- =====================================================================================

MERGE INTO etl_test_cases tgt
USING (
    SELECT * FROM VALUES
    -- ========== INTEGRATION TESTS ==========
    (1, 'No orphan veterans in fact_evaluation', 'INTEGRATION', 'CRITICAL',
     'SELECT COUNT(*) FROM warehouse.fact_evaluation f WHERE NOT EXISTS (SELECT 1 FROM warehouse.dim_veteran v WHERE f.veteran_sk = v.veteran_sk)',
     'COUNT_ZERO', NULL, TRUE, 'CRITICAL', 'BLOCK',
     'Ensures every evaluation has a valid veteran dimension record'),

    (2, 'No orphan evaluators in fact_evaluation', 'INTEGRATION', 'CRITICAL',
     'SELECT COUNT(*) FROM warehouse.fact_evaluation f WHERE NOT EXISTS (SELECT 1 FROM warehouse.dim_evaluator e WHERE f.evaluator_sk = e.evaluator_sk)',
     'COUNT_ZERO', NULL, TRUE, 'CRITICAL', 'BLOCK',
     'Ensures every evaluation has a valid evaluator dimension record'),

    (3, 'No orphan facilities in fact_evaluation', 'INTEGRATION', 'HIGH',
     'SELECT COUNT(*) FROM warehouse.fact_evaluation f WHERE f.facility_sk IS NOT NULL AND NOT EXISTS (SELECT 1 FROM warehouse.dim_facility fac WHERE f.facility_sk = fac.facility_sk)',
     'COUNT_ZERO', NULL, TRUE, 'HIGH', 'WARN',
     'Ensures every evaluation with a facility has a valid facility dimension record'),

    (4, 'All current dimension records have valid dates', 'DATA_QUALITY', 'HIGH',
     'SELECT COUNT(*) FROM warehouse.dim_veteran WHERE is_current = TRUE AND (effective_start_date IS NULL OR effective_end_date < effective_start_date)',
     'COUNT_ZERO', NULL, TRUE, 'HIGH', 'WARN',
     'Validates SCD Type 2 date logic in veteran dimension'),

    (5, 'No duplicate current records in dim_veteran', 'DATA_QUALITY', 'CRITICAL',
     'SELECT COUNT(*) - COUNT(DISTINCT veteran_id) FROM warehouse.dim_veteran WHERE is_current = TRUE',
     'COUNT_ZERO', NULL, TRUE, 'CRITICAL', 'BLOCK',
     'Ensures only one current record per veteran'),

    -- ========== DATA QUALITY TESTS ==========
    (6, 'Disability rating within valid range', 'DATA_QUALITY', 'HIGH',
     'SELECT COUNT(*) FROM warehouse.dim_veteran WHERE is_current = TRUE AND (current_disability_rating < 0 OR current_disability_rating > 100)',
     'COUNT_ZERO', NULL, TRUE, 'HIGH', 'WARN',
     'Validates disability ratings are between 0-100'),

    (7, 'No null business keys in dim_veteran', 'DATA_QUALITY', 'CRITICAL',
     'SELECT COUNT(*) FROM warehouse.dim_veteran WHERE veteran_id IS NULL',
     'COUNT_ZERO', NULL, TRUE, 'CRITICAL', 'BLOCK',
     'Ensures all veterans have a business key'),

    (8, 'Evaluation dates are not in future', 'DATA_QUALITY', 'HIGH',
     'SELECT COUNT(*) FROM warehouse.fact_evaluation f JOIN warehouse.dim_date d ON f.evaluation_date_sk = d.date_sk WHERE d.full_date > CURRENT_DATE()',
     'COUNT_ZERO', NULL, TRUE, 'HIGH', 'WARN',
     'Validates evaluation dates are not in the future'),

    (9, 'Staging to warehouse record count matches', 'INTEGRATION', 'MEDIUM',
     'SELECT ABS((SELECT COUNT(*) FROM staging.stg_veterans WHERE batch_id = (SELECT MAX(batch_id) FROM staging.stg_veterans)) - (SELECT COUNT(*) FROM warehouse.dim_veteran WHERE created_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())))',
     'COUNT_ZERO', NULL, TRUE, 'MEDIUM', 'WARN',
     'Validates that staging records are loaded to warehouse (rough check)'),

    (10, 'No negative durations in fact tables', 'DATA_QUALITY', 'HIGH',
     'SELECT COUNT(*) FROM warehouse.fact_evaluation WHERE evaluation_duration_minutes < 0',
     'COUNT_ZERO', NULL, TRUE, 'HIGH', 'WARN',
     'Ensures evaluation durations are positive'),

    -- ========== UNIT TESTS ==========
    (11, 'Configuration function returns values', 'UNIT', 'CRITICAL',
     'SELECT CASE WHEN fn_get_config(''pipeline'', ''default_batch_size'') IS NOT NULL THEN 1 ELSE 0 END',
     'VALUE_EQUALS', '1', TRUE, 'CRITICAL', 'BLOCK',
     'Tests configuration retrieval function'),

    (12, 'Date dimension has fiscal year data', 'UNIT', 'HIGH',
     'SELECT COUNT(*) FROM warehouse.dim_date WHERE fiscal_year IS NULL',
     'COUNT_ZERO', NULL, TRUE, 'HIGH', 'WARN',
     'Validates date dimension fiscal year population'),

    -- ========== E2E TESTS ==========
    (13, 'Pipeline executed in last 24 hours', 'E2E', 'HIGH',
     'SELECT COUNT(*) FROM metadata.pipeline_execution_history WHERE execution_start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP()) AND execution_status = ''SUCCEEDED''',
     'COUNT_POSITIVE', NULL, TRUE, 'HIGH', 'WARN',
     'Verifies pipeline is running regularly'),

    (14, 'Data freshness within SLA', 'E2E', 'CRITICAL',
     'SELECT CASE WHEN DATEDIFF(hour, MAX(created_timestamp), CURRENT_TIMESTAMP()) <= 4 THEN 0 ELSE 1 END FROM warehouse.dim_veteran WHERE is_current = TRUE',
     'COUNT_ZERO', NULL, TRUE, 'CRITICAL', 'BLOCK',
     'Ensures data is fresh (< 4 hours old)')

) AS src (test_id, test_name, test_category, test_priority, test_sql, expected_result_type, expected_result_value, is_active, failure_severity, failure_action, description)
ON tgt.test_id = src.test_id
WHEN MATCHED THEN UPDATE SET
    test_name = src.test_name,
    test_category = src.test_category,
    test_priority = src.test_priority,
    test_sql = src.test_sql,
    expected_result_type = src.expected_result_type,
    expected_result_value = src.expected_result_value,
    is_active = src.is_active,
    failure_severity = src.failure_severity,
    failure_action = src.failure_action,
    description = src.description,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    test_id, test_name, test_category, test_priority, test_sql, expected_result_type,
    expected_result_value, is_active, failure_severity, failure_action, description
) VALUES (
    src.test_id, src.test_name, src.test_category, src.test_priority, src.test_sql, src.expected_result_type,
    src.expected_result_value, src.is_active, src.failure_severity, src.failure_action, src.description
);

-- =====================================================================================
-- TEST RUNNER PROCEDURE
-- =====================================================================================

CREATE OR REPLACE PROCEDURE sp_run_etl_tests(
    p_test_category VARCHAR DEFAULT 'ALL',
    p_test_priority VARCHAR DEFAULT 'ALL',
    p_batch_id VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_test_cursor CURSOR FOR
        SELECT test_id, test_name, test_category, test_sql, expected_result_type,
               expected_result_value, failure_severity, failure_action
        FROM IDENTIFIER(:v_dw_database || '.metadata.etl_test_cases')
        WHERE is_active = TRUE
          AND (test_category = :p_test_category OR :p_test_category = 'ALL')
          AND (test_priority = :p_test_priority OR :p_test_priority = 'ALL')
        ORDER BY
            CASE test_priority
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                WHEN 'LOW' THEN 4
            END;

    v_test_id INTEGER;
    v_test_name VARCHAR;
    v_test_category VARCHAR;
    v_test_sql VARCHAR;
    v_expected_result_type VARCHAR;
    v_expected_result_value VARCHAR;
    v_failure_severity VARCHAR;
    v_failure_action VARCHAR;
    v_actual_result VARCHAR;
    v_test_status VARCHAR;
    v_error_message VARCHAR;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_duration_ms INTEGER;
    v_total_tests INTEGER DEFAULT 0;
    v_passed_tests INTEGER DEFAULT 0;
    v_failed_tests INTEGER DEFAULT 0;
    v_error_tests INTEGER DEFAULT 0;
    v_blocked BOOLEAN DEFAULT FALSE;
    v_result_set RESULTSET;
BEGIN
    -- Loop through all matching test cases
    FOR test_record IN v_test_cursor DO
        v_test_id := test_record.test_id;
        v_test_name := test_record.test_name;
        v_test_category := test_record.test_category;
        v_test_sql := test_record.test_sql;
        v_expected_result_type := test_record.expected_result_type;
        v_expected_result_value := test_record.expected_result_value;
        v_failure_severity := test_record.failure_severity;
        v_failure_action := test_record.failure_action;
        v_start_time := CURRENT_TIMESTAMP();
        v_total_tests := v_total_tests + 1;

        BEGIN
            -- Execute test SQL
            v_result_set := (EXECUTE IMMEDIATE :v_test_sql);
            LET c1 CURSOR FOR v_result_set;
            OPEN c1;
            FETCH c1 INTO v_actual_result;
            CLOSE c1;

            -- Evaluate result based on expected result type
            v_test_status := CASE
                WHEN v_expected_result_type = 'COUNT_ZERO' AND TRY_TO_NUMBER(v_actual_result) = 0 THEN 'PASSED'
                WHEN v_expected_result_type = 'COUNT_POSITIVE' AND TRY_TO_NUMBER(v_actual_result) > 0 THEN 'PASSED'
                WHEN v_expected_result_type = 'VALUE_EQUALS' AND v_actual_result = v_expected_result_value THEN 'PASSED'
                WHEN v_expected_result_type = 'BOOLEAN_TRUE' AND TRY_TO_BOOLEAN(v_actual_result) = TRUE THEN 'PASSED'
                ELSE 'FAILED'
            END;

            v_end_time := CURRENT_TIMESTAMP();
            v_duration_ms := DATEDIFF(millisecond, v_start_time, v_end_time);

            -- Update counters
            IF (v_test_status = 'PASSED') THEN
                v_passed_tests := v_passed_tests + 1;
            ELSE
                v_failed_tests := v_failed_tests + 1;
                -- Check if we should block pipeline
                IF (v_failure_action = 'BLOCK') THEN
                    v_blocked := TRUE;
                END IF;
            END IF;

            -- Log test execution
            INSERT INTO IDENTIFIER(:v_dw_database || '.metadata.test_execution_history')
                (test_id, test_name, test_status, actual_result, expected_result, execution_duration_ms, batch_id)
            VALUES
                (:v_test_id, :v_test_name, :v_test_status, :v_actual_result,
                 COALESCE(:v_expected_result_value, :v_expected_result_type), :v_duration_ms, :p_batch_id);

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                v_test_status := 'ERROR';
                v_error_tests := v_error_tests + 1;
                v_end_time := CURRENT_TIMESTAMP();
                v_duration_ms := DATEDIFF(millisecond, v_start_time, v_end_time);

                -- Log error
                INSERT INTO IDENTIFIER(:v_dw_database || '.metadata.test_execution_history')
                    (test_id, test_name, test_status, actual_result, expected_result, execution_duration_ms, error_message, batch_id)
                VALUES
                    (:v_test_id, :v_test_name, :v_test_status, NULL,
                     COALESCE(:v_expected_result_value, :v_expected_result_type), :v_duration_ms, :v_error_message, :p_batch_id);
        END;
    END FOR;

    -- Return summary
    IF (v_blocked = TRUE) THEN
        RETURN '❌ TESTS BLOCKED PIPELINE | Total: ' || v_total_tests || ' | Passed: ' || v_passed_tests ||
               ' | Failed: ' || v_failed_tests || ' | Errors: ' || v_error_tests ||
               ' | Review failures with failure_action=BLOCK';
    ELSIF (v_failed_tests > 0 OR v_error_tests > 0) THEN
        RETURN '⚠️  TESTS COMPLETED WITH FAILURES | Total: ' || v_total_tests || ' | Passed: ' || v_passed_tests ||
               ' | Failed: ' || v_failed_tests || ' | Errors: ' || v_error_tests;
    ELSE
        RETURN '✅ ALL TESTS PASSED | Total: ' || v_total_tests || ' | Passed: ' || v_passed_tests;
    END IF;
END;
$$;

-- =====================================================================================
-- TEST RESULT VIEWS
-- =====================================================================================

-- Latest test results
CREATE OR REPLACE VIEW vw_latest_test_results AS
SELECT
    t.test_category,
    t.test_priority,
    t.test_name,
    h.test_status,
    h.actual_result,
    h.expected_result,
    h.execution_timestamp,
    h.execution_duration_ms,
    t.failure_action,
    h.error_message
FROM test_execution_history h
JOIN etl_test_cases t ON h.test_id = t.test_id
WHERE h.execution_timestamp = (
    SELECT MAX(execution_timestamp)
    FROM test_execution_history
    WHERE test_id = h.test_id
)
ORDER BY
    CASE h.test_status
        WHEN 'FAILED' THEN 1
        WHEN 'ERROR' THEN 2
        WHEN 'PASSED' THEN 3
    END,
    CASE t.test_priority
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END;

-- Test summary by category
CREATE OR REPLACE VIEW vw_test_summary_by_category AS
SELECT
    test_category,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN test_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_tests,
    SUM(CASE WHEN test_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_tests,
    SUM(CASE WHEN test_status = 'ERROR' THEN 1 ELSE 0 END) AS error_tests,
    ROUND(SUM(CASE WHEN test_status = 'PASSED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate_pct,
    MAX(execution_timestamp) AS last_run_time
FROM (
    SELECT
        t.test_category,
        h.test_status,
        h.execution_timestamp,
        ROW_NUMBER() OVER (PARTITION BY h.test_id ORDER BY h.execution_timestamp DESC) AS rn
    FROM test_execution_history h
    JOIN etl_test_cases t ON h.test_id = t.test_id
) latest
WHERE rn = 1
GROUP BY test_category
ORDER BY pass_rate_pct ASC;

-- Failed tests requiring attention
CREATE OR REPLACE VIEW vw_failed_tests AS
SELECT
    t.test_category,
    t.test_priority,
    t.test_name,
    h.actual_result,
    h.expected_result,
    t.failure_action,
    t.failure_severity,
    h.execution_timestamp,
    h.error_message,
    t.description
FROM test_execution_history h
JOIN etl_test_cases t ON h.test_id = t.test_id
WHERE h.test_status IN ('FAILED', 'ERROR')
  AND h.execution_timestamp = (
      SELECT MAX(execution_timestamp)
      FROM test_execution_history
      WHERE test_id = h.test_id
  )
ORDER BY
    CASE t.failure_action
        WHEN 'BLOCK' THEN 1
        WHEN 'WARN' THEN 2
        ELSE 3
    END,
    CASE t.test_priority
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- Run all tests
CALL sp_run_etl_tests('ALL', 'ALL', 'BATCH_20251121_083000');

-- Run only critical tests
CALL sp_run_etl_tests('ALL', 'CRITICAL', 'BATCH_20251121_083000');

-- Run only integration tests
CALL sp_run_etl_tests('INTEGRATION', 'ALL', 'BATCH_20251121_083000');

-- View latest test results
SELECT * FROM vw_latest_test_results;

-- View failed tests
SELECT * FROM vw_failed_tests;

-- View test summary
SELECT * FROM vw_test_summary_by_category;

-- Add a new test case
INSERT INTO etl_test_cases (
    test_name, test_category, test_priority, test_sql,
    expected_result_type, is_active, failure_severity, failure_action, description
) VALUES (
    'Custom validation rule',
    'DATA_QUALITY',
    'HIGH',
    'SELECT COUNT(*) FROM warehouse.dim_veteran WHERE email NOT LIKE ''%@%''',
    'COUNT_ZERO',
    TRUE,
    'HIGH',
    'WARN',
    'Validates email format'
);

-- Disable a test
UPDATE etl_test_cases
SET is_active = FALSE
WHERE test_name = 'Test to disable';
*/

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

SELECT
    'Automated Testing Framework Deployed' AS status,
    COUNT(*) AS total_test_cases,
    COUNT(CASE WHEN is_active = TRUE THEN 1 END) AS active_test_cases,
    COUNT(DISTINCT test_category) AS test_categories
FROM etl_test_cases;
