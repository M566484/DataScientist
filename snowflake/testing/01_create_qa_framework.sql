-- =====================================================
-- VES QA Testing Framework
-- =====================================================
-- Purpose: Automated testing infrastructure for data warehouse
-- Usage: Deploy this first, then load test definitions
-- Standards: VES Snowflake Naming Conventions v1.0

USE DATABASE VETERAN_EVALUATION_DW;

-- Create QA schema for test framework
CREATE SCHEMA IF NOT EXISTS QA_FRAMEWORK;
USE SCHEMA QA_FRAMEWORK;

-- =====================================================
-- Test Metadata Tables
-- =====================================================

-- Stores test definitions
CREATE OR REPLACE TABLE qa_test_definitions (
    test_id INTEGER AUTOINCREMENT PRIMARY KEY,
    test_name VARCHAR(200) NOT NULL UNIQUE,
    test_description TEXT,
    test_category VARCHAR(50) NOT NULL,  -- UNIT, INTEGRATION, E2E, SMOKE
    test_layer VARCHAR(50),               -- ODS, STAGING, WAREHOUSE, PIPELINE, REFERENCE
    test_sql TEXT NOT NULL,               -- The actual test query to execute
    expected_result VARCHAR(1000),        -- Expected result (e.g., '0', 'PASS', specific value)
    severity VARCHAR(20) NOT NULL,        -- CRITICAL, HIGH, MEDIUM, LOW
    active_flag BOOLEAN DEFAULT TRUE,
    timeout_seconds INTEGER DEFAULT 300,  -- Test timeout
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMMENT = 'Stores all automated test definitions'
)
COMMENT = 'Test definitions for automated QA validation';

-- Stores test execution metadata
CREATE OR REPLACE TABLE qa_test_executions (
    execution_id INTEGER AUTOINCREMENT PRIMARY KEY,
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    execution_type VARCHAR(50),           -- SCHEDULED, MANUAL, PRE_DEPLOYMENT, POST_ETL
    test_category_filter VARCHAR(50),     -- Filter used (ALL, UNIT, INTEGRATION, etc.)
    batch_id VARCHAR(50),                 -- Related ETL batch if applicable
    total_tests INTEGER,
    passed_tests INTEGER,
    failed_tests INTEGER,
    skipped_tests INTEGER,
    error_tests INTEGER,
    execution_duration_seconds INTEGER,
    overall_status VARCHAR(20),           -- PASS, FAIL, PARTIAL
    executed_by VARCHAR(100) DEFAULT CURRENT_USER(),
    COMMENT = 'Tracks test execution runs'
)
COMMENT = 'Test execution metadata and summary';

-- Stores individual test results
CREATE OR REPLACE TABLE qa_test_results (
    result_id INTEGER AUTOINCREMENT PRIMARY KEY,
    execution_id INTEGER NOT NULL,
    test_id INTEGER NOT NULL,
    test_name VARCHAR(200) NOT NULL,
    test_category VARCHAR(50),
    test_severity VARCHAR(20),
    actual_result VARCHAR(1000),
    expected_result VARCHAR(1000),
    test_status VARCHAR(20) NOT NULL,     -- PASS, FAIL, SKIP, ERROR
    error_message TEXT,
    execution_duration_seconds DECIMAL(10,3),
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (execution_id) REFERENCES qa_test_executions(execution_id),
    FOREIGN KEY (test_id) REFERENCES qa_test_definitions(test_id),
    COMMENT = 'Individual test execution results'
)
COMMENT = 'Detailed results for each test execution';

-- Stores test assertions for detailed validation
CREATE OR REPLACE TABLE qa_test_assertions (
    assertion_id INTEGER AUTOINCREMENT PRIMARY KEY,
    result_id INTEGER NOT NULL,
    assertion_name VARCHAR(200),
    assertion_description TEXT,
    assertion_value VARCHAR(1000),
    assertion_status VARCHAR(20),         -- PASS, FAIL
    assertion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (result_id) REFERENCES qa_test_results(result_id),
    COMMENT = 'Detailed assertion results'
)
COMMENT = 'Granular assertion tracking for complex tests';

-- =====================================================
-- Test Execution Procedures
-- =====================================================

-- Main test execution procedure
CREATE OR REPLACE PROCEDURE sp_execute_qa_tests(
    p_test_category VARCHAR DEFAULT 'ALL',
    p_batch_id VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_execution_id INTEGER;
    v_test_count INTEGER DEFAULT 0;
    v_passed INTEGER DEFAULT 0;
    v_failed INTEGER DEFAULT 0;
    v_skipped INTEGER DEFAULT 0;
    v_error INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_NTZ;
    v_test_cursor CURSOR FOR
        SELECT test_id, test_name, test_sql, expected_result, severity, test_category
        FROM qa_test_definitions
        WHERE active_flag = TRUE
          AND (p_test_category = 'ALL' OR test_category = p_test_category)
        ORDER BY CASE severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END, test_id;
    v_test_id INTEGER;
    v_test_name VARCHAR;
    v_test_sql TEXT;
    v_expected_result VARCHAR;
    v_severity VARCHAR;
    v_category VARCHAR;
    v_actual_result VARCHAR;
    v_test_status VARCHAR;
    v_error_msg TEXT;
    v_test_start TIMESTAMP_NTZ;
    v_test_duration DECIMAL;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();

    -- Create execution record
    INSERT INTO qa_test_executions (
        execution_type,
        test_category_filter,
        batch_id,
        overall_status
    )
    VALUES (
        'MANUAL',
        :p_test_category,
        :p_batch_id,
        'RUNNING'
    );

    SELECT MAX(execution_id) INTO v_execution_id FROM qa_test_executions;

    -- Execute each test
    FOR test_record IN v_test_cursor DO
        v_test_id := test_record.test_id;
        v_test_name := test_record.test_name;
        v_test_sql := test_record.test_sql;
        v_expected_result := test_record.expected_result;
        v_severity := test_record.severity;
        v_category := test_record.test_category;
        v_test_start := CURRENT_TIMESTAMP();
        v_test_count := v_test_count + 1;

        BEGIN
            -- Execute the test SQL
            EXECUTE IMMEDIATE v_test_sql INTO v_actual_result;

            -- Compare actual vs expected
            IF (v_actual_result = v_expected_result OR
                (v_actual_result LIKE '%PASS%' AND v_expected_result = 'PASS')) THEN
                v_test_status := 'PASS';
                v_passed := v_passed + 1;
                v_error_msg := NULL;
            ELSE
                v_test_status := 'FAIL';
                v_failed := v_failed + 1;
                v_error_msg := 'Expected: ' || v_expected_result || ', Got: ' || v_actual_result;
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_test_status := 'ERROR';
                v_error := v_error + 1;
                v_error_msg := SQLERRM;
                v_actual_result := 'ERROR';
        END;

        v_test_duration := DATEDIFF(millisecond, v_test_start, CURRENT_TIMESTAMP()) / 1000.0;

        -- Record test result
        INSERT INTO qa_test_results (
            execution_id,
            test_id,
            test_name,
            test_category,
            test_severity,
            actual_result,
            expected_result,
            test_status,
            error_message,
            execution_duration_seconds
        ) VALUES (
            v_execution_id,
            v_test_id,
            v_test_name,
            v_category,
            v_severity,
            v_actual_result,
            v_expected_result,
            v_test_status,
            v_error_msg,
            v_test_duration
        );
    END FOR;

    -- Update execution summary
    UPDATE qa_test_executions
    SET total_tests = v_test_count,
        passed_tests = v_passed,
        failed_tests = v_failed,
        skipped_tests = v_skipped,
        error_tests = v_error,
        execution_duration_seconds = DATEDIFF(second, v_start_time, CURRENT_TIMESTAMP()),
        overall_status = CASE
            WHEN v_failed = 0 AND v_error = 0 THEN 'PASS'
            WHEN v_failed > 0 OR v_error > 0 THEN 'FAIL'
            ELSE 'PARTIAL'
        END
    WHERE execution_id = v_execution_id;

    RETURN 'Test execution completed. Execution ID: ' || v_execution_id ||
           ' | Total: ' || v_test_count ||
           ' | Passed: ' || v_passed ||
           ' | Failed: ' || v_failed ||
           ' | Errors: ' || v_error ||
           ' | Status: ' || CASE WHEN v_failed = 0 AND v_error = 0 THEN 'PASS' ELSE 'FAIL' END;
END;
$$;

-- Procedure to run a single test by name
CREATE OR REPLACE PROCEDURE sp_execute_single_test(
    p_test_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_test_sql TEXT;
    v_expected_result VARCHAR;
    v_actual_result VARCHAR;
BEGIN
    -- Get test definition
    SELECT test_sql, expected_result
    INTO v_test_sql, v_expected_result
    FROM qa_test_definitions
    WHERE test_name = :p_test_name
      AND active_flag = TRUE;

    -- Execute test
    EXECUTE IMMEDIATE v_test_sql INTO v_actual_result;

    RETURN 'Test: ' || p_test_name ||
           ' | Expected: ' || v_expected_result ||
           ' | Actual: ' || v_actual_result ||
           ' | Result: ' || CASE WHEN v_actual_result = v_expected_result THEN 'PASS' ELSE 'FAIL' END;
END;
$$;

-- =====================================================
-- Test Result Reporting Views
-- =====================================================

-- Daily test summary
CREATE OR REPLACE VIEW vw_qa_daily_test_summary AS
SELECT
    DATE(execution_timestamp) as test_date,
    execution_type,
    COUNT(DISTINCT execution_id) as total_executions,
    SUM(total_tests) as total_tests_run,
    SUM(passed_tests) as total_passed,
    SUM(failed_tests) as total_failed,
    SUM(error_tests) as total_errors,
    ROUND(SUM(passed_tests) * 100.0 / NULLIF(SUM(total_tests), 0), 2) as pass_rate_pct,
    ROUND(AVG(execution_duration_seconds), 1) as avg_duration_seconds,
    MAX(execution_timestamp) as last_run_time
FROM qa_test_executions
WHERE execution_timestamp >= CURRENT_DATE - 30
GROUP BY DATE(execution_timestamp), execution_type
ORDER BY test_date DESC;

-- Failed tests detail view
CREATE OR REPLACE VIEW vw_qa_failed_tests AS
SELECT
    r.execution_timestamp,
    r.test_name,
    d.test_category,
    d.test_layer,
    r.test_severity as severity,
    r.expected_result,
    r.actual_result,
    r.error_message,
    e.batch_id,
    r.execution_duration_seconds
FROM qa_test_results r
JOIN qa_test_definitions d ON r.test_id = d.test_id
JOIN qa_test_executions e ON r.execution_id = e.execution_id
WHERE r.test_status IN ('FAIL', 'ERROR')
  AND r.execution_timestamp >= CURRENT_DATE - 7
ORDER BY r.execution_timestamp DESC, r.test_severity;

-- Test stability analysis
CREATE OR REPLACE VIEW vw_qa_test_stability AS
SELECT
    d.test_name,
    d.test_category,
    d.test_layer,
    d.severity,
    COUNT(*) as total_executions,
    SUM(CASE WHEN r.test_status = 'PASS' THEN 1 ELSE 0 END) as pass_count,
    SUM(CASE WHEN r.test_status = 'FAIL' THEN 1 ELSE 0 END) as fail_count,
    SUM(CASE WHEN r.test_status = 'ERROR' THEN 1 ELSE 0 END) as error_count,
    ROUND(SUM(CASE WHEN r.test_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stability_pct,
    MAX(r.execution_timestamp) as last_run,
    ROUND(AVG(r.execution_duration_seconds), 3) as avg_duration_seconds
FROM qa_test_results r
JOIN qa_test_definitions d ON r.test_id = d.test_id
WHERE r.execution_timestamp >= CURRENT_DATE - 30
GROUP BY d.test_name, d.test_category, d.test_layer, d.severity
ORDER BY stability_pct ASC, fail_count DESC;

-- Latest test execution summary
CREATE OR REPLACE VIEW vw_qa_latest_execution AS
WITH latest_exec AS (
    SELECT MAX(execution_id) as execution_id
    FROM qa_test_executions
)
SELECT
    e.execution_id,
    e.execution_timestamp,
    e.execution_type,
    e.test_category_filter,
    e.batch_id,
    e.total_tests,
    e.passed_tests,
    e.failed_tests,
    e.error_tests,
    e.overall_status,
    e.execution_duration_seconds,
    ROUND(e.passed_tests * 100.0 / NULLIF(e.total_tests, 0), 2) as pass_rate_pct
FROM qa_test_executions e
JOIN latest_exec l ON e.execution_id = l.execution_id;

-- Test coverage by layer
CREATE OR REPLACE VIEW vw_qa_test_coverage AS
SELECT
    test_layer,
    test_category,
    severity,
    COUNT(*) as test_count,
    SUM(CASE WHEN active_flag = TRUE THEN 1 ELSE 0 END) as active_tests,
    SUM(CASE WHEN active_flag = FALSE THEN 1 ELSE 0 END) as inactive_tests
FROM qa_test_definitions
GROUP BY test_layer, test_category, severity
ORDER BY test_layer, test_category,
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END;

-- =====================================================
-- Helper Procedures
-- =====================================================

-- Disable a flaky test
CREATE OR REPLACE PROCEDURE sp_disable_test(
    p_test_name VARCHAR,
    p_reason VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE qa_test_definitions
    SET active_flag = FALSE,
        updated_date = CURRENT_TIMESTAMP()
    WHERE test_name = :p_test_name;

    RETURN 'Test "' || p_test_name || '" disabled. Reason: ' || p_reason;
END;
$$;

-- Re-enable a test
CREATE OR REPLACE PROCEDURE sp_enable_test(
    p_test_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE qa_test_definitions
    SET active_flag = TRUE,
        updated_date = CURRENT_TIMESTAMP()
    WHERE test_name = :p_test_name;

    RETURN 'Test "' || p_test_name || '" enabled.';
END;
$$;

-- Clear old test results (retention policy)
CREATE OR REPLACE PROCEDURE sp_cleanup_test_results(
    p_retention_days INTEGER DEFAULT 90
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_deleted_assertions INTEGER;
    v_deleted_results INTEGER;
    v_deleted_executions INTEGER;
BEGIN
    -- Delete assertions first (FK constraint)
    DELETE FROM qa_test_assertions
    WHERE result_id IN (
        SELECT result_id
        FROM qa_test_results
        WHERE execution_timestamp < DATEADD(day, -:p_retention_days, CURRENT_DATE())
    );
    v_deleted_assertions := SQLROWCOUNT;

    -- Delete results
    DELETE FROM qa_test_results
    WHERE execution_timestamp < DATEADD(day, -:p_retention_days, CURRENT_DATE());
    v_deleted_results := SQLROWCOUNT;

    -- Delete executions
    DELETE FROM qa_test_executions
    WHERE execution_timestamp < DATEADD(day, -:p_retention_days, CURRENT_DATE());
    v_deleted_executions := SQLROWCOUNT;

    RETURN 'Cleanup complete. Deleted: ' ||
           v_deleted_executions || ' executions, ' ||
           v_deleted_results || ' results, ' ||
           v_deleted_assertions || ' assertions.';
END;
$$;

-- =====================================================
-- Reporting Procedures
-- =====================================================

-- Generate test summary report
CREATE OR REPLACE PROCEDURE sp_generate_test_summary_report()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_report VARCHAR;
    v_latest_exec_id INTEGER;
    v_total_tests INTEGER;
    v_passed INTEGER;
    v_failed INTEGER;
    v_pass_rate DECIMAL;
BEGIN
    -- Get latest execution
    SELECT execution_id, total_tests, passed_tests, failed_tests
    INTO v_latest_exec_id, v_total_tests, v_passed, v_failed
    FROM qa_test_executions
    ORDER BY execution_timestamp DESC
    LIMIT 1;

    v_pass_rate := ROUND(v_passed * 100.0 / NULLIF(v_total_tests, 0), 2);

    v_report := '
╔════════════════════════════════════════╗
║     QA TEST EXECUTION SUMMARY          ║
╠════════════════════════════════════════╣
║ Execution ID: ' || LPAD(v_latest_exec_id::VARCHAR, 24) || ' ║
║ Total Tests:  ' || LPAD(v_total_tests::VARCHAR, 24) || ' ║
║ Passed:       ' || LPAD(v_passed::VARCHAR, 24) || ' ║
║ Failed:       ' || LPAD(v_failed::VARCHAR, 24) || ' ║
║ Pass Rate:    ' || LPAD(v_pass_rate::VARCHAR || '%', 24) || ' ║
╚════════════════════════════════════════╝
    ';

    RETURN v_report;
END;
$$;

-- =====================================================
-- Initial Setup
-- =====================================================

-- Grant permissions (adjust as needed for your environment)
-- GRANT USAGE ON SCHEMA QA_FRAMEWORK TO ROLE DATA_ENGINEER;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA QA_FRAMEWORK TO ROLE DATA_ENGINEER;
-- GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA QA_FRAMEWORK TO ROLE DATA_ENGINEER;

-- =====================================================
-- Usage Examples (Commented Out)
-- =====================================================

-- Run all tests
-- CALL sp_execute_qa_tests('ALL', NULL);

-- Run only unit tests
-- CALL sp_execute_qa_tests('UNIT', NULL);

-- Run integration tests for specific batch
-- CALL sp_execute_qa_tests('INTEGRATION', 'BATCH_20250117_120000');

-- Run single test by name
-- CALL sp_execute_single_test('ODS_TABLES_EXIST');

-- View latest execution results
-- SELECT * FROM vw_qa_latest_execution;

-- View all failed tests in last 7 days
-- SELECT * FROM vw_qa_failed_tests;

-- View test stability
-- SELECT * FROM vw_qa_test_stability WHERE stability_pct < 100;

-- Generate summary report
-- CALL sp_generate_test_summary_report();

-- Disable a flaky test
-- CALL sp_disable_test('TEST_NAME', 'Test is flaky due to timing issues');

-- Cleanup old test results (keep last 90 days)
-- CALL sp_cleanup_test_results(90);

COMMENT ON SCHEMA QA_FRAMEWORK IS 'Automated testing framework for VES data warehouse quality assurance';
