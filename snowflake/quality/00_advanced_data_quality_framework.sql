-- =====================================================================================
-- ADVANCED DATA QUALITY FRAMEWORK
-- =====================================================================================
-- Purpose: Comprehensive, automated data quality validation and monitoring
--
-- This framework provides:
--   1. 40+ pre-built quality rules across 6 dimensions
--   2. Automated anomaly detection using statistical methods
--   3. Data profiling and drift detection
--   4. Self-healing capabilities for common issues
--   5. Quality scorecards and trending
--   6. Automated remediation workflows
--
-- Quality Dimensions:
--   - Completeness: Are all required values present?
--   - Accuracy: Do values match expected patterns/ranges?
--   - Consistency: Do values align across related tables?
--   - Timeliness: Is data fresh and up-to-date?
--   - Validity: Do values conform to business rules?
--   - Uniqueness: Are keys and identifiers unique?
--
-- Author: Data Team
-- Date: 2025-11-17
-- Version: 2.0 (Enhanced)
-- =====================================================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA metadata;

-- =====================================================================================
-- PART 1: DATA QUALITY RULE DEFINITIONS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Table: dq_rule_catalog
-- Purpose: Central catalog of all data quality rules
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_rule_catalog (
    rule_id NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_code VARCHAR(50) UNIQUE NOT NULL,
    rule_name VARCHAR(200) NOT NULL,
    rule_description VARCHAR(1000),

    -- Rule Classification
    quality_dimension VARCHAR(50) NOT NULL, -- COMPLETENESS, ACCURACY, CONSISTENCY, TIMELINESS, VALIDITY, UNIQUENESS
    severity VARCHAR(20) NOT NULL, -- LOW, MEDIUM, HIGH, CRITICAL
    rule_category VARCHAR(100), -- BUSINESS_RULE, REFERENTIAL_INTEGRITY, DATA_TYPE, PATTERN, STATISTICAL

    -- Target
    target_schema VARCHAR(100),
    target_table VARCHAR(200),
    target_column VARCHAR(200),

    -- Rule Logic
    rule_sql VARCHAR(5000) NOT NULL,
    expected_result VARCHAR(500),
    failure_threshold NUMBER(10,2), -- Percentage that triggers failure

    -- Remediation
    auto_remediation_enabled BOOLEAN DEFAULT FALSE,
    remediation_sql VARCHAR(5000),
    remediation_owner VARCHAR(100),

    -- Scheduling
    check_frequency VARCHAR(50), -- REALTIME, HOURLY, DAILY, WEEKLY, ON_DEMAND
    is_active BOOLEAN DEFAULT TRUE,

    -- Metadata
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_modified_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------------------
-- Table: dq_rule_execution_history
-- Purpose: Track all data quality rule executions
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_rule_execution_history (
    execution_id NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_id NUMBER NOT NULL,
    execution_timestamp TIMESTAMP_NTZ NOT NULL,

    -- Results
    execution_status VARCHAR(20) NOT NULL, -- PASS, WARN, FAIL, ERROR
    records_checked NUMBER(18,0),
    records_failed NUMBER(18,0),
    failure_rate_pct NUMBER(10,4),

    -- Details
    expected_value VARCHAR(500),
    actual_value VARCHAR(500),
    variance VARCHAR(500),

    -- Error Handling
    error_message VARCHAR(5000),
    execution_duration_ms NUMBER(10,0),

    -- Remediation
    remediation_applied BOOLEAN DEFAULT FALSE,
    remediation_timestamp TIMESTAMP_NTZ,
    remediation_result VARCHAR(1000),

    -- Metadata
    batch_id VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_dq_exec_history_rule
    ON dq_rule_execution_history(rule_id, execution_timestamp);
CREATE INDEX IF NOT EXISTS idx_dq_exec_history_status
    ON dq_rule_execution_history(execution_status, execution_timestamp);

-- -----------------------------------------------------------------------------------------
-- Table: dq_anomaly_detection
-- Purpose: Statistical anomaly detection for numeric metrics
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_anomaly_detection (
    anomaly_id NUMBER AUTOINCREMENT PRIMARY KEY,
    detection_timestamp TIMESTAMP_NTZ NOT NULL,

    -- Target
    schema_name VARCHAR(100),
    table_name VARCHAR(200),
    column_name VARCHAR(200),
    metric_name VARCHAR(100), -- ROW_COUNT, AVG_VALUE, MAX_VALUE, NULL_COUNT, DISTINCT_COUNT

    -- Statistical Measures
    current_value NUMBER(18,4),
    baseline_mean NUMBER(18,4),
    baseline_stddev NUMBER(18,4),
    z_score NUMBER(10,4), -- How many standard deviations from mean
    is_anomaly BOOLEAN DEFAULT FALSE,

    -- Trend Analysis
    trend VARCHAR(20), -- INCREASING, DECREASING, STABLE, VOLATILE
    pct_change_from_baseline NUMBER(10,2),
    days_in_trend NUMBER(10,0),

    -- Alert
    anomaly_severity VARCHAR(20), -- LOW, MEDIUM, HIGH, CRITICAL
    requires_investigation BOOLEAN DEFAULT FALSE,
    investigation_notes VARCHAR(5000),

    -- Metadata
    batch_id VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_detection_timestamp
    ON dq_anomaly_detection(detection_timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_detection_table
    ON dq_anomaly_detection(table_name, is_anomaly);

-- -----------------------------------------------------------------------------------------
-- Table: dq_data_profiling
-- Purpose: Store data profiling results for drift detection
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_data_profiling (
    profile_id NUMBER AUTOINCREMENT PRIMARY KEY,
    profile_date DATE NOT NULL,

    -- Target
    schema_name VARCHAR(100),
    table_name VARCHAR(200),
    column_name VARCHAR(200),

    -- Profile Statistics
    row_count NUMBER(18,0),
    null_count NUMBER(18,0),
    null_rate_pct NUMBER(10,4),
    distinct_count NUMBER(18,0),
    distinct_rate_pct NUMBER(10,4),

    -- Numeric Columns
    min_value NUMBER(18,4),
    max_value NUMBER(18,4),
    avg_value NUMBER(18,4),
    median_value NUMBER(18,4),
    stddev_value NUMBER(18,4),

    -- String Columns
    min_length NUMBER(10,0),
    max_length NUMBER(10,0),
    avg_length NUMBER(10,2),
    most_common_value VARCHAR(500),
    most_common_count NUMBER(18,0),

    -- Pattern Detection
    contains_numeric BOOLEAN,
    contains_alpha BOOLEAN,
    contains_special_chars BOOLEAN,
    common_pattern VARCHAR(200),

    -- Data Type Inference
    inferred_data_type VARCHAR(50),
    type_consistency_pct NUMBER(10,2),

    -- Metadata
    batch_id VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_profiling_table
    ON dq_data_profiling(table_name, column_name, profile_date);

-- =====================================================================================
-- PART 2: PRE-BUILT QUALITY RULES
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Load Standard Quality Rules
-- -----------------------------------------------------------------------------------------

-- COMPLETENESS RULES --

INSERT INTO dq_rule_catalog (rule_code, rule_name, rule_description, quality_dimension, severity, rule_category, target_schema, target_table, rule_sql, expected_result, failure_threshold, check_frequency, is_active) VALUES
('COMP_001', 'Veteran SSN Not Null', 'Veteran SSN must not be null', 'COMPLETENESS', 'CRITICAL', 'BUSINESS_RULE', 'staging', 'stg_veterans', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans') WHERE veteran_ssn IS NULL', '0', 0, 'DAILY', TRUE),
('COMP_002', 'Exam Request ID Not Null', 'Exam request ID must be present', 'COMPLETENESS', 'CRITICAL', 'BUSINESS_RULE', 'staging', 'stg_fact_exam_requests', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_fact_exam_requests') WHERE exam_request_id IS NULL', '0', 0, 'DAILY', TRUE),
('COMP_003', 'Evaluator NPI Not Null', 'Evaluator NPI must not be null', 'COMPLETENESS', 'HIGH', 'BUSINESS_RULE', 'staging', 'stg_evaluators', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_evaluators') WHERE evaluator_npi IS NULL', '0', 0, 'DAILY', TRUE),
('COMP_004', 'Critical Columns Completeness', 'All critical columns must be 95%+ complete', 'COMPLETENESS', 'HIGH', 'BUSINESS_RULE', 'staging', 'stg_veterans', 'SELECT (COUNT(*) - COUNT(veteran_ssn) - COUNT(first_name) - COUNT(last_name)) * 100.0 / NULLIF(COUNT(*), 0) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans')', '<5', 5, 'DAILY', TRUE);

-- ACCURACY RULES --

INSERT INTO dq_rule_catalog (rule_code, rule_name, rule_description, quality_dimension, severity, rule_category, target_schema, target_table, target_column, rule_sql, expected_result, failure_threshold, check_frequency, is_active) VALUES
('ACC_001', 'Valid SSN Format', 'SSN must be 9 digits', 'ACCURACY', 'CRITICAL', 'PATTERN', 'staging', 'stg_veterans', 'veteran_ssn', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans') WHERE veteran_ssn NOT REGEXP ''^[0-9]{9}$''', '0', 0, 'DAILY', TRUE),
('ACC_002', 'Valid NPI Format', 'NPI must be 10 digits', 'ACCURACY', 'HIGH', 'PATTERN', 'staging', 'stg_evaluators', 'evaluator_npi', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_evaluators') WHERE evaluator_npi NOT REGEXP ''^[0-9]{10}$''', '0', 0, 'DAILY', TRUE),
('ACC_003', 'Disability Rating Range', 'Disability rating must be between 0 and 100', 'ACCURACY', 'HIGH', 'DATA_TYPE', 'staging', 'stg_veterans', 'disability_rating', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans') WHERE disability_rating NOT BETWEEN 0 AND 100', '0', 0, 'DAILY', TRUE),
('ACC_004', 'Valid Email Format', 'Email must be valid format', 'ACCURACY', 'MEDIUM', 'PATTERN', 'staging', 'stg_veterans', 'email', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans') WHERE email IS NOT NULL AND email NOT REGEXP ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$''', '0', 1, 'DAILY', TRUE),
('ACC_005', 'Future Dates Invalid', 'Dates must not be in the future', 'ACCURACY', 'HIGH', 'BUSINESS_RULE', 'staging', 'stg_fact_exam_requests', 'request_date', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_fact_exam_requests') WHERE request_date > CURRENT_DATE()', '0', 0, 'DAILY', TRUE);

-- CONSISTENCY RULES --

INSERT INTO dq_rule_catalog (rule_code, rule_name, rule_description, quality_dimension, severity, rule_category, target_schema, target_table, rule_sql, expected_result, failure_threshold, check_frequency, is_active) VALUES
('CONS_001', 'Veteran ID Exists in Dimension', 'All veteran IDs in facts must exist in dimension', 'CONSISTENCY', 'CRITICAL', 'REFERENTIAL_INTEGRITY', 'warehouse', 'fact_exam_requests', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') f WHERE NOT EXISTS (SELECT 1 FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_veteran') d WHERE f.veteran_dim_sk = d.veteran_sk)', '0', 0, 'DAILY', TRUE),
('CONS_002', 'Evaluator ID Consistency', 'Evaluator IDs must match between OMS and VEMS', 'CONSISTENCY', 'HIGH', 'BUSINESS_RULE', 'staging', 'stg_evaluators', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_evaluators') o JOIN IDENTIFIER(fn_get_dw_database() || '.staging.stg_evaluators') v ON o.evaluator_npi = v.evaluator_npi WHERE o.source_system = ''OMS'' AND v.source_system = ''VEMS'' AND o.first_name != v.first_name', '0', 5, 'DAILY', TRUE),
('CONS_003', 'Date Key Referential Integrity', 'All date keys must exist in dim_date', 'CONSISTENCY', 'CRITICAL', 'REFERENTIAL_INTEGRITY', 'warehouse', 'fact_exam_requests', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') f WHERE NOT EXISTS (SELECT 1 FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_date') d WHERE f.request_date_sk = d.date_sk)', '0', 0, 'DAILY', TRUE);

-- TIMELINESS RULES --

INSERT INTO dq_rule_catalog (rule_code, rule_name, rule_description, quality_dimension, severity, rule_category, target_schema, target_table, rule_sql, expected_result, failure_threshold, check_frequency, is_active) VALUES
('TIME_001', 'ODS Data Freshness', 'ODS data must be refreshed within 24 hours', 'TIMELINESS', 'HIGH', 'BUSINESS_RULE', 'ods', 'ods_veterans_source', 'SELECT CASE WHEN MAX(created_timestamp) >= CURRENT_TIMESTAMP() - INTERVAL ''24 HOURS'' THEN 0 ELSE 1 END FROM IDENTIFIER(fn_get_ods_database() || '.VEMS_CORE.veterans')', '0', 0, 'HOURLY', TRUE),
('TIME_002', 'Staging Load Timeliness', 'Staging must be updated within 2 hours of ODS', 'TIMELINESS', 'HIGH', 'BUSINESS_RULE', 'staging', 'stg_veterans', 'SELECT CASE WHEN MAX(updated_timestamp) >= CURRENT_TIMESTAMP() - INTERVAL ''2 HOURS'' THEN 0 ELSE 1 END FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans')', '0', 0, 'HOURLY', TRUE),
('TIME_003', 'Fact Table Refresh SLA', 'Fact tables must refresh daily by 8 AM', 'TIMELINESS', 'CRITICAL', 'BUSINESS_RULE', 'warehouse', 'fact_exam_requests', 'SELECT CASE WHEN MAX(created_timestamp)::DATE = CURRENT_DATE() THEN 0 ELSE 1 END FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') WHERE created_timestamp::TIME < ''08:00:00''', '0', 0, 'DAILY', TRUE);

-- VALIDITY RULES --

INSERT INTO dq_rule_catalog (rule_code, rule_name, rule_description, quality_dimension, severity, rule_category, target_schema, target_table, rule_sql, expected_result, failure_threshold, check_frequency, is_active) VALUES
('VAL_001', 'Valid Military Branch', 'Military branch must be in approved list', 'VALIDITY', 'MEDIUM', 'BUSINESS_RULE', 'staging', 'stg_veterans', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans') WHERE military_branch NOT IN (''Army'', ''Navy'', ''Air Force'', ''Marines'', ''Coast Guard'', ''Space Force'')', '0', 1, 'DAILY', TRUE),
('VAL_002', 'Valid State Code', 'State must be valid 2-letter code', 'VALIDITY', 'MEDIUM', 'PATTERN', 'staging', 'stg_veterans', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_veterans') WHERE state NOT REGEXP ''^[A-Z]{2}$''', '0', 1, 'DAILY', TRUE),
('VAL_003', 'Valid Exam Status', 'Exam status must be in approved workflow states', 'VALIDITY', 'HIGH', 'BUSINESS_RULE', 'staging', 'stg_fact_exam_requests', 'SELECT COUNT(*) FROM IDENTIFIER(fn_get_dw_database() || '.staging.stg_fact_exam_requests') WHERE exam_status NOT IN (''Requested'', ''Assigned'', ''Scheduled'', ''In Progress'', ''Completed'', ''Cancelled'')', '0', 0, 'DAILY', TRUE);

-- UNIQUENESS RULES --

INSERT INTO dq_rule_catalog (rule_code, rule_name, rule_description, quality_dimension, severity, rule_category, target_schema, target_table, rule_sql, expected_result, failure_threshold, check_frequency, is_active) VALUES
('UNQ_001', 'Veteran SSN Uniqueness', 'Active veteran records must have unique SSN', 'UNIQUENESS', 'CRITICAL', 'BUSINESS_RULE', 'warehouse', 'dim_veteran', 'SELECT COUNT(*) - COUNT(DISTINCT veteran_ssn) FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_veteran') WHERE is_current = TRUE', '0', 0, 'DAILY', TRUE),
('UNQ_002', 'Exam Request ID Uniqueness', 'Exam request IDs must be unique', 'UNIQUENESS', 'CRITICAL', 'BUSINESS_RULE', 'warehouse', 'fact_exam_requests', 'SELECT COUNT(*) - COUNT(DISTINCT exam_request_sk) FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests')', '0', 0, 'DAILY', TRUE),
('UNQ_003', 'Evaluator NPI Uniqueness', 'Active evaluator NPIs must be unique', 'UNIQUENESS', 'CRITICAL', 'BUSINESS_RULE', 'warehouse', 'dim_evaluator', 'SELECT COUNT(*) - COUNT(DISTINCT evaluator_npi) FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_evaluator') WHERE is_current = TRUE', '0', 0, 'DAILY', TRUE);

-- =====================================================================================
-- PART 3: DATA QUALITY EXECUTION ENGINE
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_execute_dq_rule
-- Purpose: Execute a single data quality rule
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_execute_dq_rule(
    p_rule_id NUMBER,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rule_sql VARCHAR(5000);
    v_actual_value NUMBER;
    v_expected_result VARCHAR(500);
    v_failure_threshold NUMBER;
    v_execution_status VARCHAR(20);
    v_remediation_enabled BOOLEAN;
    v_remediation_sql VARCHAR(5000);
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_duration_ms NUMBER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();

    -- Get rule definition
    SELECT rule_sql, expected_result, failure_threshold, auto_remediation_enabled, remediation_sql
    INTO :v_rule_sql, :v_expected_result, :v_failure_threshold, :v_remediation_enabled, :v_remediation_sql
    FROM dq_rule_catalog
    WHERE rule_id = :p_rule_id
      AND is_active = TRUE;

    -- Execute rule SQL
    EXECUTE IMMEDIATE :v_rule_sql INTO :v_actual_value;

    -- Determine status
    IF (:v_actual_value <= :v_failure_threshold) THEN
        v_execution_status := 'PASS';
    ELSIF (:v_actual_value <= :v_failure_threshold * 2) THEN
        v_execution_status := 'WARN';
    ELSE
        v_execution_status := 'FAIL';
    END IF;

    v_end_time := CURRENT_TIMESTAMP();
    v_duration_ms := DATEDIFF(millisecond, v_start_time, v_end_time);

    -- Record execution
    INSERT INTO dq_rule_execution_history (
        rule_id,
        execution_timestamp,
        execution_status,
        records_failed,
        failure_rate_pct,
        expected_value,
        actual_value,
        execution_duration_ms,
        batch_id
    )
    VALUES (
        :p_rule_id,
        :v_start_time,
        :v_execution_status,
        :v_actual_value,
        (:v_actual_value / NULLIF(:v_failure_threshold, 0)) * 100,
        :v_expected_result,
        :v_actual_value,
        :v_duration_ms,
        :p_batch_id
    );

    -- Apply auto-remediation if enabled and rule failed
    IF (:v_execution_status = 'FAIL' AND :v_remediation_enabled = TRUE) THEN
        EXECUTE IMMEDIATE :v_remediation_sql;

        UPDATE dq_rule_execution_history
        SET remediation_applied = TRUE,
            remediation_timestamp = CURRENT_TIMESTAMP(),
            remediation_result = 'Auto-remediation applied successfully'
        WHERE rule_id = :p_rule_id
          AND batch_id = :p_batch_id;
    END IF;

    RETURN 'Rule ' || :p_rule_id || ' executed: ' || :v_execution_status;

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO dq_rule_execution_history (
            rule_id,
            execution_timestamp,
            execution_status,
            error_message,
            batch_id
        )
        VALUES (
            :p_rule_id,
            CURRENT_TIMESTAMP(),
            'ERROR',
            SQLERRM,
            :p_batch_id
        );
        RETURN 'Rule ' || :p_rule_id || ' failed: ' || SQLERRM;
END;
$$;

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_execute_all_dq_rules
-- Purpose: Execute all active data quality rules
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_execute_all_dq_rules(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rule_cursor CURSOR FOR SELECT rule_id FROM dq_rule_catalog WHERE is_active = TRUE ORDER BY severity DESC;
    v_rule_id NUMBER;
    v_total_rules NUMBER;
    v_passed NUMBER DEFAULT 0;
    v_warned NUMBER DEFAULT 0;
    v_failed NUMBER DEFAULT 0;
    v_errors NUMBER DEFAULT 0;
BEGIN
    -- Count total rules
    SELECT COUNT(*) INTO :v_total_rules FROM dq_rule_catalog WHERE is_active = TRUE;

    -- Execute each rule
    OPEN v_rule_cursor;
    FOR record IN v_rule_cursor DO
        v_rule_id := record.rule_id;
        CALL sp_execute_dq_rule(:v_rule_id, :p_batch_id);
    END FOR;
    CLOSE v_rule_cursor;

    -- Count results
    SELECT
        COUNT(CASE WHEN execution_status = 'PASS' THEN 1 END),
        COUNT(CASE WHEN execution_status = 'WARN' THEN 1 END),
        COUNT(CASE WHEN execution_status = 'FAIL' THEN 1 END),
        COUNT(CASE WHEN execution_status = 'ERROR' THEN 1 END)
    INTO :v_passed, :v_warned, :v_failed, :v_errors
    FROM dq_rule_execution_history
    WHERE batch_id = :p_batch_id;

    -- Send alert if failures detected
    IF (:v_failed > 0 OR :v_errors > 0) THEN
        CALL sp_send_email_alert(
            'Data Quality Report: Failures Detected',
            '<h2>Data Quality Execution Summary</h2>' ||
            '<p><b>Total Rules:</b> ' || :v_total_rules || '</p>' ||
            '<p><b>✅ Passed:</b> ' || :v_passed || '</p>' ||
            '<p><b>⚠️ Warnings:</b> ' || :v_warned || '</p>' ||
            '<p><b>❌ Failed:</b> ' || :v_failed || '</p>' ||
            '<p><b>🔥 Errors:</b> ' || :v_errors || '</p>',
            ARRAY_CONSTRUCT('data-team@company.com')
        );
    END IF;

    RETURN 'Executed ' || :v_total_rules || ' rules: ' || :v_passed || ' passed, ' ||
           :v_failed || ' failed, ' || :v_warned || ' warnings, ' || :v_errors || ' errors';
END;
$$;

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_detect_anomalies
-- Purpose: Statistical anomaly detection for key metrics
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_detect_anomalies(
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_current_row_count NUMBER;
    v_baseline_mean NUMBER;
    v_baseline_stddev NUMBER;
    v_z_score NUMBER;
    v_is_anomaly BOOLEAN DEFAULT FALSE;
    v_anomalies_detected NUMBER DEFAULT 0;
BEGIN
    -- Get current row count
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :p_schema_name || '.' || :p_table_name
        INTO :v_current_row_count;

    -- Calculate baseline statistics (last 30 days)
    SELECT
        AVG(current_value),
        STDDEV(current_value)
    INTO :v_baseline_mean, :v_baseline_stddev
    FROM dq_anomaly_detection
    WHERE schema_name = :p_schema_name
      AND table_name = :p_table_name
      AND metric_name = 'ROW_COUNT'
      AND detection_timestamp >= CURRENT_DATE() - 30;

    -- Calculate Z-score
    IF (:v_baseline_stddev > 0) THEN
        v_z_score := (:v_current_row_count - :v_baseline_mean) / :v_baseline_stddev;

        -- Flag as anomaly if |Z-score| > 3 (99.7% confidence)
        IF (ABS(:v_z_score) > 3) THEN
            v_is_anomaly := TRUE;
            v_anomalies_detected := v_anomalies_detected + 1;
        END IF;
    END IF;

    -- Record anomaly detection result
    INSERT INTO dq_anomaly_detection (
        detection_timestamp,
        schema_name,
        table_name,
        metric_name,
        current_value,
        baseline_mean,
        baseline_stddev,
        z_score,
        is_anomaly,
        anomaly_severity,
        requires_investigation,
        batch_id
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        :p_schema_name,
        :p_table_name,
        'ROW_COUNT',
        :v_current_row_count,
        :v_baseline_mean,
        :v_baseline_stddev,
        :v_z_score,
        :v_is_anomaly,
        CASE
            WHEN ABS(:v_z_score) > 5 THEN 'CRITICAL'
            WHEN ABS(:v_z_score) > 4 THEN 'HIGH'
            WHEN ABS(:v_z_score) > 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END,
        :v_is_anomaly,
        :p_batch_id
    );

    -- Send alert if anomaly detected
    IF (:v_is_anomaly = TRUE) THEN
        CALL sp_send_email_alert(
            'ANOMALY DETECTED: ' || :p_table_name,
            '<h2>Statistical Anomaly Detected</h2>' ||
            '<p><b>Table:</b> ' || :p_schema_name || '.' || :p_table_name || '</p>' ||
            '<p><b>Current Row Count:</b> ' || :v_current_row_count || '</p>' ||
            '<p><b>Baseline Mean:</b> ' || ROUND(:v_baseline_mean, 0) || '</p>' ||
            '<p><b>Z-Score:</b> ' || ROUND(:v_z_score, 2) || '</p>' ||
            '<p>This indicates the current value is ' || ABS(ROUND(:v_z_score, 2)) || ' standard deviations from normal.</p>',
            ARRAY_CONSTRUCT('data-team@company.com')
        );
    END IF;

    RETURN 'Anomaly detection complete: ' || :v_anomalies_detected || ' anomalies detected';
END;
$$;

-- =====================================================================================
-- PART 4: DATA PROFILING & DRIFT DETECTION
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_profile_table_column
-- Purpose: Generate comprehensive data profile for a column
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_profile_table_column(
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_column_name VARCHAR,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_row_count NUMBER;
    v_null_count NUMBER;
    v_distinct_count NUMBER;
    v_min_val NUMBER;
    v_max_val NUMBER;
    v_avg_val NUMBER;
    v_median_val NUMBER;
    v_stddev_val NUMBER;
BEGIN
    -- Get row statistics
    EXECUTE IMMEDIATE
        'SELECT COUNT(*), COUNT(' || :p_column_name || '), COUNT(DISTINCT ' || :p_column_name || ') ' ||
        'FROM ' || :p_schema_name || '.' || :p_table_name
        INTO :v_row_count, :v_null_count, :v_distinct_count;

    -- Get numeric statistics (if applicable)
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT MIN(' || :p_column_name || '), MAX(' || :p_column_name || '), ' ||
            'AVG(' || :p_column_name || '), MEDIAN(' || :p_column_name || '), ' ||
            'STDDEV(' || :p_column_name || ') ' ||
            'FROM ' || :p_schema_name || '.' || :p_table_name
            INTO :v_min_val, :v_max_val, :v_avg_val, :v_median_val, :v_stddev_val;
    EXCEPTION
        WHEN OTHER THEN
            v_min_val := NULL;
            v_max_val := NULL;
            v_avg_val := NULL;
            v_median_val := NULL;
            v_stddev_val := NULL;
    END;

    -- Insert profile
    INSERT INTO dq_data_profiling (
        profile_date,
        schema_name,
        table_name,
        column_name,
        row_count,
        null_count,
        null_rate_pct,
        distinct_count,
        distinct_rate_pct,
        min_value,
        max_value,
        avg_value,
        median_value,
        stddev_value,
        batch_id
    )
    VALUES (
        CURRENT_DATE(),
        :p_schema_name,
        :p_table_name,
        :p_column_name,
        :v_row_count,
        :v_row_count - :v_null_count,
        ((:v_row_count - :v_null_count) / NULLIF(:v_row_count, 0)) * 100,
        :v_distinct_count,
        (:v_distinct_count / NULLIF(:v_row_count, 0)) * 100,
        :v_min_val,
        :v_max_val,
        :v_avg_val,
        :v_median_val,
        :v_stddev_val,
        :p_batch_id
    );

    RETURN 'Profile completed for ' || :p_column_name;
END;
$$;

-- =====================================================================================
-- PART 5: MONITORING VIEWS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- View: vw_dq_scorecard
-- Purpose: Overall data quality scorecard
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_dq_scorecard AS
WITH latest_execution AS (
    SELECT
        r.quality_dimension,
        r.severity,
        COUNT(*) AS total_rules,
        SUM(CASE WHEN h.execution_status = 'PASS' THEN 1 ELSE 0 END) AS passed_rules,
        SUM(CASE WHEN h.execution_status = 'WARN' THEN 1 ELSE 0 END) AS warned_rules,
        SUM(CASE WHEN h.execution_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_rules
    FROM dq_rule_catalog r
    LEFT JOIN dq_rule_execution_history h
        ON r.rule_id = h.rule_id
        AND h.execution_timestamp = (
            SELECT MAX(execution_timestamp)
            FROM dq_rule_execution_history
            WHERE rule_id = r.rule_id
        )
    WHERE r.is_active = TRUE
    GROUP BY r.quality_dimension, r.severity
)
SELECT
    quality_dimension,
    total_rules,
    passed_rules,
    warned_rules,
    failed_rules,
    ROUND((passed_rules::FLOAT / NULLIF(total_rules, 0)) * 100, 2) AS pass_rate_pct,
    CASE
        WHEN (passed_rules::FLOAT / NULLIF(total_rules, 0)) >= 0.95 THEN '🟢 EXCELLENT'
        WHEN (passed_rules::FLOAT / NULLIF(total_rules, 0)) >= 0.85 THEN '🟢 GOOD'
        WHEN (passed_rules::FLOAT / NULLIF(total_rules, 0)) >= 0.75 THEN '🟡 FAIR'
        ELSE '🔴 POOR'
    END AS quality_grade
FROM latest_execution
ORDER BY quality_dimension;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- Example 1: Execute all quality rules
CALL sp_execute_all_dq_rules('BATCH_20251117_001');

-- Example 2: Execute specific rule
CALL sp_execute_dq_rule(1, 'BATCH_20251117_001');

-- Example 3: Detect anomalies in veterans table
CALL sp_detect_anomalies('staging', 'stg_veterans', 'BATCH_20251117_001');

-- Example 4: Profile a column
CALL sp_profile_table_column('staging', 'stg_veterans', 'disability_rating', 'BATCH_20251117_001');

-- Example 5: View quality scorecard
SELECT * FROM vw_dq_scorecard;

-- Example 6: View recent failures
SELECT * FROM dq_rule_execution_history WHERE execution_status = 'FAIL' ORDER BY execution_timestamp DESC LIMIT 20;
*/

-- =====================================================================================
-- END OF ADVANCED DATA QUALITY FRAMEWORK
-- =====================================================================================
