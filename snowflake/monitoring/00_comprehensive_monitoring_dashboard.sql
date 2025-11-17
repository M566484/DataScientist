-- =====================================================================================
-- COMPREHENSIVE MONITORING & OBSERVABILITY DASHBOARD
-- =====================================================================================
-- Purpose: Production-grade monitoring system for VES Data Warehouse
--
-- This script provides:
--   1. Real-time pipeline health monitoring
--   2. Data quality scoring and alerts
--   3. Performance metrics and bottleneck detection
--   4. Cost tracking and optimization opportunities
--   5. SLA compliance monitoring
--   6. Automated alerting and escalation
--
-- Author: Data Team
-- Date: 2025-11-17
-- Version: 2.0 (Enhanced)
-- =====================================================================================

USE DATABASE VESDW_PRD;
USE SCHEMA metadata;

-- =====================================================================================
-- PART 1: CORE MONITORING TABLES
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Table: pipeline_health_metrics
-- Purpose: Real-time health metrics for all ETL pipelines
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_health_metrics (
    metric_id NUMBER AUTOINCREMENT PRIMARY KEY,
    pipeline_name VARCHAR(200) NOT NULL,
    pipeline_type VARCHAR(50) NOT NULL, -- 'ODS_LOAD', 'STAGING', 'DIMENSION', 'FACT', 'MART'
    execution_timestamp TIMESTAMP_NTZ NOT NULL,

    -- Execution Metrics
    execution_status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'RUNNING', 'WARNING'
    execution_duration_seconds NUMBER(10,2),
    records_processed NUMBER(18,0),
    records_inserted NUMBER(18,0),
    records_updated NUMBER(18,0),
    records_deleted NUMBER(18,0),
    records_rejected NUMBER(18,0),

    -- Resource Metrics
    warehouse_name VARCHAR(100),
    warehouse_size VARCHAR(20),
    credits_used NUMBER(10,4),
    bytes_scanned NUMBER(18,0),
    bytes_written NUMBER(18,0),

    -- Quality Metrics
    data_quality_score NUMBER(5,2), -- 0-100
    validation_errors NUMBER(10,0),
    business_rule_violations NUMBER(10,0),

    -- Performance Indicators
    avg_query_time_ms NUMBER(10,2),
    slowest_query_time_ms NUMBER(10,2),
    query_count NUMBER(10,0),

    -- SLA Tracking
    sla_target_minutes NUMBER(10,2),
    sla_actual_minutes NUMBER(10,2),
    sla_compliance_pct NUMBER(5,2),
    sla_breach BOOLEAN DEFAULT FALSE,

    -- Error Details
    error_message VARCHAR(5000),
    error_code VARCHAR(50),
    error_severity VARCHAR(20), -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'

    -- Metadata
    batch_id VARCHAR(100),
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_health_timestamp
    ON pipeline_health_metrics(execution_timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_health_status
    ON pipeline_health_metrics(execution_status, pipeline_name);

-- -----------------------------------------------------------------------------------------
-- Table: data_quality_checks
-- Purpose: Comprehensive data quality validation results
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id NUMBER AUTOINCREMENT PRIMARY KEY,
    check_timestamp TIMESTAMP_NTZ NOT NULL,

    -- Check Definition
    table_name VARCHAR(200) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL, -- 'COMPLETENESS', 'ACCURACY', 'CONSISTENCY', 'TIMELINESS', 'VALIDITY'
    check_name VARCHAR(200) NOT NULL,
    check_description VARCHAR(1000),
    check_sql VARCHAR(5000),

    -- Check Results
    check_status VARCHAR(20) NOT NULL, -- 'PASS', 'WARN', 'FAIL'
    expected_value VARCHAR(500),
    actual_value VARCHAR(500),
    variance_pct NUMBER(10,2),

    -- Impact Assessment
    severity VARCHAR(20) NOT NULL, -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    impact_area VARCHAR(100), -- 'REPORTING', 'OPERATIONS', 'COMPLIANCE', 'FINANCE'
    affected_records NUMBER(18,0),

    -- Quality Score
    quality_dimension_score NUMBER(5,2), -- 0-100 for this dimension
    overall_quality_score NUMBER(5,2), -- 0-100 overall

    -- Remediation
    remediation_required BOOLEAN DEFAULT FALSE,
    remediation_action VARCHAR(1000),
    remediation_owner VARCHAR(100),
    remediation_deadline TIMESTAMP_NTZ,

    -- Metadata
    batch_id VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_dq_checks_timestamp
    ON data_quality_checks(check_timestamp);
CREATE INDEX IF NOT EXISTS idx_dq_checks_table
    ON data_quality_checks(table_name, check_status);

-- -----------------------------------------------------------------------------------------
-- Table: performance_metrics
-- Purpose: Detailed performance tracking for queries and operations
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS performance_metrics (
    metric_id NUMBER AUTOINCREMENT PRIMARY KEY,
    metric_timestamp TIMESTAMP_NTZ NOT NULL,

    -- Operation Details
    operation_type VARCHAR(50) NOT NULL, -- 'LOAD', 'TRANSFORM', 'AGGREGATE', 'EXPORT'
    operation_name VARCHAR(200) NOT NULL,
    object_name VARCHAR(200), -- Table/View name

    -- Performance Metrics
    execution_time_ms NUMBER(18,0),
    rows_scanned NUMBER(18,0),
    rows_returned NUMBER(18,0),
    bytes_scanned NUMBER(18,0),
    partitions_scanned NUMBER(10,0),
    partitions_total NUMBER(10,0),

    -- Resource Usage
    warehouse_name VARCHAR(100),
    warehouse_size VARCHAR(20),
    credits_used NUMBER(10,6),
    spillage_to_local_storage_bytes NUMBER(18,0),
    spillage_to_remote_storage_bytes NUMBER(18,0),

    -- Query Optimization Flags
    full_table_scan BOOLEAN DEFAULT FALSE,
    missing_statistics BOOLEAN DEFAULT FALSE,
    missing_clustering BOOLEAN DEFAULT FALSE,
    cache_hit BOOLEAN DEFAULT FALSE,

    -- Recommendations
    optimization_opportunity VARCHAR(1000),
    estimated_savings_pct NUMBER(5,2),

    -- Metadata
    query_id VARCHAR(100),
    session_id VARCHAR(100),
    user_name VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_perf_metrics_timestamp
    ON performance_metrics(metric_timestamp);
CREATE INDEX IF NOT EXISTS idx_perf_metrics_operation
    ON performance_metrics(operation_name, execution_time_ms);

-- -----------------------------------------------------------------------------------------
-- Table: cost_tracking
-- Purpose: Granular cost tracking and allocation
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cost_tracking (
    cost_id NUMBER AUTOINCREMENT PRIMARY KEY,
    cost_date DATE NOT NULL,

    -- Cost Breakdown
    warehouse_name VARCHAR(100) NOT NULL,
    cost_center VARCHAR(100), -- 'ODS', 'STAGING', 'WAREHOUSE', 'ANALYTICS', 'ADHOC'
    operation_type VARCHAR(50),

    -- Usage Metrics
    credits_used NUMBER(10,4),
    compute_cost_usd NUMBER(10,2),
    storage_cost_usd NUMBER(10,2),
    data_transfer_cost_usd NUMBER(10,2),
    total_cost_usd NUMBER(10,2),

    -- Storage Details
    storage_bytes NUMBER(18,0),
    storage_tb NUMBER(10,4),
    failsafe_bytes NUMBER(18,0),
    time_travel_bytes NUMBER(18,0),

    -- Optimization Opportunities
    idle_time_minutes NUMBER(10,2),
    idle_cost_usd NUMBER(10,2),
    cache_spillage_bytes NUMBER(18,0),
    potential_savings_usd NUMBER(10,2),

    -- Budget Tracking
    budget_allocated_usd NUMBER(10,2),
    budget_consumed_pct NUMBER(5,2),
    budget_remaining_usd NUMBER(10,2),
    budget_forecast_end_of_month_usd NUMBER(10,2),

    -- Metadata
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_cost_tracking_date
    ON cost_tracking(cost_date);
CREATE INDEX IF NOT EXISTS idx_cost_tracking_warehouse
    ON cost_tracking(warehouse_name, cost_date);

-- -----------------------------------------------------------------------------------------
-- Table: sla_compliance_tracking
-- Purpose: Track SLA compliance across all critical pipelines
-- -----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sla_compliance_tracking (
    sla_id NUMBER AUTOINCREMENT PRIMARY KEY,
    sla_date DATE NOT NULL,

    -- SLA Definition
    pipeline_name VARCHAR(200) NOT NULL,
    sla_type VARCHAR(50) NOT NULL, -- 'DATA_FRESHNESS', 'PROCESSING_TIME', 'AVAILABILITY', 'QUALITY'
    sla_description VARCHAR(1000),

    -- SLA Targets
    target_value NUMBER(10,2),
    target_unit VARCHAR(20), -- 'MINUTES', 'HOURS', 'RECORDS', 'PERCENTAGE'
    warning_threshold NUMBER(10,2),
    critical_threshold NUMBER(10,2),

    -- Actual Performance
    actual_value NUMBER(10,2),
    compliance_status VARCHAR(20), -- 'MET', 'WARNING', 'BREACHED'
    variance_from_target NUMBER(10,2),
    variance_pct NUMBER(5,2),

    -- Impact
    business_impact VARCHAR(20), -- 'NONE', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    affected_users NUMBER(10,0),
    affected_reports NUMBER(10,0),

    -- Resolution
    breach_acknowledged BOOLEAN DEFAULT FALSE,
    root_cause VARCHAR(1000),
    corrective_action VARCHAR(1000),
    prevented_future_breach BOOLEAN DEFAULT FALSE,

    -- Metadata
    batch_id VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_sla_compliance_date
    ON sla_compliance_tracking(sla_date);
CREATE INDEX IF NOT EXISTS idx_sla_compliance_status
    ON sla_compliance_tracking(compliance_status, pipeline_name);

-- =====================================================================================
-- PART 2: MONITORING STORED PROCEDURES
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_record_pipeline_health
-- Purpose: Record pipeline execution metrics
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_record_pipeline_health(
    p_pipeline_name VARCHAR,
    p_pipeline_type VARCHAR,
    p_status VARCHAR,
    p_duration_seconds NUMBER,
    p_records_processed NUMBER,
    p_warehouse_name VARCHAR,
    p_credits_used NUMBER,
    p_dq_score NUMBER,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_sla_breach BOOLEAN DEFAULT FALSE;
    v_sla_target NUMBER;
BEGIN
    -- Determine SLA target based on pipeline type
    SELECT CASE :p_pipeline_type
        WHEN 'ODS_LOAD' THEN 30
        WHEN 'STAGING' THEN 45
        WHEN 'DIMENSION' THEN 15
        WHEN 'FACT' THEN 60
        WHEN 'MART' THEN 30
        ELSE 60
    END INTO :v_sla_target;

    -- Check if SLA was breached
    IF (:p_duration_seconds / 60 > :v_sla_target) THEN
        v_sla_breach := TRUE;
    END IF;

    -- Insert metrics
    INSERT INTO pipeline_health_metrics (
        pipeline_name,
        pipeline_type,
        execution_timestamp,
        execution_status,
        execution_duration_seconds,
        records_processed,
        warehouse_name,
        credits_used,
        data_quality_score,
        sla_target_minutes,
        sla_actual_minutes,
        sla_breach,
        batch_id
    )
    VALUES (
        :p_pipeline_name,
        :p_pipeline_type,
        CURRENT_TIMESTAMP(),
        :p_status,
        :p_duration_seconds,
        :p_records_processed,
        :p_warehouse_name,
        :p_credits_used,
        :p_dq_score,
        :v_sla_target,
        :p_duration_seconds / 60,
        :v_sla_breach,
        :p_batch_id
    );

    -- Send alert if SLA breached or pipeline failed
    IF (:v_sla_breach = TRUE OR :p_status = 'FAILED') THEN
        CALL sp_send_email_alert(
            'Pipeline Alert: ' || :p_pipeline_name,
            '<h2>Pipeline ' || :p_status || '</h2>' ||
            '<p><b>Pipeline:</b> ' || :p_pipeline_name || '</p>' ||
            '<p><b>Duration:</b> ' || ROUND(:p_duration_seconds / 60, 2) || ' minutes (Target: ' || :v_sla_target || ' min)</p>' ||
            '<p><b>Records:</b> ' || :p_records_processed || '</p>' ||
            '<p><b>DQ Score:</b> ' || :p_dq_score || '</p>',
            ARRAY_CONSTRUCT('data-team@company.com')
        );
    END IF;

    RETURN 'Pipeline health recorded: ' || :p_pipeline_name;
END;
$$;

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_run_data_quality_checks
-- Purpose: Execute comprehensive data quality validation
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_run_data_quality_checks(
    p_table_name VARCHAR,
    p_schema_name VARCHAR,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_completeness_score NUMBER;
    v_accuracy_score NUMBER;
    v_overall_score NUMBER;
    v_critical_failures NUMBER;
    v_row_count NUMBER;
    v_null_count NUMBER;
    v_duplicate_count NUMBER;
BEGIN
    -- Get basic table statistics
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :p_schema_name || '.' || :p_table_name
        INTO :v_row_count;

    -- Check 1: Completeness - NULL values in critical columns
    -- (This is a template - customize for each table's critical columns)
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :p_schema_name || '.' || :p_table_name ||
        ' WHERE created_timestamp IS NULL OR batch_id IS NULL'
        INTO :v_null_count;

    v_completeness_score := CASE
        WHEN :v_row_count = 0 THEN 0
        ELSE ((v_row_count - v_null_count) / v_row_count) * 100
    END;

    INSERT INTO data_quality_checks (
        check_timestamp,
        table_name,
        schema_name,
        check_type,
        check_name,
        check_status,
        expected_value,
        actual_value,
        severity,
        quality_dimension_score,
        batch_id
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        :p_table_name,
        :p_schema_name,
        'COMPLETENESS',
        'Critical Columns Null Check',
        CASE WHEN :v_completeness_score >= 95 THEN 'PASS'
             WHEN :v_completeness_score >= 90 THEN 'WARN'
             ELSE 'FAIL' END,
        '100%',
        ROUND(:v_completeness_score, 2) || '%',
        CASE WHEN :v_completeness_score < 90 THEN 'HIGH' ELSE 'LOW' END,
        :v_completeness_score,
        :p_batch_id
    );

    -- Check 2: Accuracy - Data type validation
    -- Check 3: Consistency - Referential integrity
    -- Check 4: Timeliness - Data freshness
    -- Check 5: Validity - Business rule validation
    -- ... Additional checks based on table-specific requirements

    -- Calculate overall score
    SELECT AVG(quality_dimension_score)
    INTO :v_overall_score
    FROM data_quality_checks
    WHERE table_name = :p_table_name
      AND batch_id = :p_batch_id;

    -- Update overall score
    UPDATE data_quality_checks
    SET overall_quality_score = :v_overall_score
    WHERE table_name = :p_table_name
      AND batch_id = :p_batch_id;

    -- Check for critical failures
    SELECT COUNT(*)
    INTO :v_critical_failures
    FROM data_quality_checks
    WHERE table_name = :p_table_name
      AND batch_id = :p_batch_id
      AND check_status = 'FAIL'
      AND severity = 'CRITICAL';

    -- Send alert if critical failures detected
    IF (:v_critical_failures > 0) THEN
        CALL sp_send_email_alert(
            'CRITICAL: Data Quality Failures - ' || :p_table_name,
            '<h2>Critical Data Quality Failures Detected</h2>' ||
            '<p><b>Table:</b> ' || :p_schema_name || '.' || :p_table_name || '</p>' ||
            '<p><b>Critical Failures:</b> ' || :v_critical_failures || '</p>' ||
            '<p><b>Overall Quality Score:</b> ' || ROUND(:v_overall_score, 2) || '</p>',
            ARRAY_CONSTRUCT('data-team@company.com', 'ops-team@company.com')
        );
    END IF;

    RETURN 'Data quality checks completed: ' || :p_table_name ||
           ' (Score: ' || ROUND(:v_overall_score, 2) || ')';
END;
$$;

-- =====================================================================================
-- PART 3: MONITORING VIEWS & DASHBOARDS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- View: vw_pipeline_health_dashboard
-- Purpose: Real-time pipeline health overview
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_pipeline_health_dashboard AS
WITH latest_runs AS (
    SELECT
        pipeline_name,
        pipeline_type,
        MAX(execution_timestamp) AS last_execution
    FROM pipeline_health_metrics
    WHERE execution_timestamp >= CURRENT_DATE() - 7
    GROUP BY pipeline_name, pipeline_type
),
pipeline_stats AS (
    SELECT
        phm.pipeline_name,
        phm.pipeline_type,
        phm.execution_status,
        phm.execution_timestamp AS last_run_time,
        phm.execution_duration_seconds,
        phm.records_processed,
        phm.data_quality_score,
        phm.sla_breach,
        phm.credits_used,

        -- Success rate last 7 days
        (SELECT COUNT(*)
         FROM pipeline_health_metrics p
         WHERE p.pipeline_name = phm.pipeline_name
           AND p.execution_status = 'SUCCESS'
           AND p.execution_timestamp >= CURRENT_DATE() - 7) AS success_count,

        (SELECT COUNT(*)
         FROM pipeline_health_metrics p
         WHERE p.pipeline_name = phm.pipeline_name
           AND p.execution_timestamp >= CURRENT_DATE() - 7) AS total_runs,

        -- Average duration
        (SELECT AVG(execution_duration_seconds)
         FROM pipeline_health_metrics p
         WHERE p.pipeline_name = phm.pipeline_name
           AND p.execution_timestamp >= CURRENT_DATE() - 7) AS avg_duration_seconds,

        -- SLA breach count
        (SELECT COUNT(*)
         FROM pipeline_health_metrics p
         WHERE p.pipeline_name = phm.pipeline_name
           AND p.sla_breach = TRUE
           AND p.execution_timestamp >= CURRENT_DATE() - 7) AS sla_breach_count

    FROM pipeline_health_metrics phm
    INNER JOIN latest_runs lr
        ON phm.pipeline_name = lr.pipeline_name
        AND phm.execution_timestamp = lr.last_execution
)
SELECT
    pipeline_name,
    pipeline_type,
    execution_status,
    last_run_time,
    ROUND(execution_duration_seconds / 60, 2) AS duration_minutes,
    records_processed,
    data_quality_score,

    -- Health Indicators
    CASE
        WHEN execution_status = 'FAILED' THEN '🔴 CRITICAL'
        WHEN sla_breach = TRUE THEN '🟡 WARNING'
        WHEN data_quality_score < 90 THEN '🟡 WARNING'
        ELSE '🟢 HEALTHY'
    END AS health_status,

    -- Performance Metrics
    ROUND((success_count::FLOAT / NULLIF(total_runs, 0)) * 100, 2) AS success_rate_pct,
    ROUND(avg_duration_seconds / 60, 2) AS avg_duration_minutes,
    sla_breach_count,

    -- Cost
    ROUND(credits_used, 4) AS credits_last_run,

    -- Time Since Last Run
    DATEDIFF(minute, last_run_time, CURRENT_TIMESTAMP()) AS minutes_since_last_run,

    -- Alert Flag
    CASE
        WHEN execution_status = 'FAILED' THEN TRUE
        WHEN sla_breach = TRUE THEN TRUE
        WHEN data_quality_score < 85 THEN TRUE
        WHEN DATEDIFF(hour, last_run_time, CURRENT_TIMESTAMP()) > 25 THEN TRUE
        ELSE FALSE
    END AS requires_attention

FROM pipeline_stats
ORDER BY
    CASE health_status
        WHEN '🔴 CRITICAL' THEN 1
        WHEN '🟡 WARNING' THEN 2
        ELSE 3
    END,
    pipeline_name;

-- -----------------------------------------------------------------------------------------
-- View: vw_data_quality_summary
-- Purpose: Data quality scorecard by table
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_data_quality_summary AS
WITH latest_checks AS (
    SELECT
        table_name,
        check_type,
        MAX(check_timestamp) AS latest_check_time
    FROM data_quality_checks
    WHERE check_timestamp >= CURRENT_DATE() - 1
    GROUP BY table_name, check_type
),
quality_scores AS (
    SELECT
        dqc.table_name,
        dqc.check_type,
        AVG(dqc.quality_dimension_score) AS dimension_score,
        COUNT(CASE WHEN dqc.check_status = 'FAIL' THEN 1 END) AS failed_checks,
        COUNT(CASE WHEN dqc.check_status = 'WARN' THEN 1 END) AS warning_checks,
        COUNT(*) AS total_checks
    FROM data_quality_checks dqc
    INNER JOIN latest_checks lc
        ON dqc.table_name = lc.table_name
        AND dqc.check_type = lc.check_type
        AND dqc.check_timestamp = lc.latest_check_time
    GROUP BY dqc.table_name, dqc.check_type
)
SELECT
    table_name,
    ROUND(AVG(dimension_score), 2) AS overall_quality_score,

    -- Dimension Scores
    MAX(CASE WHEN check_type = 'COMPLETENESS' THEN dimension_score END) AS completeness_score,
    MAX(CASE WHEN check_type = 'ACCURACY' THEN dimension_score END) AS accuracy_score,
    MAX(CASE WHEN check_type = 'CONSISTENCY' THEN dimension_score END) AS consistency_score,
    MAX(CASE WHEN check_type = 'TIMELINESS' THEN dimension_score END) AS timeliness_score,
    MAX(CASE WHEN check_type = 'VALIDITY' THEN dimension_score END) AS validity_score,

    -- Check Results
    SUM(failed_checks) AS total_failures,
    SUM(warning_checks) AS total_warnings,
    SUM(total_checks) AS total_checks_run,

    -- Quality Grade
    CASE
        WHEN AVG(dimension_score) >= 95 THEN 'A - Excellent'
        WHEN AVG(dimension_score) >= 85 THEN 'B - Good'
        WHEN AVG(dimension_score) >= 75 THEN 'C - Fair'
        WHEN AVG(dimension_score) >= 65 THEN 'D - Poor'
        ELSE 'F - Critical'
    END AS quality_grade,

    -- Status
    CASE
        WHEN SUM(failed_checks) > 0 THEN '🔴 ACTION REQUIRED'
        WHEN SUM(warning_checks) > 0 THEN '🟡 REVIEW NEEDED'
        ELSE '🟢 PASSING'
    END AS quality_status

FROM quality_scores
GROUP BY table_name
ORDER BY overall_quality_score ASC, total_failures DESC;

-- -----------------------------------------------------------------------------------------
-- View: vw_cost_optimization_opportunities
-- Purpose: Identify cost saving opportunities
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_cost_optimization_opportunities AS
WITH daily_costs AS (
    SELECT
        cost_date,
        warehouse_name,
        SUM(total_cost_usd) AS daily_cost,
        SUM(idle_cost_usd) AS daily_idle_cost,
        SUM(credits_used) AS daily_credits
    FROM cost_tracking
    WHERE cost_date >= CURRENT_DATE() - 30
    GROUP BY cost_date, warehouse_name
),
warehouse_analysis AS (
    SELECT
        warehouse_name,
        AVG(daily_cost) AS avg_daily_cost,
        AVG(daily_idle_cost) AS avg_daily_idle_cost,
        SUM(daily_cost) AS total_30day_cost,
        SUM(daily_idle_cost) AS total_30day_idle_cost,
        AVG(daily_credits) AS avg_daily_credits
    FROM daily_costs
    GROUP BY warehouse_name
)
SELECT
    warehouse_name,
    ROUND(total_30day_cost, 2) AS cost_last_30_days_usd,
    ROUND(avg_daily_cost, 2) AS avg_daily_cost_usd,
    ROUND(total_30day_idle_cost, 2) AS idle_cost_last_30_days_usd,
    ROUND((total_30day_idle_cost / NULLIF(total_30day_cost, 0)) * 100, 2) AS idle_cost_pct,

    -- Optimization Opportunities
    CASE
        WHEN (total_30day_idle_cost / NULLIF(total_30day_cost, 0)) > 0.20
        THEN 'Reduce auto-suspend timeout'
        WHEN avg_daily_credits > 100
        THEN 'Consider using smaller warehouse'
        WHEN warehouse_name LIKE '%LARGE%' AND avg_daily_credits < 50
        THEN 'Warehouse oversized - downsize recommended'
        ELSE 'Well optimized'
    END AS optimization_recommendation,

    -- Potential Savings
    ROUND(total_30day_idle_cost * 12, 2) AS potential_annual_savings_usd,

    -- Priority
    CASE
        WHEN total_30day_idle_cost > 100 THEN '🔴 HIGH PRIORITY'
        WHEN total_30day_idle_cost > 50 THEN '🟡 MEDIUM PRIORITY'
        ELSE '🟢 LOW PRIORITY'
    END AS optimization_priority

FROM warehouse_analysis
ORDER BY total_30day_idle_cost DESC;

-- -----------------------------------------------------------------------------------------
-- View: vw_sla_compliance_dashboard
-- Purpose: SLA compliance tracking and trending
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_sla_compliance_dashboard AS
WITH sla_trend AS (
    SELECT
        pipeline_name,
        sla_type,
        sla_date,
        compliance_status,
        actual_value,
        target_value,
        LAG(compliance_status) OVER (PARTITION BY pipeline_name, sla_type ORDER BY sla_date) AS prev_status
    FROM sla_compliance_tracking
    WHERE sla_date >= CURRENT_DATE() - 30
),
sla_summary AS (
    SELECT
        pipeline_name,
        sla_type,
        COUNT(*) AS total_measurements,
        SUM(CASE WHEN compliance_status = 'MET' THEN 1 ELSE 0 END) AS compliant_count,
        SUM(CASE WHEN compliance_status = 'BREACHED' THEN 1 ELSE 0 END) AS breach_count,
        AVG(actual_value) AS avg_actual_value,
        MAX(target_value) AS target_value,
        MAX(sla_date) AS last_measured_date
    FROM sla_compliance_tracking
    WHERE sla_date >= CURRENT_DATE() - 30
    GROUP BY pipeline_name, sla_type
)
SELECT
    pipeline_name,
    sla_type,
    total_measurements,
    compliant_count,
    breach_count,
    ROUND((compliant_count::FLOAT / NULLIF(total_measurements, 0)) * 100, 2) AS compliance_rate_pct,
    ROUND(avg_actual_value, 2) AS avg_actual_value,
    target_value,
    last_measured_date,

    -- Compliance Status
    CASE
        WHEN (compliant_count::FLOAT / NULLIF(total_measurements, 0)) >= 0.99 THEN '🟢 EXCELLENT'
        WHEN (compliant_count::FLOAT / NULLIF(total_measurements, 0)) >= 0.95 THEN '🟢 GOOD'
        WHEN (compliant_count::FLOAT / NULLIF(total_measurements, 0)) >= 0.90 THEN '🟡 NEEDS IMPROVEMENT'
        ELSE '🔴 CRITICAL'
    END AS sla_health,

    -- Trend
    CASE
        WHEN breach_count = 0 THEN 'Stable - No Breaches'
        WHEN breach_count = 1 THEN 'Stable - Single Breach'
        WHEN breach_count > 1 THEN 'Deteriorating - Multiple Breaches'
        ELSE 'Unknown'
    END AS trend,

    DATEDIFF(day, last_measured_date, CURRENT_DATE()) AS days_since_last_check

FROM sla_summary
ORDER BY compliance_rate_pct ASC, breach_count DESC;

-- =====================================================================================
-- PART 4: AUTOMATED MONITORING TASKS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Task: task_hourly_health_check
-- Purpose: Hourly health check of all critical pipelines
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE TASK task_hourly_health_check
    WAREHOUSE = etl_task_wh
    SCHEDULE = '60 MINUTE'
AS
DECLARE
    v_critical_issues NUMBER;
    v_warning_issues NUMBER;
BEGIN
    -- Count critical issues
    SELECT COUNT(*)
    INTO :v_critical_issues
    FROM vw_pipeline_health_dashboard
    WHERE health_status = '🔴 CRITICAL';

    -- Count warning issues
    SELECT COUNT(*)
    INTO :v_warning_issues
    FROM vw_pipeline_health_dashboard
    WHERE health_status = '🟡 WARNING';

    -- Send alert if critical issues found
    IF (:v_critical_issues > 0) THEN
        CALL sp_send_email_alert(
            'CRITICAL: ' || :v_critical_issues || ' Pipeline(s) in Critical Status',
            '<h2>Critical Pipeline Health Alert</h2>' ||
            '<p><b>Critical Pipelines:</b> ' || :v_critical_issues || '</p>' ||
            '<p><b>Warning Pipelines:</b> ' || :v_warning_issues || '</p>' ||
            '<p>Check vw_pipeline_health_dashboard for details.</p>',
            ARRAY_CONSTRUCT('data-team@company.com', 'ops-team@company.com')
        );
    END IF;
END;

-- -----------------------------------------------------------------------------------------
-- Task: task_daily_quality_report
-- Purpose: Daily data quality summary report
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE TASK task_daily_quality_report
    WAREHOUSE = etl_task_wh
    SCHEDULE = 'USING CRON 0 7 * * * America/New_York' -- 7 AM EST daily
AS
DECLARE
    v_failing_tables NUMBER;
    v_avg_quality_score NUMBER;
BEGIN
    -- Count failing tables
    SELECT
        COUNT(*),
        AVG(overall_quality_score)
    INTO :v_failing_tables, :v_avg_quality_score
    FROM vw_data_quality_summary
    WHERE quality_status = '🔴 ACTION REQUIRED';

    -- Send daily quality report
    CALL sp_send_email_alert(
        'Daily Data Quality Report - ' || TO_VARCHAR(CURRENT_DATE()),
        '<h2>Data Quality Summary</h2>' ||
        '<p><b>Average Quality Score:</b> ' || ROUND(:v_avg_quality_score, 2) || '</p>' ||
        '<p><b>Tables Requiring Action:</b> ' || :v_failing_tables || '</p>' ||
        '<p>Review vw_data_quality_summary for full details.</p>',
        ARRAY_CONSTRUCT('data-team@company.com')
    );
END;

-- Resume monitoring tasks
ALTER TASK task_daily_quality_report RESUME;
ALTER TASK task_hourly_health_check RESUME;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- Example 1: Check pipeline health
SELECT * FROM vw_pipeline_health_dashboard WHERE health_status != '🟢 HEALTHY';

-- Example 2: Review data quality issues
SELECT * FROM vw_data_quality_summary WHERE quality_grade IN ('D - Poor', 'F - Critical');

-- Example 3: Identify cost savings
SELECT * FROM vw_cost_optimization_opportunities WHERE optimization_priority = '🔴 HIGH PRIORITY';

-- Example 4: Check SLA compliance
SELECT * FROM vw_sla_compliance_dashboard WHERE sla_health LIKE '%CRITICAL%';

-- Example 5: Record pipeline execution
CALL sp_record_pipeline_health(
    'daily_veteran_load',
    'ODS_LOAD',
    'SUCCESS',
    125.5,
    15000,
    'ETL_WH',
    2.5,
    98.5,
    'BATCH_20251117_001'
);

-- Example 6: Run data quality checks
CALL sp_run_data_quality_checks('stg_veterans', 'staging', 'BATCH_20251117_001');
*/

-- =====================================================================================
-- END OF COMPREHENSIVE MONITORING DASHBOARD
-- =====================================================================================
