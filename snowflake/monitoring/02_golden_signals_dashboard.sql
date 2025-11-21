-- =====================================================================================
-- GOLDEN SIGNALS MONITORING DASHBOARD
-- =====================================================================================
-- Purpose: Simplified monitoring focused on 5 critical metrics
-- Replaces dozens of monitoring views with ONE essential dashboard
--
-- The 5 Golden Signals:
-- 1. LATENCY    - How long does the pipeline take?
-- 2. ERRORS     - How many failures in the last 24 hours?
-- 3. QUALITY    - What's the average data quality score?
-- 4. COST       - What's the credit burn rate?
-- 5. FRESHNESS  - When was the last successful load?
--
-- Benefits:
-- - Single query shows complete system health (< 5 seconds)
-- - Reduces monitoring overhead from 15+ views to 1
-- - Clear thresholds for alerting (red/yellow/green)
-- - Perfect for morning health checks
--
-- Author: Data Team
-- Date: 2025-11-21
-- =====================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA metadata;

-- =====================================================================================
-- PIPELINE EXECUTION HISTORY TABLE (Track all pipeline runs)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS pipeline_execution_history (
    execution_id INTEGER AUTOINCREMENT PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_start_time TIMESTAMP_NTZ NOT NULL,
    execution_end_time TIMESTAMP_NTZ,
    execution_status VARCHAR(20), -- RUNNING, SUCCEEDED, FAILED, CANCELLED
    duration_minutes NUMBER(10,2),
    records_processed INTEGER,
    records_failed INTEGER,
    error_message VARCHAR(5000),
    batch_id VARCHAR(50),
    triggered_by VARCHAR(100) DEFAULT CURRENT_USER()
);

-- =====================================================================================
-- DATA QUALITY SCORES TABLE (Track DQ scores over time)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS data_quality_scores (
    dq_check_id INTEGER AUTOINCREMENT PRIMARY KEY,
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name VARCHAR(200) NOT NULL,
    dq_score NUMBER(5,2), -- 0-100
    total_records INTEGER,
    records_with_issues INTEGER,
    critical_issues INTEGER,
    warning_issues INTEGER,
    check_details VARIANT -- JSON with detailed breakdown
);

-- =====================================================================================
-- GOLDEN SIGNALS VIEW - The ONE dashboard to rule them all
-- =====================================================================================

CREATE OR REPLACE VIEW vw_golden_signals_dashboard AS
WITH
-- Signal 1: LATENCY (Pipeline Performance)
latency_signal AS (
    SELECT
        AVG(duration_minutes) AS avg_duration_minutes,
        MAX(duration_minutes) AS max_duration_minutes,
        MIN(duration_minutes) AS min_duration_minutes,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_minutes) AS p95_duration_minutes,
        COUNT(*) AS total_runs_last_7days
    FROM pipeline_execution_history
    WHERE execution_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
      AND execution_status = 'SUCCEEDED'
),

-- Signal 2: ERRORS (Failure Rate)
error_signal AS (
    SELECT
        COUNT(*) AS total_errors_24h,
        COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) AS failed_pipelines_24h,
        COUNT(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 END) AS succeeded_pipelines_24h,
        ROUND(
            COUNT(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 END) * 100.0 /
            NULLIF(COUNT(*), 0),
            2
        ) AS success_rate_pct_24h
    FROM pipeline_execution_history
    WHERE execution_start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
),

-- Signal 3: QUALITY (Data Quality Score)
quality_signal AS (
    SELECT
        AVG(dq_score) AS avg_dq_score_24h,
        MIN(dq_score) AS min_dq_score_24h,
        SUM(critical_issues) AS total_critical_issues_24h,
        SUM(warning_issues) AS total_warning_issues_24h,
        COUNT(DISTINCT table_name) AS tables_checked_24h
    FROM data_quality_scores
    WHERE check_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
),

-- Signal 4: COST (Credit Usage)
cost_signal AS (
    SELECT
        COALESCE(SUM(credits_used), 0) AS credits_used_24h,
        COALESCE(SUM(credits_used), 0) * 30 AS projected_monthly_credits,
        COALESCE(AVG(credits_used), 0) AS avg_hourly_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
),

-- Signal 5: FRESHNESS (Data Recency)
freshness_signal AS (
    SELECT
        MAX(execution_end_time) AS last_successful_pipeline_run,
        DATEDIFF(hour, MAX(execution_end_time), CURRENT_TIMESTAMP()) AS hours_since_last_run,
        MAX(pipeline_name) AS last_pipeline_name
    FROM pipeline_execution_history
    WHERE execution_status = 'SUCCEEDED'
),

-- Configuration thresholds from system_configuration
thresholds AS (
    SELECT
        fn_get_config_number('sla', 'pipeline_max_duration_hours') AS max_duration_hours,
        fn_get_config_number('sla', 'min_success_rate_percentage') AS min_success_rate,
        fn_get_config_number('quality', 'min_dq_score_critical') AS min_dq_score,
        fn_get_config_number('cost', 'daily_credit_limit') AS daily_credit_limit,
        fn_get_config_number('sla', 'data_freshness_max_hours') AS max_freshness_hours
)

-- Combine all signals with health status
SELECT
    CURRENT_TIMESTAMP() AS dashboard_refresh_time,

    -- ==================== SIGNAL 1: LATENCY ====================
    '1️⃣ LATENCY' AS signal_1,
    ROUND(l.avg_duration_minutes, 2) AS latency_avg_minutes,
    ROUND(l.p95_duration_minutes, 2) AS latency_p95_minutes,
    CASE
        WHEN l.avg_duration_minutes <= (t.max_duration_hours * 60 * 0.5) THEN '🟢 GOOD'
        WHEN l.avg_duration_minutes <= (t.max_duration_hours * 60 * 0.8) THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END AS latency_status,
    CONCAT(
        'Avg: ', ROUND(l.avg_duration_minutes, 1), ' min | ',
        'P95: ', ROUND(l.p95_duration_minutes, 1), ' min | ',
        'Threshold: ', (t.max_duration_hours * 60), ' min'
    ) AS latency_details,

    -- ==================== SIGNAL 2: ERRORS ====================
    '2️⃣ ERRORS' AS signal_2,
    e.total_errors_24h AS errors_total_24h,
    e.failed_pipelines_24h AS errors_failed_24h,
    e.success_rate_pct_24h AS errors_success_rate_pct,
    CASE
        WHEN e.success_rate_pct_24h >= t.min_success_rate THEN '🟢 GOOD'
        WHEN e.success_rate_pct_24h >= (t.min_success_rate * 0.9) THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END AS errors_status,
    CONCAT(
        'Success Rate: ', e.success_rate_pct_24h, '% | ',
        'Failed: ', e.failed_pipelines_24h, ' | ',
        'Min Required: ', t.min_success_rate, '%'
    ) AS errors_details,

    -- ==================== SIGNAL 3: QUALITY ====================
    '3️⃣ QUALITY' AS signal_3,
    ROUND(q.avg_dq_score_24h, 2) AS quality_avg_score,
    q.total_critical_issues_24h AS quality_critical_issues,
    q.total_warning_issues_24h AS quality_warning_issues,
    CASE
        WHEN q.avg_dq_score_24h >= t.min_dq_score THEN '🟢 GOOD'
        WHEN q.avg_dq_score_24h >= (t.min_dq_score * 0.9) THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END AS quality_status,
    CONCAT(
        'Avg Score: ', ROUND(q.avg_dq_score_24h, 1), ' | ',
        'Critical Issues: ', q.total_critical_issues_24h, ' | ',
        'Min Required: ', t.min_dq_score
    ) AS quality_details,

    -- ==================== SIGNAL 4: COST ====================
    '4️⃣ COST' AS signal_4,
    ROUND(c.credits_used_24h, 2) AS cost_credits_24h,
    ROUND(c.projected_monthly_credits, 0) AS cost_projected_monthly,
    CASE
        WHEN c.credits_used_24h <= (t.daily_credit_limit * 0.75) THEN '🟢 GOOD'
        WHEN c.credits_used_24h <= (t.daily_credit_limit * 0.9) THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END AS cost_status,
    CONCAT(
        'Used: ', ROUND(c.credits_used_24h, 1), ' credits | ',
        'Limit: ', t.daily_credit_limit, ' credits/day | ',
        'Projected: ', ROUND(c.projected_monthly_credits, 0), ' credits/month'
    ) AS cost_details,

    -- ==================== SIGNAL 5: FRESHNESS ====================
    '5️⃣ FRESHNESS' AS signal_5,
    f.last_successful_pipeline_run AS freshness_last_run,
    f.hours_since_last_run AS freshness_hours_ago,
    CASE
        WHEN f.hours_since_last_run <= (t.max_freshness_hours * 0.5) THEN '🟢 GOOD'
        WHEN f.hours_since_last_run <= t.max_freshness_hours THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END AS freshness_status,
    CONCAT(
        'Last Run: ', TO_VARCHAR(f.last_successful_pipeline_run, 'YYYY-MM-DD HH24:MI'), ' | ',
        'Age: ', f.hours_since_last_run, ' hours | ',
        'Max Allowed: ', t.max_freshness_hours, ' hours'
    ) AS freshness_details,

    -- ==================== OVERALL HEALTH ====================
    CASE
        WHEN (
            CASE WHEN l.avg_duration_minutes <= (t.max_duration_hours * 60 * 0.5) THEN 1 ELSE 0 END +
            CASE WHEN e.success_rate_pct_24h >= t.min_success_rate THEN 1 ELSE 0 END +
            CASE WHEN q.avg_dq_score_24h >= t.min_dq_score THEN 1 ELSE 0 END +
            CASE WHEN c.credits_used_24h <= (t.daily_credit_limit * 0.75) THEN 1 ELSE 0 END +
            CASE WHEN f.hours_since_last_run <= (t.max_freshness_hours * 0.5) THEN 1 ELSE 0 END
        ) = 5 THEN '✅ ALL SYSTEMS HEALTHY'
        WHEN (
            CASE WHEN l.avg_duration_minutes > (t.max_duration_hours * 60 * 0.8) THEN 1 ELSE 0 END +
            CASE WHEN e.success_rate_pct_24h < (t.min_success_rate * 0.9) THEN 1 ELSE 0 END +
            CASE WHEN q.avg_dq_score_24h < (t.min_dq_score * 0.9) THEN 1 ELSE 0 END +
            CASE WHEN c.credits_used_24h > (t.daily_credit_limit * 0.9) THEN 1 ELSE 0 END +
            CASE WHEN f.hours_since_last_run > t.max_freshness_hours THEN 1 ELSE 0 END
        ) > 0 THEN '🔴 CRITICAL - IMMEDIATE ACTION REQUIRED'
        ELSE '🟡 WARNING - MONITORING REQUIRED'
    END AS overall_health_status

FROM latency_signal l
CROSS JOIN error_signal e
CROSS JOIN quality_signal q
CROSS JOIN cost_signal c
CROSS JOIN freshness_signal f
CROSS JOIN thresholds t;

-- =====================================================================================
-- SIMPLIFIED ALERT VIEW - What needs attention RIGHT NOW?
-- =====================================================================================

CREATE OR REPLACE VIEW vw_alerts_active AS
WITH golden_signals AS (
    SELECT * FROM vw_golden_signals_dashboard
)
SELECT
    CURRENT_TIMESTAMP() AS alert_timestamp,
    'LATENCY' AS alert_type,
    latency_status AS severity,
    latency_details AS alert_message
FROM golden_signals
WHERE latency_status IN ('🔴 CRITICAL', '🟡 WARNING')

UNION ALL

SELECT
    CURRENT_TIMESTAMP(),
    'ERRORS',
    errors_status,
    errors_details
FROM golden_signals
WHERE errors_status IN ('🔴 CRITICAL', '🟡 WARNING')

UNION ALL

SELECT
    CURRENT_TIMESTAMP(),
    'QUALITY',
    quality_status,
    quality_details
FROM golden_signals
WHERE quality_status IN ('🔴 CRITICAL', '🟡 WARNING')

UNION ALL

SELECT
    CURRENT_TIMESTAMP(),
    'COST',
    cost_status,
    cost_details
FROM golden_signals
WHERE cost_status IN ('🔴 CRITICAL', '🟡 WARNING')

UNION ALL

SELECT
    CURRENT_TIMESTAMP(),
    'FRESHNESS',
    freshness_status,
    freshness_details
FROM golden_signals
WHERE freshness_status IN ('🔴 CRITICAL', '🟡 WARNING')

ORDER BY
    CASE severity
        WHEN '🔴 CRITICAL' THEN 1
        WHEN '🟡 WARNING' THEN 2
        ELSE 3
    END;

-- =====================================================================================
-- HELPER PROCEDURES - Log pipeline executions and DQ scores
-- =====================================================================================

-- Procedure to log pipeline execution
CREATE OR REPLACE PROCEDURE sp_log_pipeline_execution(
    p_pipeline_name VARCHAR,
    p_execution_status VARCHAR,
    p_duration_minutes NUMBER,
    p_records_processed INTEGER,
    p_records_failed INTEGER,
    p_error_message VARCHAR DEFAULT NULL,
    p_batch_id VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
BEGIN
    INSERT INTO IDENTIFIER(:v_dw_database || '.metadata.pipeline_execution_history')
        (pipeline_name, execution_start_time, execution_end_time, execution_status,
         duration_minutes, records_processed, records_failed, error_message, batch_id)
    VALUES
        (:p_pipeline_name,
         DATEADD(minute, -:p_duration_minutes, CURRENT_TIMESTAMP()),
         CURRENT_TIMESTAMP(),
         :p_execution_status,
         :p_duration_minutes,
         :p_records_processed,
         :p_records_failed,
         :p_error_message,
         :p_batch_id);

    RETURN 'Pipeline execution logged: ' || :p_pipeline_name;
END;
$$;

-- Procedure to log data quality score
CREATE OR REPLACE PROCEDURE sp_log_dq_score(
    p_table_name VARCHAR,
    p_dq_score NUMBER,
    p_total_records INTEGER,
    p_records_with_issues INTEGER,
    p_critical_issues INTEGER DEFAULT 0,
    p_warning_issues INTEGER DEFAULT 0
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
BEGIN
    INSERT INTO IDENTIFIER(:v_dw_database || '.metadata.data_quality_scores')
        (table_name, dq_score, total_records, records_with_issues,
         critical_issues, warning_issues)
    VALUES
        (:p_table_name, :p_dq_score, :p_total_records, :p_records_with_issues,
         :p_critical_issues, :p_warning_issues);

    RETURN 'DQ score logged for: ' || :p_table_name;
END;
$$;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- MORNING HEALTH CHECK (5 seconds - ONE query to rule them all!)
SELECT * FROM vw_golden_signals_dashboard;

-- See only active alerts (if any)
SELECT * FROM vw_alerts_active;

-- Log a pipeline execution (call from ETL procedures)
CALL sp_log_pipeline_execution(
    'daily_veteran_load',
    'SUCCEEDED',
    45.5,
    10000,
    0,
    NULL,
    'BATCH_20251121_083000'
);

-- Log a data quality score (call from DQ checks)
CALL sp_log_dq_score(
    'warehouse.dim_veteran',
    98.5,
    50000,
    750,
    5,
    100
);

-- View pipeline history
SELECT * FROM pipeline_execution_history
WHERE execution_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY execution_start_time DESC;

-- View DQ score trends
SELECT
    DATE_TRUNC('day', check_timestamp) AS check_date,
    table_name,
    AVG(dq_score) AS avg_dq_score,
    SUM(critical_issues) AS total_critical_issues
FROM data_quality_scores
WHERE check_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('day', check_timestamp), table_name
ORDER BY check_date DESC, table_name;
*/

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

SELECT 'Golden Signals Dashboard Deployed' AS status,
       'Run: SELECT * FROM vw_golden_signals_dashboard;' AS next_step;
