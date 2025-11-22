-- =====================================================
-- Dynamic Tables Monitoring & Observability
-- =====================================================
-- Purpose: Monitor health, performance, and costs of Dynamic Tables
-- Author: Data Team
-- Date: 2025-11-20
-- =====================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================
-- MONITORING VIEW 1: Dynamic Table Health Dashboard
-- =====================================================

CREATE OR REPLACE VIEW monitoring.vw_dynamic_table_health AS
SELECT
    dt.name as table_name,
    dt.database_name,
    dt.schema_name,
    dt.target_lag,
    dt.refresh_mode,
    dt.warehouse_name,
    dt.scheduling_state,
    dt.data_timestamp,
    dt.last_refresh_start_time,
    dt.last_refresh_end_time,
    DATEDIFF(second, dt.last_refresh_start_time, dt.last_refresh_end_time) as last_refresh_duration_seconds,
    dt.next_refresh_time,

    -- Health indicators
    CASE
        WHEN dt.scheduling_state = 'ACTIVE' THEN 'HEALTHY'
        WHEN dt.scheduling_state = 'SUSPENDED' THEN 'SUSPENDED'
        WHEN dt.scheduling_state = 'FAILED' THEN 'UNHEALTHY'
        ELSE 'UNKNOWN'
    END as health_status,

    -- Freshness check (is data within target lag?)
    DATEDIFF(second, dt.data_timestamp, CURRENT_TIMESTAMP()) as data_age_seconds,
    CASE
        WHEN DATEDIFF(second, dt.data_timestamp, CURRENT_TIMESTAMP()) <=
             (CASE
                 WHEN dt.target_lag LIKE '%minute%' THEN TRY_CAST(SPLIT_PART(dt.target_lag, ' ', 1) AS INTEGER) * 60
                 WHEN dt.target_lag LIKE '%hour%' THEN TRY_CAST(SPLIT_PART(dt.target_lag, ' ', 1) AS INTEGER) * 3600
                 ELSE 3600
              END)
        THEN 'FRESH'
        ELSE 'STALE'
    END as freshness_status,

    -- Time until next refresh
    DATEDIFF(second, CURRENT_TIMESTAMP(), dt.next_refresh_time) as seconds_until_next_refresh,

    CURRENT_TIMESTAMP() as report_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES()) dt
WHERE dt.database_name = $dw_database
  AND dt.schema_name IN ('REFERENCE', 'STAGING')
ORDER BY dt.schema_name, dt.name;

-- =====================================================
-- MONITORING VIEW 2: Dynamic Table Refresh History
-- =====================================================

CREATE OR REPLACE VIEW monitoring.vw_dynamic_table_refresh_history AS
SELECT
    h.name as table_name,
    h.database_name,
    h.schema_name,
    h.state,
    h.refresh_start_time,
    h.refresh_end_time,
    DATEDIFF(second, h.refresh_start_time, h.refresh_end_time) as duration_seconds,
    h.refresh_action,  -- INCREMENTAL or FULL
    h.completion_target,

    -- Performance classification
    CASE
        WHEN DATEDIFF(second, h.refresh_start_time, h.refresh_end_time) < 60 THEN 'FAST'
        WHEN DATEDIFF(second, h.refresh_start_time, h.refresh_end_time) < 300 THEN 'MODERATE'
        ELSE 'SLOW'
    END as performance_class,

    -- Success indicator
    CASE
        WHEN h.state = 'SUCCEEDED' THEN TRUE
        ELSE FALSE
    END as refresh_succeeded,

    CURRENT_TIMESTAMP() as report_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    DATE_RANGE_START => DATEADD(day, -7, CURRENT_DATE())
)) h
WHERE h.database_name = $dw_database
  AND h.schema_name IN ('REFERENCE', 'STAGING')
ORDER BY h.refresh_start_time DESC;

-- =====================================================
-- MONITORING VIEW 3: Dynamic Table Performance Trends
-- =====================================================

CREATE OR REPLACE VIEW monitoring.vw_dynamic_table_performance_trends AS
WITH refresh_stats AS (
    SELECT
        name as table_name,
        DATE(refresh_start_time) as refresh_date,
        COUNT(*) as refresh_count,
        AVG(DATEDIFF(second, refresh_start_time, refresh_end_time)) as avg_duration_seconds,
        MIN(DATEDIFF(second, refresh_start_time, refresh_end_time)) as min_duration_seconds,
        MAX(DATEDIFF(second, refresh_start_time, refresh_end_time)) as max_duration_seconds,
        SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) as success_count,
        SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failure_count,
        SUM(CASE WHEN refresh_action = 'INCREMENTAL' THEN 1 ELSE 0 END) as incremental_count,
        SUM(CASE WHEN refresh_action = 'FULL' THEN 1 ELSE 0 END) as full_refresh_count
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
        DATE_RANGE_START => DATEADD(day, -30, CURRENT_DATE())
    ))
    WHERE database_name = $dw_database
      AND schema_name IN ('REFERENCE', 'STAGING')
    GROUP BY name, DATE(refresh_start_time)
)
SELECT
    table_name,
    refresh_date,
    refresh_count,
    ROUND(avg_duration_seconds, 2) as avg_duration_seconds,
    min_duration_seconds,
    max_duration_seconds,
    success_count,
    failure_count,
    ROUND((success_count::DECIMAL / NULLIF(refresh_count, 0)) * 100, 2) as success_rate_pct,
    incremental_count,
    full_refresh_count,
    ROUND((incremental_count::DECIMAL / NULLIF(refresh_count, 0)) * 100, 2) as incremental_pct
FROM refresh_stats
ORDER BY refresh_date DESC, table_name;

-- =====================================================
-- MONITORING VIEW 4: Data Quality Trends
-- =====================================================

CREATE OR REPLACE VIEW monitoring.vw_data_quality_trends AS
SELECT
    table_name,
    entity_name,
    total_records,
    avg_dq_score,
    excellent_records,
    good_records,
    acceptable_records,
    poor_records,
    records_with_conflicts,
    health_status,
    pct_excellent,
    pct_poor,
    last_refresh_timestamp,

    -- Trend indicators (requires historical data)
    LAG(avg_dq_score) OVER (PARTITION BY table_name ORDER BY last_refresh_timestamp) as prev_avg_dq_score,
    avg_dq_score - LAG(avg_dq_score) OVER (PARTITION BY table_name ORDER BY last_refresh_timestamp) as dq_score_change,

    LAG(poor_records) OVER (PARTITION BY table_name ORDER BY last_refresh_timestamp) as prev_poor_records,
    poor_records - LAG(poor_records) OVER (PARTITION BY table_name ORDER BY last_refresh_timestamp) as poor_records_change,

    CURRENT_TIMESTAMP() as report_timestamp
FROM IDENTIFIER($dw_database || '.STAGING.dt_vw_staging_dq_summary')
ORDER BY last_refresh_timestamp DESC, table_name;

-- =====================================================
-- QUERY 1: Current Health Status
-- =====================================================
-- Purpose: Get real-time health status of all dynamic tables
-- Use: Run frequently (every 5-10 minutes) via monitoring dashboard
-- =====================================================

SELECT
    table_name,
    schema_name,
    health_status,
    freshness_status,
    target_lag,
    CONCAT(
        FLOOR(data_age_seconds / 60), 'm ',
        MOD(data_age_seconds, 60), 's'
    ) as data_age,
    CONCAT(
        FLOOR(last_refresh_duration_seconds / 60), 'm ',
        MOD(last_refresh_duration_seconds, 60), 's'
    ) as last_refresh_duration,
    next_refresh_time
FROM monitoring.vw_dynamic_table_health
ORDER BY
    CASE health_status
        WHEN 'UNHEALTHY' THEN 1
        WHEN 'SUSPENDED' THEN 2
        ELSE 3
    END,
    CASE freshness_status
        WHEN 'STALE' THEN 1
        ELSE 2
    END;

-- =====================================================
-- QUERY 2: Failed Refreshes (Last 24 Hours)
-- =====================================================
-- Purpose: Identify and investigate failed refreshes
-- Use: Run when alerts fire or during troubleshooting
-- =====================================================

SELECT
    table_name,
    refresh_start_time,
    refresh_end_time,
    duration_seconds,
    refresh_action,
    state
FROM monitoring.vw_dynamic_table_refresh_history
WHERE state != 'SUCCEEDED'
  AND refresh_start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY refresh_start_time DESC;

-- =====================================================
-- QUERY 3: Performance Degradation Detection
-- =====================================================
-- Purpose: Detect tables with increasing refresh times
-- Use: Run daily as part of health checks
-- =====================================================

WITH recent_performance AS (
    SELECT
        table_name,
        AVG(CASE WHEN refresh_date >= DATEADD(day, -1, CURRENT_DATE())
                 THEN avg_duration_seconds END) as avg_duration_last_1d,
        AVG(CASE WHEN refresh_date >= DATEADD(day, -7, CURRENT_DATE())
                 THEN avg_duration_seconds END) as avg_duration_last_7d,
        AVG(CASE WHEN refresh_date >= DATEADD(day, -30, CURRENT_DATE())
                 THEN avg_duration_seconds END) as avg_duration_last_30d
    FROM monitoring.vw_dynamic_table_performance_trends
    GROUP BY table_name
)
SELECT
    table_name,
    ROUND(avg_duration_last_1d, 2) as avg_seconds_1d,
    ROUND(avg_duration_last_7d, 2) as avg_seconds_7d,
    ROUND(avg_duration_last_30d, 2) as avg_seconds_30d,
    ROUND(((avg_duration_last_1d - avg_duration_last_7d) / NULLIF(avg_duration_last_7d, 0)) * 100, 2) as pct_change_1d_vs_7d,
    CASE
        WHEN ((avg_duration_last_1d - avg_duration_last_7d) / NULLIF(avg_duration_last_7d, 0)) > 0.5
        THEN '⚠️ DEGRADED'
        WHEN ((avg_duration_last_1d - avg_duration_last_7d) / NULLIF(avg_duration_last_7d, 0)) > 0.25
        THEN '⚠️ WATCH'
        ELSE '✅ NORMAL'
    END as performance_status
FROM recent_performance
WHERE avg_duration_last_1d IS NOT NULL
ORDER BY pct_change_1d_vs_7d DESC;

-- =====================================================
-- QUERY 4: Data Quality Issues
-- =====================================================
-- Purpose: Identify tables with declining data quality
-- Use: Run daily for data quality monitoring
-- =====================================================

SELECT
    table_name,
    entity_name,
    health_status,
    avg_dq_score,
    dq_score_change,
    poor_records,
    poor_records_change,
    pct_poor,
    records_with_conflicts,
    last_refresh_timestamp,
    CASE
        WHEN avg_dq_score < 70 THEN '🔴 CRITICAL'
        WHEN avg_dq_score < 80 THEN '🟡 WARNING'
        WHEN dq_score_change < -5 THEN '🟡 DECLINING'
        ELSE '🟢 GOOD'
    END as dq_status
FROM monitoring.vw_data_quality_trends
WHERE last_refresh_timestamp >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
ORDER BY avg_dq_score ASC, poor_records DESC;

-- =====================================================
-- QUERY 5: Refresh Frequency Analysis
-- =====================================================
-- Purpose: Understand refresh patterns and costs
-- Use: Run weekly for capacity planning
-- =====================================================

SELECT
    table_name,
    COUNT(*) as total_refreshes_last_7d,
    SUM(CASE WHEN refresh_action = 'INCREMENTAL' THEN 1 ELSE 0 END) as incremental_refreshes,
    SUM(CASE WHEN refresh_action = 'FULL' THEN 1 ELSE 0 END) as full_refreshes,
    ROUND(AVG(duration_seconds), 2) as avg_refresh_seconds,
    ROUND(SUM(duration_seconds) / 3600.0, 2) as total_compute_hours,
    ROUND((SUM(CASE WHEN refresh_action = 'INCREMENTAL' THEN 1 ELSE 0 END)::DECIMAL /
           NULLIF(COUNT(*), 0)) * 100, 2) as incremental_pct
FROM monitoring.vw_dynamic_table_refresh_history
WHERE refresh_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND state = 'SUCCEEDED'
GROUP BY table_name
ORDER BY total_compute_hours DESC;

-- =====================================================
-- QUERY 6: Dependency Chain Visualization
-- =====================================================
-- Purpose: Understand dependencies between dynamic tables
-- Use: Run when planning changes or troubleshooting
-- =====================================================

WITH RECURSIVE dependency_chain AS (
    -- Base: All dynamic tables
    SELECT
        name as table_name,
        database_name,
        schema_name,
        0 as depth,
        name as root_table,
        ARRAY_CONSTRUCT(name) as dependency_path
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
    WHERE database_name = $dw_database

    UNION ALL

    -- Recursive: Find dependencies (simplified - actual implementation may need table_lineage)
    SELECT
        dt.name,
        dt.database_name,
        dt.schema_name,
        dc.depth + 1,
        dc.root_table,
        ARRAY_APPEND(dc.dependency_path, dt.name)
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES()) dt
    INNER JOIN dependency_chain dc
        ON dt.schema_name = dc.schema_name
    WHERE dc.depth < 5  -- Prevent infinite loops
)
SELECT DISTINCT
    root_table,
    table_name,
    depth,
    ARRAY_TO_STRING(dependency_path, ' → ') as path
FROM dependency_chain
ORDER BY root_table, depth, table_name;

-- =====================================================
-- QUERY 7: Cost Analysis
-- =====================================================
-- Purpose: Estimate compute costs for dynamic tables
-- Use: Run monthly for budget tracking
-- =====================================================

WITH compute_usage AS (
    SELECT
        table_name,
        warehouse_name,
        COUNT(*) as refresh_count,
        SUM(duration_seconds) as total_compute_seconds,
        ROUND(SUM(duration_seconds) / 3600.0, 4) as total_compute_hours,
        AVG(duration_seconds) as avg_duration_seconds,
        refresh_action
    FROM monitoring.vw_dynamic_table_refresh_history
    WHERE refresh_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
      AND state = 'SUCCEEDED'
    GROUP BY table_name, warehouse_name, refresh_action
)
SELECT
    table_name,
    warehouse_name,
    refresh_count,
    total_compute_hours,
    ROUND(avg_duration_seconds, 2) as avg_duration_seconds,
    refresh_action,

    -- Cost estimate (assuming $2/credit, X-Small warehouse = 1 credit/hour)
    -- Adjust based on your warehouse size and credit costs
    ROUND(total_compute_hours * 2.0, 2) as estimated_cost_usd,

    ROUND(total_compute_hours / 30.0, 2) as avg_daily_compute_hours
FROM compute_usage
ORDER BY total_compute_hours DESC;

-- =====================================================
-- QUERY 8: Conflict Analysis (Facilities)
-- =====================================================
-- Purpose: Analyze OMS/VEMS conflicts in facilities
-- Use: Run weekly for data reconciliation review
-- =====================================================

SELECT
    conflict_type,
    COUNT(*) as conflict_count,
    ROUND((COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM IDENTIFIER($dw_database || '.STAGING.dt_stg_facilities'))) * 100, 2) as pct_of_total
FROM IDENTIFIER($dw_database || '.STAGING.dt_stg_facilities')
WHERE conflict_type IS NOT NULL
GROUP BY conflict_type
ORDER BY conflict_count DESC;

-- =====================================================
-- QUERY 9: Stale Data Alert
-- =====================================================
-- Purpose: Identify tables with data older than expected
-- Use: Run every 15 minutes via alerting system
-- =====================================================

SELECT
    table_name,
    target_lag,
    data_age_seconds,
    CONCAT(
        FLOOR(data_age_seconds / 60), ' minutes, ',
        MOD(data_age_seconds, 60), ' seconds'
    ) as data_age_formatted,
    last_refresh_start_time,
    next_refresh_time,
    scheduling_state
FROM monitoring.vw_dynamic_table_health
WHERE freshness_status = 'STALE'
   OR health_status = 'UNHEALTHY'
ORDER BY data_age_seconds DESC;

-- =====================================================
-- QUERY 10: Summary Dashboard
-- =====================================================
-- Purpose: Executive summary of dynamic tables health
-- Use: Display on monitoring dashboard
-- =====================================================

SELECT
    'Dynamic Tables Overview' as metric_category,
    COUNT(*) as total_dynamic_tables,
    SUM(CASE WHEN health_status = 'HEALTHY' THEN 1 ELSE 0 END) as healthy_count,
    SUM(CASE WHEN health_status = 'UNHEALTHY' THEN 1 ELSE 0 END) as unhealthy_count,
    SUM(CASE WHEN health_status = 'SUSPENDED' THEN 1 ELSE 0 END) as suspended_count,
    SUM(CASE WHEN freshness_status = 'FRESH' THEN 1 ELSE 0 END) as fresh_count,
    SUM(CASE WHEN freshness_status = 'STALE' THEN 1 ELSE 0 END) as stale_count,
    ROUND(AVG(last_refresh_duration_seconds), 2) as avg_refresh_seconds,
    CURRENT_TIMESTAMP() as report_timestamp
FROM monitoring.vw_dynamic_table_health

UNION ALL

SELECT
    'Data Quality Overview',
    COUNT(DISTINCT table_name),
    SUM(CASE WHEN health_status IN ('EXCELLENT', 'GOOD') THEN 1 ELSE 0 END),
    SUM(CASE WHEN health_status = 'NEEDS_ATTENTION' THEN 1 ELSE 0 END),
    NULL,
    NULL,
    NULL,
    ROUND(AVG(avg_dq_score), 2),
    CURRENT_TIMESTAMP()
FROM IDENTIFIER($dw_database || '.STAGING.dt_vw_staging_dq_summary');

-- =====================================================
-- ALERTING RECOMMENDATIONS
-- =====================================================

/*

ALERT 1: Unhealthy Dynamic Tables
Frequency: Every 5 minutes
Condition: health_status = 'UNHEALTHY'
Action: Page on-call engineer

ALERT 2: Stale Data
Frequency: Every 15 minutes
Condition: freshness_status = 'STALE' for > 30 minutes
Action: Send Slack notification

ALERT 3: Failed Refreshes
Frequency: Every 10 minutes
Condition: refresh state = 'FAILED' in last 10 minutes
Action: Send email to data team

ALERT 4: Data Quality Degradation
Frequency: Every hour
Condition: avg_dq_score < 70 OR dq_score_change < -10
Action: Send Slack notification

ALERT 5: Performance Degradation
Frequency: Daily
Condition: refresh duration > 2x normal average
Action: Create Jira ticket for investigation

*/
