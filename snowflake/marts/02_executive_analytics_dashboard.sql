-- =====================================================================================
-- EXECUTIVE ANALYTICS DASHBOARD
-- =====================================================================================
-- Purpose: Executive-level KPIs and strategic insights for VES leadership
--
-- This dashboard provides:
--   1. Strategic KPIs (exam volume, cycle time, SLA compliance)
--   2. Financial metrics (cost per exam, revenue tracking)
--   3. Capacity utilization and workforce analytics
--   4. Quality and customer satisfaction metrics
--   5. Trend analysis and forecasting
--   6. Bottleneck impact quantification
--   7. Geographic and demographic insights
--
-- Target Audience: C-Suite, VPs, Directors
-- Refresh Frequency: Hourly (via materialized views)
-- Data Latency: <1 hour
--
-- Author: Data Team
-- Date: 2025-11-17
-- Version: 2.0 (Enhanced)
-- =====================================================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA marts;

-- =====================================================================================
-- PART 1: STRATEGIC KPI VIEWS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- View: vw_exec_kpi_overview
-- Purpose: Top-level executive scorecard
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_exec_kpi_overview AS
WITH current_month AS (
    SELECT
        COUNT(DISTINCT er.exam_request_sk) AS total_exams_mtd,
        COUNT(DISTINCT CASE WHEN er.exam_status = 'Completed' THEN er.exam_request_sk END) AS completed_exams_mtd,
        AVG(DATEDIFF(day, er.request_date, er.completion_date)) AS avg_cycle_time_days,
        SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS sla_compliance_pct,
        AVG(eval.exam_quality_score) AS avg_quality_score,
        COUNT(DISTINCT er.veteran_dim_sk) AS unique_veterans_served,
        COUNT(DISTINCT er.assigned_evaluator_sk) AS active_evaluators
    FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    LEFT JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_evaluation') eval
        ON er.exam_request_sk = eval.exam_request_sk
    WHERE er.request_date >= DATE_TRUNC('month', CURRENT_DATE())
),
prior_month AS (
    SELECT
        COUNT(DISTINCT er.exam_request_sk) AS total_exams_prior,
        COUNT(DISTINCT CASE WHEN er.exam_status = 'Completed' THEN er.exam_request_sk END) AS completed_exams_prior,
        AVG(DATEDIFF(day, er.request_date, er.completion_date)) AS avg_cycle_time_prior,
        SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS sla_compliance_prior,
        AVG(eval.exam_quality_score) AS avg_quality_score_prior
    FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    LEFT JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_evaluation') eval
        ON er.exam_request_sk = eval.exam_request_sk
    WHERE er.request_date >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '1 month'
      AND er.request_date < DATE_TRUNC('month', CURRENT_DATE())
),
annual_stats AS (
    SELECT
        COUNT(DISTINCT er.exam_request_sk) AS total_exams_ytd,
        COUNT(DISTINCT CASE WHEN er.exam_status = 'Completed' THEN er.exam_request_sk END) AS completed_exams_ytd
    FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    WHERE YEAR(er.request_date) = YEAR(CURRENT_DATE())
)
SELECT
    -- Current Month KPIs
    'Exam Volume (MTD)' AS kpi_name,
    cm.total_exams_mtd AS current_value,
    pm.total_exams_prior AS prior_month_value,
    ROUND((cm.total_exams_mtd - pm.total_exams_prior) * 100.0 / NULLIF(pm.total_exams_prior, 0), 2) AS mom_change_pct,
    CASE
        WHEN (cm.total_exams_mtd - pm.total_exams_prior) * 100.0 / NULLIF(pm.total_exams_prior, 0) > 5 THEN '🟢 ↑'
        WHEN (cm.total_exams_mtd - pm.total_exams_prior) * 100.0 / NULLIF(pm.total_exams_prior, 0) < -5 THEN '🔴 ↓'
        ELSE '🟡 →'
    END AS trend_indicator,
    25000 AS target_value, -- Monthly target
    ROUND(cm.total_exams_mtd * 100.0 / 25000, 2) AS pct_to_target
FROM current_month cm, prior_month pm, annual_stats ast

UNION ALL

SELECT
    'Exams Completed (MTD)',
    cm.completed_exams_mtd,
    pm.completed_exams_prior,
    ROUND((cm.completed_exams_mtd - pm.completed_exams_prior) * 100.0 / NULLIF(pm.completed_exams_prior, 0), 2),
    CASE
        WHEN (cm.completed_exams_mtd - pm.completed_exams_prior) * 100.0 / NULLIF(pm.completed_exams_prior, 0) > 5 THEN '🟢 ↑'
        WHEN (cm.completed_exams_mtd - pm.completed_exams_prior) * 100.0 / NULLIF(pm.completed_exams_prior, 0) < -5 THEN '🔴 ↓'
        ELSE '🟡 →'
    END,
    23000,
    ROUND(cm.completed_exams_mtd * 100.0 / 23000, 2)
FROM current_month cm, prior_month pm

UNION ALL

SELECT
    'Avg Cycle Time (Days)',
    ROUND(cm.avg_cycle_time_days, 1),
    ROUND(pm.avg_cycle_time_prior, 1),
    ROUND((cm.avg_cycle_time_days - pm.avg_cycle_time_prior) * 100.0 / NULLIF(pm.avg_cycle_time_prior, 0), 2),
    CASE
        WHEN (cm.avg_cycle_time_days - pm.avg_cycle_time_prior) < -0.5 THEN '🟢 ↓' -- Lower is better
        WHEN (cm.avg_cycle_time_days - pm.avg_cycle_time_prior) > 0.5 THEN '🔴 ↑'
        ELSE '🟡 →'
    END,
    14.0, -- 14-day target
    ROUND((14.0 - cm.avg_cycle_time_days) * 100.0 / 14.0, 2)
FROM current_month cm, prior_month pm

UNION ALL

SELECT
    'SLA Compliance (%)',
    ROUND(cm.sla_compliance_pct, 2),
    ROUND(pm.sla_compliance_prior, 2),
    ROUND(cm.sla_compliance_pct - pm.sla_compliance_prior, 2),
    CASE
        WHEN cm.sla_compliance_pct >= 95 THEN '🟢 ↑'
        WHEN cm.sla_compliance_pct >= 90 THEN '🟡 →'
        ELSE '🔴 ↓'
    END,
    95.0,
    ROUND(cm.sla_compliance_pct * 100.0 / 95.0, 2)
FROM current_month cm, prior_month pm

UNION ALL

SELECT
    'Quality Score (Avg)',
    ROUND(cm.avg_quality_score, 1),
    ROUND(pm.avg_quality_score_prior, 1),
    ROUND(cm.avg_quality_score - pm.avg_quality_score_prior, 2),
    CASE
        WHEN cm.avg_quality_score >= 90 THEN '🟢 ↑'
        WHEN cm.avg_quality_score >= 85 THEN '🟡 →'
        ELSE '🔴 ↓'
    END,
    90.0,
    ROUND(cm.avg_quality_score * 100.0 / 90.0, 2)
FROM current_month cm, prior_month pm

UNION ALL

SELECT
    'Active Evaluators',
    cm.active_evaluators,
    NULL,
    NULL,
    '🟢',
    500,
    ROUND(cm.active_evaluators * 100.0 / 500, 2)
FROM current_month cm;

-- -----------------------------------------------------------------------------------------
-- View: vw_exec_financial_metrics
-- Purpose: Financial performance tracking
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_exec_financial_metrics AS
WITH exam_costs AS (
    SELECT
        d.fiscal_year,
        d.fiscal_quarter,
        d.fiscal_month,
        COUNT(DISTINCT er.exam_request_sk) AS total_exams,
        -- Assume avg cost per exam is $350 (customize based on actual contracts)
        COUNT(DISTINCT er.exam_request_sk) * 350 AS total_exam_cost_usd,
        -- Revenue per exam varies by complexity
        SUM(CASE
            WHEN er.exam_complexity = 'Simple' THEN 250
            WHEN er.exam_complexity = 'Moderate' THEN 350
            WHEN er.exam_complexity = 'Complex' THEN 500
            ELSE 350
        END) AS total_revenue_usd,
        -- Calculate margin
        SUM(CASE
            WHEN er.exam_complexity = 'Simple' THEN 250
            WHEN er.exam_complexity = 'Moderate' THEN 350
            WHEN er.exam_complexity = 'Complex' THEN 500
            ELSE 350
        END) - (COUNT(DISTINCT er.exam_request_sk) * 350) AS gross_margin_usd
    FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    INNER JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_date') d
        ON er.request_date_sk = d.date_sk
    WHERE er.exam_status = 'Completed'
    GROUP BY d.fiscal_year, d.fiscal_quarter, d.fiscal_month
)
SELECT
    fiscal_year,
    fiscal_quarter,
    fiscal_month,
    total_exams,
    total_exam_cost_usd,
    total_revenue_usd,
    gross_margin_usd,
    ROUND(gross_margin_usd * 100.0 / NULLIF(total_revenue_usd, 0), 2) AS gross_margin_pct,
    ROUND(total_revenue_usd / NULLIF(total_exams, 0), 2) AS revenue_per_exam,
    ROUND(total_exam_cost_usd / NULLIF(total_exams, 0), 2) AS cost_per_exam,
    -- Year-over-year comparison
    LAG(total_revenue_usd) OVER (PARTITION BY fiscal_quarter ORDER BY fiscal_year) AS prior_year_revenue,
    ROUND((total_revenue_usd - LAG(total_revenue_usd) OVER (PARTITION BY fiscal_quarter ORDER BY fiscal_year)) * 100.0 /
          NULLIF(LAG(total_revenue_usd) OVER (PARTITION BY fiscal_quarter ORDER BY fiscal_year), 0), 2) AS yoy_revenue_growth_pct
FROM exam_costs
ORDER BY fiscal_year DESC, fiscal_quarter DESC, fiscal_month DESC;

-- -----------------------------------------------------------------------------------------
-- View: vw_exec_capacity_utilization
-- Purpose: Workforce and capacity analytics
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_exec_capacity_utilization AS
WITH evaluator_workload AS (
    SELECT
        e.evaluator_npi,
        e.first_name || ' ' || e.last_name AS evaluator_name,
        e.specialty,
        e.state AS evaluator_state,
        COUNT(DISTINCT er.exam_request_sk) AS exams_completed_mtd,
        AVG(DATEDIFF(day, er.assignment_date, er.completion_date)) AS avg_completion_days,
        -- Assume capacity is 50 exams/month per evaluator
        50 AS monthly_capacity,
        ROUND(COUNT(DISTINCT er.exam_request_sk) * 100.0 / 50, 2) AS utilization_pct,
        CASE
            WHEN COUNT(DISTINCT er.exam_request_sk) >= 45 THEN '🔴 OVERUTILIZED'
            WHEN COUNT(DISTINCT er.exam_request_sk) >= 35 THEN '🟢 OPTIMAL'
            WHEN COUNT(DISTINCT er.exam_request_sk) >= 20 THEN '🟡 UNDERUTILIZED'
            ELSE '🔵 VERY LOW'
        END AS utilization_status
    FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_evaluator') e
    INNER JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
        ON e.evaluator_sk = er.assigned_evaluator_sk
    WHERE e.is_current = TRUE
      AND er.request_date >= DATE_TRUNC('month', CURRENT_DATE())
      AND er.exam_status = 'Completed'
    GROUP BY e.evaluator_npi, e.first_name, e.last_name, e.specialty, e.state
)
SELECT
    specialty,
    evaluator_state,
    COUNT(*) AS total_evaluators,
    SUM(exams_completed_mtd) AS total_exams_completed,
    AVG(exams_completed_mtd) AS avg_exams_per_evaluator,
    AVG(utilization_pct) AS avg_utilization_pct,
    SUM(monthly_capacity) AS total_capacity,
    SUM(exams_completed_mtd) AS capacity_used,
    SUM(monthly_capacity) - SUM(exams_completed_mtd) AS capacity_available,
    ROUND((SUM(monthly_capacity) - SUM(exams_completed_mtd)) * 100.0 / NULLIF(SUM(monthly_capacity), 0), 2) AS capacity_available_pct,
    COUNT(CASE WHEN utilization_status = '🔴 OVERUTILIZED' THEN 1 END) AS overutilized_count,
    COUNT(CASE WHEN utilization_status = '🔵 VERY LOW' THEN 1 END) AS underutilized_count
FROM evaluator_workload
GROUP BY specialty, evaluator_state
ORDER BY total_exams_completed DESC;

-- -----------------------------------------------------------------------------------------
-- View: vw_exec_bottleneck_impact
-- Purpose: Quantify financial impact of bottlenecks
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_exec_bottleneck_impact AS
WITH bottleneck_analysis AS (
    SELECT
        b.primary_bottleneck_stage,
        b.primary_bottleneck_type,
        COUNT(*) AS affected_exams,
        AVG(b.primary_bottleneck_hours) AS avg_delay_hours,
        SUM(b.primary_bottleneck_hours) AS total_delay_hours,
        -- Cost of delay: $50/hour (opportunity cost + veteran satisfaction impact)
        SUM(b.primary_bottleneck_hours) * 50 AS estimated_delay_cost_usd,
        -- Calculate SLA breach rate
        SUM(CASE WHEN b.sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS sla_breach_rate_pct
    FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_processing_bottlenecks') b
    WHERE b.request_date >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '3 months'
    GROUP BY b.primary_bottleneck_stage, b.primary_bottleneck_type
)
SELECT
    primary_bottleneck_stage,
    primary_bottleneck_type,
    affected_exams,
    ROUND(avg_delay_hours, 1) AS avg_delay_hours,
    ROUND(total_delay_hours, 0) AS total_delay_hours,
    estimated_delay_cost_usd,
    ROUND(sla_breach_rate_pct, 2) AS sla_breach_rate_pct,
    -- Priority ranking
    ROW_NUMBER() OVER (ORDER BY estimated_delay_cost_usd DESC) AS cost_impact_rank,
    CASE
        WHEN estimated_delay_cost_usd > 100000 THEN '🔴 CRITICAL - IMMEDIATE ACTION'
        WHEN estimated_delay_cost_usd > 50000 THEN '🟡 HIGH - PRIORITIZE'
        WHEN estimated_delay_cost_usd > 10000 THEN '🟢 MEDIUM - MONITOR'
        ELSE '🔵 LOW - TRACK'
    END AS priority_level
FROM bottleneck_analysis
ORDER BY estimated_delay_cost_usd DESC;

-- -----------------------------------------------------------------------------------------
-- View: vw_exec_geographic_distribution
-- Purpose: Exam volume and performance by state
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_exec_geographic_distribution AS
SELECT
    v.state,
    COUNT(DISTINCT er.exam_request_sk) AS total_exams,
    COUNT(DISTINCT CASE WHEN er.exam_status = 'Completed' THEN er.exam_request_sk END) AS completed_exams,
    ROUND(COUNT(DISTINCT CASE WHEN er.exam_status = 'Completed' THEN er.exam_request_sk END) * 100.0 /
          NULLIF(COUNT(DISTINCT er.exam_request_sk), 0), 2) AS completion_rate_pct,
    AVG(DATEDIFF(day, er.request_date, er.completion_date)) AS avg_cycle_time_days,
    SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS sla_compliance_pct,
    -- Veteran demographics
    COUNT(DISTINCT er.veteran_dim_sk) AS unique_veterans,
    AVG(v.disability_rating) AS avg_disability_rating,
    -- Evaluator availability
    COUNT(DISTINCT er.assigned_evaluator_sk) AS evaluators_serving_state,
    -- Identify states needing attention
    CASE
        WHEN SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) < 90 THEN '🔴 SLA ISSUE'
        WHEN AVG(DATEDIFF(day, er.request_date, er.completion_date)) > 21 THEN '🟡 SLOW PROCESSING'
        WHEN COUNT(DISTINCT er.assigned_evaluator_sk) < 5 THEN '🟡 LOW EVALUATOR COUNT'
        ELSE '🟢 HEALTHY'
    END AS state_health_status
FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_veteran') v
INNER JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    ON v.veteran_sk = er.veteran_dim_sk
WHERE v.is_current = TRUE
  AND er.request_date >= CURRENT_DATE() - 90
GROUP BY v.state
ORDER BY total_exams DESC;

-- =====================================================================================
-- PART 2: MATERIALIZED VIEWS FOR FAST DASHBOARD LOADING
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- MV: mv_exec_daily_trends
-- Purpose: Pre-aggregated daily trends for fast charting
-- -----------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_exec_daily_trends AS
SELECT
    d.full_date,
    d.day_of_week_name,
    d.fiscal_year,
    d.fiscal_quarter,
    d.fiscal_month,
    -- Volume metrics
    COUNT(DISTINCT er.exam_request_sk) AS exam_requests,
    COUNT(DISTINCT CASE WHEN er.exam_status = 'Completed' THEN er.exam_request_sk END) AS exams_completed,
    COUNT(DISTINCT CASE WHEN er.exam_status = 'Cancelled' THEN er.exam_request_sk END) AS exams_cancelled,
    -- Performance metrics
    AVG(DATEDIFF(day, er.request_date, er.completion_date)) AS avg_cycle_time_days,
    MEDIAN(DATEDIFF(day, er.request_date, er.completion_date)) AS median_cycle_time_days,
    -- SLA metrics
    SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) AS sla_met_count,
    SUM(CASE WHEN er.sla_met = FALSE THEN 1 ELSE 0 END) AS sla_breached_count,
    ROUND(SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_compliance_pct,
    -- Quality metrics
    AVG(eval.exam_quality_score) AS avg_quality_score,
    -- Workforce metrics
    COUNT(DISTINCT er.assigned_evaluator_sk) AS active_evaluators,
    COUNT(DISTINCT er.veteran_dim_sk) AS unique_veterans
FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_date') d
LEFT JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    ON d.date_sk = er.request_date_sk
LEFT JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_evaluation') eval
    ON er.exam_request_sk = eval.exam_request_sk
WHERE d.full_date >= CURRENT_DATE() - 365 -- Last 12 months
GROUP BY d.full_date, d.day_of_week_name, d.fiscal_year, d.fiscal_quarter, d.fiscal_month;

-- -----------------------------------------------------------------------------------------
-- MV: mv_exec_evaluator_scorecard
-- Purpose: Top/bottom performer identification
-- -----------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_exec_evaluator_scorecard AS
SELECT
    e.evaluator_npi,
    e.first_name || ' ' || e.last_name AS evaluator_name,
    e.specialty,
    e.state,
    -- Volume metrics
    COUNT(DISTINCT er.exam_request_sk) AS total_exams_completed,
    COUNT(DISTINCT er.veteran_dim_sk) AS unique_veterans_served,
    -- Performance metrics
    AVG(DATEDIFF(day, er.assignment_date, er.completion_date)) AS avg_completion_time_days,
    AVG(eval.exam_quality_score) AS avg_quality_score,
    -- QA metrics
    SUM(CASE WHEN qa.qa_status = 'PASSED_FIRST_REVIEW' THEN 1 ELSE 0 END) AS first_pass_qa_count,
    ROUND(SUM(CASE WHEN qa.qa_status = 'PASSED_FIRST_REVIEW' THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(DISTINCT qa.qa_event_sk), 0), 2) AS first_pass_qa_rate_pct,
    -- Efficiency
    SUM(eval.exam_duration_minutes) / NULLIF(COUNT(DISTINCT er.exam_request_sk), 0) AS avg_exam_duration_minutes,
    -- Last activity
    MAX(er.completion_date) AS last_exam_date,
    DATEDIFF(day, MAX(er.completion_date), CURRENT_DATE()) AS days_since_last_exam,
    -- Performance tier
    NTILE(4) OVER (ORDER BY AVG(eval.exam_quality_score) DESC) AS quality_quartile,
    NTILE(4) OVER (ORDER BY COUNT(DISTINCT er.exam_request_sk) DESC) AS volume_quartile,
    -- Overall rating
    CASE
        WHEN AVG(eval.exam_quality_score) >= 95 AND COUNT(DISTINCT er.exam_request_sk) >= 40 THEN '⭐⭐⭐⭐⭐ TOP PERFORMER'
        WHEN AVG(eval.exam_quality_score) >= 90 AND COUNT(DISTINCT er.exam_request_sk) >= 30 THEN '⭐⭐⭐⭐ HIGH PERFORMER'
        WHEN AVG(eval.exam_quality_score) >= 85 AND COUNT(DISTINCT er.exam_request_sk) >= 20 THEN '⭐⭐⭐ GOOD'
        WHEN AVG(eval.exam_quality_score) >= 80 THEN '⭐⭐ NEEDS IMPROVEMENT'
        ELSE '⭐ REQUIRES ATTENTION'
    END AS performance_rating
FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_evaluator') e
INNER JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests') er
    ON e.evaluator_sk = er.assigned_evaluator_sk
INNER JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_evaluation') eval
    ON er.exam_request_sk = eval.exam_request_sk
LEFT JOIN IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_evaluation_qa_events') qa
    ON eval.evaluation_sk = qa.evaluation_sk
WHERE e.is_current = TRUE
  AND er.exam_status = 'Completed'
  AND er.request_date >= CURRENT_DATE() - 90 -- Last 90 days
GROUP BY e.evaluator_npi, e.first_name, e.last_name, e.specialty, e.state;

-- =====================================================================================
-- PART 3: EXECUTIVE REPORT PROCEDURES
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Procedure: sp_generate_weekly_executive_report
-- Purpose: Automated weekly report sent to leadership
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_generate_weekly_executive_report()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total_exams NUMBER;
    v_sla_compliance NUMBER;
    v_avg_cycle_time NUMBER;
    v_top_bottleneck VARCHAR;
    v_bottleneck_cost NUMBER;
    v_report_html VARCHAR;
BEGIN
    -- Gather key metrics
    SELECT
        SUM(exam_requests),
        AVG(sla_compliance_pct),
        AVG(avg_cycle_time_days)
    INTO :v_total_exams, :v_sla_compliance, :v_avg_cycle_time
    FROM IDENTIFIER(fn_get_dw_database() || '.marts.mv_exec_daily_trends
    WHERE full_date >= CURRENT_DATE() - 7;

    SELECT
        primary_bottleneck_stage,
        estimated_delay_cost_usd
    INTO :v_top_bottleneck, :v_bottleneck_cost
    FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_bottleneck_impact
    ORDER BY estimated_delay_cost_usd DESC
    LIMIT 1;

    -- Build HTML report
    v_report_html := '<html><body>' ||
        '<h1>VES Executive Weekly Report</h1>' ||
        '<h2>Week of ' || TO_VARCHAR(CURRENT_DATE() - 7, 'YYYY-MM-DD') || '</h2>' ||
        '<hr>' ||
        '<h3>📊 Key Performance Indicators</h3>' ||
        '<table border="1" cellpadding="10">' ||
        '<tr><th>Metric</th><th>Value</th><th>Status</th></tr>' ||
        '<tr><td>Total Exams (Week)</td><td>' || :v_total_exams || '</td><td>' ||
            CASE WHEN :v_total_exams >= 5000 THEN '🟢' ELSE '🔴' END || '</td></tr>' ||
        '<tr><td>SLA Compliance</td><td>' || ROUND(:v_sla_compliance, 1) || '%</td><td>' ||
            CASE WHEN :v_sla_compliance >= 95 THEN '🟢' ELSE '🔴' END || '</td></tr>' ||
        '<tr><td>Avg Cycle Time</td><td>' || ROUND(:v_avg_cycle_time, 1) || ' days</td><td>' ||
            CASE WHEN :v_avg_cycle_time <= 14 THEN '🟢' ELSE '🔴' END || '</td></tr>' ||
        '</table>' ||
        '<hr>' ||
        '<h3>🚨 Top Bottleneck</h3>' ||
        '<p><b>Stage:</b> ' || :v_top_bottleneck || '</p>' ||
        '<p><b>Estimated Cost Impact:</b> $' || :v_bottleneck_cost || '</p>' ||
        '<hr>' ||
        '<p><i>For detailed analysis, access the <a href="https://app.snowflake.com/dashboard">Executive Dashboard</a></i></p>' ||
        '</body></html>';

    -- Send email
    CALL sp_send_email_alert(
        'VES Weekly Executive Report - ' || TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD'),
        :v_report_html,
        ARRAY_CONSTRUCT('ceo@company.com', 'coo@company.com', 'vp-operations@company.com')
    );

    RETURN 'Weekly executive report sent successfully';
END;
$$;

-- Schedule weekly report (Mondays at 8 AM)
CREATE OR REPLACE TASK task_weekly_executive_report
    WAREHOUSE = etl_task_wh
    SCHEDULE = 'USING CRON 0 8 * * 1 America/New_York'
AS
    CALL sp_generate_weekly_executive_report();

ALTER TASK task_weekly_executive_report RESUME;

-- =====================================================================================
-- PART 4: PREDICTIVE ANALYTICS & FORECASTING
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- View: vw_exec_forecast_demand
-- Purpose: Predict next month's exam volume based on trends
-- -----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_exec_forecast_demand AS
WITH monthly_trends AS (
    SELECT
        fiscal_year,
        fiscal_month,
        SUM(exam_requests) AS total_exams,
        LAG(SUM(exam_requests), 1) OVER (ORDER BY fiscal_year, fiscal_month) AS prev_month_exams,
        LAG(SUM(exam_requests), 12) OVER (ORDER BY fiscal_year, fiscal_month) AS same_month_last_year
    FROM IDENTIFIER(fn_get_dw_database() || '.marts.mv_exec_daily_trends
    GROUP BY fiscal_year, fiscal_month
),
growth_rates AS (
    SELECT
        fiscal_year,
        fiscal_month,
        total_exams,
        prev_month_exams,
        same_month_last_year,
        (total_exams - prev_month_exams) * 100.0 / NULLIF(prev_month_exams, 0) AS mom_growth_pct,
        (total_exams - same_month_last_year) * 100.0 / NULLIF(same_month_last_year, 0) AS yoy_growth_pct
    FROM monthly_trends
)
SELECT
    fiscal_year,
    fiscal_month,
    total_exams,
    ROUND(mom_growth_pct, 2) AS mom_growth_pct,
    ROUND(yoy_growth_pct, 2) AS yoy_growth_pct,
    -- Simple forecast: Apply average growth rate to next month
    ROUND(total_exams * (1 + AVG(mom_growth_pct) OVER (ORDER BY fiscal_year, fiscal_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) / 100), 0) AS forecast_next_month,
    -- Seasonality-adjusted forecast using same month last year
    ROUND(same_month_last_year * (1 + AVG(yoy_growth_pct) OVER (ORDER BY fiscal_year, fiscal_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) / 100), 0) AS forecast_seasonal_adjusted
FROM growth_rates
ORDER BY fiscal_year DESC, fiscal_month DESC;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- Example 1: View executive KPI overview
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_kpi_overview;

-- Example 2: Financial performance this quarter
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_financial_metrics
WHERE fiscal_quarter = QUARTER(CURRENT_DATE())
  AND fiscal_year = YEAR(CURRENT_DATE());

-- Example 3: Capacity utilization by specialty
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_capacity_utilization
WHERE specialty = 'Orthopedic'
ORDER BY avg_utilization_pct DESC;

-- Example 4: Top 5 costliest bottlenecks
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_bottleneck_impact
ORDER BY estimated_delay_cost_usd DESC
LIMIT 5;

-- Example 5: States requiring attention
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_geographic_distribution
WHERE state_health_status LIKE '%ISSUE%'
ORDER BY total_exams DESC;

-- Example 6: Daily trend last 30 days
SELECT
    full_date,
    exam_requests,
    exams_completed,
    sla_compliance_pct,
    avg_cycle_time_days
FROM IDENTIFIER(fn_get_dw_database() || '.marts.mv_exec_daily_trends
WHERE full_date >= CURRENT_DATE() - 30
ORDER BY full_date DESC;

-- Example 7: Top performers this quarter
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.mv_exec_evaluator_scorecard
WHERE performance_rating LIKE '%TOP PERFORMER%'
ORDER BY avg_quality_score DESC
LIMIT 20;

-- Example 8: Demand forecast next 3 months
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.marts.vw_exec_forecast_demand
ORDER BY fiscal_year DESC, fiscal_month DESC
LIMIT 3;

-- Example 9: Send weekly report manually
CALL sp_generate_weekly_executive_report();
*/

-- =====================================================================================
-- END OF EXECUTIVE ANALYTICS DASHBOARD
-- =====================================================================================
