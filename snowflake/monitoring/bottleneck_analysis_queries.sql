-- =====================================================================================
-- BOTTLENECK ANALYSIS QUERIES
-- =====================================================================================
-- Purpose: Identify and analyze bottlenecks in the exam processing workflow
-- Usage: Run these queries to identify where delays occur, both internally and externally
--
-- Query Categories:
--   1. Stage-by-Stage Bottleneck Analysis
--   2. Internal vs External Process Bottlenecks
--   3. Capacity & Workload Bottlenecks
--   4. Quality & Rework Bottlenecks
--   5. SLA Risk & Performance Analysis
--   6. Root Cause Analysis
--   7. Dashboard Monitoring Queries
--
-- Author: Data Team
-- Date: 2025-11-17
-- =====================================================================================

SET dw_database = (SELECT get_dw_database());

-- =====================================================================================
-- 1. STAGE-BY-STAGE BOTTLENECK ANALYSIS
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 1.1: Average Duration by Processing Stage
-- Purpose: Identify which stages take the longest on average
-- -----------------------------------------------------------------------------
SELECT
    'Intake & Validation' AS processing_stage,
    'INTERNAL_VEMS' AS stage_type,
    AVG(intake_to_validation_hours) AS avg_duration_hours,
    MEDIAN(intake_to_validation_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY intake_to_validation_hours) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY intake_to_validation_hours) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN validation_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN validation_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE intake_to_validation_hours IS NOT NULL

UNION ALL

SELECT
    'Queue Wait' AS processing_stage,
    'INTERNAL_VEMS' AS stage_type,
    AVG(queue_wait_hours) AS avg_duration_hours,
    MEDIAN(queue_wait_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY queue_wait_hours) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY queue_wait_hours) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN queue_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN queue_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE queue_wait_hours IS NOT NULL

UNION ALL

SELECT
    'Examiner Assignment' AS processing_stage,
    'INTERNAL_VEMS' AS stage_type,
    AVG(time_to_examiner_response_hours) AS avg_duration_hours,
    MEDIAN(time_to_examiner_response_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_to_examiner_response_hours) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_to_examiner_response_hours) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN assignment_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN assignment_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE time_to_examiner_response_hours IS NOT NULL

UNION ALL

SELECT
    'Scheduling Coordination' AS processing_stage,
    'MIXED' AS stage_type,
    AVG(assignment_to_scheduling_hours) AS avg_duration_hours,
    MEDIAN(assignment_to_scheduling_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY assignment_to_scheduling_hours) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY assignment_to_scheduling_hours) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN scheduling_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN scheduling_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE assignment_to_scheduling_hours IS NOT NULL

UNION ALL

SELECT
    'Appointment Wait Time' AS processing_stage,
    'EXTERNAL_VETERAN' AS stage_type,
    AVG(scheduled_to_appointment_hours) AS avg_duration_hours,
    MEDIAN(scheduled_to_appointment_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY scheduled_to_appointment_hours) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY scheduled_to_appointment_hours) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN appointment_wait_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN appointment_wait_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE scheduled_to_appointment_hours IS NOT NULL

UNION ALL

SELECT
    'Exam Execution' AS processing_stage,
    'INTERNAL_VEMS' AS stage_type,
    AVG(exam_duration_minutes / 60.0) AS avg_duration_hours,
    MEDIAN(exam_duration_minutes / 60.0) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY exam_duration_minutes / 60.0) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY exam_duration_minutes / 60.0) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN exam_execution_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN exam_execution_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE exam_duration_minutes IS NOT NULL

UNION ALL

SELECT
    'QA Review Process' AS processing_stage,
    'INTERNAL_VEMS' AS stage_type,
    AVG(total_qa_process_hours) AS avg_duration_hours,
    MEDIAN(total_qa_process_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_qa_process_hours) AS p75_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_qa_process_hours) AS p90_duration_hours,
    COUNT(*) AS request_count,
    SUM(CASE WHEN qa_review_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN qa_review_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_percentage
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE total_qa_process_hours IS NOT NULL

ORDER BY avg_duration_hours DESC;

-- -----------------------------------------------------------------------------
-- Query 1.2: Top Bottleneck Stages by Frequency
-- Purpose: Show which stages are most commonly the primary bottleneck
-- -----------------------------------------------------------------------------
SELECT
    primary_bottleneck_stage,
    primary_bottleneck_type,
    COUNT(*) AS bottleneck_occurrence_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total,
    AVG(primary_bottleneck_hours) AS avg_bottleneck_duration_hours,
    MEDIAN(primary_bottleneck_hours) AS median_bottleneck_duration_hours,
    AVG(total_cycle_time_hours) AS avg_total_cycle_time_hours,
    ROUND(AVG(primary_bottleneck_hours) / NULLIF(AVG(total_cycle_time_hours), 0) * 100, 2) AS avg_pct_of_total_time
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE primary_bottleneck_stage IS NOT NULL
GROUP BY primary_bottleneck_stage, primary_bottleneck_type
ORDER BY bottleneck_occurrence_count DESC;

-- -----------------------------------------------------------------------------
-- Query 1.3: Bottleneck Trend Over Time
-- Purpose: Track how bottlenecks change over time (by month)
-- -----------------------------------------------------------------------------
SELECT
    dd.year_month AS request_month,
    fb.primary_bottleneck_stage,
    fb.primary_bottleneck_type,
    COUNT(*) AS bottleneck_count,
    AVG(fb.primary_bottleneck_hours) AS avg_bottleneck_hours,
    AVG(fb.total_cycle_time_hours) AS avg_total_cycle_time_hours,
    ROUND(AVG(fb.primary_bottleneck_hours) / NULLIF(AVG(fb.total_cycle_time_hours), 0) * 100, 2) AS pct_of_total_time
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_dates') dd ON fb.request_date_sk = dd.date_sk
WHERE fb.primary_bottleneck_stage IS NOT NULL
    AND dd.year_month >= DATEADD(month, -6, CURRENT_DATE()) -- Last 6 months
GROUP BY dd.year_month, fb.primary_bottleneck_stage, fb.primary_bottleneck_type
ORDER BY dd.year_month DESC, bottleneck_count DESC;

-- =====================================================================================
-- 2. INTERNAL VS EXTERNAL PROCESS BOTTLENECKS
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 2.1: Internal vs External Time Distribution
-- Purpose: Compare time spent in internal VEMS processes vs external dependencies
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN internal_percentage >= 75 THEN 'Primarily Internal (75%+)'
        WHEN internal_percentage >= 50 THEN 'Mostly Internal (50-74%)'
        WHEN internal_percentage >= 25 THEN 'Mostly External (25-49%)'
        ELSE 'Primarily External (<25%)'
    END AS time_distribution_category,
    COUNT(*) AS request_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_requests,
    AVG(internal_process_hours) AS avg_internal_hours,
    AVG(external_dependency_hours) AS avg_external_hours,
    AVG(total_cycle_time_hours) AS avg_total_hours,
    AVG(internal_percentage) AS avg_internal_pct,
    AVG(external_percentage) AS avg_external_pct,
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE total_cycle_time_hours > 0
GROUP BY
    CASE
        WHEN internal_percentage >= 75 THEN 'Primarily Internal (75%+)'
        WHEN internal_percentage >= 50 THEN 'Mostly Internal (50-74%)'
        WHEN internal_percentage >= 25 THEN 'Mostly External (25-49%)'
        ELSE 'Primarily External (<25%)'
    END
ORDER BY request_count DESC;

-- -----------------------------------------------------------------------------
-- Query 2.2: Internal VEMS Bottlenecks Detail
-- Purpose: Break down internal bottlenecks by specific stage
-- -----------------------------------------------------------------------------
SELECT
    'Queue Wait' AS internal_stage,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN queue_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN queue_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_rate_pct,
    AVG(queue_wait_hours) AS avg_duration_hours,
    MEDIAN(queue_wait_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY queue_wait_hours) AS p90_duration_hours,
    AVG(total_cycle_time_hours) AS avg_impact_on_total_cycle
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE queue_wait_hours IS NOT NULL

UNION ALL

SELECT
    'QA Review' AS internal_stage,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN qa_review_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN qa_review_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_rate_pct,
    AVG(total_qa_process_hours) AS avg_duration_hours,
    MEDIAN(total_qa_process_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_qa_process_hours) AS p90_duration_hours,
    AVG(total_cycle_time_hours) AS avg_impact_on_total_cycle
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE total_qa_process_hours IS NOT NULL

UNION ALL

SELECT
    'Validation' AS internal_stage,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN validation_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN validation_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_rate_pct,
    AVG(validation_duration_hours) AS avg_duration_hours,
    MEDIAN(validation_duration_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY validation_duration_hours) AS p90_duration_hours,
    AVG(total_cycle_time_hours) AS avg_impact_on_total_cycle
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE validation_duration_hours IS NOT NULL

UNION ALL

SELECT
    'Examiner Assignment' AS internal_stage,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN assignment_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN assignment_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_rate_pct,
    AVG(time_to_examiner_response_hours) AS avg_duration_hours,
    MEDIAN(time_to_examiner_response_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_to_examiner_response_hours) AS p90_duration_hours,
    AVG(total_cycle_time_hours) AS avg_impact_on_total_cycle
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE time_to_examiner_response_hours IS NOT NULL

ORDER BY avg_duration_hours DESC;

-- -----------------------------------------------------------------------------
-- Query 2.3: External Dependency Bottlenecks Detail
-- Purpose: Analyze external bottlenecks (veteran availability, VA dependencies)
-- -----------------------------------------------------------------------------
SELECT
    'Appointment Wait (Veteran)' AS external_dependency,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN appointment_wait_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN appointment_wait_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_rate_pct,
    AVG(scheduled_to_appointment_hours) AS avg_wait_hours,
    MEDIAN(scheduled_to_appointment_hours) AS median_wait_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY scheduled_to_appointment_hours) AS p90_wait_hours,
    SUM(CASE WHEN reschedule_count > 0 THEN 1 ELSE 0 END) AS reschedule_incidents,
    AVG(reschedule_count) AS avg_reschedules_per_request,
    SUM(CASE WHEN no_show_count > 0 THEN 1 ELSE 0 END) AS no_show_incidents
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE scheduled_to_appointment_hours IS NOT NULL

UNION ALL

SELECT
    'VA Delivery & Acknowledgment' AS external_dependency,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN delivery_bottleneck_flag = TRUE THEN 1 ELSE 0 END) AS bottleneck_count,
    ROUND(SUM(CASE WHEN delivery_bottleneck_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bottleneck_rate_pct,
    AVG(qa_approval_to_delivery_hours) AS avg_wait_hours,
    MEDIAN(qa_approval_to_delivery_hours) AS median_wait_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY qa_approval_to_delivery_hours) AS p90_wait_hours,
    0 AS reschedule_incidents,
    0 AS avg_reschedules_per_request,
    0 AS no_show_incidents
FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE qa_approval_to_delivery_hours IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Query 2.4: VEMS Process Efficiency Analysis
-- Purpose: Measure efficiency of VEMS-controlled processes
-- -----------------------------------------------------------------------------
SELECT
    df.facility_name,
    ds.specialty_name,
    COUNT(*) AS request_count,
    AVG(fb.internal_process_hours) AS avg_internal_hours,
    AVG(fb.external_dependency_hours) AS avg_external_hours,
    AVG(fb.internal_percentage) AS avg_internal_pct,

    -- Internal efficiency metrics
    AVG(fb.queue_wait_hours) AS avg_queue_hours,
    AVG(fb.total_qa_process_hours) AS avg_qa_hours,
    AVG(fb.time_to_examiner_response_hours) AS avg_assignment_hours,

    -- Bottleneck counts
    SUM(CASE WHEN fb.primary_bottleneck_type = 'INTERNAL_VEMS' THEN 1 ELSE 0 END) AS internal_bottleneck_count,
    ROUND(SUM(CASE WHEN fb.primary_bottleneck_type = 'INTERNAL_VEMS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS internal_bottleneck_rate,

    -- Performance rating
    SUM(CASE WHEN fb.overall_performance_rating IN ('EXCELLENT', 'GOOD') THEN 1 ELSE 0 END) AS good_performance_count,
    ROUND(SUM(CASE WHEN fb.overall_performance_rating IN ('EXCELLENT', 'GOOD') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS good_performance_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_facilities') df ON fb.facility_dim_sk = df.facility_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_specialties') ds ON fb.specialty_dim_sk = ds.specialty_sk
GROUP BY df.facility_name, ds.specialty_name
HAVING COUNT(*) >= 10 -- Only facilities/specialties with sufficient volume
ORDER BY internal_bottleneck_rate DESC, request_count DESC
LIMIT 50;

-- =====================================================================================
-- 3. CAPACITY & WORKLOAD BOTTLENECKS
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 3.1: Examiner Capacity Bottleneck Analysis
-- Purpose: Identify examiners with capacity constraints causing delays
-- -----------------------------------------------------------------------------
SELECT
    de.examiner_name,
    ds.specialty_name,
    df.facility_name,
    COUNT(*) AS total_assignments,

    -- Capacity indicators
    SUM(CASE WHEN fb.examiner_overload_flag = TRUE THEN 1 ELSE 0 END) AS overload_count,
    ROUND(SUM(CASE WHEN fb.examiner_overload_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS overload_rate_pct,
    AVG(fb.examiner_workload_at_assignment) AS avg_workload,
    AVG(fb.examiner_utilization_pct) AS avg_utilization_pct,

    -- Performance impact
    AVG(fb.time_to_examiner_response_hours) AS avg_response_time_hours,
    AVG(CASE WHEN fb.examiner_overload_flag = TRUE THEN fb.time_to_examiner_response_hours END) AS avg_response_time_when_overloaded,
    SUM(fb.examiner_rejection_count) AS total_rejections,
    AVG(fb.reassignment_count) AS avg_reassignments,

    -- Bottleneck correlation
    SUM(CASE WHEN fb.capacity_constraint_pattern_flag = TRUE THEN 1 ELSE 0 END) AS capacity_bottleneck_count,
    ROUND(SUM(CASE WHEN fb.capacity_constraint_pattern_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS capacity_bottleneck_rate,

    -- SLA impact
    SUM(CASE WHEN fb.sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN fb.sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_evaluators') de ON fb.examiner_dim_sk = de.examiner_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_specialties') ds ON fb.specialty_dim_sk = ds.specialty_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_facilities') df ON fb.facility_dim_sk = df.facility_sk
WHERE fb.examiner_dim_sk IS NOT NULL
GROUP BY de.examiner_name, df.facility_name, ds.specialty_name
HAVING COUNT(*) >= 5 -- Minimum assignments for statistical relevance
ORDER BY overload_rate_pct DESC, total_assignments DESC
LIMIT 100;

-- -----------------------------------------------------------------------------
-- Query 3.2: Facility Capacity Constraints
-- Purpose: Identify facilities with capacity bottlenecks
-- -----------------------------------------------------------------------------
SELECT
    df.facility_name,
    df.facility_state,
    df.facility_region,
    COUNT(*) AS total_requests,

    -- Capacity metrics
    SUM(CASE WHEN fb.facility_capacity_constraint_flag = TRUE THEN 1 ELSE 0 END) AS capacity_constraint_count,
    ROUND(SUM(CASE WHEN fb.facility_capacity_constraint_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS capacity_constraint_rate,
    AVG(fb.facility_utilization_pct) AS avg_utilization_pct,
    AVG(fb.facility_backlog_count) AS avg_backlog_count,

    -- Performance impact
    AVG(fb.queue_wait_hours) AS avg_queue_wait_hours,
    AVG(CASE WHEN fb.facility_capacity_constraint_flag = TRUE THEN fb.queue_wait_hours END) AS avg_queue_wait_when_constrained,
    AVG(fb.assignment_to_scheduling_hours) AS avg_scheduling_hours,

    -- Bottleneck analysis
    SUM(CASE WHEN fb.primary_bottleneck_stage = 'QUEUE_WAIT' THEN 1 ELSE 0 END) AS queue_bottleneck_count,
    SUM(CASE WHEN fb.capacity_constraint_pattern_flag = TRUE THEN 1 ELSE 0 END) AS capacity_pattern_count,

    -- SLA impact
    AVG(fb.total_cycle_time_hours / 24) AS avg_cycle_time_days,
    SUM(CASE WHEN fb.sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN fb.sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_facilities') df ON fb.facility_dim_sk = df.facility_sk
GROUP BY df.facility_name, df.facility_state, df.facility_region
HAVING COUNT(*) >= 20 -- Minimum volume for analysis
ORDER BY capacity_constraint_rate DESC, total_requests DESC;

-- -----------------------------------------------------------------------------
-- Query 3.3: Specialty Availability Bottlenecks
-- Purpose: Identify specialties with examiner shortages
-- -----------------------------------------------------------------------------
SELECT
    ds.specialty_name,
    ds.specialty_category,
    COUNT(*) AS total_requests,

    -- Availability indicators
    SUM(CASE WHEN fb.specialty_shortage_flag = TRUE THEN 1 ELSE 0 END) AS shortage_count,
    ROUND(SUM(CASE WHEN fb.specialty_shortage_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS shortage_rate_pct,
    AVG(fb.specialty_examiner_count) AS avg_available_examiners,
    AVG(fb.specialty_avg_wait_days) AS avg_specialty_wait_days,

    -- Performance impact
    AVG(fb.queue_wait_hours) AS avg_queue_hours,
    AVG(fb.assignment_attempt_count) AS avg_assignment_attempts,
    AVG(fb.reassignment_count) AS avg_reassignments,
    AVG(fb.alternative_examiners_available) AS avg_alternatives,

    -- Wait time analysis
    MEDIAN(fb.queue_wait_hours) AS median_queue_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fb.queue_wait_hours) AS p90_queue_hours,

    -- SLA impact
    SUM(CASE WHEN fb.sla_at_risk_flag = TRUE THEN 1 ELSE 0 END) AS sla_at_risk_count,
    ROUND(SUM(CASE WHEN fb.sla_at_risk_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_at_risk_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_specialties') ds ON fb.specialty_dim_sk = ds.specialty_sk
GROUP BY ds.specialty_name, ds.specialty_category
ORDER BY avg_queue_hours DESC, shortage_rate_pct DESC
LIMIT 50;

-- =====================================================================================
-- 4. QUALITY & REWORK BOTTLENECKS
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 4.1: QA Rework Impact Analysis
-- Purpose: Identify how QA rework cycles create bottlenecks
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN qa_cycle_count = 1 THEN '1 - First Pass Approval'
        WHEN qa_cycle_count = 2 THEN '2 - One Rework Cycle'
        WHEN qa_cycle_count = 3 THEN '3 - Two Rework Cycles'
        WHEN qa_cycle_count >= 4 THEN '4+ - Multiple Rework Cycles'
        ELSE 'Unknown'
    END AS qa_cycle_category,

    COUNT(*) AS request_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total,

    -- QA timing impact
    AVG(total_qa_process_hours) AS avg_qa_hours,
    MEDIAN(total_qa_process_hours) AS median_qa_hours,
    AVG(qa_clarification_cycle_hours) AS avg_clarification_hours,

    -- Total cycle time impact
    AVG(total_cycle_time_hours) AS avg_total_cycle_hours,
    AVG(total_cycle_time_hours / 24) AS avg_total_cycle_days,

    -- Quality metrics
    AVG(overall_quality_score) AS avg_quality_score,
    AVG(deficiency_count) AS avg_deficiency_count,

    -- Bottleneck correlation
    SUM(CASE WHEN primary_bottleneck_stage = 'QA_REVIEW' THEN 1 ELSE 0 END) AS qa_bottleneck_count,
    ROUND(SUM(CASE WHEN primary_bottleneck_stage = 'QA_REVIEW' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS qa_bottleneck_rate,

    -- SLA impact
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE qa_cycle_count IS NOT NULL
GROUP BY
    CASE
        WHEN qa_cycle_count = 1 THEN '1 - First Pass Approval'
        WHEN qa_cycle_count = 2 THEN '2 - One Rework Cycle'
        WHEN qa_cycle_count = 3 THEN '3 - Two Rework Cycles'
        WHEN qa_cycle_count >= 4 THEN '4+ - Multiple Rework Cycles'
        ELSE 'Unknown'
    END
ORDER BY qa_cycle_count;

-- -----------------------------------------------------------------------------
-- Query 4.2: Common QA Deficiencies Causing Bottlenecks
-- Purpose: Identify which types of deficiencies cause the most delays
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN documentation_deficiency_flag = TRUE THEN 'Incomplete Documentation'
        WHEN nexus_opinion_deficiency_flag = TRUE THEN 'Missing Nexus Opinion'
        WHEN medical_rationale_deficiency_flag = TRUE THEN 'Insufficient Medical Rationale'
        WHEN dbq_completion_deficiency_flag = TRUE THEN 'Incomplete DBQ'
        ELSE 'Other/Multiple'
    END AS primary_deficiency_type,

    COUNT(*) AS deficiency_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_deficiencies,

    -- QA impact
    AVG(qa_cycle_count) AS avg_qa_cycles,
    AVG(total_qa_process_hours) AS avg_qa_hours,
    AVG(clarification_count) AS avg_clarifications,

    -- Quality scores
    AVG(completeness_score) AS avg_completeness_score,
    AVG(accuracy_score) AS avg_accuracy_score,
    AVG(overall_quality_score) AS avg_quality_score,

    -- Total impact
    AVG(total_cycle_time_hours / 24) AS avg_total_cycle_days,
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE qa_rework_flag = TRUE
GROUP BY
    CASE
        WHEN documentation_deficiency_flag = TRUE THEN 'Incomplete Documentation'
        WHEN nexus_opinion_deficiency_flag = TRUE THEN 'Missing Nexus Opinion'
        WHEN medical_rationale_deficiency_flag = TRUE THEN 'Insufficient Medical Rationale'
        WHEN dbq_completion_deficiency_flag = TRUE THEN 'Incomplete DBQ'
        ELSE 'Other/Multiple'
    END
ORDER BY deficiency_count DESC;

-- -----------------------------------------------------------------------------
-- Query 4.3: Quality Score vs Cycle Time Analysis
-- Purpose: Correlate quality scores with processing efficiency
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN overall_quality_score >= 90 THEN 'Excellent (90-100)'
        WHEN overall_quality_score >= 80 THEN 'Good (80-89)'
        WHEN overall_quality_score >= 70 THEN 'Satisfactory (70-79)'
        WHEN overall_quality_score >= 60 THEN 'Poor (60-69)'
        ELSE 'Critical (<60)'
    END AS quality_tier,

    COUNT(*) AS request_count,

    -- Quality metrics
    AVG(overall_quality_score) AS avg_quality_score,
    AVG(completeness_score) AS avg_completeness,
    AVG(accuracy_score) AS avg_accuracy,

    -- QA efficiency
    AVG(qa_cycle_count) AS avg_qa_cycles,
    SUM(CASE WHEN first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) AS first_pass_count,
    ROUND(SUM(CASE WHEN first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS first_pass_rate,
    AVG(total_qa_process_hours) AS avg_qa_hours,

    -- Overall cycle time
    AVG(total_cycle_time_hours / 24) AS avg_total_cycle_days,
    MEDIAN(total_cycle_time_hours / 24) AS median_total_cycle_days,

    -- SLA performance
    SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) AS sla_met_count,
    ROUND(SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_met_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE overall_quality_score IS NOT NULL
GROUP BY
    CASE
        WHEN overall_quality_score >= 90 THEN 'Excellent (90-100)'
        WHEN overall_quality_score >= 80 THEN 'Good (80-89)'
        WHEN overall_quality_score >= 70 THEN 'Satisfactory (70-79)'
        WHEN overall_quality_score >= 60 THEN 'Poor (60-69)'
        ELSE 'Critical (<60)'
    END
ORDER BY
    CASE quality_tier
        WHEN 'Excellent (90-100)' THEN 1
        WHEN 'Good (80-89)' THEN 2
        WHEN 'Satisfactory (70-79)' THEN 3
        WHEN 'Poor (60-69)' THEN 4
        ELSE 5
    END;

-- =====================================================================================
-- 5. SLA RISK & PERFORMANCE ANALYSIS
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 5.1: SLA Breach Analysis by Bottleneck Type
-- Purpose: Identify which bottlenecks cause SLA breaches
-- -----------------------------------------------------------------------------
SELECT
    primary_bottleneck_stage,
    primary_bottleneck_type,
    COUNT(*) AS total_requests,

    -- SLA performance
    SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) AS sla_met_count,
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    SUM(CASE WHEN sla_at_risk_flag = TRUE THEN 1 ELSE 0 END) AS sla_at_risk_count,

    ROUND(SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_met_rate,
    ROUND(SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate,
    ROUND(SUM(CASE WHEN sla_at_risk_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_at_risk_rate,

    -- Timing analysis
    AVG(primary_bottleneck_hours) AS avg_bottleneck_hours,
    AVG(total_cycle_time_hours / 24) AS avg_cycle_time_days,
    AVG(sla_days_allowed) AS avg_sla_target_days,
    AVG(days_until_sla_breach) AS avg_days_until_breach,

    -- Severity
    SUM(CASE WHEN days_until_sla_breach < -5 THEN 1 ELSE 0 END) AS severe_breach_count

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE primary_bottleneck_stage IS NOT NULL
GROUP BY primary_bottleneck_stage, primary_bottleneck_type
ORDER BY sla_breach_rate DESC, total_requests DESC;

-- -----------------------------------------------------------------------------
-- Query 5.2: Current At-Risk Requests Dashboard
-- Purpose: Real-time view of requests at risk of SLA breach
-- -----------------------------------------------------------------------------
SELECT
    fb.exam_request_sk,
    dv.veteran_id,
    det.exam_type_name,
    ds.specialty_name,
    df.facility_name,
    dd.full_date AS request_date,

    -- SLA status
    fb.sla_days_allowed,
    ROUND(fb.total_cycle_time_hours / 24, 1) AS days_in_process,
    fb.days_until_sla_breach,
    CASE
        WHEN fb.sla_breach_flag = TRUE THEN 'BREACHED'
        WHEN fb.days_until_sla_breach <= 1 THEN 'CRITICAL (1 day)'
        WHEN fb.days_until_sla_breach <= 3 THEN 'HIGH RISK (2-3 days)'
        WHEN fb.days_until_sla_breach <= 5 THEN 'MODERATE RISK (4-5 days)'
        ELSE 'AT RISK'
    END AS risk_level,

    -- Current bottleneck
    fb.primary_bottleneck_stage,
    fb.primary_bottleneck_type,
    ROUND(fb.primary_bottleneck_hours, 1) AS bottleneck_hours,

    -- Current stage (simulated - would come from request status)
    fb.request_status AS current_stage,
    fb.likely_root_cause,

    -- Complexity indicators
    fb.qa_cycle_count,
    fb.reassignment_count,
    fb.reschedule_count,
    fb.expedite_flag

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_veterans') dv ON fb.veteran_dim_sk = dv.veteran_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_exam_request_types') det ON fb.exam_type_dim_sk = det.exam_type_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_specialties') ds ON fb.specialty_dim_sk = ds.specialty_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_facilities') df ON fb.facility_dim_sk = df.facility_sk
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_dates') dd ON fb.request_date_sk = dd.date_sk
WHERE fb.sla_at_risk_flag = TRUE
    OR fb.sla_breach_flag = TRUE
    AND fb.completion_date_sk IS NULL -- Only open requests
ORDER BY
    CASE
        WHEN fb.sla_breach_flag = TRUE THEN 1
        WHEN fb.days_until_sla_breach <= 1 THEN 2
        WHEN fb.days_until_sla_breach <= 3 THEN 3
        WHEN fb.days_until_sla_breach <= 5 THEN 4
        ELSE 5
    END,
    fb.days_until_sla_breach ASC
LIMIT 500;

-- =====================================================================================
-- 6. ROOT CAUSE ANALYSIS
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 6.1: Top Root Causes of Bottlenecks
-- Purpose: Identify most common root causes across all bottlenecks
-- -----------------------------------------------------------------------------
SELECT
    likely_root_cause,
    COUNT(*) AS occurrence_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total,

    -- Average impact
    AVG(primary_bottleneck_hours) AS avg_bottleneck_hours,
    AVG(total_cycle_time_hours / 24) AS avg_cycle_time_days,

    -- Associated patterns
    SUM(CASE WHEN chronic_reassignment_pattern_flag = TRUE THEN 1 ELSE 0 END) AS reassignment_pattern_count,
    SUM(CASE WHEN scheduling_difficulty_pattern_flag = TRUE THEN 1 ELSE 0 END) AS scheduling_pattern_count,
    SUM(CASE WHEN quality_issue_pattern_flag = TRUE THEN 1 ELSE 0 END) AS quality_pattern_count,
    SUM(CASE WHEN capacity_constraint_pattern_flag = TRUE THEN 1 ELSE 0 END) AS capacity_pattern_count,

    -- SLA impact
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
WHERE likely_root_cause IS NOT NULL
GROUP BY likely_root_cause
ORDER BY occurrence_count DESC;

-- -----------------------------------------------------------------------------
-- Query 6.2: Pattern-Based Bottleneck Analysis
-- Purpose: Identify systemic patterns in bottleneck occurrences
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN chronic_reassignment_pattern_flag = TRUE THEN 'Chronic Reassignment'
        WHEN scheduling_difficulty_pattern_flag = TRUE THEN 'Scheduling Difficulties'
        WHEN quality_issue_pattern_flag = TRUE THEN 'Quality Issues'
        WHEN capacity_constraint_pattern_flag = TRUE THEN 'Capacity Constraints'
        ELSE 'No Pattern Identified'
    END AS bottleneck_pattern,

    COUNT(*) AS pattern_occurrence_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total,

    -- Primary bottleneck stages for this pattern
    MODE() WITHIN GROUP (ORDER BY primary_bottleneck_stage) AS most_common_stage,

    -- Average metrics
    AVG(total_cycle_time_hours / 24) AS avg_cycle_time_days,
    AVG(primary_bottleneck_hours) AS avg_bottleneck_hours,

    -- Specific pattern metrics
    AVG(reassignment_count) AS avg_reassignments,
    AVG(reschedule_count) AS avg_reschedules,
    AVG(qa_cycle_count) AS avg_qa_cycles,
    AVG(examiner_workload_at_assignment) AS avg_examiner_workload,

    -- SLA impact
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_breach_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
GROUP BY
    CASE
        WHEN chronic_reassignment_pattern_flag = TRUE THEN 'Chronic Reassignment'
        WHEN scheduling_difficulty_pattern_flag = TRUE THEN 'Scheduling Difficulties'
        WHEN quality_issue_pattern_flag = TRUE THEN 'Quality Issues'
        WHEN capacity_constraint_pattern_flag = TRUE THEN 'Capacity Constraints'
        ELSE 'No Pattern Identified'
    END
ORDER BY pattern_occurrence_count DESC;

-- =====================================================================================
-- 7. DASHBOARD MONITORING QUERIES
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 7.1: Executive Dashboard - Key Bottleneck Metrics
-- Purpose: High-level overview for leadership
-- -----------------------------------------------------------------------------
SELECT
    'Overall Performance' AS metric_category,
    COUNT(*) AS total_requests,
    ROUND(AVG(total_cycle_time_hours / 24), 1) AS avg_cycle_time_days,
    ROUND(MEDIAN(total_cycle_time_hours / 24), 1) AS median_cycle_time_days,

    -- Internal vs External breakdown
    ROUND(AVG(internal_process_hours), 1) AS avg_internal_hours,
    ROUND(AVG(external_dependency_hours), 1) AS avg_external_hours,
    ROUND(AVG(internal_percentage), 1) AS avg_internal_pct,

    -- Top bottlenecks
    MODE() WITHIN GROUP (ORDER BY primary_bottleneck_stage) AS most_common_bottleneck,
    MODE() WITHIN GROUP (ORDER BY primary_bottleneck_type) AS most_common_type,

    -- SLA performance
    SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) AS sla_met_count,
    ROUND(SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_met_rate,
    SUM(CASE WHEN sla_breach_flag = TRUE THEN 1 ELSE 0 END) AS sla_breach_count,
    SUM(CASE WHEN sla_at_risk_flag = TRUE THEN 1 ELSE 0 END) AS sla_at_risk_count,

    -- Quality impact
    ROUND(AVG(qa_cycle_count), 2) AS avg_qa_cycles,
    SUM(CASE WHEN first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) AS first_pass_count,
    ROUND(SUM(CASE WHEN first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS first_pass_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks');

-- -----------------------------------------------------------------------------
-- Query 7.2: Weekly Bottleneck Trends
-- Purpose: Track bottleneck metrics week over week
-- -----------------------------------------------------------------------------
SELECT
    dd.year_week AS week,
    COUNT(*) AS request_count,

    -- Cycle time trends
    ROUND(AVG(fb.total_cycle_time_hours / 24), 1) AS avg_cycle_time_days,

    -- Top bottlenecks
    SUM(CASE WHEN fb.primary_bottleneck_stage = 'QUEUE_WAIT' THEN 1 ELSE 0 END) AS queue_bottleneck_count,
    SUM(CASE WHEN fb.primary_bottleneck_stage = 'QA_REVIEW' THEN 1 ELSE 0 END) AS qa_bottleneck_count,
    SUM(CASE WHEN fb.primary_bottleneck_stage = 'APPOINTMENT_WAIT' THEN 1 ELSE 0 END) AS appt_bottleneck_count,
    SUM(CASE WHEN fb.primary_bottleneck_stage = 'SCHEDULING' THEN 1 ELSE 0 END) AS scheduling_bottleneck_count,

    -- Internal vs External
    ROUND(AVG(fb.internal_percentage), 1) AS avg_internal_pct,

    -- Capacity indicators
    SUM(CASE WHEN fb.capacity_constraint_pattern_flag = TRUE THEN 1 ELSE 0 END) AS capacity_constraint_count,

    -- Quality indicators
    ROUND(AVG(fb.qa_cycle_count), 2) AS avg_qa_cycles,

    -- SLA performance
    ROUND(SUM(CASE WHEN fb.sla_met_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_met_rate

FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') fb
JOIN IDENTIFIER($dw_database || '.WAREHOUSE.dim_dates') dd ON fb.request_date_sk = dd.date_sk
WHERE dd.date_sk >= DATEADD(week, -12, CURRENT_DATE()) -- Last 12 weeks
GROUP BY dd.year_week
ORDER BY dd.year_week DESC;

-- -----------------------------------------------------------------------------
-- Query 7.3: Actionable Bottleneck Alerts
-- Purpose: Identify specific areas requiring immediate attention
-- -----------------------------------------------------------------------------
WITH current_performance AS (
    SELECT
        primary_bottleneck_stage,
        COUNT(*) AS recent_count,
        AVG(primary_bottleneck_hours) AS avg_hours
    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
    WHERE request_date_sk >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY primary_bottleneck_stage
),
historical_baseline AS (
    SELECT
        primary_bottleneck_stage,
        AVG(primary_bottleneck_hours) AS baseline_avg_hours,
        STDDEV(primary_bottleneck_hours) AS baseline_stddev
    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')
    WHERE request_date_sk BETWEEN DATEADD(day, -60, CURRENT_DATE()) AND DATEADD(day, -8, CURRENT_DATE())
    GROUP BY primary_bottleneck_stage
)
SELECT
    cp.primary_bottleneck_stage,
    cp.recent_count AS last_7_days_count,
    ROUND(cp.avg_hours, 1) AS current_avg_hours,
    ROUND(hb.baseline_avg_hours, 1) AS baseline_avg_hours,
    ROUND(cp.avg_hours - hb.baseline_avg_hours, 1) AS variance_hours,
    ROUND((cp.avg_hours - hb.baseline_avg_hours) / NULLIF(hb.baseline_avg_hours, 0) * 100, 1) AS variance_pct,
    CASE
        WHEN cp.avg_hours > hb.baseline_avg_hours + (2 * hb.baseline_stddev) THEN 'CRITICAL - Significant degradation'
        WHEN cp.avg_hours > hb.baseline_avg_hours + hb.baseline_stddev THEN 'WARNING - Performance declining'
        WHEN cp.avg_hours < hb.baseline_avg_hours - hb.baseline_stddev THEN 'IMPROVED - Better than baseline'
        ELSE 'NORMAL - Within expected range'
    END AS alert_status,
    'Review capacity and workload distribution' AS recommended_action
FROM current_performance cp
JOIN historical_baseline hb ON cp.primary_bottleneck_stage = hb.primary_bottleneck_stage
WHERE cp.avg_hours > hb.baseline_avg_hours + hb.baseline_stddev -- Only show degrading performance
ORDER BY variance_pct DESC;

-- =====================================================================================
-- USAGE NOTES
-- =====================================================================================
--
-- These queries are designed to be run independently or as part of a scheduled
-- monitoring process. Key use cases:
--
-- 1. Daily Operations:
--    - Query 5.2 (At-Risk Requests Dashboard) - Run daily to prioritize work
--    - Query 7.3 (Actionable Alerts) - Run daily to identify emerging issues
--
-- 2. Weekly Reviews:
--    - Query 1.1 (Stage Duration Analysis) - Weekly trend analysis
--    - Query 2.1 (Internal vs External) - Process efficiency review
--    - Query 7.2 (Weekly Trends) - Performance tracking
--
-- 3. Monthly Analysis:
--    - Query 3.1-3.3 (Capacity Analysis) - Resource planning
--    - Query 4.1-4.3 (Quality Analysis) - Training needs identification
--    - Query 6.1-6.2 (Root Cause Analysis) - Process improvement planning
--
-- 4. Executive Reporting:
--    - Query 7.1 (Executive Dashboard) - Monthly board reporting
--    - Query 1.3 (Bottleneck Trends) - Strategic planning
--
-- =====================================================================================
