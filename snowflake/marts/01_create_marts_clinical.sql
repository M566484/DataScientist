-- =====================================================
-- VES Data Pipeline - Clinical Operations Mart
-- =====================================================
-- Purpose: Business-specific views and aggregations for clinical operations
-- Pattern: Pre-aggregated views optimized for business reporting
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================
-- Create Marts Schema - Clinical Operations
-- =====================================================

CREATE SCHEMA IF NOT EXISTS MARTS_CLINICAL
    COMMENT = 'Clinical operations data mart - exam quality, medical outcomes, evaluator performance';

USE SCHEMA MARTS_CLINICAL;

-- =====================================================
-- Mart View: Evaluator Performance Summary
-- =====================================================

CREATE OR REPLACE VIEW vw_evaluator_performance AS
SELECT
    -- Evaluator Demographics
    e.evaluator_sk,
    e.evaluator_id,
    e.full_name AS evaluator_name,
    e.specialty,
    e.credentials,
    e.years_of_experience,
    e.va_certified_flag,

    -- Volume Metrics
    COUNT(DISTINCT f.evaluation_id) AS total_evaluations,
    COUNT(DISTINCT CASE WHEN d.fiscal_year = YEAR(CURRENT_DATE()) THEN f.evaluation_id END) AS evaluations_current_fy,
    COUNT(DISTINCT CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN f.evaluation_id END) AS evaluations_last_3_months,

    -- Quality Metrics
    AVG(f.report_completeness_score) AS avg_completeness_score,
    SUM(CASE WHEN f.sufficient_exam_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS sufficient_exam_rate_pct,
    SUM(CASE WHEN f.nexus_opinion_provided = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS nexus_opinion_rate_pct,

    -- QA Metrics
    SUM(CASE WHEN f.qa_reviewed_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS qa_review_rate_pct,
    SUM(CASE WHEN f.first_pass_qa_approval = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN f.qa_reviewed_flag = TRUE THEN 1 ELSE 0 END), 0) AS first_pass_approval_rate_pct,
    AVG(CASE WHEN f.qa_cycles_count IS NOT NULL THEN f.qa_cycles_count ELSE NULL END) AS avg_qa_cycles,

    -- Timeliness Metrics
    AVG(f.evaluation_duration_minutes) AS avg_evaluation_duration_minutes,
    AVG(f.days_in_qa) AS avg_days_in_qa,

    -- Specialty Distribution
    COUNT(DISTINCT f.medical_condition_sk) AS conditions_evaluated_count,

    -- Telehealth Adoption
    SUM(CASE WHEN f.telehealth_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS telehealth_rate_pct,

    -- Last Evaluation
    MAX(d.full_date) AS last_evaluation_date,

    -- Current Status
    e.active_flag

FROM IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_evaluators') e
LEFT JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.fact_evaluations_completed') f
    ON e.evaluator_sk = f.evaluator_sk
LEFT JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_dates') d
    ON f.evaluation_date_sk = d.date_sk
WHERE e.is_current = TRUE
GROUP BY
    e.evaluator_sk,
    e.evaluator_id,
    e.full_name,
    e.specialty,
    e.credentials,
    e.years_of_experience,
    e.va_certified_flag,
    e.active_flag;

COMMENT ON VIEW vw_evaluator_performance IS 'Evaluator performance summary - quality, volume, and timeliness metrics';

-- =====================================================
-- Mart View: Exam Request Performance
-- =====================================================

CREATE OR REPLACE VIEW vw_exam_request_performance AS
SELECT
    -- Time Period
    d.fiscal_year,
    d.fiscal_quarter,
    d.year_month,

    -- Request Type
    rt.request_type_name,
    rt.request_category,
    rt.priority_level,

    -- Volume Metrics
    COUNT(DISTINCT f.exam_request_id) AS total_requests,
    COUNT(DISTINCT CASE WHEN f.eligibility_status = 'ELIGIBLE' THEN f.exam_request_id END) AS eligible_requests,
    COUNT(DISTINCT CASE WHEN f.completed_flag = TRUE THEN f.exam_request_id END) AS completed_requests,

    -- Assignment Metrics
    AVG(f.days_to_assignment) AS avg_days_to_assignment,
    AVG(f.assignment_attempts) AS avg_assignment_attempts,
    SUM(CASE WHEN f.assignment_rejections > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS assignment_rejection_rate_pct,

    -- Scheduling Metrics
    AVG(f.days_to_scheduling) AS avg_days_to_scheduling,
    SUM(CASE WHEN f.scheduled_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS scheduling_rate_pct,

    -- SLA Metrics
    AVG(f.total_cycle_time_days) AS avg_total_cycle_time_days,
    AVG(rt.default_sla_days) AS avg_sla_days_allowed,
    SUM(CASE WHEN f.sla_met_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN f.completed_flag = TRUE THEN 1 ELSE 0 END), 0) AS sla_compliance_rate_pct,
    AVG(CASE WHEN f.sla_met_flag = FALSE THEN f.sla_variance_days ELSE NULL END) AS avg_sla_breach_days,

    -- Complexity
    SUM(CASE WHEN f.complex_case_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS complex_case_rate_pct,
    SUM(CASE WHEN f.requires_specialist_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS specialist_required_rate_pct

FROM IDENTIFIER(get_dw_database() || '.WAREHOUSE.fact_exam_requests') f
JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_dates') d
    ON f.request_received_date_sk = d.date_sk
LEFT JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_exam_request_types') rt
    ON f.exam_request_type_sk = rt.exam_request_type_sk
GROUP BY
    d.fiscal_year,
    d.fiscal_quarter,
    d.year_month,
    rt.request_type_name,
    rt.request_category,
    rt.priority_level;

COMMENT ON VIEW vw_exam_request_performance IS 'Exam request performance metrics - volume, SLA compliance, and assignment efficiency';

-- =====================================================
-- Mart View: QA Performance Metrics
-- =====================================================

CREATE OR REPLACE VIEW vw_qa_performance_metrics AS
SELECT
    -- Time Period
    d.fiscal_year,
    d.fiscal_quarter,
    d.year_month,

    -- QA Reviewer
    qa.qa_reviewer_id,
    qa.qa_reviewer_name,

    -- Volume Metrics
    COUNT(DISTINCT qa.evaluation_id) AS evaluations_reviewed,
    COUNT(DISTINCT qa.qa_event_id) AS total_qa_events,
    AVG(qa.qa_cycle_number) AS avg_qa_cycles_per_evaluation,

    -- Quality Scores
    AVG(qa.overall_quality_score) AS avg_overall_quality_score,
    AVG(qa.completeness_score) AS avg_completeness_score,
    AVG(qa.accuracy_score) AS avg_accuracy_score,

    -- Outcome Metrics
    SUM(CASE WHEN qa.review_outcome = 'APPROVED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS approval_rate_pct,
    SUM(CASE WHEN qa.first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT qa.evaluation_id), 0) AS first_pass_approval_rate_pct,
    SUM(CASE WHEN qa.clarification_requested_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS clarification_request_rate_pct,

    -- Deficiency Metrics
    SUM(CASE WHEN qa.deficiency_found_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS deficiency_rate_pct,
    AVG(CASE WHEN qa.deficiency_count IS NOT NULL THEN qa.deficiency_count ELSE NULL END) AS avg_deficiencies_per_review,

    -- Timeliness Metrics
    AVG(qa.review_duration_hours) AS avg_review_duration_hours,
    AVG(qa.turnaround_time_hours) AS avg_turnaround_time_hours,
    SUM(CASE WHEN qa.turnaround_time_hours <= 48 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS sla_48hr_compliance_rate_pct

FROM IDENTIFIER(get_dw_database() || '.WAREHOUSE.fact_evaluation_qa_events') qa
JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_dates') d
    ON qa.event_date_sk = d.date_sk
WHERE qa.event_type IN ('QA_REVIEW_STARTED', 'QA_REVIEW_COMPLETED')
GROUP BY
    d.fiscal_year,
    d.fiscal_quarter,
    d.year_month,
    qa.qa_reviewer_id,
    qa.qa_reviewer_name;

COMMENT ON VIEW vw_qa_performance_metrics IS 'QA performance metrics - review volume, quality scores, and turnaround times';

-- =====================================================
-- Mart View: Appointment Lifecycle Analytics
-- =====================================================

CREATE OR REPLACE VIEW vw_appointment_lifecycle_analytics AS
SELECT
    -- Time Period
    d.fiscal_year,
    d.fiscal_quarter,

    -- Appointment Type
    ae.appointment_location_type,

    -- Volume Metrics
    COUNT(DISTINCT ae.appointment_id) AS total_appointments,
    COUNT(DISTINCT CASE WHEN ae.event_type = 'SCHEDULED' THEN ae.appointment_id END) AS scheduled_count,
    COUNT(DISTINCT CASE WHEN ae.event_type = 'COMPLETED' THEN ae.appointment_id END) AS completed_count,
    COUNT(DISTINCT CASE WHEN ae.event_type = 'CANCELLED' THEN ae.appointment_id END) AS cancelled_count,
    COUNT(DISTINCT CASE WHEN ae.event_type = 'NO_SHOW' THEN ae.appointment_id END) AS no_show_count,
    COUNT(DISTINCT CASE WHEN ae.event_type = 'RESCHEDULED' THEN ae.appointment_id END) AS rescheduled_count,

    -- Completion Rates
    COUNT(DISTINCT CASE WHEN ae.event_type = 'COMPLETED' THEN ae.appointment_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN ae.event_type = 'SCHEDULED' THEN ae.appointment_id END), 0) AS completion_rate_pct,

    -- No-Show Rates
    COUNT(DISTINCT CASE WHEN ae.event_type = 'NO_SHOW' THEN ae.appointment_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN ae.event_type = 'SCHEDULED' THEN ae.appointment_id END), 0) AS no_show_rate_pct,

    -- Cancellation Rates
    COUNT(DISTINCT CASE WHEN ae.event_type = 'CANCELLED' THEN ae.appointment_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN ae.event_type = 'SCHEDULED' THEN ae.appointment_id END), 0) AS cancellation_rate_pct,

    -- Rescheduling Rates
    COUNT(DISTINCT CASE WHEN ae.event_type = 'RESCHEDULED' THEN ae.appointment_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT CASE WHEN ae.event_type = 'SCHEDULED' THEN ae.appointment_id END), 0) AS rescheduling_rate_pct,

    -- Average Duration (for completed appointments)
    AVG(CASE WHEN ae.event_type = 'COMPLETED' THEN ae.duration_minutes ELSE NULL END) AS avg_appointment_duration_minutes

FROM IDENTIFIER(get_dw_database() || '.WAREHOUSE.fact_appointment_events') ae
JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_dates') d
    ON ae.event_date_sk = d.date_sk
GROUP BY
    d.fiscal_year,
    d.fiscal_quarter,
    ae.appointment_location_type;

COMMENT ON VIEW vw_appointment_lifecycle_analytics IS 'Appointment lifecycle analytics - completion, no-show, cancellation, and rescheduling rates';

-- =====================================================
-- Mart View: Medical Condition Analytics
-- =====================================================

CREATE OR REPLACE VIEW vw_medical_condition_analytics AS
SELECT
    -- Condition Information
    mc.condition_name,
    mc.body_system,
    mc.specialty_required,

    -- Volume Metrics
    COUNT(DISTINCT f.evaluation_id) AS total_evaluations,
    COUNT(DISTINCT f.veteran_sk) AS unique_veterans,

    -- Service Connection Metrics
    SUM(CASE WHEN f.service_connected_opinion = 'YES' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS service_connected_rate_pct,

    -- Disability Rating Distribution
    AVG(CASE WHEN f.recommended_rating_percentage IS NOT NULL THEN f.recommended_rating_percentage ELSE NULL END) AS avg_recommended_rating,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.recommended_rating_percentage) AS median_recommended_rating,

    -- Severity Distribution
    SUM(CASE WHEN f.current_severity = 'MILD' THEN 1 ELSE 0 END) AS mild_count,
    SUM(CASE WHEN f.current_severity = 'MODERATE' THEN 1 ELSE 0 END) AS moderate_count,
    SUM(CASE WHEN f.current_severity = 'SEVERE' THEN 1 ELSE 0 END) AS severe_count,

    -- Quality Metrics
    AVG(f.report_completeness_score) AS avg_completeness_score,
    SUM(CASE WHEN f.sufficient_exam_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS sufficient_exam_rate_pct,

    -- Exam Duration
    AVG(f.evaluation_duration_minutes) AS avg_evaluation_duration_minutes,

    -- Complexity Indicator
    CASE
        WHEN AVG(f.evaluation_duration_minutes) > 120 THEN 'HIGH COMPLEXITY'
        WHEN AVG(f.evaluation_duration_minutes) > 60 THEN 'MODERATE COMPLEXITY'
        ELSE 'LOW COMPLEXITY'
    END AS complexity_level

FROM IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_medical_conditions') mc
JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.fact_evaluations_completed') f
    ON mc.medical_condition_sk = f.medical_condition_sk
GROUP BY
    mc.condition_name,
    mc.body_system,
    mc.specialty_required
HAVING COUNT(*) >= 10  -- Only show conditions with sufficient sample size
ORDER BY total_evaluations DESC;

COMMENT ON VIEW vw_medical_condition_analytics IS 'Medical condition analytics - service connection rates, severity distribution, and complexity';

-- =====================================================
-- Mart View: Facility Performance Dashboard
-- =====================================================

CREATE OR REPLACE VIEW vw_facility_performance_dashboard AS
SELECT
    -- Facility Information
    fac.facility_id,
    fac.facility_name,
    fac.facility_type,
    fac.state,
    fac.visn_name,

    -- Current Period (Last 30 Days)
    COUNT(DISTINCT CASE WHEN d.full_date >= DATEADD(day, -30, CURRENT_DATE()) THEN f.evaluation_id END) AS evaluations_last_30_days,
    COUNT(DISTINCT CASE WHEN d.full_date >= DATEADD(day, -30, CURRENT_DATE()) THEN f.veteran_sk END) AS veterans_served_last_30_days,

    -- Current Fiscal Year
    COUNT(DISTINCT CASE WHEN d.fiscal_year = YEAR(CURRENT_DATE()) THEN f.evaluation_id END) AS evaluations_current_fy,

    -- Quality Metrics
    AVG(f.report_completeness_score) AS avg_completeness_score,
    SUM(CASE WHEN f.sufficient_exam_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS sufficient_exam_rate_pct,
    SUM(CASE WHEN f.first_pass_qa_approval = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN f.qa_reviewed_flag = TRUE THEN 1 ELSE 0 END), 0) AS first_pass_qa_approval_rate_pct,

    -- Timeliness
    AVG(f.evaluation_duration_minutes) AS avg_evaluation_duration_minutes,
    AVG(f.days_in_qa) AS avg_days_in_qa,

    -- Telehealth Adoption
    SUM(CASE WHEN f.telehealth_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS telehealth_rate_pct,

    -- Staff Count
    COUNT(DISTINCT f.evaluator_sk) AS evaluator_count,

    -- Status
    fac.active_flag

FROM IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_facilities') fac
LEFT JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.fact_evaluations_completed') f
    ON fac.facility_sk = f.facility_sk
LEFT JOIN IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_dates') d
    ON f.evaluation_date_sk = d.date_sk
WHERE fac.is_current = TRUE
GROUP BY
    fac.facility_id,
    fac.facility_name,
    fac.facility_type,
    fac.state,
    fac.visn_name,
    fac.active_flag;

COMMENT ON VIEW vw_facility_performance_dashboard IS 'Facility performance dashboard - volume, quality, and operational metrics';

-- =====================================================
-- Verification
-- =====================================================

-- Show all mart views
SELECT
    table_name,
    comment
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'MARTS_CLINICAL'
ORDER BY table_name;
