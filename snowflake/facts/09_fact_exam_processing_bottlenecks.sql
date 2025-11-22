-- =====================================================================================
-- FACT TABLE: fact_exam_processing_bottlenecks
-- =====================================================================================
-- Purpose: Comprehensive bottleneck detection and analysis for exam processing
-- Grain: One row per exam request with all stage timings and bottleneck indicators
-- Type: Accumulating Snapshot (updated as requests progress through stages)
--
-- Business Questions Answered:
--   1. Which stages cause the most delays?
--   2. Where are internal VEMS bottlenecks vs external dependencies?
--   3. Which examiners/facilities have capacity constraints?
--   4. What quality issues cause rework and delays?
--   5. What are the SLA breach risk factors?
--
-- Data Sources:
--   - fact_exam_requests (request lifecycle)
--   - fact_examiner_assignments (assignment performance)
--   - fact_appointment_events (scheduling performance)
--   - fact_evaluation_qa_events (QA cycle performance)
--   - fact_evaluations_completed (exam execution)
--
-- Author: Data Team
-- Date: 2025-11-17
-- =====================================================================================

SET dw_database = (SELECT get_dw_database());

CREATE OR REPLACE TABLE IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') (

    -- ==========================================
    -- PRIMARY KEY
    -- ==========================================
    exam_request_sk NUMBER(38,0) NOT NULL,

    -- ==========================================
    -- DIMENSION FOREIGN KEYS
    -- ==========================================
    veteran_dim_sk NUMBER(38,0),
    examiner_dim_sk NUMBER(38,0),
    facility_dim_sk NUMBER(38,0),
    exam_type_dim_sk NUMBER(38,0),
    specialty_dim_sk NUMBER(38,0),
    request_date_sk NUMBER(8,0),
    completion_date_sk NUMBER(8,0),

    -- ==========================================
    -- STAGE TIMING METRICS (Hours)
    -- ==========================================

    -- Stage 1: Intake & Validation (INTERNAL VEMS)
    intake_to_validation_hours NUMBER(10,2),
    validation_duration_hours NUMBER(10,2),
    missing_info_delay_hours NUMBER(10,2),
    medical_records_delay_hours NUMBER(10,2),

    -- Stage 2: Queue & Assignment (INTERNAL VEMS)
    queue_wait_hours NUMBER(10,2),
    assignment_attempt_hours NUMBER(10,2),
    time_to_examiner_response_hours NUMBER(10,2),
    reassignment_delay_hours NUMBER(10,2),

    -- Stage 3: Scheduling (MIXED - Internal + External Veteran)
    assignment_to_scheduling_hours NUMBER(10,2),
    scheduling_coordination_hours NUMBER(10,2),
    veteran_availability_delay_hours NUMBER(10,2),
    reschedule_delay_hours NUMBER(10,2),

    -- Stage 4: Appointment Wait (EXTERNAL - Veteran Dependent)
    scheduled_to_appointment_hours NUMBER(10,2),

    -- Stage 5: Exam Execution (INTERNAL VEMS)
    exam_duration_minutes NUMBER(10,2),
    exam_complexity_delay_minutes NUMBER(10,2),

    -- Stage 6: QA Review (INTERNAL VEMS)
    exam_to_qa_submission_hours NUMBER(10,2),
    qa_initial_review_hours NUMBER(10,2),
    qa_clarification_cycle_hours NUMBER(10,2),
    qa_rework_hours NUMBER(10,2),
    total_qa_process_hours NUMBER(10,2),

    -- Stage 7: VA Delivery (EXTERNAL - VA Dependent)
    qa_approval_to_delivery_hours NUMBER(10,2),
    va_receipt_acknowledgment_hours NUMBER(10,2),

    -- Stage 8: Payment (INTERNAL VEMS)
    delivery_to_payment_hours NUMBER(10,2),

    -- Total End-to-End
    total_cycle_time_hours NUMBER(10,2),

    -- ==========================================
    -- BOTTLENECK CLASSIFICATION FLAGS
    -- ==========================================

    -- Primary Bottleneck (Slowest Stage)
    primary_bottleneck_stage VARCHAR(100),
    primary_bottleneck_hours NUMBER(10,2),
    primary_bottleneck_type VARCHAR(50), -- 'INTERNAL_VEMS', 'EXTERNAL_VA', 'EXTERNAL_VETERAN', 'MIXED'

    -- Secondary Bottleneck
    secondary_bottleneck_stage VARCHAR(100),
    secondary_bottleneck_hours NUMBER(10,2),
    secondary_bottleneck_type VARCHAR(50),

    -- Stage Performance Flags
    intake_bottleneck_flag BOOLEAN,
    validation_bottleneck_flag BOOLEAN,
    queue_bottleneck_flag BOOLEAN,
    assignment_bottleneck_flag BOOLEAN,
    scheduling_bottleneck_flag BOOLEAN,
    appointment_wait_bottleneck_flag BOOLEAN,
    exam_execution_bottleneck_flag BOOLEAN,
    qa_review_bottleneck_flag BOOLEAN,
    delivery_bottleneck_flag BOOLEAN,
    payment_bottleneck_flag BOOLEAN,

    -- Internal vs External Classification
    internal_process_hours NUMBER(10,2), -- Time in VEMS control
    external_dependency_hours NUMBER(10,2), -- Time waiting on VA/Veteran
    internal_percentage NUMBER(5,2), -- % of time in internal processes
    external_percentage NUMBER(5,2), -- % of time in external dependencies

    -- ==========================================
    -- CAPACITY & WORKLOAD BOTTLENECK INDICATORS
    -- ==========================================

    -- Examiner Capacity
    examiner_overload_flag BOOLEAN,
    examiner_workload_at_assignment NUMBER(5,0),
    examiner_utilization_pct NUMBER(5,2),
    examiner_rejection_count NUMBER(3,0),
    alternative_examiners_available NUMBER(5,0),

    -- Facility Capacity
    facility_capacity_constraint_flag BOOLEAN,
    facility_utilization_pct NUMBER(5,2),
    facility_backlog_count NUMBER(10,0),

    -- Specialty Availability
    specialty_shortage_flag BOOLEAN,
    specialty_examiner_count NUMBER(5,0),
    specialty_avg_wait_days NUMBER(10,2),

    -- ==========================================
    -- QUALITY & REWORK BOTTLENECK INDICATORS
    -- ==========================================

    qa_rework_flag BOOLEAN,
    qa_cycle_count NUMBER(3,0),
    first_pass_approval_flag BOOLEAN,
    clarification_count NUMBER(3,0),
    deficiency_count NUMBER(5,0),

    -- Quality Score Impact
    completeness_score NUMBER(5,2),
    accuracy_score NUMBER(5,2),
    overall_quality_score NUMBER(5,2),
    below_quality_threshold_flag BOOLEAN,

    -- Common Deficiency Types (causing rework)
    documentation_deficiency_flag BOOLEAN,
    nexus_opinion_deficiency_flag BOOLEAN,
    medical_rationale_deficiency_flag BOOLEAN,
    dbq_completion_deficiency_flag BOOLEAN,

    -- ==========================================
    -- SLA & URGENCY METRICS
    -- ==========================================

    sla_days_allowed NUMBER(5,0),
    sla_days_consumed NUMBER(10,2),
    days_until_sla_breach NUMBER(10,2),
    sla_met_flag BOOLEAN,
    sla_breach_flag BOOLEAN,
    sla_at_risk_flag BOOLEAN, -- Within 20% of SLA

    priority_level VARCHAR(20),
    expedite_flag BOOLEAN,

    -- ==========================================
    -- EVENT COUNTS (Complexity Indicators)
    -- ==========================================

    assignment_attempt_count NUMBER(3,0),
    reassignment_count NUMBER(3,0),
    reschedule_count NUMBER(3,0),
    cancellation_count NUMBER(3,0),
    no_show_count NUMBER(3,0),

    -- ==========================================
    -- COMPARATIVE BENCHMARKS
    -- ==========================================

    -- Compare to averages for same exam type
    variance_from_avg_cycle_time_hours NUMBER(10,2),
    variance_from_avg_qa_time_hours NUMBER(10,2),
    variance_from_avg_queue_time_hours NUMBER(10,2),

    -- Percentile Rankings
    cycle_time_percentile NUMBER(3,0), -- 1-100
    qa_time_percentile NUMBER(3,0),
    queue_time_percentile NUMBER(3,0),

    -- Performance Rating
    overall_performance_rating VARCHAR(20), -- 'EXCELLENT', 'GOOD', 'AVERAGE', 'POOR', 'CRITICAL'

    -- ==========================================
    -- ROOT CAUSE INDICATORS
    -- ==========================================

    likely_root_cause VARCHAR(200),
    contributing_factors VARCHAR(500),

    -- Pattern Flags
    chronic_reassignment_pattern_flag BOOLEAN, -- Multiple reassignments
    scheduling_difficulty_pattern_flag BOOLEAN, -- Multiple reschedules
    quality_issue_pattern_flag BOOLEAN, -- Multiple QA cycles
    capacity_constraint_pattern_flag BOOLEAN, -- High workload correlation

    -- ==========================================
    -- SOURCE SYSTEM TRACKING
    -- ==========================================

    source_system VARCHAR(20), -- 'OMS', 'VEMS', 'MERGED'
    source_system_id VARCHAR(100),

    -- ==========================================
    -- METADATA
    -- ==========================================

    record_created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    record_updated_timestamp TIMESTAMP_NTZ,
    last_stage_completed VARCHAR(100),
    request_status VARCHAR(50),

    -- ==========================================
    -- CONSTRAINTS
    -- ==========================================

    CONSTRAINT pk_fact_exam_processing_bottlenecks PRIMARY KEY (exam_request_sk),
    CONSTRAINT fk_bottleneck_veteran FOREIGN KEY (veteran_dim_sk)
        REFERENCES IDENTIFIER($dw_database || '.WAREHOUSE.dim_veterans')(veteran_sk),
    CONSTRAINT fk_bottleneck_examiner FOREIGN KEY (examiner_dim_sk)
        REFERENCES IDENTIFIER($dw_database || '.WAREHOUSE.dim_evaluators')(evaluator_sk),
    CONSTRAINT fk_bottleneck_facility FOREIGN KEY (facility_dim_sk)
        REFERENCES IDENTIFIER($dw_database || '.WAREHOUSE.dim_facilities')(facility_sk),
    CONSTRAINT fk_bottleneck_exam_type FOREIGN KEY (exam_type_dim_sk)
        REFERENCES IDENTIFIER($dw_database || '.WAREHOUSE.dim_exam_request_types')(exam_request_type_sk)
)
COMMENT = 'Comprehensive exam processing bottleneck detection and analysis'
;

-- =====================================================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================================================

-- Query by bottleneck type
CREATE INDEX idx_bottleneck_type
    ON IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')(primary_bottleneck_type);

-- Query by stage bottleneck
CREATE INDEX idx_bottleneck_stage
    ON IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')(primary_bottleneck_stage);

-- Query by SLA status
CREATE INDEX idx_sla_status
    ON IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')(sla_breach_flag, sla_at_risk_flag);

-- Query by facility
CREATE INDEX idx_facility
    ON IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')(facility_dim_sk);

-- Query by examiner
CREATE INDEX idx_examiner
    ON IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')(examiner_dim_sk);

-- Query by date range
CREATE INDEX idx_request_date
    ON IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks')(request_date_sk);

-- =====================================================================================
-- POPULATION QUERY
-- =====================================================================================

CREATE OR REPLACE VIEW IDENTIFIER($dw_database || '.WAREHOUSE.vw_populate_bottleneck_fact') AS
WITH exam_request_base AS (
    SELECT
        fer.exam_request_sk,
        fer.veteran_dim_sk,
        fer.examiner_dim_sk,
        fer.facility_dim_sk,
        fer.exam_type_dim_sk,
        fer.specialty_dim_sk,
        fer.request_received_date_sk AS request_date_sk,
        fer.request_closed_date_sk AS completion_date_sk,
        fer.source_system,
        fer.source_system_id,
        fer.current_status AS request_status,
        fer.priority_level,
        fer.expedite_flag,
        fer.sla_days_allowed,
        fer.sla_met_flag,
        fer.days_until_sla_breach,

        -- Stage Timings (converting days to hours where needed)
        fer.time_in_validation_hours AS validation_duration_hours,
        fer.time_in_queue_hours AS queue_wait_hours,
        fer.days_to_assignment * 24 AS assignment_attempt_hours,
        fer.days_to_scheduling * 24 AS assignment_to_scheduling_hours,
        fer.total_cycle_time_days * 24 AS total_cycle_time_hours,

        -- Calculate derived stage durations
        DATEDIFF(hour, fer.request_received_date_sk, fer.eligibility_confirmed_date_sk) AS intake_to_validation_hours,
        DATEDIFF(hour, fer.eligibility_confirmed_date_sk, fer.assignment_started_date_sk) AS queue_wait_hours_calc,
        DATEDIFF(hour, fer.examiner_accepted_date_sk, fer.appointment_scheduled_date_sk) AS scheduling_coordination_hours,
        DATEDIFF(hour, fer.appointment_scheduled_date_sk, fer.exam_completed_date_sk) AS scheduled_to_appointment_hours

    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_requests') fer
    WHERE fer.request_received_date_sk IS NOT NULL
),

assignment_metrics AS (
    SELECT
        fea.exam_request_sk,

        -- Assignment performance
        AVG(fea.time_to_response_hours) AS avg_time_to_examiner_response_hours,
        SUM(CASE WHEN fea.assignment_event_type = 'REJECTED' THEN 1 ELSE 0 END) AS examiner_rejection_count,
        COUNT(DISTINCT fea.assignment_event_id) AS assignment_attempt_count,
        SUM(CASE WHEN fea.assignment_event_type = 'REASSIGNED' THEN 1 ELSE 0 END) AS reassignment_count,

        -- Capacity metrics (from most recent assignment)
        MAX(fea.examiner_current_workload) AS examiner_workload_at_assignment,
        MAX(fea.examiner_utilization_percentage) AS examiner_utilization_pct,
        MAX(fea.alternative_examiners_available) AS alternative_examiners_available,

        -- Flags
        MAX(CASE WHEN fea.examiner_current_workload > 20 THEN TRUE ELSE FALSE END) AS examiner_overload_flag,

        -- Reassignment delay
        SUM(CASE WHEN fea.assignment_event_type = 'REASSIGNED'
            THEN DATEDIFF(hour, fea.assignment_event_timestamp, LEAD(fea.assignment_event_timestamp)
                OVER (PARTITION BY fea.exam_request_sk ORDER BY fea.assignment_event_timestamp))
            ELSE 0 END) AS reassignment_delay_hours

    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_examiner_assignments') fea
    GROUP BY fea.exam_request_sk
),

appointment_metrics AS (
    SELECT
        fae.exam_request_sk,

        -- Scheduling delays
        SUM(CASE WHEN fae.appointment_event_type = 'RESCHEDULED' THEN 1 ELSE 0 END) AS reschedule_count,
        SUM(CASE WHEN fae.appointment_event_type = 'CANCELLED' THEN 1 ELSE 0 END) AS cancellation_count,
        SUM(CASE WHEN fae.appointment_event_type = 'NO_SHOW' THEN 1 ELSE 0 END) AS no_show_count,

        -- Reschedule delays (time added by rescheduling)
        SUM(CASE WHEN fae.appointment_event_type = 'RESCHEDULED'
            THEN fae.wait_time_days_at_event * 24
            ELSE 0 END) AS reschedule_delay_hours,

        -- Veteran availability impact
        AVG(fae.wait_time_days_at_event * 24) AS avg_appointment_wait_hours

    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_appointment_events') fae
    GROUP BY fae.exam_request_sk
),

qa_metrics AS (
    SELECT
        feqe.exam_request_sk,

        -- QA cycle performance
        MAX(feqe.qa_cycle_number) AS qa_cycle_count,
        MAX(feqe.first_pass_approval_flag) AS first_pass_approval_flag,
        SUM(feqe.clarification_requested_count) AS clarification_count,
        SUM(feqe.deficiency_count) AS deficiency_count,

        -- QA timing
        SUM(CASE WHEN feqe.qa_event_type = 'QA_REVIEW'
            THEN DATEDIFF(hour, feqe.qa_event_timestamp, feqe.review_completed_timestamp)
            ELSE 0 END) AS qa_review_hours,

        SUM(CASE WHEN feqe.qa_event_type IN ('CLARIFICATION_REQUESTED', 'CLARIFICATION_SUBMITTED')
            THEN DATEDIFF(hour, feqe.qa_event_timestamp, LEAD(feqe.qa_event_timestamp)
                OVER (PARTITION BY feqe.exam_request_sk ORDER BY feqe.qa_event_timestamp))
            ELSE 0 END) AS qa_clarification_cycle_hours,

        -- Time from exam completion to initial QA submission
        MIN(CASE WHEN feqe.qa_event_type = 'INITIAL_SUBMISSION'
            THEN feqe.qa_event_timestamp END) AS initial_submission_timestamp,

        -- Total QA process time
        DATEDIFF(hour,
            MIN(CASE WHEN feqe.qa_event_type = 'INITIAL_SUBMISSION' THEN feqe.qa_event_timestamp END),
            MAX(CASE WHEN feqe.qa_event_type = 'APPROVED' THEN feqe.qa_event_timestamp END)
        ) AS total_qa_process_hours,

        -- Quality scores
        AVG(feqe.completeness_score) AS completeness_score,
        AVG(feqe.accuracy_score) AS accuracy_score,
        AVG(feqe.overall_quality_score) AS overall_quality_score,

        -- Deficiency flags
        MAX(CASE WHEN feqe.primary_deficiency_type = 'INCOMPLETE_DOCUMENTATION' THEN TRUE ELSE FALSE END) AS documentation_deficiency_flag,
        MAX(CASE WHEN feqe.primary_deficiency_type = 'NEXUS_OPINION_MISSING' THEN TRUE ELSE FALSE END) AS nexus_opinion_deficiency_flag,
        MAX(CASE WHEN feqe.primary_deficiency_type = 'INSUFFICIENT_MEDICAL_RATIONALE' THEN TRUE ELSE FALSE END) AS medical_rationale_deficiency_flag,
        MAX(CASE WHEN feqe.primary_deficiency_type = 'DBQ_INCOMPLETE' THEN TRUE ELSE FALSE END) AS dbq_completion_deficiency_flag

    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_evaluation_qa_events') feqe
    GROUP BY feqe.exam_request_sk
),

evaluation_metrics AS (
    SELECT
        fec.exam_request_sk,
        fec.evaluation_duration_minutes AS exam_duration_minutes,
        fec.exam_complexity_score
    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_evaluations_completed') fec
),

facility_capacity AS (
    SELECT
        fdfs.facility_dim_sk,
        fdfs.snapshot_date_sk,
        fdfs.utilization_percentage AS facility_utilization_pct,
        fdfs.active_request_count AS facility_backlog_count,
        CASE WHEN fdfs.utilization_percentage > 90 THEN TRUE ELSE FALSE END AS facility_capacity_constraint_flag
    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_daily_facility_snapshot') fdfs
    WHERE fdfs.snapshot_date_sk = (SELECT MAX(snapshot_date_sk) FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_daily_facility_snapshot'))
),

-- Calculate benchmarks for comparison
benchmarks AS (
    SELECT
        fer.exam_type_dim_sk,
        fer.specialty_dim_sk,
        AVG(fer.total_cycle_time_days * 24) AS avg_cycle_time_hours,
        AVG(fer.time_in_queue_hours) AS avg_queue_time_hours,
        STDDEV(fer.total_cycle_time_days * 24) AS stddev_cycle_time_hours
    FROM IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_requests') fer
    WHERE fer.request_closed_date_sk IS NOT NULL
    GROUP BY fer.exam_type_dim_sk, fer.specialty_dim_sk
)

SELECT
    -- Primary Key & Dimensions
    erb.exam_request_sk,
    erb.veteran_dim_sk,
    erb.examiner_dim_sk,
    erb.facility_dim_sk,
    erb.exam_type_dim_sk,
    erb.specialty_dim_sk,
    erb.request_date_sk,
    erb.completion_date_sk,

    -- ==========================================
    -- STAGE TIMING METRICS
    -- ==========================================

    -- Stage 1: Intake & Validation (INTERNAL)
    erb.intake_to_validation_hours,
    erb.validation_duration_hours,
    0 AS missing_info_delay_hours, -- TODO: Need source data
    0 AS medical_records_delay_hours, -- TODO: Need source data

    -- Stage 2: Queue & Assignment (INTERNAL)
    erb.queue_wait_hours,
    erb.assignment_attempt_hours,
    am.avg_time_to_examiner_response_hours AS time_to_examiner_response_hours,
    COALESCE(am.reassignment_delay_hours, 0) AS reassignment_delay_hours,

    -- Stage 3: Scheduling (MIXED)
    erb.assignment_to_scheduling_hours,
    erb.scheduling_coordination_hours,
    0 AS veteran_availability_delay_hours, -- TODO: Derive from appointment events
    COALESCE(apm.reschedule_delay_hours, 0) AS reschedule_delay_hours,

    -- Stage 4: Appointment Wait (EXTERNAL)
    erb.scheduled_to_appointment_hours,

    -- Stage 5: Exam Execution (INTERNAL)
    em.exam_duration_minutes,
    0 AS exam_complexity_delay_minutes, -- TODO: Define threshold

    -- Stage 6: QA Review (INTERNAL)
    0 AS exam_to_qa_submission_hours, -- TODO: Calculate from eval completion to QA submission
    qm.qa_review_hours AS qa_initial_review_hours,
    COALESCE(qm.qa_clarification_cycle_hours, 0) AS qa_clarification_cycle_hours,
    0 AS qa_rework_hours, -- TODO: Sum of clarification cycles
    COALESCE(qm.total_qa_process_hours, 0) AS total_qa_process_hours,

    -- Stage 7: VA Delivery (EXTERNAL)
    0 AS qa_approval_to_delivery_hours, -- TODO: Need source data
    0 AS va_receipt_acknowledgment_hours, -- TODO: Need source data

    -- Stage 8: Payment (INTERNAL)
    0 AS delivery_to_payment_hours, -- TODO: Need payment fact table

    -- Total
    erb.total_cycle_time_hours,

    -- ==========================================
    -- BOTTLENECK CLASSIFICATION
    -- ==========================================

    -- Determine primary bottleneck (stage with longest duration)
    CASE
        WHEN GREATEST(
            COALESCE(erb.queue_wait_hours, 0),
            COALESCE(qm.total_qa_process_hours, 0),
            COALESCE(erb.scheduling_coordination_hours, 0),
            COALESCE(erb.scheduled_to_appointment_hours, 0),
            COALESCE(erb.validation_duration_hours, 0)
        ) = COALESCE(erb.queue_wait_hours, 0) THEN 'QUEUE_WAIT'
        WHEN GREATEST(
            COALESCE(erb.queue_wait_hours, 0),
            COALESCE(qm.total_qa_process_hours, 0),
            COALESCE(erb.scheduling_coordination_hours, 0),
            COALESCE(erb.scheduled_to_appointment_hours, 0),
            COALESCE(erb.validation_duration_hours, 0)
        ) = COALESCE(qm.total_qa_process_hours, 0) THEN 'QA_REVIEW'
        WHEN GREATEST(
            COALESCE(erb.queue_wait_hours, 0),
            COALESCE(qm.total_qa_process_hours, 0),
            COALESCE(erb.scheduling_coordination_hours, 0),
            COALESCE(erb.scheduled_to_appointment_hours, 0),
            COALESCE(erb.validation_duration_hours, 0)
        ) = COALESCE(erb.scheduling_coordination_hours, 0) THEN 'SCHEDULING'
        WHEN GREATEST(
            COALESCE(erb.queue_wait_hours, 0),
            COALESCE(qm.total_qa_process_hours, 0),
            COALESCE(erb.scheduling_coordination_hours, 0),
            COALESCE(erb.scheduled_to_appointment_hours, 0),
            COALESCE(erb.validation_duration_hours, 0)
        ) = COALESCE(erb.scheduled_to_appointment_hours, 0) THEN 'APPOINTMENT_WAIT'
        ELSE 'VALIDATION'
    END AS primary_bottleneck_stage,

    GREATEST(
        COALESCE(erb.queue_wait_hours, 0),
        COALESCE(qm.total_qa_process_hours, 0),
        COALESCE(erb.scheduling_coordination_hours, 0),
        COALESCE(erb.scheduled_to_appointment_hours, 0),
        COALESCE(erb.validation_duration_hours, 0)
    ) AS primary_bottleneck_hours,

    -- Classify bottleneck type
    CASE
        WHEN GREATEST(
            COALESCE(erb.queue_wait_hours, 0),
            COALESCE(qm.total_qa_process_hours, 0),
            COALESCE(erb.scheduling_coordination_hours, 0),
            COALESCE(erb.scheduled_to_appointment_hours, 0),
            COALESCE(erb.validation_duration_hours, 0)
        ) IN (COALESCE(erb.queue_wait_hours, 0), COALESCE(qm.total_qa_process_hours, 0), COALESCE(erb.validation_duration_hours, 0))
        THEN 'INTERNAL_VEMS'
        WHEN GREATEST(
            COALESCE(erb.queue_wait_hours, 0),
            COALESCE(qm.total_qa_process_hours, 0),
            COALESCE(erb.scheduling_coordination_hours, 0),
            COALESCE(erb.scheduled_to_appointment_hours, 0),
            COALESCE(erb.validation_duration_hours, 0)
        ) = COALESCE(erb.scheduled_to_appointment_hours, 0) THEN 'EXTERNAL_VETERAN'
        ELSE 'MIXED'
    END AS primary_bottleneck_type,

    NULL AS secondary_bottleneck_stage, -- TODO: Second highest duration stage
    NULL AS secondary_bottleneck_hours,
    NULL AS secondary_bottleneck_type,

    -- Stage bottleneck flags (>75th percentile for that stage)
    FALSE AS intake_bottleneck_flag, -- TODO: Calculate percentiles
    CASE WHEN erb.validation_duration_hours > 48 THEN TRUE ELSE FALSE END AS validation_bottleneck_flag,
    CASE WHEN erb.queue_wait_hours > 72 THEN TRUE ELSE FALSE END AS queue_bottleneck_flag,
    CASE WHEN am.avg_time_to_examiner_response_hours > 24 THEN TRUE ELSE FALSE END AS assignment_bottleneck_flag,
    CASE WHEN erb.scheduling_coordination_hours > 48 THEN TRUE ELSE FALSE END AS scheduling_bottleneck_flag,
    CASE WHEN erb.scheduled_to_appointment_hours > 168 THEN TRUE ELSE FALSE END AS appointment_wait_bottleneck_flag,
    FALSE AS exam_execution_bottleneck_flag,
    CASE WHEN qm.total_qa_process_hours > 72 THEN TRUE ELSE FALSE END AS qa_review_bottleneck_flag,
    FALSE AS delivery_bottleneck_flag,
    FALSE AS payment_bottleneck_flag,

    -- Internal vs External Time Distribution
    (
        COALESCE(erb.validation_duration_hours, 0) +
        COALESCE(erb.queue_wait_hours, 0) +
        COALESCE(am.avg_time_to_examiner_response_hours, 0) +
        COALESCE(em.exam_duration_minutes / 60.0, 0) +
        COALESCE(qm.total_qa_process_hours, 0)
    ) AS internal_process_hours,

    (
        COALESCE(erb.scheduled_to_appointment_hours, 0)
    ) AS external_dependency_hours,

    CASE
        WHEN erb.total_cycle_time_hours > 0 THEN
            ((COALESCE(erb.validation_duration_hours, 0) + COALESCE(erb.queue_wait_hours, 0) +
              COALESCE(am.avg_time_to_examiner_response_hours, 0) + COALESCE(em.exam_duration_minutes / 60.0, 0) +
              COALESCE(qm.total_qa_process_hours, 0)) / erb.total_cycle_time_hours * 100)
        ELSE 0
    END AS internal_percentage,

    CASE
        WHEN erb.total_cycle_time_hours > 0 THEN
            (COALESCE(erb.scheduled_to_appointment_hours, 0) / erb.total_cycle_time_hours * 100)
        ELSE 0
    END AS external_percentage,

    -- ==========================================
    -- CAPACITY & WORKLOAD INDICATORS
    -- ==========================================

    COALESCE(am.examiner_overload_flag, FALSE) AS examiner_overload_flag,
    am.examiner_workload_at_assignment,
    am.examiner_utilization_pct,
    am.examiner_rejection_count,
    am.alternative_examiners_available,

    COALESCE(fc.facility_capacity_constraint_flag, FALSE) AS facility_capacity_constraint_flag,
    fc.facility_utilization_pct,
    fc.facility_backlog_count,

    FALSE AS specialty_shortage_flag, -- TODO: Calculate from specialty metrics
    0 AS specialty_examiner_count,
    0 AS specialty_avg_wait_days,

    -- ==========================================
    -- QUALITY & REWORK INDICATORS
    -- ==========================================

    CASE WHEN COALESCE(qm.qa_cycle_count, 0) > 1 THEN TRUE ELSE FALSE END AS qa_rework_flag,
    COALESCE(qm.qa_cycle_count, 0) AS qa_cycle_count,
    COALESCE(qm.first_pass_approval_flag, FALSE) AS first_pass_approval_flag,
    COALESCE(qm.clarification_count, 0) AS clarification_count,
    COALESCE(qm.deficiency_count, 0) AS deficiency_count,

    qm.completeness_score,
    qm.accuracy_score,
    qm.overall_quality_score,
    CASE WHEN COALESCE(qm.overall_quality_score, 100) < 70 THEN TRUE ELSE FALSE END AS below_quality_threshold_flag,

    COALESCE(qm.documentation_deficiency_flag, FALSE) AS documentation_deficiency_flag,
    COALESCE(qm.nexus_opinion_deficiency_flag, FALSE) AS nexus_opinion_deficiency_flag,
    COALESCE(qm.medical_rationale_deficiency_flag, FALSE) AS medical_rationale_deficiency_flag,
    COALESCE(qm.dbq_completion_deficiency_flag, FALSE) AS dbq_completion_deficiency_flag,

    -- ==========================================
    -- SLA & URGENCY
    -- ==========================================

    erb.sla_days_allowed,
    erb.total_cycle_time_hours / 24 AS sla_days_consumed,
    erb.days_until_sla_breach,
    erb.sla_met_flag,
    CASE WHEN erb.days_until_sla_breach < 0 THEN TRUE ELSE FALSE END AS sla_breach_flag,
    CASE WHEN erb.days_until_sla_breach BETWEEN 0 AND erb.sla_days_allowed * 0.2 THEN TRUE ELSE FALSE END AS sla_at_risk_flag,

    erb.priority_level,
    erb.expedite_flag,

    -- ==========================================
    -- EVENT COUNTS
    -- ==========================================

    COALESCE(am.assignment_attempt_count, 0) AS assignment_attempt_count,
    COALESCE(am.reassignment_count, 0) AS reassignment_count,
    COALESCE(apm.reschedule_count, 0) AS reschedule_count,
    COALESCE(apm.cancellation_count, 0) AS cancellation_count,
    COALESCE(apm.no_show_count, 0) AS no_show_count,

    -- ==========================================
    -- COMPARATIVE BENCHMARKS
    -- ==========================================

    erb.total_cycle_time_hours - COALESCE(bm.avg_cycle_time_hours, erb.total_cycle_time_hours) AS variance_from_avg_cycle_time_hours,
    COALESCE(qm.total_qa_process_hours, 0) - 48 AS variance_from_avg_qa_time_hours, -- TODO: Calculate actual avg from benchmarks
    erb.queue_wait_hours - COALESCE(bm.avg_queue_time_hours, erb.queue_wait_hours) AS variance_from_avg_queue_time_hours,

    -- Percentile (simplified - would need window function over full dataset)
    CASE
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) - COALESCE(bm.stddev_cycle_time_hours, 0) THEN 25
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) THEN 50
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) + COALESCE(bm.stddev_cycle_time_hours, 0) THEN 75
        ELSE 90
    END AS cycle_time_percentile,

    50 AS qa_time_percentile, -- TODO: Calculate actual
    50 AS queue_time_percentile, -- TODO: Calculate actual

    -- Performance rating
    CASE
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) * 0.8 THEN 'EXCELLENT'
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) THEN 'GOOD'
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) * 1.2 THEN 'AVERAGE'
        WHEN erb.total_cycle_time_hours <= COALESCE(bm.avg_cycle_time_hours, 0) * 1.5 THEN 'POOR'
        ELSE 'CRITICAL'
    END AS overall_performance_rating,

    -- ==========================================
    -- ROOT CAUSE ANALYSIS
    -- ==========================================

    CASE
        WHEN COALESCE(am.examiner_overload_flag, FALSE) = TRUE AND erb.queue_wait_hours > 72 THEN 'Examiner capacity shortage'
        WHEN COALESCE(qm.qa_cycle_count, 0) > 2 THEN 'Multiple QA rework cycles'
        WHEN COALESCE(apm.reschedule_count, 0) > 2 THEN 'Repeated scheduling difficulties'
        WHEN erb.scheduled_to_appointment_hours > 168 THEN 'Extended veteran wait time'
        WHEN COALESCE(am.reassignment_count, 0) > 1 THEN 'Multiple examiner reassignments'
        ELSE 'Standard processing variation'
    END AS likely_root_cause,

    CONCAT_WS('; ',
        CASE WHEN COALESCE(am.examiner_overload_flag, FALSE) = TRUE THEN 'High examiner workload' END,
        CASE WHEN COALESCE(fc.facility_capacity_constraint_flag, FALSE) = TRUE THEN 'Facility at capacity' END,
        CASE WHEN COALESCE(qm.qa_cycle_count, 0) > 1 THEN 'QA rework required' END,
        CASE WHEN COALESCE(apm.reschedule_count, 0) > 0 THEN 'Appointment rescheduled' END,
        CASE WHEN erb.expedite_flag = TRUE THEN 'Expedited request' END
    ) AS contributing_factors,

    -- Pattern flags
    CASE WHEN COALESCE(am.reassignment_count, 0) > 1 THEN TRUE ELSE FALSE END AS chronic_reassignment_pattern_flag,
    CASE WHEN COALESCE(apm.reschedule_count, 0) > 2 THEN TRUE ELSE FALSE END AS scheduling_difficulty_pattern_flag,
    CASE WHEN COALESCE(qm.qa_cycle_count, 0) > 2 THEN TRUE ELSE FALSE END AS quality_issue_pattern_flag,
    CASE WHEN COALESCE(am.examiner_overload_flag, FALSE) = TRUE OR COALESCE(fc.facility_capacity_constraint_flag, FALSE) = TRUE
        THEN TRUE ELSE FALSE END AS capacity_constraint_pattern_flag,

    -- ==========================================
    -- SOURCE & METADATA
    -- ==========================================

    erb.source_system,
    erb.source_system_id,
    CURRENT_TIMESTAMP() AS record_created_timestamp,
    NULL AS record_updated_timestamp,
    NULL AS last_stage_completed, -- TODO: Determine from latest milestone
    erb.request_status

FROM exam_request_base erb
LEFT JOIN assignment_metrics am ON erb.exam_request_sk = am.exam_request_sk
LEFT JOIN appointment_metrics apm ON erb.exam_request_sk = apm.exam_request_sk
LEFT JOIN qa_metrics qm ON erb.exam_request_sk = qm.exam_request_sk
LEFT JOIN evaluation_metrics em ON erb.exam_request_sk = em.exam_request_sk
LEFT JOIN facility_capacity fc ON erb.facility_dim_sk = fc.facility_dim_sk
LEFT JOIN benchmarks bm ON erb.exam_type_dim_sk = bm.exam_type_dim_sk
    AND erb.specialty_dim_sk = bm.specialty_dim_sk
;

-- =====================================================================================
-- COMMENTS
-- =====================================================================================

COMMENT ON TABLE IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks') IS
'Comprehensive fact table for identifying and analyzing bottlenecks in exam processing. Tracks timing at each stage, classifies internal vs external delays, monitors capacity constraints, and identifies root causes of delays.';

COMMENT ON COLUMN IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks').primary_bottleneck_stage IS
'The processing stage with the longest duration (QUEUE_WAIT, QA_REVIEW, SCHEDULING, APPOINTMENT_WAIT, etc.)';

COMMENT ON COLUMN IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks').primary_bottleneck_type IS
'Classification of bottleneck: INTERNAL_VEMS (controlled by VES), EXTERNAL_VA (dependent on VA), EXTERNAL_VETERAN (dependent on veteran), or MIXED';

COMMENT ON COLUMN IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks').internal_process_hours IS
'Total hours spent in VEMS-controlled processes (validation, queue, assignment, exam, QA)';

COMMENT ON COLUMN IDENTIFIER($dw_database || '.WAREHOUSE.fact_exam_processing_bottlenecks').external_dependency_hours IS
'Total hours waiting on external parties (veteran availability, VA response)';
