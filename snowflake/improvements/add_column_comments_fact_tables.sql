-- =====================================================================================================================
-- ADD COLUMN COMMENTS TO FACT TABLES
-- =====================================================================================================================
-- Purpose: Add comprehensive column-level documentation to all fact tables
-- Priority: 2 (High)
-- Date: 2025-11-22
--
-- Benefits:
-- - Improves data dictionary completeness
-- - Self-documenting schema
-- - Easier for analysts to understand data
-- - Better metadata for BI tools
--
-- Author: Phase 2 Improvements
-- =====================================================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA WAREHOUSE;

-- =====================================================================================================================
-- FACT TABLE: fact_evaluations_completed
-- =====================================================================================================================

COMMENT ON COLUMN fact_evaluations_completed.evaluation_sk IS 'Surrogate key for evaluation fact (primary key)';
COMMENT ON COLUMN fact_evaluations_completed.evaluation_id IS 'Degenerate dimension - business identifier for evaluation';
COMMENT ON COLUMN fact_evaluations_completed.exam_request_id IS 'Degenerate dimension - business identifier for exam request';
COMMENT ON COLUMN fact_evaluations_completed.dbq_form_id IS 'Degenerate dimension - DBQ form identifier';

-- Dimension foreign keys
COMMENT ON COLUMN fact_evaluations_completed.veteran_sk IS 'Foreign key to dim_veterans (who was evaluated)';
COMMENT ON COLUMN fact_evaluations_completed.evaluator_sk IS 'Foreign key to dim_evaluators (who performed evaluation)';
COMMENT ON COLUMN fact_evaluations_completed.facility_sk IS 'Foreign key to dim_facilities (where evaluation occurred)';
COMMENT ON COLUMN fact_evaluations_completed.evaluation_type_sk IS 'Foreign key to dim_evaluation_types (type of evaluation)';
COMMENT ON COLUMN fact_evaluations_completed.medical_condition_sk IS 'Foreign key to dim_medical_conditions (condition evaluated)';
COMMENT ON COLUMN fact_evaluations_completed.evaluation_date_sk IS 'Foreign key to dim_dates (when evaluation occurred)';
COMMENT ON COLUMN fact_evaluations_completed.scheduled_date_sk IS 'Foreign key to dim_dates (when evaluation was scheduled)';
COMMENT ON COLUMN fact_evaluations_completed.claim_date_sk IS 'Foreign key to dim_dates (claim submission date)';

-- Metrics
COMMENT ON COLUMN fact_evaluations_completed.evaluation_duration_minutes IS 'Actual duration of evaluation in minutes';
COMMENT ON COLUMN fact_evaluations_completed.scheduled_duration_minutes IS 'Originally scheduled duration in minutes';
COMMENT ON COLUMN fact_evaluations_completed.variance_minutes IS 'Difference between actual and scheduled duration (can be negative)';
COMMENT ON COLUMN fact_evaluations_completed.days_from_request_to_schedule IS 'Wait time from request to scheduled date in days';
COMMENT ON COLUMN fact_evaluations_completed.days_from_schedule_to_evaluation IS 'Days between scheduled and actual evaluation';
COMMENT ON COLUMN fact_evaluations_completed.total_wait_days IS 'Total wait time from request to evaluation completion';
COMMENT ON COLUMN fact_evaluations_completed.evaluation_cost_amount IS 'Cost of the evaluation';
COMMENT ON COLUMN fact_evaluations_completed.contractor_payment_amount IS 'Amount paid to contractor for evaluation';

-- Attributes
COMMENT ON COLUMN fact_evaluations_completed.evaluation_location_type IS 'Type of location (IN_PERSON, TELEHEALTH, etc.)';
COMMENT ON COLUMN fact_evaluations_completed.telehealth_flag IS 'TRUE if evaluation was conducted via telehealth';
COMMENT ON COLUMN fact_evaluations_completed.evaluation_completed_flag IS 'TRUE if evaluation was completed successfully';
COMMENT ON COLUMN fact_evaluations_completed.dbq_submitted_flag IS 'TRUE if DBQ form was submitted';
COMMENT ON COLUMN fact_evaluations_completed.nexus_opinion_provided IS 'TRUE if nexus opinion was provided';
COMMENT ON COLUMN fact_evaluations_completed.service_connected_opinion IS 'Opinion on service connection (YES, NO, AT_LEAST_AS_LIKELY)';
COMMENT ON COLUMN fact_evaluations_completed.sufficient_exam_flag IS 'TRUE if exam was sufficient for rating decision';
COMMENT ON COLUMN fact_evaluations_completed.qa_reviewed_flag IS 'TRUE if evaluation underwent QA review';
COMMENT ON COLUMN fact_evaluations_completed.first_pass_qa_approval IS 'TRUE if approved on first QA review';
COMMENT ON COLUMN fact_evaluations_completed.va_delivery_confirmed IS 'TRUE if delivery to VA was confirmed';

SELECT 'Column comments added to fact_evaluations_completed' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_claim_status
-- =====================================================================================================================

COMMENT ON COLUMN fact_claim_status.claim_status_sk IS 'Surrogate key for claim status fact (primary key)';
COMMENT ON COLUMN fact_claim_status.claim_id IS 'Degenerate dimension - business identifier for claim';
COMMENT ON COLUMN fact_claim_status.veteran_sk IS 'Foreign key to dim_veterans (claimant)';
COMMENT ON COLUMN fact_claim_status.claim_sk IS 'Foreign key to dim_claims (claim details)';
COMMENT ON COLUMN fact_claim_status.status_date_sk IS 'Foreign key to dim_dates (date status was recorded)';
COMMENT ON COLUMN fact_claim_status.claim_status IS 'Current status of claim (SUBMITTED, IN_REVIEW, APPROVED, DENIED, etc.)';
COMMENT ON COLUMN fact_claim_status.status_duration_days IS 'Number of days claim has been in this status';
COMMENT ON COLUMN fact_claim_status.total_claim_duration_days IS 'Total days since claim submission';
COMMENT ON COLUMN fact_claim_status.is_current_status IS 'TRUE if this is the current status of the claim';

SELECT 'Column comments added to fact_claim_status' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_appointments
-- =====================================================================================================================

COMMENT ON COLUMN fact_appointments.appointment_sk IS 'Surrogate key for appointment fact (primary key)';
COMMENT ON COLUMN fact_appointments.appointment_id IS 'Degenerate dimension - business identifier for appointment';
COMMENT ON COLUMN fact_appointments.veteran_sk IS 'Foreign key to dim_veterans (patient)';
COMMENT ON COLUMN fact_appointments.evaluator_sk IS 'Foreign key to dim_evaluators (provider)';
COMMENT ON COLUMN fact_appointments.facility_sk IS 'Foreign key to dim_facilities (location)';
COMMENT ON COLUMN fact_appointments.appointment_date_sk IS 'Foreign key to dim_dates (appointment date)';
COMMENT ON COLUMN fact_appointments.appointment_type IS 'Type of appointment (C&P_EXAM, CONSULTATION, etc.)';
COMMENT ON COLUMN fact_appointments.appointment_status IS 'Status (SCHEDULED, COMPLETED, CANCELLED, NO_SHOW)';
COMMENT ON COLUMN fact_appointments.scheduled_duration_minutes IS 'Scheduled appointment duration';
COMMENT ON COLUMN fact_appointments.actual_duration_minutes IS 'Actual appointment duration';
COMMENT ON COLUMN fact_appointments.wait_time_minutes IS 'Time veteran waited past scheduled start';
COMMENT ON COLUMN fact_appointments.telehealth_flag IS 'TRUE if appointment is via telehealth';
COMMENT ON COLUMN fact_appointments.completed_flag IS 'TRUE if appointment was completed';
COMMENT ON COLUMN fact_appointments.cancelled_flag IS 'TRUE if appointment was cancelled';
COMMENT ON COLUMN fact_appointments.no_show_flag IS 'TRUE if veteran did not show up';
COMMENT ON COLUMN fact_appointments.same_day_cancel_flag IS 'TRUE if cancelled same day';

SELECT 'Column comments added to fact_appointments' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_daily_snapshot
-- =====================================================================================================================

COMMENT ON COLUMN fact_daily_snapshot.snapshot_sk IS 'Surrogate key for daily snapshot (primary key)';
COMMENT ON COLUMN fact_daily_snapshot.snapshot_date_sk IS 'Foreign key to dim_dates (snapshot date)';
COMMENT ON COLUMN fact_daily_snapshot.facility_sk IS 'Foreign key to dim_facilities (facility for metrics)';
COMMENT ON COLUMN fact_daily_snapshot.total_veterans_enrolled IS 'Count of enrolled veterans as of snapshot date';
COMMENT ON COLUMN fact_daily_snapshot.total_active_evaluators IS 'Count of active evaluators as of snapshot date';
COMMENT ON COLUMN fact_daily_snapshot.total_pending_requests IS 'Count of pending exam requests';
COMMENT ON COLUMN fact_daily_snapshot.total_in_progress_evals IS 'Count of evaluations in progress';
COMMENT ON COLUMN fact_daily_snapshot.total_completed_evals_mtd IS 'Evaluations completed month-to-date';
COMMENT ON COLUMN fact_daily_snapshot.total_completed_evals_ytd IS 'Evaluations completed year-to-date';
COMMENT ON COLUMN fact_daily_snapshot.avg_wait_days_mtd IS 'Average wait time in days (month-to-date)';
COMMENT ON COLUMN fact_daily_snapshot.avg_cycle_time_days_mtd IS 'Average cycle time in days (month-to-date)';
COMMENT ON COLUMN fact_daily_snapshot.sla_compliance_pct_mtd IS 'SLA compliance percentage (month-to-date)';

SELECT 'Column comments added to fact_daily_snapshot' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_appointment_events
-- =====================================================================================================================

COMMENT ON COLUMN fact_appointment_events.event_sk IS 'Surrogate key for appointment event (primary key)';
COMMENT ON COLUMN fact_appointment_events.event_id IS 'Degenerate dimension - business identifier for event';
COMMENT ON COLUMN fact_appointment_events.appointment_id IS 'Degenerate dimension - related appointment identifier';
COMMENT ON COLUMN fact_appointment_events.appointment_sk IS 'Foreign key to dim_appointments';
COMMENT ON COLUMN fact_appointment_events.veteran_sk IS 'Foreign key to dim_veterans';
COMMENT ON COLUMN fact_appointment_events.evaluator_sk IS 'Foreign key to dim_evaluators';
COMMENT ON COLUMN fact_appointment_events.facility_sk IS 'Foreign key to dim_facilities';
COMMENT ON COLUMN fact_appointment_events.event_date_sk IS 'Foreign key to dim_dates (event date)';
COMMENT ON COLUMN fact_appointment_events.event_type IS 'Type of event (SCHEDULED, CONFIRMED, RESCHEDULED, CANCELLED, NO_SHOW, COMPLETED)';
COMMENT ON COLUMN fact_appointment_events.event_status IS 'Status of the event';
COMMENT ON COLUMN fact_appointment_events.event_sequence_number IS 'Sequence number of event for this appointment';
COMMENT ON COLUMN fact_appointment_events.rescheduling_reason IS 'Reason for rescheduling if applicable';
COMMENT ON COLUMN fact_appointment_events.cancellation_reason IS 'Reason for cancellation if applicable';
COMMENT ON COLUMN fact_appointment_events.no_show_flag IS 'TRUE if this was a no-show event';
COMMENT ON COLUMN fact_appointment_events.veteran_initiated_flag IS 'TRUE if event was initiated by veteran';

SELECT 'Column comments added to fact_appointment_events' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_evaluation_qa_events
-- =====================================================================================================================

COMMENT ON COLUMN fact_evaluation_qa_events.qa_event_sk IS 'Surrogate key for QA event (primary key)';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_event_id IS 'Degenerate dimension - business identifier for QA event';
COMMENT ON COLUMN fact_evaluation_qa_events.evaluation_id IS 'Degenerate dimension - related evaluation identifier';
COMMENT ON COLUMN fact_evaluation_qa_events.evaluation_sk IS 'Foreign key to fact_evaluations_completed';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_reviewer_sk IS 'Foreign key to dim_evaluators (QA reviewer)';
COMMENT ON COLUMN fact_evaluation_qa_events.event_date_sk IS 'Foreign key to dim_dates (QA event date)';
COMMENT ON COLUMN fact_evaluation_qa_events.event_type IS 'Type of QA event (REVIEW_STARTED, DEFICIENCY_FOUND, APPROVED, REJECTED)';
COMMENT ON COLUMN fact_evaluation_qa_events.event_status IS 'Status of the QA event';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_cycle_number IS 'Which QA review cycle (1st, 2nd, etc.)';
COMMENT ON COLUMN fact_evaluation_qa_events.review_outcome IS 'Outcome of review (APPROVED, CLARIFICATION_NEEDED, REJECTED)';
COMMENT ON COLUMN fact_evaluation_qa_events.overall_quality_score IS 'Overall quality score (0-100)';
COMMENT ON COLUMN fact_evaluation_qa_events.deficiency_count IS 'Number of deficiencies found';
COMMENT ON COLUMN fact_evaluation_qa_events.deficiency_severity IS 'Severity of deficiencies (LOW, MEDIUM, HIGH, CRITICAL)';
COMMENT ON COLUMN fact_evaluation_qa_events.approved_flag IS 'TRUE if evaluation was approved';
COMMENT ON COLUMN fact_evaluation_qa_events.first_pass_approval_flag IS 'TRUE if approved on first QA review';
COMMENT ON COLUMN fact_evaluation_qa_events.review_duration_hours IS 'Duration of QA review in hours';

SELECT 'Column comments added to fact_evaluation_qa_events' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_exam_requests
-- =====================================================================================================================

COMMENT ON COLUMN fact_exam_requests.exam_request_sk IS 'Surrogate key for exam request (primary key)';
COMMENT ON COLUMN fact_exam_requests.exam_request_id IS 'Degenerate dimension - business identifier for request';
COMMENT ON COLUMN fact_exam_requests.veteran_sk IS 'Foreign key to dim_veterans (requester)';
COMMENT ON COLUMN fact_exam_requests.facility_sk IS 'Foreign key to dim_facilities (requesting facility)';
COMMENT ON COLUMN fact_exam_requests.request_received_date_sk IS 'Foreign key to dim_dates (request received)';
COMMENT ON COLUMN fact_exam_requests.request_priority IS 'Priority level (ROUTINE, PRIORITY, URGENT)';
COMMENT ON COLUMN fact_exam_requests.requested_conditions_count IS 'Number of conditions requested for evaluation';
COMMENT ON COLUMN fact_exam_requests.complex_case_flag IS 'TRUE if case is marked as complex';
COMMENT ON COLUMN fact_exam_requests.specialist_required_flag IS 'TRUE if specialist evaluation required';
COMMENT ON COLUMN fact_exam_requests.days_to_assignment IS 'Days from request to evaluator assignment';
COMMENT ON COLUMN fact_exam_requests.days_to_scheduling IS 'Days from request to appointment scheduled';
COMMENT ON COLUMN fact_exam_requests.total_cycle_time_days IS 'Total days from request to completion';
COMMENT ON COLUMN fact_exam_requests.sla_days_allowed IS 'SLA target days for this request type';
COMMENT ON COLUMN fact_exam_requests.sla_met_flag IS 'TRUE if SLA was met';
COMMENT ON COLUMN fact_exam_requests.sla_variance_days IS 'Difference from SLA (negative = early, positive = late)';

SELECT 'Column comments added to fact_exam_requests' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_examiner_assignments
-- =====================================================================================================================

COMMENT ON COLUMN fact_examiner_assignments.assignment_sk IS 'Surrogate key for assignment (primary key)';
COMMENT ON COLUMN fact_examiner_assignments.assignment_id IS 'Degenerate dimension - business identifier for assignment';
COMMENT ON COLUMN fact_examiner_assignments.exam_request_id IS 'Degenerate dimension - related exam request';
COMMENT ON COLUMN fact_examiner_assignments.evaluator_sk IS 'Foreign key to dim_evaluators (assigned evaluator)';
COMMENT ON COLUMN fact_examiner_assignments.veteran_sk IS 'Foreign key to dim_veterans';
COMMENT ON COLUMN fact_examiner_assignments.facility_sk IS 'Foreign key to dim_facilities';
COMMENT ON COLUMN fact_examiner_assignments.assignment_date_sk IS 'Foreign key to dim_dates (assignment date)';
COMMENT ON COLUMN fact_examiner_assignments.assignment_method IS 'How assignment was made (AUTO, MANUAL, SPECIALTY_MATCH)';
COMMENT ON COLUMN fact_examiner_assignments.assignment_status IS 'Status (ASSIGNED, ACCEPTED, REJECTED, COMPLETED)';
COMMENT ON COLUMN fact_examiner_assignments.assignment_attempt_number IS 'Which assignment attempt (if reassigned)';
COMMENT ON COLUMN fact_examiner_assignments.accepted_flag IS 'TRUE if evaluator accepted assignment';
COMMENT ON COLUMN fact_examiner_assignments.rejected_flag IS 'TRUE if evaluator rejected assignment';
COMMENT ON COLUMN fact_examiner_assignments.rejection_reason IS 'Reason for rejection if applicable';
COMMENT ON COLUMN fact_examiner_assignments.auto_assigned_flag IS 'TRUE if automatically assigned by system';
COMMENT ON COLUMN fact_examiner_assignments.specialty_match_flag IS 'TRUE if evaluator specialty matches requirement';

SELECT 'Column comments added to fact_examiner_assignments' AS status;

-- =====================================================================================================================
-- FACT TABLE: fact_exam_processing_bottlenecks
-- =====================================================================================================================

COMMENT ON COLUMN fact_exam_processing_bottlenecks.bottleneck_sk IS 'Surrogate key for bottleneck (primary key)';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.bottleneck_id IS 'Degenerate dimension - business identifier';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.exam_request_id IS 'Degenerate dimension - affected exam request';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.facility_sk IS 'Foreign key to dim_facilities (where bottleneck occurred)';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.detection_date_sk IS 'Foreign key to dim_dates (when bottleneck detected)';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.bottleneck_type_sk IS 'Foreign key to dim_bottleneck_types';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.bottleneck_stage IS 'Stage where bottleneck occurred (ASSIGNMENT, SCHEDULING, EVALUATION, QA)';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.delay_days IS 'Number of days delayed due to bottleneck';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.bottleneck_severity IS 'Severity (LOW, MEDIUM, HIGH, CRITICAL)';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.resolved_flag IS 'TRUE if bottleneck has been resolved';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.resolution_date IS 'Date bottleneck was resolved';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.resolution_time_days IS 'Days to resolve bottleneck';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.root_cause IS 'Root cause of bottleneck';
COMMENT ON COLUMN fact_exam_processing_bottlenecks.preventable_flag IS 'TRUE if bottleneck was preventable';

SELECT 'Column comments added to fact_exam_processing_bottlenecks' AS status;

-- =====================================================================================================================
-- SUMMARY
-- =====================================================================================================================

SELECT '========================================' AS summary;
SELECT 'COLUMN COMMENTS ADDED SUCCESSFULLY' AS summary;
SELECT '========================================' AS summary;

-- Count commented columns per fact table
SELECT
    table_name,
    COUNT(*) AS columns_with_comments
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'fact_%'
  AND comment IS NOT NULL
GROUP BY table_name
ORDER BY table_name;

SELECT 'All fact tables now have comprehensive column-level documentation' AS status;

-- =====================================================================================================================
-- END OF SCRIPT
-- =====================================================================================================================
