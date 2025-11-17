-- =====================================================
-- fact_evaluation_qa_events - Evaluation QA Lifecycle Events
-- =====================================================
-- Purpose: Track complete history of quality assurance review process for evaluations
-- Grain: One row per QA event (submission, review, clarification request, clarification submission, approval, rejection)
-- Type: Transaction Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0
-- Complements: fact_evaluations_completed (accumulating snapshot)

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE fact_evaluation_qa_events (
    qa_event_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    veteran_sk INTEGER NOT NULL,
    evaluator_sk INTEGER NOT NULL,  -- Original examiner
    facility_sk INTEGER NOT NULL,
    evaluation_type_sk INTEGER,
    medical_condition_sk INTEGER,
    claim_sk INTEGER,

    -- QA Reviewer (different from original evaluator)
    qa_reviewer_sk INTEGER,  -- FK to dim_evaluators for QA staff

    -- Date Foreign Keys
    event_date_sk INTEGER NOT NULL,  -- Date this QA event occurred
    evaluation_date_sk INTEGER,  -- Date original evaluation occurred

    -- Degenerate Dimensions
    evaluation_id VARCHAR(50) NOT NULL,  -- NOT UNIQUE - multiple QA events per evaluation
    qa_event_id VARCHAR(50) NOT NULL UNIQUE,  -- Unique QA event identifier
    qa_cycle_id VARCHAR(50),  -- Groups events in same QA cycle
    dbq_form_id VARCHAR(50),
    submission_id VARCHAR(50),  -- Unique ID for each submission attempt

    -- Event Details
    event_type VARCHAR(50) NOT NULL,  -- INITIAL_SUBMISSION, QA_REVIEW_STARTED, QA_REVIEW_COMPLETED, CLARIFICATION_REQUESTED, CLARIFICATION_SUBMITTED, APPROVED, REJECTED, SENT_TO_VA
    event_status VARCHAR(50) NOT NULL,  -- IN_PROGRESS, COMPLETED, CANCELLED
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    event_sequence_number INTEGER,  -- 1, 2, 3... for this evaluation
    qa_cycle_number INTEGER,  -- Which QA cycle (1st review, 2nd review after clarification, etc.)

    -- QA Review Outcome
    review_outcome VARCHAR(50),  -- APPROVED, NEEDS_CLARIFICATION, REJECTED, INSUFFICIENT, RETURNED_TO_EXAMINER
    review_outcome_reason_code VARCHAR(20),
    review_outcome_description VARCHAR(500),

    -- Deficiency Tracking
    deficiency_found_flag BOOLEAN DEFAULT FALSE,
    deficiency_count INTEGER DEFAULT 0,
    deficiency_severity VARCHAR(20),  -- MINOR, MODERATE, MAJOR, CRITICAL
    deficiency_category VARCHAR(100),  -- INCOMPLETE_EXAM, MISSING_NEXUS, INSUFFICIENT_RATIONALE, MISSING_DBQ_ITEMS, etc.
    deficiency_details TEXT,

    -- Clarification Request Details
    is_clarification_request BOOLEAN DEFAULT FALSE,
    clarification_type VARCHAR(50),  -- ADDITIONAL_TESTING, NEXUS_EXPLANATION, DBQ_COMPLETION, RATIONALE_EXPANSION, etc.
    clarification_description TEXT,
    clarification_due_date DATE,
    clarification_priority VARCHAR(20),  -- ROUTINE, URGENT, CRITICAL
    specific_dbq_items_flagged VARCHAR(500),  -- Comma-separated list of DBQ item numbers

    -- Clarification Response Details
    is_clarification_response BOOLEAN DEFAULT FALSE,
    clarification_response_text TEXT,
    clarification_response_complete BOOLEAN,
    clarification_response_method VARCHAR(50),  -- ADDENDUM, REVISED_DBQ, PHONE_CONSULT, ADDITIONAL_EXAM

    -- QA Reviewer Information
    qa_reviewer_name VARCHAR(255),
    qa_reviewer_id_legacy VARCHAR(50),
    qa_team VARCHAR(100),  -- Which QA team reviewed (Medical, Psych, Orthopedic, etc.)
    qa_reviewer_experience_level VARCHAR(20),  -- JUNIOR, SENIOR, LEAD, MANAGER

    -- Time Tracking
    review_started_timestamp TIMESTAMP_NTZ,
    review_completed_timestamp TIMESTAMP_NTZ,
    review_duration_minutes INTEGER,
    time_since_submission_hours INTEGER,
    turnaround_time_hours INTEGER,  -- Time from submission to outcome

    -- Quality Assessment Scores
    completeness_score INTEGER,  -- 0-100
    accuracy_score INTEGER,  -- 0-100
    clarity_score INTEGER,  -- 0-100
    nexus_quality_score INTEGER,  -- 0-100
    overall_quality_score INTEGER,  -- 0-100 (weighted average)

    -- Compliance Checks
    all_dbq_items_completed BOOLEAN,
    nexus_opinion_provided BOOLEAN,
    medical_rationale_adequate BOOLEAN,
    diagnostic_criteria_met BOOLEAN,
    functional_assessment_complete BOOLEAN,
    regulatory_compliance_flag BOOLEAN DEFAULT TRUE,
    compliance_issues_found TEXT,

    -- Document Tracking
    document_version_number INTEGER,  -- Version of the evaluation report
    document_page_count INTEGER,
    attachments_count INTEGER,
    images_count INTEGER,
    document_completeness_flag BOOLEAN,

    -- Escalation Tracking
    escalated_flag BOOLEAN DEFAULT FALSE,
    escalated_to VARCHAR(100),  -- QA Manager, Medical Director, etc.
    escalation_reason VARCHAR(255),
    escalation_timestamp TIMESTAMP_NTZ,

    -- Approval Details
    is_final_approval BOOLEAN DEFAULT FALSE,
    approved_by VARCHAR(255),
    approved_timestamp TIMESTAMP_NTZ,
    approval_notes TEXT,
    conditional_approval_flag BOOLEAN,  -- Approved with minor edits
    conditions_for_approval TEXT,

    -- VA Submission Details
    sent_to_va_flag BOOLEAN DEFAULT FALSE,
    sent_to_va_timestamp TIMESTAMP_NTZ,
    va_submission_method VARCHAR(50),  -- ELECTRONIC, MAIL, FAX
    va_confirmation_number VARCHAR(100),

    -- Performance Metrics
    first_pass_approval_flag BOOLEAN,  -- Approved on first QA review
    total_qa_cycles_at_event INTEGER,  -- How many QA cycles so far
    total_clarifications_requested INTEGER,  -- Cumulative count
    days_in_qa_process INTEGER,  -- Total days in QA at time of event

    -- SLA Tracking
    sla_days_allowed INTEGER,  -- SLA for this type of evaluation
    sla_met_flag BOOLEAN,
    sla_variance_days INTEGER,  -- Positive = early, Negative = late
    sla_risk_level VARCHAR(20),  -- LOW, MEDIUM, HIGH based on time remaining

    -- Communication Tracking
    examiner_notified_flag BOOLEAN DEFAULT FALSE,
    examiner_notification_method VARCHAR(50),  -- EMAIL, PORTAL, PHONE
    examiner_notification_timestamp TIMESTAMP_NTZ,
    examiner_acknowledged_flag BOOLEAN,
    examiner_acknowledgement_timestamp TIMESTAMP_NTZ,

    -- System/Process Information
    qa_system_used VARCHAR(50),  -- QA Platform name
    automated_checks_run_flag BOOLEAN,
    automated_checks_passed INTEGER,
    automated_checks_failed INTEGER,
    manual_review_required_flag BOOLEAN DEFAULT TRUE,

    -- Business Context
    exam_complexity_level VARCHAR(20),  -- ROUTINE, MODERATE, COMPLEX, HIGH_COMPLEXITY
    examiner_quality_tier VARCHAR(20),  -- Based on historical performance
    risk_adjusted_review_flag BOOLEAN,  -- Higher scrutiny for high-risk cases

    -- Error Tracking
    error_flag BOOLEAN DEFAULT FALSE,
    error_code VARCHAR(20),
    error_description VARCHAR(255),
    system_issue_flag BOOLEAN DEFAULT FALSE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100),
    event_notes TEXT,

    -- Foreign Key Constraints
    FOREIGN KEY (veteran_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans(veteran_sk),
    FOREIGN KEY (evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (qa_reviewer_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (facility_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities(facility_sk),
    FOREIGN KEY (evaluation_type_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluation_types(evaluation_type_sk),
    FOREIGN KEY (medical_condition_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_medical_conditions(medical_condition_sk),
    FOREIGN KEY (claim_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_claims(claim_sk),
    FOREIGN KEY (event_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk),
    FOREIGN KEY (evaluation_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk)
)
COMMENT = 'Transaction fact table capturing complete QA lifecycle for evaluation reports - one row per QA event'
CLUSTER BY (event_date_sk, evaluation_id);

-- Column comments for data dictionary
COMMENT ON COLUMN fact_evaluation_qa_events.qa_event_sk IS 'Surrogate primary key for the QA event fact';
COMMENT ON COLUMN fact_evaluation_qa_events.evaluation_id IS 'Evaluation identifier - can have multiple QA events per evaluation';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_event_id IS 'Unique QA event identifier';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_cycle_id IS 'Groups all events in the same QA review cycle';
COMMENT ON COLUMN fact_evaluation_qa_events.event_type IS 'Type of QA event: INITIAL_SUBMISSION, QA_REVIEW_STARTED, QA_REVIEW_COMPLETED, CLARIFICATION_REQUESTED, CLARIFICATION_SUBMITTED, APPROVED, REJECTED, SENT_TO_VA';
COMMENT ON COLUMN fact_evaluation_qa_events.event_status IS 'Status of event: IN_PROGRESS, COMPLETED, CANCELLED';
COMMENT ON COLUMN fact_evaluation_qa_events.event_sequence_number IS 'Sequential number for this evaluation (1=first event, 2=second event, etc.)';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_cycle_number IS 'Which QA review cycle: 1=initial review, 2=after first clarification, 3=after second clarification, etc.';
COMMENT ON COLUMN fact_evaluation_qa_events.review_outcome IS 'Outcome of QA review: APPROVED, NEEDS_CLARIFICATION, REJECTED, INSUFFICIENT, RETURNED_TO_EXAMINER';
COMMENT ON COLUMN fact_evaluation_qa_events.deficiency_found_flag IS 'TRUE if deficiencies were found during QA review';
COMMENT ON COLUMN fact_evaluation_qa_events.deficiency_count IS 'Number of deficiencies identified in this review';
COMMENT ON COLUMN fact_evaluation_qa_events.deficiency_severity IS 'Severity of deficiencies: MINOR, MODERATE, MAJOR, CRITICAL';
COMMENT ON COLUMN fact_evaluation_qa_events.deficiency_category IS 'Category of deficiency: INCOMPLETE_EXAM, MISSING_NEXUS, INSUFFICIENT_RATIONALE, MISSING_DBQ_ITEMS, etc.';
COMMENT ON COLUMN fact_evaluation_qa_events.clarification_type IS 'Type of clarification requested: ADDITIONAL_TESTING, NEXUS_EXPLANATION, DBQ_COMPLETION, RATIONALE_EXPANSION, etc.';
COMMENT ON COLUMN fact_evaluation_qa_events.clarification_due_date IS 'Date by which clarification is due';
COMMENT ON COLUMN fact_evaluation_qa_events.clarification_priority IS 'Priority of clarification request: ROUTINE, URGENT, CRITICAL';
COMMENT ON COLUMN fact_evaluation_qa_events.specific_dbq_items_flagged IS 'Comma-separated list of specific DBQ item numbers that need clarification';
COMMENT ON COLUMN fact_evaluation_qa_events.clarification_response_method IS 'How examiner provided clarification: ADDENDUM, REVISED_DBQ, PHONE_CONSULT, ADDITIONAL_EXAM';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_reviewer_sk IS 'Foreign key to dim_evaluators for QA reviewer (different from original examiner)';
COMMENT ON COLUMN fact_evaluation_qa_events.qa_team IS 'Which QA team reviewed: Medical, Psych, Orthopedic, etc.';
COMMENT ON COLUMN fact_evaluation_qa_events.review_duration_minutes IS 'Time spent on this QA review';
COMMENT ON COLUMN fact_evaluation_qa_events.turnaround_time_hours IS 'Time from submission to QA outcome';
COMMENT ON COLUMN fact_evaluation_qa_events.completeness_score IS 'Score for completeness of evaluation report (0-100)';
COMMENT ON COLUMN fact_evaluation_qa_events.accuracy_score IS 'Score for accuracy of medical findings (0-100)';
COMMENT ON COLUMN fact_evaluation_qa_events.clarity_score IS 'Score for clarity of writing and rationale (0-100)';
COMMENT ON COLUMN fact_evaluation_qa_events.nexus_quality_score IS 'Score for quality of nexus opinion (0-100)';
COMMENT ON COLUMN fact_evaluation_qa_events.overall_quality_score IS 'Overall quality score - weighted average of component scores (0-100)';
COMMENT ON COLUMN fact_evaluation_qa_events.all_dbq_items_completed IS 'TRUE if all DBQ items were completed';
COMMENT ON COLUMN fact_evaluation_qa_events.nexus_opinion_provided IS 'TRUE if nexus opinion was provided';
COMMENT ON COLUMN fact_evaluation_qa_events.medical_rationale_adequate IS 'TRUE if medical rationale was adequate';
COMMENT ON COLUMN fact_evaluation_qa_events.diagnostic_criteria_met IS 'TRUE if diagnostic criteria were met';
COMMENT ON COLUMN fact_evaluation_qa_events.functional_assessment_complete IS 'TRUE if functional assessment was complete';
COMMENT ON COLUMN fact_evaluation_qa_events.document_version_number IS 'Version number of the evaluation report';
COMMENT ON COLUMN fact_evaluation_qa_events.escalated_flag IS 'TRUE if case was escalated to senior QA staff or management';
COMMENT ON COLUMN fact_evaluation_qa_events.escalation_reason IS 'Reason for escalation';
COMMENT ON COLUMN fact_evaluation_qa_events.is_final_approval IS 'TRUE if this is the final approval before sending to VA';
COMMENT ON COLUMN fact_evaluation_qa_events.sent_to_va_flag IS 'TRUE if evaluation was sent to VA after this event';
COMMENT ON COLUMN fact_evaluation_qa_events.first_pass_approval_flag IS 'TRUE if approved on first QA review without any clarifications';
COMMENT ON COLUMN fact_evaluation_qa_events.total_qa_cycles_at_event IS 'Number of QA review cycles completed at time of this event';
COMMENT ON COLUMN fact_evaluation_qa_events.days_in_qa_process IS 'Total days evaluation has been in QA process at time of this event';
COMMENT ON COLUMN fact_evaluation_qa_events.sla_met_flag IS 'TRUE if SLA was met for this QA process';
COMMENT ON COLUMN fact_evaluation_qa_events.sla_variance_days IS 'Days early (positive) or late (negative) compared to SLA';
COMMENT ON COLUMN fact_evaluation_qa_events.automated_checks_run_flag IS 'TRUE if automated QA checks were run';
COMMENT ON COLUMN fact_evaluation_qa_events.automated_checks_passed IS 'Number of automated checks that passed';
COMMENT ON COLUMN fact_evaluation_qa_events.automated_checks_failed IS 'Number of automated checks that failed';
COMMENT ON COLUMN fact_evaluation_qa_events.exam_complexity_level IS 'Complexity level of the examination: ROUTINE, MODERATE, COMPLEX, HIGH_COMPLEXITY';
COMMENT ON COLUMN fact_evaluation_qa_events.examiner_quality_tier IS 'Examiner quality tier based on historical performance';
COMMENT ON COLUMN fact_evaluation_qa_events.risk_adjusted_review_flag IS 'TRUE if this received enhanced scrutiny due to risk factors';
COMMENT ON COLUMN fact_evaluation_qa_events.created_timestamp IS 'Timestamp when QA event record was created in data warehouse';
