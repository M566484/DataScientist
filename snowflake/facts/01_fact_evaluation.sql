-- =====================================================
-- fct_evaluations_completed - Evaluation Fact Table
-- =====================================================
-- Purpose: Core fact table for medical evaluations
-- Grain: One row per evaluation per medical condition
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE fct_evaluations_completed (
    evaluation_fact_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    veteran_sk INTEGER NOT NULL,
    evaluator_sk INTEGER NOT NULL,
    facility_sk INTEGER NOT NULL,
    evaluation_type_sk INTEGER NOT NULL,
    medical_condition_sk INTEGER NOT NULL,
    claim_sk INTEGER NOT NULL,
    appointment_sk INTEGER,

    -- Date Foreign Keys
    evaluation_date_sk INTEGER NOT NULL,
    scheduled_date_sk INTEGER,
    claim_date_sk INTEGER,

    -- Degenerate Dimensions (transaction identifiers)
    evaluation_id VARCHAR(50) NOT NULL UNIQUE,
    dbq_form_id VARCHAR(50),
    exam_request_id VARCHAR(50),

    -- Evaluation Metrics
    evaluation_duration_minutes INTEGER,
    scheduled_duration_minutes INTEGER,
    variance_minutes INTEGER,  -- Actual vs Scheduled

    -- Attendance Metrics
    attended_flag BOOLEAN DEFAULT TRUE,
    no_show_flag BOOLEAN DEFAULT FALSE,
    cancelled_flag BOOLEAN DEFAULT FALSE,
    rescheduled_flag BOOLEAN DEFAULT FALSE,

    -- Wait Time Metrics
    days_from_request_to_schedule INTEGER,
    days_from_schedule_to_evaluation INTEGER,
    total_wait_days INTEGER,

    -- Evaluation Results
    evaluation_completed_flag BOOLEAN DEFAULT FALSE,
    dbq_submitted_flag BOOLEAN DEFAULT FALSE,
    dbq_submission_date DATE,
    nexus_opinion_provided BOOLEAN DEFAULT FALSE,
    nexus_opinion VARCHAR(50),  -- At Least As Likely As Not, Less Likely, etc.

    -- Disability Assessment
    current_severity VARCHAR(50),
    functional_impact_score INTEGER,  -- 0-100 scale
    recommended_rating_percentage INTEGER,

    -- Service Connection Assessment
    service_connected_opinion VARCHAR(50),  -- Yes, No, Possible
    in_service_incurrence_flag BOOLEAN,
    aggravation_flag BOOLEAN,
    secondary_condition_flag BOOLEAN,

    -- Quality Metrics
    report_completeness_score DECIMAL(5,2),  -- 0-100
    report_timeliness_days INTEGER,
    sufficient_exam_flag BOOLEAN DEFAULT TRUE,
    addendum_required_flag BOOLEAN DEFAULT FALSE,

    -- Financial Metrics
    evaluation_cost_amount DECIMAL(10,2),
    contractor_payment_amount DECIMAL(10,2),
    travel_reimbursement_amount DECIMAL(10,2),

    -- Administrative
    review_required_flag BOOLEAN DEFAULT FALSE,
    qa_reviewed_flag BOOLEAN DEFAULT FALSE,
    qa_reviewer_id VARCHAR(50),
    qa_review_date DATE,

    -- Telehealth Specific
    telehealth_flag BOOLEAN DEFAULT FALSE,
    telehealth_platform VARCHAR(50),
    technical_issues_flag BOOLEAN DEFAULT FALSE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Foreign Key Constraints
    FOREIGN KEY (veteran_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans(veteran_sk),
    FOREIGN KEY (evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (facility_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities(facility_sk),
    FOREIGN KEY (evaluation_type_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluation_types(evaluation_type_sk),
    FOREIGN KEY (medical_condition_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_medical_conditions(medical_condition_sk),
    FOREIGN KEY (claim_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_claims(claim_sk),
    FOREIGN KEY (appointment_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_appointments(appointment_sk),
    FOREIGN KEY (evaluation_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk)
)
COMMENT = 'Transaction fact table for medical evaluations at the evaluation-condition grain'
CLUSTER BY (evaluation_date_sk, facility_sk);

-- Column comments for data dictionary
COMMENT ON COLUMN fct_evaluations_completed.evaluation_fact_sk IS 'Surrogate primary key for the evaluation fact';
COMMENT ON COLUMN fct_evaluations_completed.veteran_sk IS 'Foreign key to dim_veterans dimension';
COMMENT ON COLUMN fct_evaluations_completed.evaluator_sk IS 'Foreign key to dim_evaluators dimension';
COMMENT ON COLUMN fct_evaluations_completed.facility_sk IS 'Foreign key to dim_facilities dimension';
COMMENT ON COLUMN fct_evaluations_completed.evaluation_type_sk IS 'Foreign key to dim_evaluation_types dimension';
COMMENT ON COLUMN fct_evaluations_completed.medical_condition_sk IS 'Foreign key to dim_medical_conditions dimension';
COMMENT ON COLUMN fct_evaluations_completed.claim_sk IS 'Foreign key to dim_claims dimension';
COMMENT ON COLUMN fct_evaluations_completed.appointment_sk IS 'Foreign key to dim_appointments dimension';
COMMENT ON COLUMN fct_evaluations_completed.evaluation_date_sk IS 'Foreign key to dim_dates - date evaluation was performed';
COMMENT ON COLUMN fct_evaluations_completed.scheduled_date_sk IS 'Foreign key to dim_dates - date evaluation was originally scheduled';
COMMENT ON COLUMN fct_evaluations_completed.claim_date_sk IS 'Foreign key to dim_dates - date claim was filed';
COMMENT ON COLUMN fct_evaluations_completed.evaluation_id IS 'Unique evaluation identifier (degenerate dimension)';
COMMENT ON COLUMN fct_evaluations_completed.dbq_form_id IS 'Disability Benefits Questionnaire form identifier';
COMMENT ON COLUMN fct_evaluations_completed.exam_request_id IS 'Exam request identifier';
COMMENT ON COLUMN fct_evaluations_completed.evaluation_duration_minutes IS 'Actual duration of evaluation in minutes';
COMMENT ON COLUMN fct_evaluations_completed.scheduled_duration_minutes IS 'Originally scheduled duration in minutes';
COMMENT ON COLUMN fct_evaluations_completed.variance_minutes IS 'Difference between actual and scheduled duration (positive = over time)';
COMMENT ON COLUMN fct_evaluations_completed.attended_flag IS 'TRUE if veteran attended the evaluation';
COMMENT ON COLUMN fct_evaluations_completed.no_show_flag IS 'TRUE if veteran did not show up';
COMMENT ON COLUMN fct_evaluations_completed.cancelled_flag IS 'TRUE if evaluation was cancelled';
COMMENT ON COLUMN fct_evaluations_completed.rescheduled_flag IS 'TRUE if evaluation was rescheduled';
COMMENT ON COLUMN fct_evaluations_completed.days_from_request_to_schedule IS 'Days between exam request and scheduled date';
COMMENT ON COLUMN fct_evaluations_completed.days_from_schedule_to_evaluation IS 'Days between scheduled date and actual evaluation';
COMMENT ON COLUMN fct_evaluations_completed.total_wait_days IS 'Total days from request to evaluation';
COMMENT ON COLUMN fct_evaluations_completed.evaluation_completed_flag IS 'TRUE if evaluation was completed';
COMMENT ON COLUMN fct_evaluations_completed.dbq_submitted_flag IS 'TRUE if DBQ form was submitted';
COMMENT ON COLUMN fct_evaluations_completed.dbq_submission_date IS 'Date DBQ was submitted';
COMMENT ON COLUMN fct_evaluations_completed.nexus_opinion_provided IS 'TRUE if medical nexus opinion was provided';
COMMENT ON COLUMN fct_evaluations_completed.nexus_opinion IS 'Nexus opinion (At Least As Likely As Not, Less Likely, etc.)';
COMMENT ON COLUMN fct_evaluations_completed.current_severity IS 'Current severity assessment';
COMMENT ON COLUMN fct_evaluations_completed.functional_impact_score IS 'Functional impact score (0-100 scale)';
COMMENT ON COLUMN fct_evaluations_completed.recommended_rating_percentage IS 'Evaluator recommended disability rating percentage';
COMMENT ON COLUMN fct_evaluations_completed.service_connected_opinion IS 'Service connection opinion (Yes, No, Possible)';
COMMENT ON COLUMN fct_evaluations_completed.in_service_incurrence_flag IS 'TRUE if condition incurred during service';
COMMENT ON COLUMN fct_evaluations_completed.aggravation_flag IS 'TRUE if service aggravated pre-existing condition';
COMMENT ON COLUMN fct_evaluations_completed.secondary_condition_flag IS 'TRUE if this is a secondary condition';
COMMENT ON COLUMN fct_evaluations_completed.report_completeness_score IS 'Quality score for report completeness (0-100)';
COMMENT ON COLUMN fct_evaluations_completed.report_timeliness_days IS 'Days from evaluation to report submission';
COMMENT ON COLUMN fct_evaluations_completed.sufficient_exam_flag IS 'TRUE if exam was sufficient for rating decision';
COMMENT ON COLUMN fct_evaluations_completed.addendum_required_flag IS 'TRUE if addendum to report was required';
COMMENT ON COLUMN fct_evaluations_completed.evaluation_cost_amount IS 'Cost of the evaluation';
COMMENT ON COLUMN fct_evaluations_completed.contractor_payment_amount IS 'Amount paid to contractor evaluator';
COMMENT ON COLUMN fct_evaluations_completed.travel_reimbursement_amount IS 'Travel reimbursement paid to veteran';
COMMENT ON COLUMN fct_evaluations_completed.review_required_flag IS 'TRUE if quality review is required';
COMMENT ON COLUMN fct_evaluations_completed.qa_reviewed_flag IS 'TRUE if QA review was completed';
COMMENT ON COLUMN fct_evaluations_completed.qa_reviewer_id IS 'ID of quality assurance reviewer';
COMMENT ON COLUMN fct_evaluations_completed.qa_review_date IS 'Date of QA review';
COMMENT ON COLUMN fct_evaluations_completed.telehealth_flag IS 'TRUE if evaluation was conducted via telehealth';
COMMENT ON COLUMN fct_evaluations_completed.telehealth_platform IS 'Telehealth platform used (Zoom, Teams, etc.)';
COMMENT ON COLUMN fct_evaluations_completed.technical_issues_flag IS 'TRUE if technical issues occurred during telehealth';
COMMENT ON COLUMN fct_evaluations_completed.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN fct_evaluations_completed.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN fct_evaluations_completed.updated_timestamp IS 'Timestamp when record was last updated';
