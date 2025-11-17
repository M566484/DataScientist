-- =====================================================
-- fact_exam_requests - Exam Request Tracking
-- =====================================================
-- Purpose: Track exam requests from VA to VES through complete lifecycle
-- Grain: One row per exam request (updated as request progresses)
-- Type: Accumulating Snapshot Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE fact_exam_requests (
    exam_request_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    veteran_sk INTEGER NOT NULL,
    facility_sk INTEGER,  -- Assigned facility
    exam_request_type_sk INTEGER NOT NULL,
    claim_sk INTEGER,
    assigned_evaluator_sk INTEGER,  -- FK to dim_evaluators

    -- Date Foreign Keys (Accumulating Snapshot Pattern)
    request_received_date_sk INTEGER NOT NULL,
    request_validated_date_sk INTEGER,
    eligibility_confirmed_date_sk INTEGER,
    assignment_started_date_sk INTEGER,
    examiner_assigned_date_sk INTEGER,
    examiner_accepted_date_sk INTEGER,
    appointment_scheduled_date_sk INTEGER,
    exam_completed_date_sk INTEGER,
    request_closed_date_sk INTEGER,
    request_cancelled_date_sk INTEGER,

    -- Degenerate Dimensions
    exam_request_id VARCHAR(50) NOT NULL UNIQUE,
    va_request_number VARCHAR(50),
    vba_claim_number VARCHAR(50),
    va_regional_office VARCHAR(100),

    -- Request Source Information
    request_source_system VARCHAR(50),  -- VBA, Appeals, Supplemental
    request_channel VARCHAR(50),  -- Electronic, Mail, Fax, Portal
    request_priority VARCHAR(20),  -- ROUTINE, PRIORITY, URGENT, EXPEDITE
    request_reason VARCHAR(100),  -- Initial, Re-exam, Appeal, Supplement
    expedite_reason VARCHAR(255),
    expedite_approved_flag BOOLEAN DEFAULT FALSE,

    -- Request Details
    requested_conditions TEXT,  -- List of conditions to evaluate
    requested_conditions_count INTEGER,
    multiple_conditions_flag BOOLEAN DEFAULT FALSE,
    requires_specialist_flag BOOLEAN DEFAULT FALSE,
    required_specialty VARCHAR(100),
    complex_case_flag BOOLEAN DEFAULT FALSE,

    -- Eligibility and Validation
    eligibility_status VARCHAR(50),  -- ELIGIBLE, NOT_ELIGIBLE, PENDING_VERIFICATION
    eligibility_checked_flag BOOLEAN DEFAULT FALSE,
    eligibility_issues TEXT,
    missing_information_flag BOOLEAN DEFAULT FALSE,
    missing_information_details TEXT,
    additional_info_requested_flag BOOLEAN DEFAULT FALSE,
    additional_info_received_flag BOOLEAN DEFAULT FALSE,

    -- Assignment Tracking
    assignment_status VARCHAR(50),  -- UNASSIGNED, IN_QUEUE, ASSIGNED, ACCEPTED, REJECTED
    assignment_method VARCHAR(50),  -- AUTO_ASSIGNMENT, MANUAL, PREFERRED_EXAMINER
    assignment_attempts INTEGER DEFAULT 0,
    assignment_rejections INTEGER DEFAULT 0,
    rejection_reasons TEXT,
    current_queue VARCHAR(100),
    queue_position INTEGER,

    -- Geographic and Location
    veteran_zip_code VARCHAR(10),
    veteran_city VARCHAR(100),
    veteran_state VARCHAR(2),
    requested_exam_location VARCHAR(100),
    travel_distance_miles DECIMAL(8,2),
    telehealth_requested BOOLEAN DEFAULT FALSE,
    telehealth_approved BOOLEAN DEFAULT FALSE,

    -- SLA Tracking
    sla_days_allowed INTEGER,
    sla_due_date DATE,
    days_to_assignment INTEGER,
    days_to_scheduling INTEGER,
    days_to_completion INTEGER,
    total_cycle_time_days INTEGER,
    sla_met_flag BOOLEAN,
    sla_variance_days INTEGER,  -- Positive = early, Negative = late
    at_risk_flag BOOLEAN DEFAULT FALSE,

    -- Workload and Capacity
    examiner_workload_at_assignment INTEGER,  -- Number of active cases
    facility_capacity_at_request DECIMAL(5,2),  -- Utilization percentage
    expected_wait_time_days INTEGER,
    estimated_completion_date DATE,

    -- Status Flags
    request_active_flag BOOLEAN DEFAULT TRUE,
    assigned_flag BOOLEAN DEFAULT FALSE,
    scheduled_flag BOOLEAN DEFAULT FALSE,
    completed_flag BOOLEAN DEFAULT FALSE,
    cancelled_flag BOOLEAN DEFAULT FALSE,
    on_hold_flag BOOLEAN DEFAULT FALSE,

    -- Cancellation Details
    cancelled_by VARCHAR(50),  -- Veteran, VA, VES, System
    cancellation_reason_code VARCHAR(20),
    cancellation_reason_description VARCHAR(255),
    cancellation_date DATE,

    -- Quality and Compliance
    pre_assignment_review_required BOOLEAN DEFAULT FALSE,
    pre_assignment_review_completed BOOLEAN DEFAULT FALSE,
    medical_records_requested BOOLEAN DEFAULT FALSE,
    medical_records_received BOOLEAN DEFAULT FALSE,
    authorization_required BOOLEAN DEFAULT FALSE,
    authorization_obtained BOOLEAN DEFAULT FALSE,

    -- Financial
    estimated_cost DECIMAL(10,2),
    authorized_amount DECIMAL(10,2),
    cost_category VARCHAR(50),  -- STANDARD, COMPLEX, SPECIALIST, DIAGNOSTIC

    -- Communication Tracking
    veteran_notified_flag BOOLEAN DEFAULT FALSE,
    veteran_notification_date DATE,
    veteran_notification_method VARCHAR(50),
    veteran_acknowledgement_flag BOOLEAN DEFAULT FALSE,
    reminder_sent_count INTEGER DEFAULT 0,
    last_contact_date DATE,

    -- System and Process
    routing_rule_applied VARCHAR(100),
    auto_assignment_eligible BOOLEAN DEFAULT TRUE,
    manual_intervention_required BOOLEAN DEFAULT FALSE,
    manual_intervention_reason VARCHAR(255),
    exception_flag BOOLEAN DEFAULT FALSE,
    exception_details TEXT,

    -- Performance Metrics (Calculated)
    time_in_validation_hours DECIMAL(10,2),
    time_in_queue_hours DECIMAL(10,2),
    time_to_first_assignment_hours DECIMAL(10,2),
    time_to_acceptance_hours DECIMAL(10,2),

    -- Historical Tracking
    previous_exam_flag BOOLEAN DEFAULT FALSE,
    previous_exam_date DATE,
    previous_examiner_sk INTEGER,
    re_exam_reason VARCHAR(255),

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_status_change_timestamp TIMESTAMP_NTZ,

    -- Foreign Key Constraints
    FOREIGN KEY (veteran_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans(veteran_sk),
    FOREIGN KEY (facility_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities(facility_sk),
    FOREIGN KEY (exam_request_type_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_exam_request_types(exam_request_type_sk),
    FOREIGN KEY (claim_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_claims(claim_sk),
    FOREIGN KEY (assigned_evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (previous_examiner_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (request_received_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk)
)
COMMENT = 'Accumulating snapshot fact table for exam requests from VA to VES - tracks complete request lifecycle'
CLUSTER BY (request_received_date_sk, assignment_status);

-- Column comments for data dictionary
COMMENT ON COLUMN fact_exam_requests.exam_request_sk IS 'Surrogate primary key for exam request';
COMMENT ON COLUMN fact_exam_requests.exam_request_id IS 'Unique exam request identifier';
COMMENT ON COLUMN fact_exam_requests.va_request_number IS 'VA-provided request number';
COMMENT ON COLUMN fact_exam_requests.vba_claim_number IS 'VBA claim number associated with request';
COMMENT ON COLUMN fact_exam_requests.va_regional_office IS 'VA Regional Office originating the request';
COMMENT ON COLUMN fact_exam_requests.request_received_date_sk IS 'Date request was received from VA';
COMMENT ON COLUMN fact_exam_requests.examiner_assigned_date_sk IS 'Date examiner was assigned';
COMMENT ON COLUMN fact_exam_requests.exam_completed_date_sk IS 'Date exam was completed';
COMMENT ON COLUMN fact_exam_requests.request_source_system IS 'Source system: VBA, Appeals, Supplemental';
COMMENT ON COLUMN fact_exam_requests.request_priority IS 'Priority level: ROUTINE, PRIORITY, URGENT, EXPEDITE';
COMMENT ON COLUMN fact_exam_requests.expedite_reason IS 'Reason if expedited processing requested';
COMMENT ON COLUMN fact_exam_requests.requested_conditions IS 'List of medical conditions to evaluate';
COMMENT ON COLUMN fact_exam_requests.requested_conditions_count IS 'Number of conditions in request';
COMMENT ON COLUMN fact_exam_requests.eligibility_status IS 'Eligibility status: ELIGIBLE, NOT_ELIGIBLE, PENDING_VERIFICATION';
COMMENT ON COLUMN fact_exam_requests.missing_information_flag IS 'TRUE if missing required information';
COMMENT ON COLUMN fact_exam_requests.assignment_status IS 'Assignment status: UNASSIGNED, IN_QUEUE, ASSIGNED, ACCEPTED, REJECTED';
COMMENT ON COLUMN fact_exam_requests.assignment_method IS 'How assigned: AUTO_ASSIGNMENT, MANUAL, PREFERRED_EXAMINER';
COMMENT ON COLUMN fact_exam_requests.assignment_attempts IS 'Number of assignment attempts';
COMMENT ON COLUMN fact_exam_requests.assignment_rejections IS 'Number of times examiner rejected assignment';
COMMENT ON COLUMN fact_exam_requests.sla_days_allowed IS 'SLA turnaround time in days';
COMMENT ON COLUMN fact_exam_requests.sla_due_date IS 'Date by which request must be completed per SLA';
COMMENT ON COLUMN fact_exam_requests.days_to_assignment IS 'Days from request to examiner assignment';
COMMENT ON COLUMN fact_exam_requests.total_cycle_time_days IS 'Total days from request to completion';
COMMENT ON COLUMN fact_exam_requests.sla_met_flag IS 'TRUE if SLA was met';
COMMENT ON COLUMN fact_exam_requests.sla_variance_days IS 'Days early (positive) or late (negative) vs SLA';
COMMENT ON COLUMN fact_exam_requests.at_risk_flag IS 'TRUE if at risk of missing SLA';
COMMENT ON COLUMN fact_exam_requests.examiner_workload_at_assignment IS 'Examiner active case count at time of assignment';
COMMENT ON COLUMN fact_exam_requests.facility_capacity_at_request IS 'Facility utilization percentage at time of request';
COMMENT ON COLUMN fact_exam_requests.cancelled_by IS 'Who cancelled: Veteran, VA, VES, System';
COMMENT ON COLUMN fact_exam_requests.cancellation_reason_code IS 'Coded cancellation reason';
COMMENT ON COLUMN fact_exam_requests.medical_records_requested IS 'TRUE if medical records were requested';
COMMENT ON COLUMN fact_exam_requests.medical_records_received IS 'TRUE if medical records were received';
COMMENT ON COLUMN fact_exam_requests.estimated_cost IS 'Estimated cost for this exam request';
COMMENT ON COLUMN fact_exam_requests.routing_rule_applied IS 'Business rule used for routing/assignment';
COMMENT ON COLUMN fact_exam_requests.auto_assignment_eligible IS 'TRUE if eligible for automatic assignment';
COMMENT ON COLUMN fact_exam_requests.manual_intervention_required IS 'TRUE if requires manual intervention';
COMMENT ON COLUMN fact_exam_requests.exception_flag IS 'TRUE if exception occurred during processing';
COMMENT ON COLUMN fact_exam_requests.time_in_queue_hours IS 'Time spent in assignment queue';
COMMENT ON COLUMN fact_exam_requests.previous_exam_flag IS 'TRUE if veteran had previous exam for same condition';
COMMENT ON COLUMN fact_exam_requests.re_exam_reason IS 'Reason if this is a re-examination';
COMMENT ON COLUMN fact_exam_requests.updated_timestamp IS 'Timestamp when record was last updated';
