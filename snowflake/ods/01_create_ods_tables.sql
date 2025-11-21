-- =====================================================
-- VES Data Pipeline - ODS (Operational Data Store) Layer
-- =====================================================
-- Purpose: Raw data landing zone from source systems
-- Pattern: Mirror source system structures
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================
-- Create ODS Schema
-- =====================================================

CREATE SCHEMA IF NOT EXISTS ODS_RAW
    COMMENT = 'Operational Data Store - Raw data from source systems without transformation';

USE SCHEMA ODS_RAW;

-- =====================================================
-- ODS Tables - Veterans Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_veterans_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Raw Veteran Data (as received from source)
    veteran_ssn VARCHAR(11),  -- SSN from source
    veteran_va_id VARCHAR(50),
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(20),

    -- Contact Information
    email VARCHAR(255),
    phone_primary VARCHAR(20),
    phone_secondary VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    country VARCHAR(50),

    -- Military Service Information
    service_branch VARCHAR(50),
    service_start_date DATE,
    service_end_date DATE,
    discharge_status VARCHAR(50),
    service_era VARCHAR(50),
    combat_veteran_flag BOOLEAN,
    military_rank VARCHAR(50),
    military_occupation VARCHAR(100),

    -- Disability Information
    disability_rating INTEGER,
    service_connected_flag BOOLEAN,

    -- VA Benefits Status
    va_enrolled_flag BOOLEAN,
    va_enrollment_date DATE,
    priority_group INTEGER,

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw veteran data from source systems';

-- =====================================================
-- ODS Tables - Evaluators Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_evaluators_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Raw Evaluator Data
    evaluator_npi VARCHAR(10),
    evaluator_license_number VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),

    -- Professional Information
    specialty VARCHAR(100),
    sub_specialty VARCHAR(100),
    credentials VARCHAR(100),
    license_state VARCHAR(2),
    license_expiration_date DATE,

    -- Employment Information
    employer_name VARCHAR(255),
    employment_type VARCHAR(50),
    hire_date DATE,
    termination_date DATE,

    -- Qualifications
    years_of_experience INTEGER,
    va_certified_flag BOOLEAN,
    certification_date DATE,
    board_certified_flag BOOLEAN,

    -- Performance Metrics
    average_evaluation_time_minutes INTEGER,
    total_evaluations_completed INTEGER,

    -- Status
    active_flag BOOLEAN,

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw evaluator data from source systems';

-- =====================================================
-- ODS Tables - Facilities Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_facilities_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Raw Facility Data
    facility_code VARCHAR(20),
    facility_name VARCHAR(255),
    facility_type VARCHAR(50),

    -- Location Information
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR(100),

    -- VA Organization
    visn_code VARCHAR(10),
    visn_name VARCHAR(255),
    parent_facility_code VARCHAR(20),

    -- Contact Information
    phone VARCHAR(20),
    fax VARCHAR(20),
    email VARCHAR(255),
    website VARCHAR(255),

    -- Operational Details
    active_flag BOOLEAN,
    operating_hours VARCHAR(255),
    weekend_hours VARCHAR(255),

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw facility data from source systems';

-- =====================================================
-- ODS Tables - Exam Requests Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_exam_requests_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Request Identification
    exam_request_id VARCHAR(50),
    va_request_number VARCHAR(50),
    request_type VARCHAR(50),

    -- Veteran Information
    veteran_ssn VARCHAR(11),
    veteran_va_id VARCHAR(50),

    -- Request Details
    request_received_date DATE,
    request_priority VARCHAR(20),
    requested_conditions VARCHAR(1000),  -- Comma-separated list
    requires_specialist_flag BOOLEAN,
    required_specialty VARCHAR(100),

    -- Eligibility
    eligibility_status VARCHAR(50),
    eligibility_confirmed_date DATE,

    -- Assignment
    assigned_evaluator_npi VARCHAR(10),
    assignment_date DATE,
    assignment_method VARCHAR(50),

    -- Scheduling
    appointment_scheduled_date DATE,
    scheduled_exam_date DATE,

    -- Completion
    exam_completed_date DATE,
    exam_location VARCHAR(100),

    -- SLA
    sla_days_allowed INTEGER,
    sla_met_flag BOOLEAN,

    -- Status
    request_status VARCHAR(50),
    request_closed_date DATE,

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw exam request data from source systems';

-- =====================================================
-- ODS Tables - Evaluations Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_evaluations_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Evaluation Identification
    evaluation_id VARCHAR(50),
    exam_request_id VARCHAR(50),
    dbq_form_id VARCHAR(50),

    -- Participants
    veteran_ssn VARCHAR(11),
    veteran_va_id VARCHAR(50),
    evaluator_npi VARCHAR(10),
    facility_code VARCHAR(20),

    -- Evaluation Details
    evaluation_date DATE,
    evaluation_type VARCHAR(50),
    medical_condition_code VARCHAR(20),
    medical_condition_name VARCHAR(255),

    -- Evaluation Metrics
    evaluation_duration_minutes INTEGER,
    evaluation_location_type VARCHAR(50),
    telehealth_flag BOOLEAN,

    -- Assessment Results
    current_severity VARCHAR(50),
    functional_impact_score INTEGER,
    recommended_rating_percentage INTEGER,

    -- Service Connection
    nexus_opinion VARCHAR(50),
    service_connected_opinion VARCHAR(50),

    -- Quality
    report_completeness_score DECIMAL(5,2),
    sufficient_exam_flag BOOLEAN,

    -- QA
    qa_reviewed_flag BOOLEAN,
    qa_review_date DATE,
    qa_outcome VARCHAR(50),

    -- Delivery
    report_delivered_to_va_date DATE,
    va_delivery_confirmed BOOLEAN,

    -- Financial
    evaluation_cost_amount DECIMAL(10,2),
    contractor_payment_amount DECIMAL(10,2),

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw evaluation data from source systems';

-- =====================================================
-- ODS Tables - Appointments Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_appointments_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Appointment Identification
    appointment_id VARCHAR(50),
    exam_request_id VARCHAR(50),

    -- Appointment Details
    appointment_date DATE,
    appointment_time TIME,
    appointment_status VARCHAR(50),
    appointment_type VARCHAR(50),

    -- Participants
    veteran_ssn VARCHAR(11),
    evaluator_npi VARCHAR(10),
    facility_code VARCHAR(20),

    -- Location
    appointment_location_type VARCHAR(50),
    appointment_address VARCHAR(255),

    -- Status Events
    scheduled_date DATE,
    confirmed_date DATE,
    cancelled_date DATE,
    rescheduled_date DATE,
    completed_date DATE,

    -- Cancellation
    cancellation_reason VARCHAR(255),
    cancelled_by VARCHAR(50),

    -- Rescheduling
    previous_appointment_id VARCHAR(50),
    rescheduling_reason VARCHAR(255),

    -- No-Show
    no_show_flag BOOLEAN,
    no_show_reason VARCHAR(255),

    -- Veteran Notifications
    veteran_notified_flag BOOLEAN,
    notification_date DATE,
    notification_method VARCHAR(50),

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw appointment data from source systems';

-- =====================================================
-- ODS Tables - QA Events Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_qa_events_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- QA Event Identification
    qa_event_id VARCHAR(50),
    evaluation_id VARCHAR(50),

    -- Event Details
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP_NTZ,
    qa_cycle_number INTEGER,

    -- QA Reviewer
    qa_reviewer_id VARCHAR(50),
    qa_reviewer_name VARCHAR(255),

    -- Review Outcome
    review_outcome VARCHAR(50),
    overall_quality_score INTEGER,
    completeness_score INTEGER,
    accuracy_score INTEGER,

    -- Deficiencies
    deficiency_found_flag BOOLEAN,
    deficiency_count INTEGER,
    deficiency_category VARCHAR(100),
    deficiency_description TEXT,

    -- Clarification
    clarification_requested_flag BOOLEAN,
    clarification_type VARCHAR(50),
    clarification_description TEXT,
    clarification_due_date DATE,

    -- Approval
    approved_flag BOOLEAN,
    approved_timestamp TIMESTAMP_NTZ,
    first_pass_approval_flag BOOLEAN,

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw QA event data from source systems';

-- =====================================================
-- ODS Tables - Claims Source Data
-- =====================================================

CREATE OR REPLACE TABLE ods_claims_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Claim Identification
    claim_number VARCHAR(50),
    claim_type VARCHAR(50),

    -- Veteran Information
    veteran_ssn VARCHAR(11),
    veteran_va_id VARCHAR(50),

    -- Claim Details
    claim_filed_date DATE,
    claim_received_date DATE,
    claim_status VARCHAR(50),

    -- Claimed Conditions
    claimed_conditions VARCHAR(1000),  -- Comma-separated
    number_of_conditions INTEGER,

    -- Processing Dates
    initial_review_date DATE,
    development_start_date DATE,
    evidence_gathering_date DATE,
    rating_decision_date DATE,
    notification_date DATE,

    -- Outcome
    claim_decision VARCHAR(50),
    granted_conditions VARCHAR(1000),
    denied_conditions VARCHAR(1000),
    combined_disability_rating INTEGER,

    -- Appeal
    appeal_filed_flag BOOLEAN,
    appeal_date DATE,

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
)
COMMENT = 'ODS table for raw claim data from source systems';

-- =====================================================
-- ODS Batch Control Table
-- =====================================================

CREATE OR REPLACE TABLE ods_batch_control (
    batch_id VARCHAR(50) PRIMARY KEY,
    batch_name VARCHAR(255),
    source_system VARCHAR(50) NOT NULL,
    extraction_type VARCHAR(50) NOT NULL,  -- FULL, INCREMENTAL, CDC

    -- Timing
    batch_start_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    batch_end_timestamp TIMESTAMP_NTZ,

    -- Execution
    batch_status VARCHAR(20),  -- RUNNING, COMPLETED, FAILED
    error_message TEXT,

    -- Metrics
    records_extracted INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,

    -- Data Window
    extraction_start_date DATE,
    extraction_end_date DATE,

    -- Metadata
    created_by VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Batch control table for tracking ODS data loads';

-- =====================================================
-- ODS Error Log Table
-- =====================================================

CREATE OR REPLACE TABLE ods_error_log (
    error_id INTEGER AUTOINCREMENT PRIMARY KEY,
    batch_id VARCHAR(50),
    source_table VARCHAR(100),
    source_record_id VARCHAR(100),
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    error_type VARCHAR(50),
    error_message TEXT,
    error_details VARIANT,  -- JSON with additional context

    FOREIGN KEY (batch_id) REFERENCES ods_batch_control(batch_id)
)
COMMENT = 'Error log table for ODS data load failures';

-- =====================================================
-- Verification Queries
-- =====================================================

-- Show all ODS tables
SELECT
    table_name,
    row_count,
    bytes / (1024*1024) AS size_mb,
    comment
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'ODS_RAW'
ORDER BY table_name;
