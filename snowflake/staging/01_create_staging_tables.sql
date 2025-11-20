-- =====================================================
-- VES Data Pipeline - Staging Layer
-- =====================================================
-- Purpose: Cleansed, conformed, and business-rule-validated data
-- Pattern: One-to-one with dimensions and facts, with transformations applied
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================
-- Create Staging Schema
-- =====================================================

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging layer - cleansed and transformed data ready for warehouse loading';

USE SCHEMA STAGING;

-- =====================================================
-- Staging Table - Veterans
-- =====================================================

CREATE OR REPLACE TABLE stg_veterans (
    -- Business Key
    veteran_id VARCHAR(50) NOT NULL,  -- Derived: COALESCE(va_id, 'SSN-'||ssn)

    -- Personal Information (cleansed)
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),  -- Derived: last_name || ', ' || first_name
    date_of_birth DATE,
    age INTEGER,  -- Derived: DATEDIFF(year, date_of_birth, CURRENT_DATE())
    gender VARCHAR(20),

    -- Contact Information (validated)
    email VARCHAR(255),
    phone VARCHAR(20),  -- Standardized format
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),  -- Validated against state codes
    zip_code VARCHAR(10),  -- Standardized format
    country VARCHAR(50) DEFAULT 'USA',

    -- Military Service Information
    service_branch VARCHAR(50),
    service_start_date DATE,
    service_end_date DATE,
    years_of_service DECIMAL(5,2),  -- Derived
    discharge_status VARCHAR(50),
    service_era VARCHAR(50),
    combat_veteran_flag BOOLEAN,
    military_rank VARCHAR(50),
    military_occupation VARCHAR(100),

    -- Disability Information
    current_disability_rating INTEGER,  -- Validated 0-100
    disability_rating_category VARCHAR(20),  -- Derived
    service_connected_flag BOOLEAN,

    -- VA Benefits Status
    va_enrolled_flag BOOLEAN,
    va_enrollment_date DATE,
    priority_group INTEGER,  -- Validated 1-8

    -- Change Detection
    source_record_hash VARCHAR(64),  -- MD5 hash for change detection

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    dq_score DECIMAL(5,2),  -- Data quality score
    dq_issues VARCHAR(1000),  -- List of DQ issues
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for veterans with cleansed and transformed data';

-- =====================================================
-- Staging Table - Evaluators
-- =====================================================

CREATE OR REPLACE TABLE stg_evaluators (
    -- Business Key
    evaluator_id VARCHAR(50) NOT NULL,  -- Derived: COALESCE(npi, license_number)

    -- Personal Information
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),  -- Derived

    -- Professional Information
    specialty VARCHAR(100),
    sub_specialty VARCHAR(100),
    credentials VARCHAR(100),
    license_number VARCHAR(50),
    license_state VARCHAR(2),
    license_expiration_date DATE,
    npi_number VARCHAR(10),

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

    -- Change Detection
    source_record_hash VARCHAR(64),

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    dq_score DECIMAL(5,2),
    dq_issues VARCHAR(1000),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for evaluators with cleansed and transformed data';

-- =====================================================
-- Staging Table - Facilities
-- =====================================================

CREATE OR REPLACE TABLE stg_facilities (
    -- Business Key
    facility_id VARCHAR(50) NOT NULL,  -- facility_code

    -- Basic Information
    facility_name VARCHAR(255),
    facility_type VARCHAR(50),

    -- Location Information
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR(100),
    full_address VARCHAR(500),  -- Derived

    -- VA Organization
    visn_code VARCHAR(10),
    visn_name VARCHAR(255),
    parent_facility_id VARCHAR(50),

    -- Contact Information
    phone VARCHAR(20),
    fax VARCHAR(20),
    email VARCHAR(255),
    website VARCHAR(255),

    -- Operational Details
    active_flag BOOLEAN,
    operating_hours VARCHAR(255),
    weekend_hours VARCHAR(255),

    -- Change Detection
    source_record_hash VARCHAR(64),

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    dq_score DECIMAL(5,2),
    dq_issues VARCHAR(1000),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for facilities with cleansed and transformed data';

-- =====================================================
-- Staging Table - Exam Requests (Fact)
-- =====================================================

CREATE OR REPLACE TABLE stg_fact_exam_requests (
    -- Degenerate Dimensions
    exam_request_id VARCHAR(50) NOT NULL,
    va_request_number VARCHAR(50),

    -- Dimension Keys (to be resolved in ETL)
    veteran_id VARCHAR(50),
    assigned_evaluator_id VARCHAR(50),
    facility_id VARCHAR(50),
    exam_request_type_id VARCHAR(50),

    -- Date Keys (to be resolved in ETL)
    request_received_date DATE,
    eligibility_confirmed_date DATE,
    examiner_assigned_date DATE,
    appointment_scheduled_date DATE,
    exam_completed_date DATE,
    request_closed_date DATE,

    -- Request Details
    request_priority VARCHAR(20),
    requested_conditions VARCHAR(1000),
    requested_conditions_count INTEGER,  -- Derived
    requires_specialist_flag BOOLEAN,
    required_specialty VARCHAR(100),
    complex_case_flag BOOLEAN,  -- Derived from conditions count

    -- Eligibility
    eligibility_status VARCHAR(50),

    -- Assignment
    assignment_status VARCHAR(50),
    assignment_method VARCHAR(50),
    assignment_attempts INTEGER,  -- Derived
    assignment_rejections INTEGER,  -- Derived

    -- Scheduling
    scheduled_flag BOOLEAN,

    -- Completion
    completed_flag BOOLEAN,
    exam_location VARCHAR(100),

    -- SLA Metrics
    sla_days_allowed INTEGER,
    days_to_assignment INTEGER,  -- Derived
    days_to_scheduling INTEGER,  -- Derived
    total_cycle_time_days INTEGER,  -- Derived
    sla_met_flag BOOLEAN,  -- Derived
    sla_variance_days INTEGER,  -- Derived

    -- Status
    request_status VARCHAR(50),

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for exam request facts';

-- =====================================================
-- Staging Table - Evaluations (Fact)
-- =====================================================

CREATE OR REPLACE TABLE stg_fact_evaluations (
    -- Degenerate Dimensions
    evaluation_id VARCHAR(50) NOT NULL,
    exam_request_id VARCHAR(50),
    dbq_form_id VARCHAR(50),

    -- Dimension Keys (to be resolved)
    veteran_id VARCHAR(50),
    evaluator_id VARCHAR(50),
    facility_id VARCHAR(50),
    evaluation_type_id VARCHAR(50),
    medical_condition_code VARCHAR(20),

    -- Date Keys
    evaluation_date DATE,
    scheduled_date DATE,
    claim_date DATE,

    -- Evaluation Metrics
    evaluation_duration_minutes INTEGER,
    scheduled_duration_minutes INTEGER,  -- From appointment
    variance_minutes INTEGER,  -- Derived

    -- Location
    evaluation_location_type VARCHAR(50),
    telehealth_flag BOOLEAN,

    -- Wait Time Metrics (Derived)
    days_from_request_to_schedule INTEGER,
    days_from_schedule_to_evaluation INTEGER,
    total_wait_days INTEGER,

    -- Evaluation Results
    evaluation_completed_flag BOOLEAN,
    dbq_submitted_flag BOOLEAN,
    dbq_submission_date DATE,
    nexus_opinion_provided BOOLEAN,
    nexus_opinion VARCHAR(50),

    -- Disability Assessment
    current_severity VARCHAR(50),
    functional_impact_score INTEGER,
    recommended_rating_percentage INTEGER,

    -- Service Connection
    service_connected_opinion VARCHAR(50),

    -- Quality Metrics
    report_completeness_score DECIMAL(5,2),
    sufficient_exam_flag BOOLEAN,

    -- QA
    qa_reviewed_flag BOOLEAN,
    qa_review_date DATE,
    qa_approved_date DATE,
    first_pass_qa_approval BOOLEAN,  -- Derived
    qa_cycles_count INTEGER,  -- Derived
    days_in_qa INTEGER,  -- Derived

    -- Delivery
    report_delivered_to_va_date DATE,
    va_delivery_confirmed BOOLEAN,

    -- Financial
    evaluation_cost_amount DECIMAL(10,2),
    contractor_payment_amount DECIMAL(10,2),

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for evaluation facts';

-- =====================================================
-- Staging Table - Appointment Events (Fact)
-- =====================================================

CREATE OR REPLACE TABLE stg_fact_appointment_events (
    -- Degenerate Dimensions
    event_id VARCHAR(50) NOT NULL,
    appointment_id VARCHAR(50) NOT NULL,
    exam_request_id VARCHAR(50),

    -- Dimension Keys
    veteran_id VARCHAR(50),
    evaluator_id VARCHAR(50),
    facility_id VARCHAR(50),

    -- Date Keys
    event_date DATE,
    event_timestamp TIMESTAMP_NTZ,

    -- Event Details
    event_type VARCHAR(50),  -- SCHEDULED, CONFIRMED, RESCHEDULED, CANCELLED, NO_SHOW, COMPLETED
    event_status VARCHAR(50),
    event_sequence_number INTEGER,  -- Derived

    -- Rescheduling
    previous_appointment_id VARCHAR(50),
    new_appointment_id VARCHAR(50),
    rescheduling_reason VARCHAR(255),

    -- Cancellation
    cancellation_reason VARCHAR(255),
    cancelled_by VARCHAR(50),

    -- No-Show
    no_show_flag BOOLEAN,
    no_show_reason VARCHAR(255),

    -- Location
    appointment_location_type VARCHAR(50),
    appointment_location VARCHAR(255),

    -- Scheduling
    scheduled_date DATE,
    scheduled_time TIME,

    -- Completion
    completed_timestamp TIMESTAMP_NTZ,
    duration_minutes INTEGER,

    -- Veteran Communication
    veteran_notified_flag BOOLEAN,
    veteran_notification_date DATE,
    veteran_confirmed_flag BOOLEAN,

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for appointment event facts';

-- =====================================================
-- Staging Table - QA Events (Fact)
-- =====================================================

CREATE OR REPLACE TABLE stg_fact_qa_events (
    -- Degenerate Dimensions
    qa_event_id VARCHAR(50) NOT NULL,
    evaluation_id VARCHAR(50) NOT NULL,

    -- Dimension Keys
    qa_reviewer_id VARCHAR(50),

    -- Date Keys
    event_date DATE,
    event_timestamp TIMESTAMP_NTZ,

    -- Event Details
    event_type VARCHAR(50),
    event_status VARCHAR(50),
    qa_cycle_number INTEGER,

    -- Review Details
    review_outcome VARCHAR(50),
    overall_quality_score INTEGER,
    completeness_score INTEGER,
    accuracy_score INTEGER,
    clarity_score INTEGER,  -- Derived
    nexus_quality_score INTEGER,  -- Derived

    -- Deficiencies
    deficiency_found_flag BOOLEAN,
    deficiency_count INTEGER,
    deficiency_severity VARCHAR(20),  -- Derived
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

    -- Timing (Derived)
    review_duration_hours DECIMAL(10,2),
    turnaround_time_hours DECIMAL(10,2),
    days_in_qa_at_event INTEGER,

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table for QA event facts';

-- =====================================================
-- Staging Data Quality Summary View
-- =====================================================

CREATE OR REPLACE VIEW vw_staging_dq_summary AS
SELECT
    'stg_veterans' AS table_name,
    COUNT(*) AS total_records,
    AVG(dq_score) AS avg_dq_score,
    SUM(CASE WHEN dq_score < 80 THEN 1 ELSE 0 END) AS low_quality_records,
    MAX(loaded_timestamp) AS last_load
FROM stg_veterans
UNION ALL
SELECT
    'stg_evaluators',
    COUNT(*),
    AVG(dq_score),
    SUM(CASE WHEN dq_score < 80 THEN 1 ELSE 0 END),
    MAX(loaded_timestamp)
FROM stg_evaluators
UNION ALL
SELECT
    'stg_facilities',
    COUNT(*),
    AVG(dq_score),
    SUM(CASE WHEN dq_score < 80 THEN 1 ELSE 0 END),
    MAX(loaded_timestamp)
FROM stg_facilities
ORDER BY table_name;

-- =====================================================
-- Verification Queries
-- =====================================================

-- Show all staging tables
SELECT
    table_name,
    row_count,
    bytes / (1024*1024) AS size_mb,
    comment
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'STAGING'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
