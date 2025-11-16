-- =====================================================
-- dim_exam_request_types - Exam Request Type Dimension
-- =====================================================
-- Purpose: Types of medical exam requests from VA to VES
-- Type: Type 1 Dimension
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE dim_exam_request_types (
    exam_request_type_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    exam_request_type_id VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Request Type Information
    request_type_name VARCHAR(255) NOT NULL,
    request_type_description TEXT,
    request_category VARCHAR(100),  -- C&P Exam, DBQ, Re-exam, IME, etc.
    request_source VARCHAR(100),  -- Initial Claim, Supplemental, Appeal, etc.

    -- Priority and SLA
    priority_level VARCHAR(20),  -- ROUTINE, PRIORITY, URGENT, EXPEDITE
    priority_code VARCHAR(10),
    default_sla_days INTEGER,  -- Standard turnaround time
    expedite_sla_days INTEGER,  -- Expedited turnaround time
    critical_flag BOOLEAN DEFAULT FALSE,

    -- Complexity and Effort
    complexity_level VARCHAR(20),  -- SIMPLE, MODERATE, COMPLEX, HIGH_COMPLEXITY
    complexity_score INTEGER,  -- 1-10 scale
    estimated_duration_minutes INTEGER,
    typical_examiner_level VARCHAR(50),  -- Entry, Mid, Senior, Specialist

    -- Requirements
    requires_specialist BOOLEAN DEFAULT FALSE,
    required_specialty VARCHAR(100),
    requires_in_person BOOLEAN DEFAULT TRUE,
    telehealth_eligible BOOLEAN DEFAULT FALSE,
    requires_multiple_exams BOOLEAN DEFAULT FALSE,
    requires_diagnostic_testing BOOLEAN DEFAULT FALSE,

    -- Form and Documentation
    dbq_form_required BOOLEAN DEFAULT TRUE,
    default_dbq_form_id VARCHAR(50),
    dbq_form_version VARCHAR(20),
    nexus_opinion_required BOOLEAN DEFAULT TRUE,
    addendum_commonly_required BOOLEAN DEFAULT FALSE,

    -- Regulatory References
    cfr_reference VARCHAR(100),  -- Code of Federal Regulations
    vba_reference VARCHAR(100),  -- VBA M21-1 reference
    va_form_number VARCHAR(50),
    regulatory_category VARCHAR(100),

    -- Volume and Capacity Planning
    annual_volume_estimate INTEGER,
    seasonal_pattern VARCHAR(50),  -- STEADY, PEAK_Q1, PEAK_Q4, etc.
    avg_requests_per_month INTEGER,

    -- Financial
    base_payment_amount DECIMAL(10,2),
    complex_case_premium DECIMAL(10,2),
    travel_reimbursement_applicable BOOLEAN DEFAULT TRUE,

    -- Quality Metrics
    typical_qa_pass_rate DECIMAL(5,2),  -- Historical pass rate
    common_qa_issues TEXT,
    requires_enhanced_qa BOOLEAN DEFAULT FALSE,

    -- Status and Availability
    active_flag BOOLEAN DEFAULT TRUE,
    effective_date DATE,
    end_date DATE,
    suspension_reason VARCHAR(255),

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for exam request types from VA to VES';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_exam_request_types.exam_request_type_sk IS 'Surrogate primary key for exam request type dimension';
COMMENT ON COLUMN dim_exam_request_types.exam_request_type_id IS 'Business key - Unique exam request type identifier';
COMMENT ON COLUMN dim_exam_request_types.request_type_name IS 'Name of the exam request type';
COMMENT ON COLUMN dim_exam_request_types.request_type_description IS 'Detailed description of the exam request type';
COMMENT ON COLUMN dim_exam_request_types.request_category IS 'Category: C&P Exam, DBQ, Re-exam, IME, etc.';
COMMENT ON COLUMN dim_exam_request_types.request_source IS 'Source of request: Initial Claim, Supplemental, Appeal, etc.';
COMMENT ON COLUMN dim_exam_request_types.priority_level IS 'Priority level: ROUTINE, PRIORITY, URGENT, EXPEDITE';
COMMENT ON COLUMN dim_exam_request_types.default_sla_days IS 'Standard service level agreement in days';
COMMENT ON COLUMN dim_exam_request_types.expedite_sla_days IS 'Expedited SLA for urgent cases';
COMMENT ON COLUMN dim_exam_request_types.critical_flag IS 'TRUE if this is a critical/high-priority request type';
COMMENT ON COLUMN dim_exam_request_types.complexity_level IS 'Complexity level: SIMPLE, MODERATE, COMPLEX, HIGH_COMPLEXITY';
COMMENT ON COLUMN dim_exam_request_types.complexity_score IS 'Complexity score on 1-10 scale';
COMMENT ON COLUMN dim_exam_request_types.estimated_duration_minutes IS 'Typical duration for this exam type';
COMMENT ON COLUMN dim_exam_request_types.typical_examiner_level IS 'Typical examiner level needed: Entry, Mid, Senior, Specialist';
COMMENT ON COLUMN dim_exam_request_types.requires_specialist IS 'TRUE if requires a medical specialist';
COMMENT ON COLUMN dim_exam_request_types.required_specialty IS 'Required medical specialty if specialist needed';
COMMENT ON COLUMN dim_exam_request_types.requires_in_person IS 'TRUE if must be in-person (not telehealth)';
COMMENT ON COLUMN dim_exam_request_types.telehealth_eligible IS 'TRUE if can be conducted via telehealth';
COMMENT ON COLUMN dim_exam_request_types.requires_multiple_exams IS 'TRUE if typically requires multiple exam sessions';
COMMENT ON COLUMN dim_exam_request_types.requires_diagnostic_testing IS 'TRUE if diagnostic testing (labs, imaging) required';
COMMENT ON COLUMN dim_exam_request_types.dbq_form_required IS 'TRUE if DBQ form is required';
COMMENT ON COLUMN dim_exam_request_types.default_dbq_form_id IS 'Default DBQ form ID for this request type';
COMMENT ON COLUMN dim_exam_request_types.dbq_form_version IS 'DBQ form version';
COMMENT ON COLUMN dim_exam_request_types.nexus_opinion_required IS 'TRUE if nexus opinion required';
COMMENT ON COLUMN dim_exam_request_types.addendum_commonly_required IS 'TRUE if addendums are commonly needed';
COMMENT ON COLUMN dim_exam_request_types.cfr_reference IS 'Code of Federal Regulations reference';
COMMENT ON COLUMN dim_exam_request_types.vba_reference IS 'VBA M21-1 manual reference';
COMMENT ON COLUMN dim_exam_request_types.va_form_number IS 'VA form number if applicable';
COMMENT ON COLUMN dim_exam_request_types.regulatory_category IS 'Regulatory category for compliance tracking';
COMMENT ON COLUMN dim_exam_request_types.annual_volume_estimate IS 'Estimated annual volume for capacity planning';
COMMENT ON COLUMN dim_exam_request_types.seasonal_pattern IS 'Seasonal volume pattern: STEADY, PEAK_Q1, PEAK_Q4, etc.';
COMMENT ON COLUMN dim_exam_request_types.avg_requests_per_month IS 'Average monthly request volume';
COMMENT ON COLUMN dim_exam_request_types.base_payment_amount IS 'Base payment amount for this exam type';
COMMENT ON COLUMN dim_exam_request_types.complex_case_premium IS 'Additional payment for complex cases';
COMMENT ON COLUMN dim_exam_request_types.travel_reimbursement_applicable IS 'TRUE if travel reimbursement applies';
COMMENT ON COLUMN dim_exam_request_types.typical_qa_pass_rate IS 'Historical QA pass rate percentage';
COMMENT ON COLUMN dim_exam_request_types.common_qa_issues IS 'Common QA issues for this exam type';
COMMENT ON COLUMN dim_exam_request_types.requires_enhanced_qa IS 'TRUE if requires enhanced QA review';
COMMENT ON COLUMN dim_exam_request_types.active_flag IS 'TRUE if this exam request type is currently active';
COMMENT ON COLUMN dim_exam_request_types.effective_date IS 'Date this exam type became effective';
COMMENT ON COLUMN dim_exam_request_types.end_date IS 'Date this exam type was discontinued';
COMMENT ON COLUMN dim_exam_request_types.suspension_reason IS 'Reason if temporarily suspended';
COMMENT ON COLUMN dim_exam_request_types.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_exam_request_types.updated_timestamp IS 'Timestamp when record was last updated';
