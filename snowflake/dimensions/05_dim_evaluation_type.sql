-- =====================================================
-- dim_evaluation_types - Evaluation Type Dimension
-- =====================================================
-- Purpose: Types of medical evaluations
-- SCD Type: Type 1
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_evaluation_types (
    evaluation_type_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    evaluation_type_id VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Evaluation Type Information
    evaluation_type_name VARCHAR(255) NOT NULL,
    evaluation_type_description TEXT,
    evaluation_category VARCHAR(100),  -- C&P Exam, Independent Medical Exam, Disability Evaluation, etc.

    -- Evaluation Characteristics
    typical_duration_minutes INTEGER,
    requires_specialist BOOLEAN DEFAULT FALSE,
    specialty_required VARCHAR(100),
    complexity_level VARCHAR(20),  -- Low, Medium, High

    -- Regulatory Information
    regulatory_requirement VARCHAR(255),
    cfr_reference VARCHAR(100),  -- Code of Federal Regulations reference
    vba_reference VARCHAR(100),  -- Veterans Benefits Administration reference

    -- Scheduling
    advance_notice_days INTEGER DEFAULT 7,
    allow_telehealth BOOLEAN DEFAULT FALSE,
    requires_fasting BOOLEAN DEFAULT FALSE,
    requires_prep BOOLEAN DEFAULT FALSE,
    prep_instructions TEXT,

    -- Status
    active_flag BOOLEAN DEFAULT TRUE,
    deprecated_date DATE,
    replacement_evaluation_type_id VARCHAR(50),

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for evaluation types and categories';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_evaluation_types.evaluation_type_sk IS 'Surrogate primary key for the evaluation type dimension';
COMMENT ON COLUMN dim_evaluation_types.evaluation_type_id IS 'Business key - Unique evaluation type identifier';
COMMENT ON COLUMN dim_evaluation_types.evaluation_type_name IS 'Name of the evaluation type';
COMMENT ON COLUMN dim_evaluation_types.evaluation_type_description IS 'Detailed description of the evaluation';
COMMENT ON COLUMN dim_evaluation_types.evaluation_category IS 'Category (C&P Exam, Independent Medical Exam, Disability Evaluation, etc.)';
COMMENT ON COLUMN dim_evaluation_types.typical_duration_minutes IS 'Expected duration in minutes';
COMMENT ON COLUMN dim_evaluation_types.requires_specialist IS 'TRUE if specialist is required';
COMMENT ON COLUMN dim_evaluation_types.specialty_required IS 'Medical specialty required for this evaluation';
COMMENT ON COLUMN dim_evaluation_types.complexity_level IS 'Complexity level (Low, Medium, High)';
COMMENT ON COLUMN dim_evaluation_types.regulatory_requirement IS 'Regulatory requirement description';
COMMENT ON COLUMN dim_evaluation_types.cfr_reference IS 'Code of Federal Regulations reference';
COMMENT ON COLUMN dim_evaluation_types.vba_reference IS 'Veterans Benefits Administration reference number';
COMMENT ON COLUMN dim_evaluation_types.advance_notice_days IS 'Number of days advance notice required (default 7)';
COMMENT ON COLUMN dim_evaluation_types.allow_telehealth IS 'TRUE if telehealth is allowed for this type';
COMMENT ON COLUMN dim_evaluation_types.requires_fasting IS 'TRUE if patient must fast before evaluation';
COMMENT ON COLUMN dim_evaluation_types.requires_prep IS 'TRUE if patient preparation is required';
COMMENT ON COLUMN dim_evaluation_types.prep_instructions IS 'Patient preparation instructions';
COMMENT ON COLUMN dim_evaluation_types.active_flag IS 'TRUE if this evaluation type is currently active';
COMMENT ON COLUMN dim_evaluation_types.deprecated_date IS 'Date this evaluation type was deprecated';
COMMENT ON COLUMN dim_evaluation_types.replacement_evaluation_type_id IS 'ID of replacement evaluation type if deprecated';
COMMENT ON COLUMN dim_evaluation_types.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_evaluation_types.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_evaluation_types.updated_timestamp IS 'Timestamp when record was last updated';
