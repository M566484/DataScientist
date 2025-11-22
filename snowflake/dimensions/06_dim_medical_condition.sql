-- =====================================================
-- dim_medical_conditions - Medical Condition Dimension
-- =====================================================
-- Purpose: Medical conditions being evaluated
-- SCD Type: Type 1
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_medical_conditions (
    medical_condition_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    medical_condition_id VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Condition Information
    condition_name VARCHAR(255) NOT NULL,
    condition_description TEXT,
    condition_category VARCHAR(100),  -- Physical, Mental Health, Neurological, etc.
    body_system VARCHAR(100),  -- Musculoskeletal, Respiratory, Cardiovascular, etc.

    -- Medical Coding
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(500),
    diagnostic_code VARCHAR(20),  -- VA Diagnostic Code
    dbq_form_number VARCHAR(50),  -- Disability Benefits Questionnaire form

    -- Condition Characteristics
    chronic_condition_flag BOOLEAN DEFAULT FALSE,
    presumptive_condition_flag BOOLEAN DEFAULT FALSE,  -- Presumptive service connection
    requires_nexus BOOLEAN DEFAULT TRUE,  -- Requires medical nexus opinion
    severity_scale VARCHAR(50),  -- Mild, Moderate, Severe, or custom scale

    -- Service Connection
    common_service_era VARIANT,  -- JSON array of common service eras
    common_exposure VARCHAR(255),  -- Agent Orange, Burn Pits, Radiation, etc.

    -- Rating Information
    min_rating_percentage INTEGER DEFAULT 0,
    max_rating_percentage INTEGER DEFAULT 100,
    rating_increment INTEGER DEFAULT 10,
    bilateral_factor_applicable BOOLEAN DEFAULT FALSE,

    -- Status
    active_flag BOOLEAN DEFAULT TRUE,
    deprecated_date DATE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for medical conditions and diagnoses';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_medical_conditions.medical_condition_sk IS 'Surrogate primary key for the medical condition dimension';
COMMENT ON COLUMN dim_medical_conditions.medical_condition_id IS 'Business key - Unique medical condition identifier';
COMMENT ON COLUMN dim_medical_conditions.condition_name IS 'Name of the medical condition';
COMMENT ON COLUMN dim_medical_conditions.condition_description IS 'Detailed description of the condition';
COMMENT ON COLUMN dim_medical_conditions.condition_category IS 'Category (Physical, Mental Health, Neurological, etc.)';
COMMENT ON COLUMN dim_medical_conditions.body_system IS 'Affected body system (Musculoskeletal, Respiratory, Cardiovascular, etc.)';
COMMENT ON COLUMN dim_medical_conditions.icd10_code IS 'ICD-10 diagnosis code';
COMMENT ON COLUMN dim_medical_conditions.icd10_description IS 'ICD-10 code description';
COMMENT ON COLUMN dim_medical_conditions.diagnostic_code IS 'VA diagnostic code for rating purposes';
COMMENT ON COLUMN dim_medical_conditions.dbq_form_number IS 'Disability Benefits Questionnaire form number';
COMMENT ON COLUMN dim_medical_conditions.chronic_condition_flag IS 'TRUE if this is a chronic condition';
COMMENT ON COLUMN dim_medical_conditions.presumptive_condition_flag IS 'TRUE if presumptive service connection applies';
COMMENT ON COLUMN dim_medical_conditions.requires_nexus IS 'TRUE if medical nexus opinion is required';
COMMENT ON COLUMN dim_medical_conditions.severity_scale IS 'Severity scale used (Mild, Moderate, Severe, or custom scale)';
COMMENT ON COLUMN dim_medical_conditions.common_service_era IS 'JSON array of common service eras for this condition';
COMMENT ON COLUMN dim_medical_conditions.common_exposure IS 'Common exposure associated (Agent Orange, Burn Pits, Radiation, etc.)';
COMMENT ON COLUMN dim_medical_conditions.min_rating_percentage IS 'Minimum disability rating percentage (typically 0)';
COMMENT ON COLUMN dim_medical_conditions.max_rating_percentage IS 'Maximum disability rating percentage (typically 100)';
COMMENT ON COLUMN dim_medical_conditions.rating_increment IS 'Rating increment step (typically 10)';
COMMENT ON COLUMN dim_medical_conditions.bilateral_factor_applicable IS 'TRUE if bilateral factor can be applied';
COMMENT ON COLUMN dim_medical_conditions.active_flag IS 'TRUE if this condition is currently active/recognized';
COMMENT ON COLUMN dim_medical_conditions.deprecated_date IS 'Date this condition was deprecated';
COMMENT ON COLUMN dim_medical_conditions.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_medical_conditions.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_medical_conditions.updated_timestamp IS 'Timestamp when record was last updated';
