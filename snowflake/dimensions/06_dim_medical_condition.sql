-- =====================================================
-- DIM_MEDICAL_CONDITION - Medical Condition Dimension
-- =====================================================
-- Purpose: Medical conditions being evaluated
-- SCD Type: Type 1

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_MEDICAL_CONDITION (
    MEDICAL_CONDITION_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
    MEDICAL_CONDITION_ID VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Condition Information
    CONDITION_NAME VARCHAR(255) NOT NULL,
    CONDITION_DESCRIPTION TEXT,
    CONDITION_CATEGORY VARCHAR(100),  -- Physical, Mental Health, Neurological, etc.
    BODY_SYSTEM VARCHAR(100),  -- Musculoskeletal, Respiratory, Cardiovascular, etc.

    -- Medical Coding
    ICD10_CODE VARCHAR(10),
    ICD10_DESCRIPTION VARCHAR(500),
    DIAGNOSTIC_CODE VARCHAR(20),  -- VA Diagnostic Code
    DBQ_FORM_NUMBER VARCHAR(50),  -- Disability Benefits Questionnaire form

    -- Condition Characteristics
    CHRONIC_CONDITION_FLAG BOOLEAN DEFAULT FALSE,
    PRESUMPTIVE_CONDITION_FLAG BOOLEAN DEFAULT FALSE,  -- Presumptive service connection
    REQUIRES_NEXUS BOOLEAN DEFAULT TRUE,  -- Requires medical nexus opinion
    SEVERITY_SCALE VARCHAR(50),  -- Mild, Moderate, Severe, or custom scale

    -- Service Connection
    COMMON_SERVICE_ERA VARIANT,  -- JSON array of common service eras
    COMMON_EXPOSURE VARCHAR(255),  -- Agent Orange, Burn Pits, Radiation, etc.

    -- Rating Information
    MIN_RATING_PERCENTAGE INTEGER DEFAULT 0,
    MAX_RATING_PERCENTAGE INTEGER DEFAULT 100,
    RATING_INCREMENT INTEGER DEFAULT 10,
    BILATERAL_FACTOR_APPLICABLE BOOLEAN DEFAULT FALSE,

    -- Status
    ACTIVE_FLAG BOOLEAN DEFAULT TRUE,
    DEPRECATED_DATE DATE,

    -- Metadata
    SOURCE_SYSTEM VARCHAR(50),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for medical conditions and diagnoses';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.MEDICAL_CONDITION_KEY IS 'Surrogate primary key for the medical condition dimension';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.MEDICAL_CONDITION_ID IS 'Business key - Unique medical condition identifier';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.CONDITION_NAME IS 'Name of the medical condition';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.CONDITION_DESCRIPTION IS 'Detailed description of the condition';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.CONDITION_CATEGORY IS 'Category (Physical, Mental Health, Neurological, etc.)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.BODY_SYSTEM IS 'Affected body system (Musculoskeletal, Respiratory, Cardiovascular, etc.)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.ICD10_CODE IS 'ICD-10 diagnosis code';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.ICD10_DESCRIPTION IS 'ICD-10 code description';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.DIAGNOSTIC_CODE IS 'VA diagnostic code for rating purposes';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.DBQ_FORM_NUMBER IS 'Disability Benefits Questionnaire form number';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.CHRONIC_CONDITION_FLAG IS 'TRUE if this is a chronic condition';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.PRESUMPTIVE_CONDITION_FLAG IS 'TRUE if presumptive service connection applies';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.REQUIRES_NEXUS IS 'TRUE if medical nexus opinion is required';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.SEVERITY_SCALE IS 'Severity scale used (Mild, Moderate, Severe, or custom scale)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.COMMON_SERVICE_ERA IS 'JSON array of common service eras for this condition';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.COMMON_EXPOSURE IS 'Common exposure associated (Agent Orange, Burn Pits, Radiation, etc.)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.MIN_RATING_PERCENTAGE IS 'Minimum disability rating percentage (typically 0)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.MAX_RATING_PERCENTAGE IS 'Maximum disability rating percentage (typically 100)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.RATING_INCREMENT IS 'Rating increment step (typically 10)';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.BILATERAL_FACTOR_APPLICABLE IS 'TRUE if bilateral factor can be applied';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.ACTIVE_FLAG IS 'TRUE if this condition is currently active/recognized';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.DEPRECATED_DATE IS 'Date this condition was deprecated';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.SOURCE_SYSTEM IS 'Source system that provided this data';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_MEDICAL_CONDITION.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
