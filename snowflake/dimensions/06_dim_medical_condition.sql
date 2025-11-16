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
