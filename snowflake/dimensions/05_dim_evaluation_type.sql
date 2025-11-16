-- =====================================================
-- DIM_EVALUATION_TYPE - Evaluation Type Dimension
-- =====================================================
-- Purpose: Types of medical evaluations
-- SCD Type: Type 1

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_EVALUATION_TYPE (
    EVALUATION_TYPE_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
    EVALUATION_TYPE_ID VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Evaluation Type Information
    EVALUATION_TYPE_NAME VARCHAR(255) NOT NULL,
    EVALUATION_TYPE_DESCRIPTION TEXT,
    EVALUATION_CATEGORY VARCHAR(100),  -- C&P Exam, Independent Medical Exam, Disability Evaluation, etc.

    -- Evaluation Characteristics
    TYPICAL_DURATION_MINUTES INTEGER,
    REQUIRES_SPECIALIST BOOLEAN DEFAULT FALSE,
    SPECIALTY_REQUIRED VARCHAR(100),
    COMPLEXITY_LEVEL VARCHAR(20),  -- Low, Medium, High

    -- Regulatory Information
    REGULATORY_REQUIREMENT VARCHAR(255),
    CFR_REFERENCE VARCHAR(100),  -- Code of Federal Regulations reference
    VBA_REFERENCE VARCHAR(100),  -- Veterans Benefits Administration reference

    -- Scheduling
    ADVANCE_NOTICE_DAYS INTEGER DEFAULT 7,
    ALLOW_TELEHEALTH BOOLEAN DEFAULT FALSE,
    REQUIRES_FASTING BOOLEAN DEFAULT FALSE,
    REQUIRES_PREP BOOLEAN DEFAULT FALSE,
    PREP_INSTRUCTIONS TEXT,

    -- Status
    ACTIVE_FLAG BOOLEAN DEFAULT TRUE,
    DEPRECATED_DATE DATE,
    REPLACEMENT_EVALUATION_TYPE_ID VARCHAR(50),

    -- Metadata
    SOURCE_SYSTEM VARCHAR(50),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for evaluation types and categories';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_EVALUATION_TYPE.EVALUATION_TYPE_KEY IS 'Surrogate primary key for the evaluation type dimension';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.EVALUATION_TYPE_ID IS 'Business key - Unique evaluation type identifier';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.EVALUATION_TYPE_NAME IS 'Name of the evaluation type';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.EVALUATION_TYPE_DESCRIPTION IS 'Detailed description of the evaluation';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.EVALUATION_CATEGORY IS 'Category (C&P Exam, Independent Medical Exam, Disability Evaluation, etc.)';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.TYPICAL_DURATION_MINUTES IS 'Expected duration in minutes';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.REQUIRES_SPECIALIST IS 'TRUE if specialist is required';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.SPECIALTY_REQUIRED IS 'Medical specialty required for this evaluation';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.COMPLEXITY_LEVEL IS 'Complexity level (Low, Medium, High)';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.REGULATORY_REQUIREMENT IS 'Regulatory requirement description';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.CFR_REFERENCE IS 'Code of Federal Regulations reference';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.VBA_REFERENCE IS 'Veterans Benefits Administration reference number';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.ADVANCE_NOTICE_DAYS IS 'Number of days advance notice required (default 7)';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.ALLOW_TELEHEALTH IS 'TRUE if telehealth is allowed for this type';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.REQUIRES_FASTING IS 'TRUE if patient must fast before evaluation';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.REQUIRES_PREP IS 'TRUE if patient preparation is required';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.PREP_INSTRUCTIONS IS 'Patient preparation instructions';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.ACTIVE_FLAG IS 'TRUE if this evaluation type is currently active';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.DEPRECATED_DATE IS 'Date this evaluation type was deprecated';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.REPLACEMENT_EVALUATION_TYPE_ID IS 'ID of replacement evaluation type if deprecated';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.SOURCE_SYSTEM IS 'Source system that provided this data';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_EVALUATION_TYPE.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
