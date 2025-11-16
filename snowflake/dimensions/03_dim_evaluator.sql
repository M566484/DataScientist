-- =====================================================
-- DIM_EVALUATOR - Medical Evaluator Dimension
-- =====================================================
-- Purpose: Medical professionals and evaluators
-- SCD Type: Type 2

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_EVALUATOR (
    EVALUATOR_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
    EVALUATOR_ID VARCHAR(50) NOT NULL,  -- Business key (NPI, License Number)

    -- Personal Information
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    FULL_NAME VARCHAR(255),

    -- Professional Information
    SPECIALTY VARCHAR(100),  -- Psychiatry, Orthopedics, Neurology, etc.
    SUB_SPECIALTY VARCHAR(100),
    CREDENTIALS VARCHAR(100),  -- MD, DO, PhD, PsyD, etc.
    LICENSE_NUMBER VARCHAR(50),
    LICENSE_STATE VARCHAR(2),
    LICENSE_EXPIRATION_DATE DATE,
    NPI_NUMBER VARCHAR(10),  -- National Provider Identifier

    -- Employment Information
    EMPLOYER_NAME VARCHAR(255),
    EMPLOYMENT_TYPE VARCHAR(50),  -- VA Staff, Contract, Private
    HIRE_DATE DATE,

    -- Qualifications
    YEARS_OF_EXPERIENCE INTEGER,
    VA_CERTIFIED_FLAG BOOLEAN DEFAULT FALSE,
    CERTIFICATION_DATE DATE,
    BOARD_CERTIFIED_FLAG BOOLEAN DEFAULT FALSE,

    -- Performance Metrics
    AVERAGE_EVALUATION_TIME_MINUTES INTEGER,
    TOTAL_EVALUATIONS_COMPLETED INTEGER DEFAULT 0,

    -- Status
    ACTIVE_FLAG BOOLEAN DEFAULT TRUE,
    TERMINATION_DATE DATE,

    -- SCD Type 2 attributes
    EFFECTIVE_START_DATE TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END_DATE TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    IS_CURRENT BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    SOURCE_SYSTEM VARCHAR(50),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for medical evaluators and professionals';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_EVALUATOR.EVALUATOR_KEY IS 'Surrogate primary key for the evaluator dimension';
COMMENT ON COLUMN DIM_EVALUATOR.EVALUATOR_ID IS 'Business key - Unique evaluator identifier (NPI, License Number)';
COMMENT ON COLUMN DIM_EVALUATOR.FIRST_NAME IS 'Evaluator first name';
COMMENT ON COLUMN DIM_EVALUATOR.LAST_NAME IS 'Evaluator last name';
COMMENT ON COLUMN DIM_EVALUATOR.FULL_NAME IS 'Evaluator full name (concatenated)';
COMMENT ON COLUMN DIM_EVALUATOR.SPECIALTY IS 'Medical specialty (Psychiatry, Orthopedics, Neurology, etc.)';
COMMENT ON COLUMN DIM_EVALUATOR.SUB_SPECIALTY IS 'Sub-specialty within primary specialty';
COMMENT ON COLUMN DIM_EVALUATOR.CREDENTIALS IS 'Professional credentials (MD, DO, PhD, PsyD, etc.)';
COMMENT ON COLUMN DIM_EVALUATOR.LICENSE_NUMBER IS 'Medical license number';
COMMENT ON COLUMN DIM_EVALUATOR.LICENSE_STATE IS 'State where licensed to practice';
COMMENT ON COLUMN DIM_EVALUATOR.LICENSE_EXPIRATION_DATE IS 'License expiration date';
COMMENT ON COLUMN DIM_EVALUATOR.NPI_NUMBER IS 'National Provider Identifier (10-digit unique ID)';
COMMENT ON COLUMN DIM_EVALUATOR.EMPLOYER_NAME IS 'Current employer organization name';
COMMENT ON COLUMN DIM_EVALUATOR.EMPLOYMENT_TYPE IS 'Employment type (VA Staff, Contract, Private)';
COMMENT ON COLUMN DIM_EVALUATOR.HIRE_DATE IS 'Date hired or contracted';
COMMENT ON COLUMN DIM_EVALUATOR.YEARS_OF_EXPERIENCE IS 'Total years of professional experience';
COMMENT ON COLUMN DIM_EVALUATOR.VA_CERTIFIED_FLAG IS 'TRUE if VA certified to perform C&P exams';
COMMENT ON COLUMN DIM_EVALUATOR.CERTIFICATION_DATE IS 'Date of VA certification';
COMMENT ON COLUMN DIM_EVALUATOR.BOARD_CERTIFIED_FLAG IS 'TRUE if board certified in specialty';
COMMENT ON COLUMN DIM_EVALUATOR.AVERAGE_EVALUATION_TIME_MINUTES IS 'Average time per evaluation in minutes';
COMMENT ON COLUMN DIM_EVALUATOR.TOTAL_EVALUATIONS_COMPLETED IS 'Lifetime count of evaluations completed';
COMMENT ON COLUMN DIM_EVALUATOR.ACTIVE_FLAG IS 'TRUE if currently active/available';
COMMENT ON COLUMN DIM_EVALUATOR.TERMINATION_DATE IS 'Date employment/contract ended';
COMMENT ON COLUMN DIM_EVALUATOR.EFFECTIVE_START_DATE IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN DIM_EVALUATOR.EFFECTIVE_END_DATE IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN DIM_EVALUATOR.IS_CURRENT IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN DIM_EVALUATOR.SOURCE_SYSTEM IS 'Source system that provided this data';
COMMENT ON COLUMN DIM_EVALUATOR.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_EVALUATOR.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
