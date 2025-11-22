-- =====================================================
-- dim_evaluators - Medical Evaluator Dimension
-- =====================================================
-- Purpose: Medical professionals and evaluators
-- SCD Type: Type 2
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_evaluators (
    evaluator_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    evaluator_id VARCHAR(50) NOT NULL,  -- Business key (NPI, License Number)

    -- Personal Information
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),

    -- Professional Information
    specialty VARCHAR(100),  -- Psychiatry, Orthopedics, Neurology, etc.
    sub_specialty VARCHAR(100),
    credentials VARCHAR(100),  -- MD, DO, PhD, PsyD, etc.
    license_number VARCHAR(50),
    license_state VARCHAR(2),
    license_expiration_date DATE,
    npi_number VARCHAR(10),  -- National Provider Identifier

    -- Employment Information
    employer_name VARCHAR(255),
    employment_type VARCHAR(50),  -- VA Staff, Contract, Private
    hire_date DATE,

    -- Qualifications
    years_of_experience INTEGER,
    va_certified_flag BOOLEAN DEFAULT FALSE,
    certification_date DATE,
    board_certified_flag BOOLEAN DEFAULT FALSE,

    -- Performance Metrics
    average_evaluation_time_minutes INTEGER,
    total_evaluations_completed INTEGER DEFAULT 0,

    -- Status
    active_flag BOOLEAN DEFAULT TRUE,
    termination_date DATE,

    -- SCD Type 2 attributes
    effective_start_date TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for medical evaluators and professionals';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_evaluators.evaluator_sk IS 'Surrogate primary key for the evaluator dimension';
COMMENT ON COLUMN dim_evaluators.evaluator_id IS 'Business key - Unique evaluator identifier (NPI, License Number)';
COMMENT ON COLUMN dim_evaluators.first_name IS 'Evaluator first name';
COMMENT ON COLUMN dim_evaluators.last_name IS 'Evaluator last name';
COMMENT ON COLUMN dim_evaluators.full_name IS 'Evaluator full name (concatenated)';
COMMENT ON COLUMN dim_evaluators.specialty IS 'Medical specialty (Psychiatry, Orthopedics, Neurology, etc.)';
COMMENT ON COLUMN dim_evaluators.sub_specialty IS 'Sub-specialty within primary specialty';
COMMENT ON COLUMN dim_evaluators.credentials IS 'Professional credentials (MD, DO, PhD, PsyD, etc.)';
COMMENT ON COLUMN dim_evaluators.license_number IS 'Medical license number';
COMMENT ON COLUMN dim_evaluators.license_state IS 'State where licensed to practice';
COMMENT ON COLUMN dim_evaluators.license_expiration_date IS 'License expiration date';
COMMENT ON COLUMN dim_evaluators.npi_number IS 'National Provider Identifier (10-digit unique ID)';
COMMENT ON COLUMN dim_evaluators.employer_name IS 'Current employer organization name';
COMMENT ON COLUMN dim_evaluators.employment_type IS 'Employment type (VA Staff, Contract, Private)';
COMMENT ON COLUMN dim_evaluators.hire_date IS 'Date hired or contracted';
COMMENT ON COLUMN dim_evaluators.years_of_experience IS 'Total years of professional experience';
COMMENT ON COLUMN dim_evaluators.va_certified_flag IS 'TRUE if VA certified to perform C&P exams';
COMMENT ON COLUMN dim_evaluators.certification_date IS 'Date of VA certification';
COMMENT ON COLUMN dim_evaluators.board_certified_flag IS 'TRUE if board certified in specialty';
COMMENT ON COLUMN dim_evaluators.average_evaluation_time_minutes IS 'Average time per evaluation in minutes';
COMMENT ON COLUMN dim_evaluators.total_evaluations_completed IS 'Lifetime count of evaluations completed';
COMMENT ON COLUMN dim_evaluators.active_flag IS 'TRUE if currently active/available';
COMMENT ON COLUMN dim_evaluators.termination_date IS 'Date employment/contract ended';
COMMENT ON COLUMN dim_evaluators.effective_start_date IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN dim_evaluators.effective_end_date IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN dim_evaluators.is_current IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN dim_evaluators.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_evaluators.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_evaluators.updated_timestamp IS 'Timestamp when record was last updated';
