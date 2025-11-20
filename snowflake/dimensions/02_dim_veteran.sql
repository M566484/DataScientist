-- =====================================================
-- dim_veterans - Veteran Dimension (Type 2 SCD)
-- =====================================================
-- Purpose: Veteran demographic and service information
-- SCD Type: Type 2 (track historical changes)
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_veterans (
    veteran_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    veteran_id VARCHAR(50) NOT NULL,  -- Business key (e.g., SSN, VA ID)

    -- Personal Information
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    date_of_birth DATE,
    age INTEGER,
    gender VARCHAR(20),

    -- Contact Information
    email VARCHAR(255),
    phone VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',

    -- Military Service Information
    service_branch VARCHAR(50),  -- Army, Navy, Air Force, Marines, Coast Guard, Space Force
    service_start_date DATE,
    service_end_date DATE,
    years_of_service DECIMAL(5,2),
    discharge_status VARCHAR(50),  -- Honorable, General, Other Than Honorable, etc.
    service_era VARCHAR(50),  -- Vietnam, Gulf War, OEF/OIF, etc.
    combat_veteran_flag BOOLEAN DEFAULT FALSE,
    military_rank VARCHAR(50),
    military_occupation VARCHAR(100),

    -- Disability Information
    current_disability_rating INTEGER,
    disability_rating_category VARCHAR(20),  -- 0%, 10-30%, 40-60%, 70-90%, 100%
    service_connected_flag BOOLEAN DEFAULT FALSE,

    -- VA Benefits Status
    va_enrolled_flag BOOLEAN DEFAULT FALSE,
    va_enrollment_date DATE,
    priority_group INTEGER,  -- VA Priority Groups 1-8

    -- SCD Type 2 attributes
    effective_start_date TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for veteran demographic and service information';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_veterans.veteran_sk IS 'Surrogate primary key for the veteran dimension';
COMMENT ON COLUMN dim_veterans.veteran_id IS 'Business key - Unique veteran identifier (SSN, VA ID, etc.)';
COMMENT ON COLUMN dim_veterans.first_name IS 'Veteran first name';
COMMENT ON COLUMN dim_veterans.middle_name IS 'Veteran middle name';
COMMENT ON COLUMN dim_veterans.last_name IS 'Veteran last name';
COMMENT ON COLUMN dim_veterans.full_name IS 'Veteran full name (concatenated)';
COMMENT ON COLUMN dim_veterans.date_of_birth IS 'Veteran date of birth';
COMMENT ON COLUMN dim_veterans.age IS 'Current age of veteran';
COMMENT ON COLUMN dim_veterans.gender IS 'Veteran gender (Male, Female, Other)';
COMMENT ON COLUMN dim_veterans.email IS 'Primary email address';
COMMENT ON COLUMN dim_veterans.phone IS 'Primary phone number';
COMMENT ON COLUMN dim_veterans.address_line1 IS 'Street address line 1';
COMMENT ON COLUMN dim_veterans.address_line2 IS 'Street address line 2 (Apt, Suite, etc.)';
COMMENT ON COLUMN dim_veterans.city IS 'City';
COMMENT ON COLUMN dim_veterans.state IS 'Two-letter state code (e.g., "CA", "TX")';
COMMENT ON COLUMN dim_veterans.zip_code IS 'ZIP or postal code';
COMMENT ON COLUMN dim_veterans.country IS 'Country (default USA)';
COMMENT ON COLUMN dim_veterans.service_branch IS 'Military branch (Army, Navy, Air Force, Marines, Coast Guard, Space Force)';
COMMENT ON COLUMN dim_veterans.service_start_date IS 'Date military service began';
COMMENT ON COLUMN dim_veterans.service_end_date IS 'Date military service ended';
COMMENT ON COLUMN dim_veterans.years_of_service IS 'Total years of military service';
COMMENT ON COLUMN dim_veterans.discharge_status IS 'Discharge status (Honorable, General, Other Than Honorable, etc.)';
COMMENT ON COLUMN dim_veterans.service_era IS 'Service era (Vietnam, Gulf War, OEF/OIF, etc.)';
COMMENT ON COLUMN dim_veterans.combat_veteran_flag IS 'TRUE if veteran served in combat zone';
COMMENT ON COLUMN dim_veterans.military_rank IS 'Highest military rank achieved';
COMMENT ON COLUMN dim_veterans.military_occupation IS 'Primary military occupational specialty (MOS)';
COMMENT ON COLUMN dim_veterans.current_disability_rating IS 'Current VA disability rating percentage (0-100)';
COMMENT ON COLUMN dim_veterans.disability_rating_category IS 'Rating category (0%, 10-30%, 40-60%, 70-90%, 100%)';
COMMENT ON COLUMN dim_veterans.service_connected_flag IS 'TRUE if veteran has service-connected disabilities';
COMMENT ON COLUMN dim_veterans.va_enrolled_flag IS 'TRUE if enrolled in VA healthcare';
COMMENT ON COLUMN dim_veterans.va_enrollment_date IS 'Date enrolled in VA healthcare system';
COMMENT ON COLUMN dim_veterans.priority_group IS 'VA priority group for healthcare (1-8, lower is higher priority)';
COMMENT ON COLUMN dim_veterans.effective_start_date IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN dim_veterans.effective_end_date IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN dim_veterans.is_current IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN dim_veterans.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_veterans.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_veterans.updated_timestamp IS 'Timestamp when record was last updated';
