-- =====================================================
-- DIM_VETERAN - Veteran Dimension (Type 2 SCD)
-- =====================================================
-- Purpose: Veteran demographic and service information
-- SCD Type: Type 2 (track historical changes)

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_VETERAN (
    VETERAN_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
    VETERAN_ID VARCHAR(50) NOT NULL,  -- Business key (e.g., SSN, VA ID)

    -- Personal Information
    FIRST_NAME VARCHAR(100),
    MIDDLE_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    FULL_NAME VARCHAR(255),
    DATE_OF_BIRTH DATE,
    AGE INTEGER,
    GENDER VARCHAR(20),

    -- Contact Information
    EMAIL VARCHAR(255),
    PHONE VARCHAR(20),
    ADDRESS_LINE1 VARCHAR(255),
    ADDRESS_LINE2 VARCHAR(255),
    CITY VARCHAR(100),
    STATE VARCHAR(2),
    ZIP_CODE VARCHAR(10),
    COUNTRY VARCHAR(50) DEFAULT 'USA',

    -- Military Service Information
    SERVICE_BRANCH VARCHAR(50),  -- Army, Navy, Air Force, Marines, Coast Guard, Space Force
    SERVICE_START_DATE DATE,
    SERVICE_END_DATE DATE,
    YEARS_OF_SERVICE DECIMAL(5,2),
    DISCHARGE_STATUS VARCHAR(50),  -- Honorable, General, Other Than Honorable, etc.
    SERVICE_ERA VARCHAR(50),  -- Vietnam, Gulf War, OEF/OIF, etc.
    COMBAT_VETERAN_FLAG BOOLEAN DEFAULT FALSE,
    MILITARY_RANK VARCHAR(50),
    MILITARY_OCCUPATION VARCHAR(100),

    -- Disability Information
    CURRENT_DISABILITY_RATING INTEGER,
    DISABILITY_RATING_CATEGORY VARCHAR(20),  -- 0%, 10-30%, 40-60%, 70-90%, 100%
    SERVICE_CONNECTED_FLAG BOOLEAN DEFAULT FALSE,

    -- VA Benefits Status
    VA_ENROLLED_FLAG BOOLEAN DEFAULT FALSE,
    VA_ENROLLMENT_DATE DATE,
    PRIORITY_GROUP INTEGER,  -- VA Priority Groups 1-8

    -- SCD Type 2 attributes
    EFFECTIVE_START_DATE TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END_DATE TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    IS_CURRENT BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    SOURCE_SYSTEM VARCHAR(50),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for veteran demographic and service information';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_VETERAN.VETERAN_KEY IS 'Surrogate primary key for the veteran dimension';
COMMENT ON COLUMN DIM_VETERAN.VETERAN_ID IS 'Business key - Unique veteran identifier (SSN, VA ID, etc.)';
COMMENT ON COLUMN DIM_VETERAN.FIRST_NAME IS 'Veteran first name';
COMMENT ON COLUMN DIM_VETERAN.MIDDLE_NAME IS 'Veteran middle name';
COMMENT ON COLUMN DIM_VETERAN.LAST_NAME IS 'Veteran last name';
COMMENT ON COLUMN DIM_VETERAN.FULL_NAME IS 'Veteran full name (concatenated)';
COMMENT ON COLUMN DIM_VETERAN.DATE_OF_BIRTH IS 'Veteran date of birth';
COMMENT ON COLUMN DIM_VETERAN.AGE IS 'Current age of veteran';
COMMENT ON COLUMN DIM_VETERAN.GENDER IS 'Veteran gender (Male, Female, Other)';
COMMENT ON COLUMN DIM_VETERAN.EMAIL IS 'Primary email address';
COMMENT ON COLUMN DIM_VETERAN.PHONE IS 'Primary phone number';
COMMENT ON COLUMN DIM_VETERAN.ADDRESS_LINE1 IS 'Street address line 1';
COMMENT ON COLUMN DIM_VETERAN.ADDRESS_LINE2 IS 'Street address line 2 (Apt, Suite, etc.)';
COMMENT ON COLUMN DIM_VETERAN.CITY IS 'City';
COMMENT ON COLUMN DIM_VETERAN.STATE IS 'Two-letter state code (e.g., "CA", "TX")';
COMMENT ON COLUMN DIM_VETERAN.ZIP_CODE IS 'ZIP or postal code';
COMMENT ON COLUMN DIM_VETERAN.COUNTRY IS 'Country (default USA)';
COMMENT ON COLUMN DIM_VETERAN.SERVICE_BRANCH IS 'Military branch (Army, Navy, Air Force, Marines, Coast Guard, Space Force)';
COMMENT ON COLUMN DIM_VETERAN.SERVICE_START_DATE IS 'Date military service began';
COMMENT ON COLUMN DIM_VETERAN.SERVICE_END_DATE IS 'Date military service ended';
COMMENT ON COLUMN DIM_VETERAN.YEARS_OF_SERVICE IS 'Total years of military service';
COMMENT ON COLUMN DIM_VETERAN.DISCHARGE_STATUS IS 'Discharge status (Honorable, General, Other Than Honorable, etc.)';
COMMENT ON COLUMN DIM_VETERAN.SERVICE_ERA IS 'Service era (Vietnam, Gulf War, OEF/OIF, etc.)';
COMMENT ON COLUMN DIM_VETERAN.COMBAT_VETERAN_FLAG IS 'TRUE if veteran served in combat zone';
COMMENT ON COLUMN DIM_VETERAN.MILITARY_RANK IS 'Highest military rank achieved';
COMMENT ON COLUMN DIM_VETERAN.MILITARY_OCCUPATION IS 'Primary military occupational specialty (MOS)';
COMMENT ON COLUMN DIM_VETERAN.CURRENT_DISABILITY_RATING IS 'Current VA disability rating percentage (0-100)';
COMMENT ON COLUMN DIM_VETERAN.DISABILITY_RATING_CATEGORY IS 'Rating category (0%, 10-30%, 40-60%, 70-90%, 100%)';
COMMENT ON COLUMN DIM_VETERAN.SERVICE_CONNECTED_FLAG IS 'TRUE if veteran has service-connected disabilities';
COMMENT ON COLUMN DIM_VETERAN.VA_ENROLLED_FLAG IS 'TRUE if enrolled in VA healthcare';
COMMENT ON COLUMN DIM_VETERAN.VA_ENROLLMENT_DATE IS 'Date enrolled in VA healthcare system';
COMMENT ON COLUMN DIM_VETERAN.PRIORITY_GROUP IS 'VA priority group for healthcare (1-8, lower is higher priority)';
COMMENT ON COLUMN DIM_VETERAN.EFFECTIVE_START_DATE IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN DIM_VETERAN.EFFECTIVE_END_DATE IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN DIM_VETERAN.IS_CURRENT IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN DIM_VETERAN.SOURCE_SYSTEM IS 'Source system that provided this data';
COMMENT ON COLUMN DIM_VETERAN.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_VETERAN.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
