-- =====================================================================================================================
-- COMMON DATA FUNCTIONS LIBRARY
-- =====================================================================================================================
-- Purpose: Reusable SQL UDFs (User-Defined Functions) for data transformation, validation, and cleansing
-- Created: 2025-11-20
-- Author: Data Engineering Team
--
-- This library contains commonly-used functions to:
--   1. Reduce code duplication across ETL procedures
--   2. Ensure consistent data transformations
--   3. Simplify maintenance (fix once, apply everywhere)
--   4. Improve code readability
--
-- Usage:
--   - These functions are marked IMMUTABLE where possible for query optimization
--   - All functions handle NULL inputs gracefully
--   - Function names follow fn_<verb>_<noun> convention
-- =====================================================================================================================

USE SCHEMA warehouse;

-- =====================================================================================================================
-- SECTION 1: STRING NORMALIZATION & CLEANING
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_normalize_string_upper
-- Purpose: Standardizes string fields by trimming whitespace and converting to uppercase
-- Use Cases: Names, addresses, cities, states, status codes, categorical fields
-- Example: fn_normalize_string_upper('  New York  ') -> 'NEW YORK'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_normalize_string_upper(input_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Trims whitespace and converts string to uppercase. Common for addresses, cities, states, and categorical fields.'
AS
$$
    UPPER(TRIM(input_value))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_normalize_string_lower
-- Purpose: Standardizes string fields by trimming whitespace and converting to lowercase
-- Use Cases: Email addresses, usernames, URLs
-- Example: fn_normalize_string_lower('  User@Example.COM  ') -> 'user@example.com'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_normalize_string_lower(input_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Trims whitespace and converts string to lowercase. Common for emails and usernames.'
AS
$$
    LOWER(TRIM(input_value))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_normalize_email
-- Purpose: Standardizes email addresses (lowercase, trimmed)
-- Use Cases: Email validation and normalization in all tables
-- Example: fn_normalize_email('  John.Doe@VA.GOV  ') -> 'john.doe@va.gov'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_normalize_email(email_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Normalizes email addresses by trimming whitespace and converting to lowercase per RFC 5321'
AS
$$
    LOWER(TRIM(email_value))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_clean_phone_number
-- Purpose: Removes all non-numeric characters from phone numbers
-- Use Cases: Phone number standardization across all dimension and fact tables
-- Example: fn_clean_phone_number('(555) 123-4567 ext.890') -> '5551234567890'
-- Note: Removes parentheses, hyphens, spaces, letters - keeps only digits
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_clean_phone_number(phone_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Removes all non-numeric characters from phone numbers. Example: (555) 123-4567 -> 5551234567'
AS
$$
    REGEXP_REPLACE(phone_value, '[^0-9]', '')
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_clean_zip_code
-- Purpose: Removes all characters except digits and hyphens from zip codes
-- Use Cases: Zip code standardization (supports both 5-digit and ZIP+4 format)
-- Example: fn_clean_zip_code('12345-6789 USA') -> '12345-6789'
-- Example: fn_clean_zip_code('ABC 90210') -> '90210'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_clean_zip_code(zip_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Removes all characters except digits and hyphens. Supports 5-digit and ZIP+4 formats. Example: "12345-6789 USA" -> "12345-6789"'
AS
$$
    REGEXP_REPLACE(zip_value, '[^0-9-]', '')
$$;

-- =====================================================================================================================
-- SECTION 2: DATA VALIDATION & CATEGORIZATION
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_validate_disability_rating
-- Purpose: Validates VA disability rating is within valid range (0-100)
-- Use Cases: Data quality validation in veteran dimension ETL
-- Example: fn_validate_disability_rating(85) -> 85
-- Example: fn_validate_disability_rating(150) -> NULL
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_validate_disability_rating(rating NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Validates VA disability rating is between 0-100 (inclusive). Returns NULL for invalid values.'
AS
$$
    CASE WHEN rating BETWEEN 0 AND 100 THEN rating ELSE NULL END
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_categorize_disability_rating
-- Purpose: Categorizes VA disability ratings into standard ranges per VA policy
-- Use Cases: Reporting, analytics, benefit eligibility calculations
-- Example: fn_categorize_disability_rating(85) -> '70-90%'
-- Example: fn_categorize_disability_rating(100) -> '100%'
-- Note: Based on VA disability compensation rate tables
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_categorize_disability_rating(rating NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Categorizes VA disability ratings into standard ranges: 0%, 10-30%, 40-60%, 70-90%, 100%. Used for reporting and benefit calculations.'
AS
$$
    CASE
        WHEN rating = 0 THEN '0%'
        WHEN rating BETWEEN 10 AND 30 THEN '10-30%'
        WHEN rating BETWEEN 40 AND 60 THEN '40-60%'
        WHEN rating BETWEEN 70 AND 90 THEN '70-90%'
        WHEN rating = 100 THEN '100%'
        ELSE NULL
    END
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_validate_priority_group
-- Purpose: Validates VA priority group is within valid range (1-8)
-- Use Cases: Enrollment priority validation
-- Example: fn_validate_priority_group(5) -> 5
-- Example: fn_validate_priority_group(10) -> NULL
-- Reference: VA Priority Groups determine order of enrollment and copay requirements
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_validate_priority_group(group_number NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Validates VA priority group is between 1-8 (inclusive). Returns NULL for invalid values. Priority groups determine enrollment order.'
AS
$$
    CASE WHEN group_number BETWEEN 1 AND 8 THEN group_number ELSE NULL END
$$;

-- =====================================================================================================================
-- SECTION 3: DATE & TIME CALCULATIONS
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_age
-- Purpose: Calculates current age from date of birth
-- Use Cases: Patient demographics, eligibility calculations
-- Example: fn_calculate_age('1980-05-15') -> 45 (as of 2025)
-- Note: Uses CURRENT_DATE() so result changes over time
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_age(birth_date DATE)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates age in years from date of birth to current date. Returns NULL if birth_date is NULL.'
AS
$$
    DATEDIFF(year, birth_date, CURRENT_DATE())
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_age_at_date
-- Purpose: Calculates age at a specific date (useful for historical analysis)
-- Use Cases: Age at time of exam, age at enrollment, historical reporting
-- Example: fn_calculate_age_at_date('1980-05-15', '2020-01-01') -> 39
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_age_at_date(birth_date DATE, as_of_date DATE)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Calculates age in years at a specific date. Useful for historical analysis.'
AS
$$
    DATEDIFF(year, birth_date, as_of_date)
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_years_of_service
-- Purpose: Calculates years of military service, handling active service (NULL end date)
-- Use Cases: Veteran benefits eligibility, demographic analysis
-- Example: fn_calculate_years_of_service('2010-01-15', '2020-01-15') -> 10.00
-- Example: fn_calculate_years_of_service('2020-01-15', NULL) -> 5.84 (calculates to current date)
-- Note: Uses 365.25 to account for leap years
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_years_of_service(
    start_date DATE,
    end_date DATE
)
RETURNS DECIMAL(5,2)
LANGUAGE SQL
COMMENT = 'Calculates years of military service. If end_date is NULL, calculates to current date (for active service). Accounts for leap years (365.25).'
AS
$$
    ROUND(DATEDIFF(day, start_date, COALESCE(end_date, CURRENT_DATE())) / 365.25, 2)
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_business_days
-- Purpose: Calculates business days (weekdays) between two dates
-- Use Cases: SLA calculations, processing time metrics
-- Example: fn_calculate_business_days('2025-01-06', '2025-01-10') -> 5 (Mon-Fri)
-- Note: Does NOT account for holidays - only excludes weekends (Sat/Sun)
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_business_days(
    start_date DATE,
    end_date DATE
)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Calculates number of weekdays (Mon-Fri) between two dates. Does NOT exclude holidays.'
AS
$$
    DATEDIFF(day, start_date, end_date) -
    (DATEDIFF(week, start_date, end_date) * 2) -
    CASE WHEN DAYOFWEEK(start_date) = 0 THEN 1 ELSE 0 END -  -- Sunday adjustment
    CASE WHEN DAYOFWEEK(end_date) = 6 THEN 1 ELSE 0 END      -- Saturday adjustment
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_cycle_time_days
-- Purpose: Calculates cycle time in days, using current date for in-progress items
-- Use Cases: SLA monitoring, process performance metrics, bottleneck analysis
-- Example: fn_calculate_cycle_time_days('2025-01-01', '2025-01-15') -> 14
-- Example: fn_calculate_cycle_time_days('2025-01-01', NULL) -> [days to today]
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_cycle_time_days(
    start_date DATE,
    end_date DATE
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates cycle time in days. If end_date is NULL, uses current date (for in-progress items).'
AS
$$
    DATEDIFF(day, start_date, COALESCE(end_date, CURRENT_DATE()))
$$;

-- =====================================================================================================================
-- SECTION 4: SLA & PERFORMANCE METRICS
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_check_sla_met
-- Purpose: Determines if SLA was met based on actual vs. allowed days
-- Use Cases: SLA compliance reporting, performance dashboards
-- Example: fn_check_sla_met(8, 10) -> TRUE (completed in 8 days, allowed 10)
-- Example: fn_check_sla_met(12, 10) -> FALSE (took 12 days, only allowed 10)
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_check_sla_met(
    actual_days NUMBER,
    sla_days_allowed NUMBER
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns TRUE if actual days is less than or equal to SLA days allowed. Returns NULL if either input is NULL.'
AS
$$
    CASE WHEN actual_days <= sla_days_allowed THEN TRUE ELSE FALSE END
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_sla_variance
-- Purpose: Calculates SLA variance (positive = late, negative = early, 0 = on time)
-- Use Cases: Performance analysis, identifying bottlenecks
-- Example: fn_calculate_sla_variance(12, 10) -> 2 (2 days late)
-- Example: fn_calculate_sla_variance(8, 10) -> -2 (2 days early)
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_sla_variance(
    actual_days NUMBER,
    sla_days_allowed NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Calculates SLA variance in days. Positive = late, Negative = early, 0 = on time. Returns NULL if either input is NULL.'
AS
$$
    actual_days - sla_days_allowed
$$;

-- =====================================================================================================================
-- SECTION 5: NULL HANDLING & DEFAULTS
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_default_false
-- Purpose: Coalesces boolean flags to FALSE when NULL
-- Use Cases: Combat veteran flag, service connected flag, VA enrolled flag, etc.
-- Example: fn_default_false(NULL) -> FALSE
-- Example: fn_default_false(TRUE) -> TRUE
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_default_false(flag_value BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns FALSE if input is NULL, otherwise returns the input value. Common for boolean flags.'
AS
$$
    COALESCE(flag_value, FALSE)
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_default_true
-- Purpose: Coalesces boolean flags to TRUE when NULL
-- Use Cases: Active status flags, enabled flags
-- Example: fn_default_true(NULL) -> TRUE
-- Example: fn_default_true(FALSE) -> FALSE
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_default_true(flag_value BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns TRUE if input is NULL, otherwise returns the input value.'
AS
$$
    COALESCE(flag_value, TRUE)
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_default_country
-- Purpose: Provides default country value (USA) for NULL or empty country fields
-- Use Cases: Address standardization, international data handling
-- Example: fn_default_country(NULL) -> 'USA'
-- Example: fn_default_country('CANADA') -> 'CANADA'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_default_country(
    country_value VARCHAR,
    default_country VARCHAR DEFAULT 'USA'
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns default country (USA) if input is NULL, after normalizing to uppercase. Configurable default.'
AS
$$
    COALESCE(UPPER(TRIM(country_value)), default_country)
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_coalesce_string
-- Purpose: Returns first non-null, non-empty string from up to 5 values
-- Use Cases: Multi-source data merging, fallback logic
-- Example: fn_coalesce_string(NULL, '', 'primary', 'backup') -> 'primary'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_coalesce_string(
    value1 VARCHAR,
    value2 VARCHAR DEFAULT NULL,
    value3 VARCHAR DEFAULT NULL,
    value4 VARCHAR DEFAULT NULL,
    value5 VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns first non-NULL and non-empty string from up to 5 values. Useful for multi-source data merging.'
AS
$$
    COALESCE(
        NULLIF(TRIM(value1), ''),
        NULLIF(TRIM(value2), ''),
        NULLIF(TRIM(value3), ''),
        NULLIF(TRIM(value4), ''),
        NULLIF(TRIM(value5), '')
    )
$$;

-- =====================================================================================================================
-- SECTION 6: HASH-BASED CHANGE DETECTION
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_generate_record_hash_5
-- Purpose: Generates MD5 hash from up to 5 fields for change detection
-- Use Cases: SCD Type 2 change detection, data versioning
-- Example: fn_generate_record_hash_5('John', 'Doe', 'john@va.gov', '5551234567', 'NY')
-- Note: All fields are coalesced to empty string and pipe-delimited before hashing
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_generate_record_hash_5(
    field1 VARCHAR DEFAULT '',
    field2 VARCHAR DEFAULT '',
    field3 VARCHAR DEFAULT '',
    field4 VARCHAR DEFAULT '',
    field5 VARCHAR DEFAULT ''
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Generates MD5 hash from up to 5 fields for change detection. Fields are coalesced to empty string and pipe-delimited.'
AS
$$
    MD5(CONCAT_WS('|',
        COALESCE(field1, ''),
        COALESCE(field2, ''),
        COALESCE(field3, ''),
        COALESCE(field4, ''),
        COALESCE(field5, '')
    ))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_generate_record_hash_10
-- Purpose: Generates MD5 hash from up to 10 fields for change detection
-- Use Cases: Complex dimension tables with many tracked attributes
-- Example: fn_generate_record_hash_10('John', 'Doe', '1980-01-01', ...)
-- Note: All fields are coalesced to empty string and pipe-delimited before hashing
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_generate_record_hash_10(
    field1 VARCHAR DEFAULT '',
    field2 VARCHAR DEFAULT '',
    field3 VARCHAR DEFAULT '',
    field4 VARCHAR DEFAULT '',
    field5 VARCHAR DEFAULT '',
    field6 VARCHAR DEFAULT '',
    field7 VARCHAR DEFAULT '',
    field8 VARCHAR DEFAULT '',
    field9 VARCHAR DEFAULT '',
    field10 VARCHAR DEFAULT ''
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Generates MD5 hash from up to 10 fields for change detection. Fields are coalesced to empty string and pipe-delimited.'
AS
$$
    MD5(CONCAT_WS('|',
        COALESCE(field1, ''),
        COALESCE(field2, ''),
        COALESCE(field3, ''),
        COALESCE(field4, ''),
        COALESCE(field5, ''),
        COALESCE(field6, ''),
        COALESCE(field7, ''),
        COALESCE(field8, ''),
        COALESCE(field9, ''),
        COALESCE(field10, '')
    ))
$$;

-- =====================================================================================================================
-- SECTION 7: DATA QUALITY SCORING
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_calculate_veteran_dq_score
-- Purpose: Calculates data quality score for veteran records (0-100 scale)
-- Use Cases: Data quality monitoring, data stewardship, source system evaluation
-- Scoring Breakdown:
--   - First Name: 15 points
--   - Last Name: 15 points
--   - Date of Birth: 15 points
--   - Veteran ID or SSN: 20 points
--   - Email: 10 points
--   - Phone: 10 points
--   - State: 5 points
--   - Valid Disability Rating: 10 points
--   Total: 100 points
-- Example: fn_calculate_veteran_dq_score('John', 'Doe', '1980-01-01', 'V123', NULL, 'email', '555', 'NY', 80) -> 85
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calculate_veteran_dq_score(
    first_name VARCHAR,
    last_name VARCHAR,
    date_of_birth DATE,
    veteran_id VARCHAR,
    ssn VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    state VARCHAR,
    disability_rating NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates data quality score for veteran records (0-100 scale). Weighted scoring: Name(15+15), DOB(15), ID(20), Email(10), Phone(10), State(5), Disability(10)'
AS
$$
    (
        (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
        (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
        (CASE WHEN date_of_birth IS NOT NULL THEN 15 ELSE 0 END) +
        (CASE WHEN veteran_id IS NOT NULL OR ssn IS NOT NULL THEN 20 ELSE 0 END) +
        (CASE WHEN email IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN phone IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
        (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
    )
$$;

-- =====================================================================================================================
-- SECTION 8: BUSINESS LOGIC HELPERS
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_is_complex_case
-- Purpose: Determines if exam request is complex based on number of requested conditions
-- Use Cases: Workload assignment, specialist routing, SLA adjustments
-- Example: fn_is_complex_case('PTSD,TBI,MST,Depression,Anxiety') -> TRUE (5 conditions > 3 threshold)
-- Example: fn_is_complex_case('PTSD,TBI', 3) -> FALSE (2 conditions <= 3 threshold)
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_is_complex_case(
    requested_conditions VARCHAR,
    complexity_threshold NUMBER DEFAULT 3
)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT = 'Determines if case is complex based on number of comma-separated conditions. Default threshold is 3 conditions.'
AS
$$
    CASE
        WHEN ARRAY_SIZE(SPLIT(requested_conditions, ',')) > complexity_threshold THEN TRUE
        ELSE FALSE
    END
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_categorize_exam_urgency
-- Purpose: Categorizes exam urgency based on priority and veteran characteristics
-- Use Cases: Scheduling priority, resource allocation
-- Example: fn_categorize_exam_urgency('URGENT', TRUE) -> 'CRITICAL'
-- Example: fn_categorize_exam_urgency('ROUTINE', FALSE) -> 'STANDARD'
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_categorize_exam_urgency(
    request_priority VARCHAR,
    combat_veteran_flag BOOLEAN
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Categorizes exam urgency: CRITICAL (urgent + combat vet), HIGH (urgent OR combat vet), STANDARD (routine)'
AS
$$
    CASE
        WHEN UPPER(request_priority) = 'URGENT' AND combat_veteran_flag = TRUE THEN 'CRITICAL'
        WHEN UPPER(request_priority) = 'URGENT' OR combat_veteran_flag = TRUE THEN 'HIGH'
        ELSE 'STANDARD'
    END
$$;

-- =====================================================================================================================
-- TESTING & VALIDATION QUERIES
-- =====================================================================================================================

-- Uncomment these queries to test the functions after creation:

/*
-- Test String Normalization
SELECT
    fn_normalize_string_upper('  new york  ') AS test_upper,  -- Should return 'NEW YORK'
    fn_normalize_email('  John.Doe@VA.GOV  ') AS test_email,  -- Should return 'john.doe@va.gov'
    fn_clean_phone_number('(555) 123-4567') AS test_phone,    -- Should return '5551234567'
    fn_clean_zip_code('12345-6789 USA') AS test_zip;          -- Should return '12345-6789'

-- Test Data Validation
SELECT
    fn_validate_disability_rating(85) AS valid_rating,        -- Should return 85
    fn_validate_disability_rating(150) AS invalid_rating,     -- Should return NULL
    fn_categorize_disability_rating(85) AS rating_category,   -- Should return '70-90%'
    fn_validate_priority_group(5) AS valid_group;             -- Should return 5

-- Test Date Calculations
SELECT
    fn_calculate_age('1980-05-15') AS age,                    -- Should return ~45
    fn_calculate_years_of_service('2010-01-01', '2020-01-01') AS years,  -- Should return 10.00
    fn_calculate_cycle_time_days('2025-01-01', '2025-01-15') AS cycle_days;  -- Should return 14

-- Test SLA Functions
SELECT
    fn_check_sla_met(8, 10) AS sla_met,                       -- Should return TRUE
    fn_check_sla_met(12, 10) AS sla_missed,                   -- Should return FALSE
    fn_calculate_sla_variance(12, 10) AS variance_late,       -- Should return 2
    fn_calculate_sla_variance(8, 10) AS variance_early;       -- Should return -2

-- Test Null Handling
SELECT
    fn_default_false(NULL) AS default_f,                      -- Should return FALSE
    fn_default_country(NULL) AS default_usa,                  -- Should return 'USA'
    fn_coalesce_string(NULL, '', 'primary', 'backup') AS first_valid;  -- Should return 'primary'

-- Test Hash Generation
SELECT
    fn_generate_record_hash_5('John', 'Doe', 'john@va.gov', '5551234567', 'NY') AS hash_5;

-- Test Data Quality Scoring
SELECT
    fn_calculate_veteran_dq_score(
        'John',           -- first_name: +15
        'Doe',            -- last_name: +15
        '1980-01-01',     -- date_of_birth: +15
        'V123456',        -- veteran_id: +20
        NULL,             -- ssn
        'john@va.gov',    -- email: +10
        '5551234567',     -- phone: +10
        'NY',             -- state: +5
        80                -- disability_rating: +10
    ) AS dq_score;        -- Should return 100

-- Test Business Logic
SELECT
    fn_is_complex_case('PTSD,TBI,MST,Depression') AS is_complex,  -- Should return TRUE (4 > 3)
    fn_categorize_exam_urgency('URGENT', TRUE) AS urgency;        -- Should return 'CRITICAL'
*/

-- =====================================================================================================================
-- MAINTENANCE LOG
-- =====================================================================================================================
-- Date       | Author              | Change
-- -----------|---------------------|------------------------------------------------------------------------
-- 2025-11-20 | Data Engineering    | Initial creation with 25+ common functions
--            |                     | Categories: String normalization, validation, dates, SLA, DQ scoring
-- =====================================================================================================================
