-- =====================================================================================================================
-- PHASE 2: HIGH PRIORITY IMPROVEMENTS
-- =====================================================================================================================
-- Purpose: Address High Priority issues from code review
-- Priority: 2 (High)
-- Date: 2025-11-22
--
-- Improvements in this script:
-- 1. Externalize hardcoded configuration values
-- 2. Add clustering keys to large tables
-- 3. Document data type standards
--
-- Author: Code Review Remediation - Phase 2
-- =====================================================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());

-- =====================================================================================================================
-- PART 1: EXTERNALIZE HARDCODED CONFIGURATION VALUES
-- =====================================================================================================================

USE SCHEMA metadata;

-- Add Date Dimension Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('date_dimension', 'start_date', '2020-01-01', 'STRING', 'Start date for date dimension population', TRUE, FALSE),
    ('date_dimension', 'end_date', '2029-12-31', 'STRING', 'End date for date dimension population', TRUE, FALSE),
    ('date_dimension', 'fiscal_year_start_month', '10', 'NUMBER', 'Fiscal year starts in October (month 10)', TRUE, FALSE),
    ('date_dimension', 'auto_extend_years', '1', 'NUMBER', 'Years to auto-extend date dimension forward', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Add Timeout and Performance Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('performance', 'query_timeout_seconds', '300', 'NUMBER', 'Default query timeout (5 minutes)', TRUE, FALSE),
    ('performance', 'stored_proc_timeout_seconds', '600', 'NUMBER', 'Default stored procedure timeout (10 minutes)', TRUE, FALSE),
    ('performance', 'etl_timeout_seconds', '3600', 'NUMBER', 'ETL job timeout (1 hour)', TRUE, FALSE),
    ('performance', 'warehouse_size_etl', 'LARGE', 'STRING', 'Warehouse size for ETL operations', TRUE, FALSE),
    ('performance', 'warehouse_size_reporting', 'MEDIUM', 'STRING', 'Warehouse size for reporting', TRUE, FALSE),
    ('performance', 'auto_suspend_minutes', '5', 'NUMBER', 'Auto-suspend warehouse after N minutes', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Add Default Values Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('defaults', 'default_country', 'USA', 'STRING', 'Default country for addresses', TRUE, FALSE),
    ('defaults', 'default_buffer_time_minutes', '15', 'NUMBER', 'Default buffer time between appointments', TRUE, FALSE),
    ('defaults', 'default_advance_notice_days', '7', 'NUMBER', 'Default advance notice for appointments', TRUE, FALSE),
    ('defaults', 'unknown_value_placeholder', 'UNKNOWN', 'STRING', 'Placeholder for unknown dimension values', TRUE, FALSE),
    ('defaults', 'not_applicable_placeholder', 'N/A', 'STRING', 'Placeholder for not applicable values', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Add Testing Framework Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('testing', 'default_test_timeout_seconds', '300', 'NUMBER', 'Default test execution timeout', TRUE, FALSE),
    ('testing', 'run_tests_on_deployment', 'TRUE', 'BOOLEAN', 'Automatically run tests after deployment', TRUE, FALSE),
    ('testing', 'fail_on_critical_test_failure', 'TRUE', 'BOOLEAN', 'Fail deployment if critical tests fail', TRUE, FALSE),
    ('testing', 'parallel_test_execution', 'TRUE', 'BOOLEAN', 'Run tests in parallel', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

SELECT 'Configuration values externalized successfully' AS status;

-- =====================================================================================================================
-- PART 2: ADD CLUSTERING KEYS TO LARGE TABLES
-- =====================================================================================================================

USE SCHEMA WAREHOUSE;

-- Clustering Keys for Dimension Tables (SCD Type 2)
-- Pattern: Business key + is_current flag for optimal lookups

ALTER TABLE dim_veterans CLUSTER BY (veteran_id, is_current);
ALTER TABLE dim_evaluators CLUSTER BY (evaluator_id, is_current);
ALTER TABLE dim_facilities CLUSTER BY (facility_id, is_current);
ALTER TABLE dim_claims CLUSTER BY (claim_id, is_current);
ALTER TABLE dim_appointments CLUSTER BY (appointment_id);

SELECT 'Clustering keys added to dimension tables' AS status;

-- Clustering Keys for Fact Tables
-- Pattern: Date surrogate key + most common join key

ALTER TABLE fact_evaluations_completed CLUSTER BY (evaluation_date_sk, facility_sk);
-- Already has clustering: ✅

ALTER TABLE fact_claim_status CLUSTER BY (status_date_sk, claim_sk);
ALTER TABLE fact_appointments CLUSTER BY (appointment_date_sk, facility_sk);
ALTER TABLE fact_daily_snapshot CLUSTER BY (snapshot_date_sk);
ALTER TABLE fact_appointment_events CLUSTER BY (event_date_sk, appointment_sk);
ALTER TABLE fact_evaluation_qa_events CLUSTER BY (event_date_sk, evaluation_sk);
ALTER TABLE fact_exam_requests CLUSTER BY (request_received_date_sk, facility_sk);
ALTER TABLE fact_examiner_assignments CLUSTER BY (assignment_date_sk, evaluator_sk);
ALTER TABLE fact_exam_processing_bottlenecks CLUSTER BY (detection_date_sk, bottleneck_type_sk);

SELECT 'Clustering keys added to fact tables' AS status;

-- =====================================================================================================================
-- PART 3: DATA TYPE STANDARDS DOCUMENTATION
-- =====================================================================================================================

USE SCHEMA metadata;

-- Create Data Type Standards Reference Table
CREATE TABLE IF NOT EXISTS data_type_standards (
    category VARCHAR(50) NOT NULL,
    field_type VARCHAR(100) NOT NULL,
    snowflake_data_type VARCHAR(100) NOT NULL,
    size_specification VARCHAR(50),
    examples VARCHAR(500),
    usage_notes VARCHAR(1000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (category, field_type)
);

-- Populate Data Type Standards
MERGE INTO data_type_standards tgt
USING (
    SELECT * FROM VALUES
    -- Identifiers and Keys
    ('identifiers', 'business_key', 'VARCHAR(50)', '50', 'veteran_id, evaluator_id, claim_id', 'Standard size for all business keys. Use consistently across dimensions and facts.'),
    ('identifiers', 'surrogate_key', 'INTEGER AUTOINCREMENT', 'N/A', 'veteran_sk, facility_sk', 'Use AUTOINCREMENT for all dimension surrogate keys.'),
    ('identifiers', 'degenerate_dimension', 'VARCHAR(50)', '50', 'exam_request_id, batch_id', 'Transaction identifiers stored in fact tables.'),

    -- Names and Descriptions
    ('text', 'person_name', 'VARCHAR(100)', '100', 'first_name, last_name', 'Sufficient for most person names.'),
    ('text', 'organization_name', 'VARCHAR(255)', '255', 'facility_name, employer_name', 'Longer for organization names.'),
    ('text', 'short_description', 'VARCHAR(500)', '500', 'claim_description, condition_description', 'For brief descriptions.'),
    ('text', 'long_description', 'VARCHAR(1000)', '1000', 'dq_issues, error_message', 'For detailed descriptions and error messages.'),
    ('text', 'full_text', 'TEXT', 'N/A', 'clinical_notes, exam_findings', 'Unlimited text for clinical documentation.'),

    -- Codes and Categories
    ('codes', 'short_code', 'VARCHAR(20)', '20', 'state_code, specialty_code', 'For standard code values.'),
    ('codes', 'long_code', 'VARCHAR(50)', '50', 'medical_condition_code', 'For longer code systems (ICD-10, etc).'),
    ('codes', 'category', 'VARCHAR(100)', '100', 'request_type, status_category', 'For category values.'),

    -- Contact Information
    ('contact', 'email', 'VARCHAR(255)', '255', 'email, notification_email', 'Standard email field size.'),
    ('contact', 'phone', 'VARCHAR(20)', '20', 'phone, fax', 'Supports international formats.'),
    ('contact', 'address_line', 'VARCHAR(255)', '255', 'address_line1, address_line2', 'Standard address field.'),
    ('contact', 'city', 'VARCHAR(100)', '100', 'city', 'City names.'),
    ('contact', 'state', 'VARCHAR(2)', '2', 'state', 'US state abbreviation (2 chars).'),
    ('contact', 'zip_code', 'VARCHAR(10)', '10', 'zip_code', 'Supports ZIP+4 format.'),
    ('contact', 'country', 'VARCHAR(50)', '50', 'country', 'Country names.'),

    -- Numeric Values
    ('numeric', 'percentage', 'DECIMAL(5,2)', '5,2', 'disability_rating, success_rate', 'Range: 0.00 to 999.99'),
    ('numeric', 'amount', 'DECIMAL(10,2)', '10,2', 'payment_amount, cost', 'Standard currency/amount field.'),
    ('numeric', 'large_amount', 'DECIMAL(18,2)', '18,2', 'annual_revenue', 'For very large amounts.'),
    ('numeric', 'count', 'INTEGER', 'N/A', 'record_count, attempt_count', 'Whole number counts.'),
    ('numeric', 'score', 'DECIMAL(5,2)', '5,2', 'dq_score, quality_score', 'Typically 0-100 scale.'),
    ('numeric', 'duration_minutes', 'INTEGER', 'N/A', 'evaluation_duration_minutes', 'Duration in minutes.'),
    ('numeric', 'duration_decimal', 'DECIMAL(10,2)', '10,2', 'execution_duration_minutes', 'When fractional minutes needed.'),

    -- Dates and Times
    ('datetime', 'date', 'DATE', 'N/A', 'date_of_birth, service_start_date', 'Calendar dates only.'),
    ('datetime', 'timestamp', 'TIMESTAMP_NTZ', 'N/A', 'created_timestamp, updated_timestamp', 'Use NTZ (no timezone) for consistency.'),
    ('datetime', 'time', 'TIME', 'N/A', 'appointment_time', 'Time of day without date.'),
    ('datetime', 'year', 'INTEGER', 'N/A', 'fiscal_year', 'Year as integer (2024, 2025).'),

    -- Boolean Flags
    ('boolean', 'flag', 'BOOLEAN', 'N/A', 'is_active, is_current, combat_veteran_flag', 'TRUE/FALSE values. Use _flag suffix.'),

    -- Hash and Metadata
    ('metadata', 'hash', 'VARCHAR(64)', '64', 'source_record_hash', 'MD5 hash (32 chars) or SHA256 (64 chars).'),
    ('metadata', 'batch_id', 'VARCHAR(50)', '50', 'batch_id', 'ETL batch identifier.'),
    ('metadata', 'source_system', 'VARCHAR(50)', '50', 'source_system', 'Source system name.'),
    ('metadata', 'user', 'VARCHAR(100)', '100', 'created_by, updated_by', 'User identifier.'),

    -- Arrays and JSON
    ('complex', 'json_data', 'VARIANT', 'N/A', 'config_details, metadata_json', 'For JSON objects.'),
    ('complex', 'array_data', 'ARRAY', 'N/A', 'business_key_columns', 'For arrays.')
) AS src (category, field_type, snowflake_data_type, size_specification, examples, usage_notes)
ON tgt.category = src.category AND tgt.field_type = src.field_type
WHEN MATCHED THEN UPDATE SET
    snowflake_data_type = src.snowflake_data_type,
    size_specification = src.size_specification,
    examples = src.examples,
    usage_notes = src.usage_notes
WHEN NOT MATCHED THEN INSERT (
    category, field_type, snowflake_data_type, size_specification, examples, usage_notes
) VALUES (
    src.category, src.field_type, src.snowflake_data_type, src.size_specification, src.examples, src.usage_notes
);

SELECT 'Data type standards documented' AS status;

-- =====================================================================================================================
-- PART 4: CREATE VIEW FOR DATA TYPE STANDARDS
-- =====================================================================================================================

CREATE OR REPLACE VIEW vw_data_type_standards AS
SELECT
    category,
    field_type,
    snowflake_data_type,
    size_specification,
    examples,
    usage_notes
FROM data_type_standards
ORDER BY category, field_type;

-- =====================================================================================================================
-- VERIFICATION QUERIES
-- =====================================================================================================================

-- Show new configuration categories
SELECT
    config_category,
    COUNT(*) AS config_count,
    STRING_AGG(config_key, ', ') AS config_keys
FROM system_configuration
WHERE config_category IN ('date_dimension', 'performance', 'defaults', 'testing')
GROUP BY config_category
ORDER BY config_category;

-- Show clustering information for dimensions
SELECT
    table_name,
    clustering_key,
    CASE
        WHEN clustering_key IS NOT NULL THEN '✅ Clustered'
        ELSE '❌ No Clustering'
    END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'dim_%'
ORDER BY table_name;

-- Show clustering information for facts
SELECT
    table_name,
    clustering_key,
    CASE
        WHEN clustering_key IS NOT NULL THEN '✅ Clustered'
        ELSE '❌ No Clustering'
    END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
  AND table_name LIKE 'fact_%'
ORDER BY table_name;

-- Show data type standards by category
SELECT
    category,
    COUNT(*) AS standards_count
FROM data_type_standards
GROUP BY category
ORDER BY category;

-- =====================================================================================================================
-- USAGE EXAMPLES
-- =====================================================================================================================

/*
-- Example 1: Use configuration instead of hardcoded values
-- OLD:
CALL populate_dim_dates('2020-01-01', '2029-12-31');

-- NEW:
CALL populate_dim_dates(
    fn_get_config('date_dimension', 'start_date'),
    fn_get_config('date_dimension', 'end_date')
);

-- Example 2: Use default values from configuration
-- OLD:
buffer_time_minutes INTEGER DEFAULT 15

-- NEW:
buffer_time_minutes INTEGER DEFAULT (SELECT fn_get_config_number('defaults', 'default_buffer_time_minutes'))

-- Example 3: Check data type standards before creating table
SELECT * FROM vw_data_type_standards WHERE field_type = 'business_key';
-- Result: Use VARCHAR(50) for all business keys

-- Example 4: Monitor clustering effectiveness
SELECT
    table_name,
    clustering_key,
    average_depth,
    average_overlaps
FROM INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY
WHERE table_schema = 'WAREHOUSE'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY table_name, start_time DESC;
*/

-- =====================================================================================================================
-- SUMMARY
-- =====================================================================================================================

SELECT '========================================' AS summary;
SELECT 'PHASE 2 IMPROVEMENTS COMPLETE' AS summary;
SELECT '========================================' AS summary;

SELECT
    'Configuration Categories Added' AS improvement,
    '4 (date_dimension, performance, defaults, testing)' AS details
UNION ALL
SELECT
    'Configuration Values Added',
    CAST(COUNT(*) AS VARCHAR) || ' values'
FROM system_configuration
WHERE config_category IN ('date_dimension', 'performance', 'defaults', 'testing')
UNION ALL
SELECT
    'Dimension Tables Clustered',
    '5 tables (veteran, evaluator, facility, claim, appointment)'
UNION ALL
SELECT
    'Fact Tables Clustered',
    '9 tables (all fact tables now have clustering)'
UNION ALL
SELECT
    'Data Type Standards Documented',
    CAST(COUNT(*) AS VARCHAR) || ' standards across ' || CAST(COUNT(DISTINCT category) AS VARCHAR) || ' categories'
FROM data_type_standards;

SELECT '========================================' AS next_steps;
SELECT 'NEXT STEPS' AS next_steps;
SELECT '========================================' AS next_steps;

SELECT
    '1. Update populate_dim_dates to use configuration values' AS step
UNION ALL
SELECT '2. Monitor clustering depth and reclustering costs'
UNION ALL
SELECT '3. Apply data type standards to new tables'
UNION ALL
SELECT '4. Update DEFAULT values to use fn_get_config()'
UNION ALL
SELECT '5. Review and standardize inconsistent data types';

-- =====================================================================================================================
-- END OF PHASE 2 IMPROVEMENTS
-- =====================================================================================================================
