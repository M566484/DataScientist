-- =====================================================================================================================
-- METADATA SCHEMA: ETL Configuration & Orchestration
-- =====================================================================================================================
-- Purpose: Metadata-driven ETL framework for configuration, orchestration, and monitoring
-- Benefits:
--   - Add new entities via configuration, not code
--   - Dynamic procedure generation based on metadata
--   - Centralized pipeline management
--   - Auditable configuration changes
--
-- Tables Created:
--   1. scd_type2_config - Configuration for generic SCD Type 2 loading
--   2. etl_pipeline_config - Pipeline orchestration metadata
--   3. dq_scoring_rules - Data quality scoring configuration
--   4. etl_execution_log - Pipeline execution tracking
-- =====================================================================================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================================================================================
-- Create Metadata Schema
-- =====================================================================================================================

CREATE SCHEMA IF NOT EXISTS METADATA
    COMMENT = 'Metadata and configuration for ETL orchestration, data quality, and pipeline management';

USE SCHEMA METADATA;

-- =====================================================================================================================
-- TABLE 1: SCD Type 2 Configuration
-- =====================================================================================================================
-- Purpose: Configuration for generic SCD Type 2 dimension loading
-- Enables: One generic procedure to load all dimension tables
-- =====================================================================================================================

CREATE OR REPLACE TABLE scd_type2_config (
    config_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name VARCHAR(100) UNIQUE NOT NULL,
    schema_name VARCHAR(50) DEFAULT 'WAREHOUSE',
    staging_schema VARCHAR(50) DEFAULT 'STAGING',
    staging_table VARCHAR(100) NOT NULL,

    -- Key columns
    business_key_columns ARRAY NOT NULL,  -- Columns that identify unique entity (e.g., ['veteran_id'])
    hash_column VARCHAR(100) DEFAULT 'source_record_hash',
    surrogate_key_column VARCHAR(100),    -- Auto-increment key to exclude from INSERT

    -- SCD configuration
    scd_type INTEGER DEFAULT 2,
    track_effective_dates BOOLEAN DEFAULT TRUE,
    track_is_current BOOLEAN DEFAULT TRUE,

    -- Additional columns to exclude from staging->warehouse INSERT
    exclude_from_insert ARRAY,  -- e.g., ['batch_id', 'extraction_timestamp'] if not in target

    -- Performance hints
    cluster_by_columns ARRAY,
    estimated_row_count INTEGER,

    -- Control flags
    active_flag BOOLEAN DEFAULT TRUE,
    enabled BOOLEAN DEFAULT TRUE,
    skip_on_error BOOLEAN DEFAULT FALSE,

    -- Metadata
    notes TEXT,
    created_by VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Configuration for generic SCD Type 2 dimension loading. Each row defines how to load one dimension table.';

-- Populate configuration for existing dimensions
INSERT INTO metadata.scd_type2_config
    (table_name, staging_table, business_key_columns, surrogate_key_column, notes)
VALUES
    ('dim_veterans', 'stg_veterans',
     ARRAY_CONSTRUCT('veteran_id'),
     'veteran_key',
     'Veteran dimension - SCD Type 2 tracking for veteran attributes'),

    ('dim_evaluators', 'stg_evaluators',
     ARRAY_CONSTRUCT('evaluator_npi'),
     'evaluator_key',
     'Evaluator dimension - SCD Type 2 tracking for provider information'),

    ('dim_facilities', 'stg_facilities',
     ARRAY_CONSTRUCT('facility_id'),
     'facility_key',
     'Facility dimension - SCD Type 2 tracking for facility attributes'),

    ('dim_clinical_conditions', 'stg_clinical_conditions',
     ARRAY_CONSTRUCT('condition_code'),
     'condition_key',
     'Clinical conditions dimension - SCD Type 2 for condition definitions'),

    ('dim_request_types', 'stg_request_types',
     ARRAY_CONSTRUCT('request_type_code'),
     'request_type_key',
     'Request types dimension - SCD Type 2 for exam request classifications'),

    ('dim_exam_locations', 'stg_exam_locations',
     ARRAY_CONSTRUCT('location_id'),
     'location_key',
     'Exam locations dimension - SCD Type 2 for location attributes');

-- =====================================================================================================================
-- TABLE 2: ETL Pipeline Configuration
-- =====================================================================================================================
-- Purpose: Orchestration metadata for ETL pipelines
-- Enables: Dynamic pipeline execution based on configuration
-- =====================================================================================================================

CREATE OR REPLACE TABLE etl_pipeline_config (
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name VARCHAR(100) UNIQUE NOT NULL,
    entity_type VARCHAR(50) NOT NULL,

    -- Execution order and dependencies
    execution_order INTEGER NOT NULL,
    parallel_execution_group INTEGER,  -- Pipelines with same group can run in parallel
    depends_on_pipelines ARRAY,        -- Array of pipeline_names that must complete first

    -- Source configuration
    source_type VARCHAR(20),  -- SINGLE_SOURCE, MULTI_SOURCE, EXTERNAL
    source_table VARCHAR(200),

    -- Pipeline procedures (use naming convention if NULL)
    transform_procedure VARCHAR(200),  -- e.g., sp_transform_veterans_multi
    load_procedure VARCHAR(200),       -- e.g., sp_load_veterans_scd2
    full_pipeline_procedure VARCHAR(200),  -- e.g., sp_pipeline_veterans_multi

    -- Target tables
    staging_table VARCHAR(200),
    target_table VARCHAR(200),
    target_schema VARCHAR(50),

    -- Configuration
    scd_type INTEGER DEFAULT 2,
    load_type VARCHAR(20) DEFAULT 'FULL',  -- FULL, INCREMENTAL, DELTA
    batch_size INTEGER,

    -- Performance expectations
    estimated_duration_minutes INTEGER,
    estimated_row_count INTEGER,

    -- Control flags
    enabled BOOLEAN DEFAULT TRUE,
    skip_on_error BOOLEAN DEFAULT FALSE,
    retry_count INTEGER DEFAULT 0,
    retry_delay_seconds INTEGER DEFAULT 60,

    -- Alerting
    alert_on_failure BOOLEAN DEFAULT TRUE,
    alert_on_long_duration BOOLEAN DEFAULT FALSE,
    alert_email_list ARRAY,

    -- Metadata
    description TEXT,
    notes TEXT,
    created_by VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'ETL pipeline orchestration configuration. Defines execution order, dependencies, and procedures for each entity pipeline.';

-- Populate pipeline configuration
INSERT INTO metadata.etl_pipeline_config
    (pipeline_name, entity_type, execution_order, parallel_execution_group,
     source_type, transform_procedure, load_procedure, target_table,
     depends_on_pipelines, estimated_duration_minutes, description)
VALUES
    -- Dimension pipelines (can run in parallel)
    ('veterans_pipeline', 'VETERAN', 100, 1,
     'MULTI_SOURCE',
     'sp_transform_multisource_ods_to_staging_veterans',
     'sp_load_dim_veterans',
     'dim_veterans',
     ARRAY_CONSTRUCT(),
     5,
     'Load veteran dimension from OMS and VEMS sources'),

    ('evaluators_pipeline', 'EVALUATOR', 100, 1,
     'MULTI_SOURCE',
     'sp_transform_multisource_ods_to_staging_evaluators',
     'sp_load_dim_evaluators',
     'dim_evaluators',
     ARRAY_CONSTRUCT(),
     3,
     'Load evaluator dimension from OMS and VEMS sources'),

    ('facilities_pipeline', 'FACILITY', 100, 1,
     'MULTI_SOURCE',
     'sp_transform_multisource_ods_to_staging_facilities',
     'sp_load_dim_facilities',
     'dim_facilities',
     ARRAY_CONSTRUCT(),
     2,
     'Load facility dimension from OMS and VEMS sources'),

    ('clinical_conditions_pipeline', 'CLINICAL_CONDITION', 100, 1,
     'SINGLE_SOURCE',
     'sp_transform_ods_to_staging_clinical_conditions',
     'sp_load_dim_clinical_conditions',
     'dim_clinical_conditions',
     ARRAY_CONSTRUCT(),
     1,
     'Load clinical conditions reference dimension'),

    -- Fact pipelines (depend on dimensions)
    ('exam_requests_pipeline', 'EXAM_REQUEST', 200, 2,
     'MULTI_SOURCE',
     'sp_transform_multisource_ods_to_staging_exam_requests',
     'sp_load_fact_exam_requests',
     'fact_exam_requests',
     ARRAY_CONSTRUCT('veterans_pipeline', 'evaluators_pipeline', 'facilities_pipeline'),
     8,
     'Load exam requests fact table - depends on dimension tables'),

    ('evaluations_pipeline', 'EVALUATION', 200, 2,
     'MULTI_SOURCE',
     'sp_transform_multisource_ods_to_staging_evaluations',
     'sp_load_fact_evaluations',
     'fact_evaluations',
     ARRAY_CONSTRUCT('veterans_pipeline', 'evaluators_pipeline', 'exam_requests_pipeline'),
     6,
     'Load evaluations fact table - depends on exam requests'),

    ('qa_events_pipeline', 'QA_EVENT', 300, 3,
     'SINGLE_SOURCE',
     'sp_transform_ods_to_staging_qa_events',
     'sp_load_fact_qa_events',
     'fact_qa_events',
     ARRAY_CONSTRUCT('evaluations_pipeline'),
     4,
     'Load QA events fact table - depends on evaluations');

-- =====================================================================================================================
-- TABLE 3: Data Quality Scoring Rules
-- =====================================================================================================================
-- Purpose: Metadata-driven data quality scoring
-- Enables: Configurable DQ scores without code changes
-- =====================================================================================================================

CREATE OR REPLACE TABLE dq_scoring_rules (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type VARCHAR(50) NOT NULL,
    field_name VARCHAR(100) NOT NULL,

    -- Rule definition
    rule_type VARCHAR(20) NOT NULL,  -- NOT_NULL, RANGE, REGEX, CUSTOM_FUNCTION, REFERENCE_CHECK
    rule_condition VARCHAR(500),     -- e.g., 'IS NOT NULL', 'BETWEEN 0 AND 100', 'MATCHES \\d{3}-\\d{2}-\\d{4}'
    custom_function VARCHAR(200),    -- e.g., 'fn_validate_ssn', 'fn_validate_email'

    -- Scoring
    points_if_met INTEGER NOT NULL,
    points_if_not_met INTEGER DEFAULT 0,
    max_total_points_for_entity INTEGER,  -- Cached for performance

    -- Metadata
    field_importance VARCHAR(20),   -- CRITICAL, HIGH, MEDIUM, LOW
    field_category VARCHAR(50),     -- IDENTITY, CONTACT, DEMOGRAPHIC, CLINICAL, ADMINISTRATIVE
    rule_description TEXT,

    -- Control
    active_flag BOOLEAN DEFAULT TRUE,
    enforce_in_etl BOOLEAN DEFAULT FALSE,  -- If TRUE, reject records that fail this rule
    effective_date DATE DEFAULT CURRENT_DATE(),

    -- Audit
    created_by VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uq_entity_field_rule UNIQUE (entity_type, field_name, rule_type)
)
COMMENT = 'Data quality scoring rules configuration. Enables metadata-driven DQ scoring without hardcoded logic.';

-- Populate DQ rules for Veterans
INSERT INTO metadata.dq_scoring_rules
    (entity_type, field_name, rule_type, rule_condition, points_if_met, field_importance, field_category, rule_description)
VALUES
    -- CRITICAL identity fields (20 points each)
    ('VETERAN', 'veteran_va_id', 'NOT_NULL', 'veteran_va_id IS NOT NULL OR veteran_ssn IS NOT NULL', 20, 'CRITICAL', 'IDENTITY', 'At least one identifier (VA ID or SSN) must be present'),

    -- CRITICAL demographic fields (15 points each)
    ('VETERAN', 'first_name', 'NOT_NULL', 'first_name IS NOT NULL', 15, 'CRITICAL', 'DEMOGRAPHIC', 'First name is required for identification'),
    ('VETERAN', 'last_name', 'NOT_NULL', 'last_name IS NOT NULL', 15, 'CRITICAL', 'DEMOGRAPHIC', 'Last name is required for identification'),
    ('VETERAN', 'date_of_birth', 'NOT_NULL', 'date_of_birth IS NOT NULL', 15, 'CRITICAL', 'DEMOGRAPHIC', 'Date of birth is required for age calculation and eligibility'),

    -- HIGH importance contact fields (10 points each)
    ('VETERAN', 'email', 'NOT_NULL', 'email IS NOT NULL', 10, 'HIGH', 'CONTACT', 'Email enables electronic communication'),
    ('VETERAN', 'phone_primary', 'NOT_NULL', 'phone_primary IS NOT NULL', 10, 'HIGH', 'CONTACT', 'Phone number enables direct contact'),

    -- MEDIUM importance fields (5 points each)
    ('VETERAN', 'state', 'NOT_NULL', 'state IS NOT NULL', 5, 'MEDIUM', 'CONTACT', 'State is useful for geographic reporting'),
    ('VETERAN', 'zip_code', 'NOT_NULL', 'zip_code IS NOT NULL', 5, 'MEDIUM', 'CONTACT', 'Zip code enables geographic analysis'),

    -- HIGH importance validated fields (10 points each)
    ('VETERAN', 'disability_rating', 'RANGE', 'disability_rating BETWEEN 0 AND 100', 10, 'HIGH', 'CLINICAL', 'Disability rating must be valid VA rating (0-100)');

-- Populate DQ rules for Evaluators
INSERT INTO metadata.dq_scoring_rules
    (entity_type, field_name, rule_type, rule_condition, points_if_met, field_importance, field_category, rule_description)
VALUES
    ('EVALUATOR', 'evaluator_npi', 'NOT_NULL', 'evaluator_npi IS NOT NULL', 25, 'CRITICAL', 'IDENTITY', 'NPI is required unique identifier for evaluators'),
    ('EVALUATOR', 'first_name', 'NOT_NULL', 'first_name IS NOT NULL', 15, 'CRITICAL', 'DEMOGRAPHIC', 'First name is required'),
    ('EVALUATOR', 'last_name', 'NOT_NULL', 'last_name IS NOT NULL', 15, 'CRITICAL', 'DEMOGRAPHIC', 'Last name is required'),
    ('EVALUATOR', 'specialty', 'NOT_NULL', 'specialty IS NOT NULL', 20, 'CRITICAL', 'CLINICAL', 'Specialty determines exam assignment'),
    ('EVALUATOR', 'license_number', 'NOT_NULL', 'license_number IS NOT NULL', 15, 'HIGH', 'ADMINISTRATIVE', 'License number validates credentials'),
    ('EVALUATOR', 'license_state', 'NOT_NULL', 'license_state IS NOT NULL', 10, 'MEDIUM', 'ADMINISTRATIVE', 'License state indicates jurisdiction');

-- =====================================================================================================================
-- TABLE 4: ETL Execution Log
-- =====================================================================================================================
-- Purpose: Track pipeline execution history and performance
-- =====================================================================================================================

CREATE OR REPLACE TABLE etl_execution_log (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100),

    -- Execution details
    execution_start_timestamp TIMESTAMP_NTZ NOT NULL,
    execution_end_timestamp TIMESTAMP_NTZ,
    duration_seconds INTEGER,
    status VARCHAR(20),  -- RUNNING, SUCCESS, FAILED, SKIPPED

    -- Results
    rows_read INTEGER,
    rows_transformed INTEGER,
    rows_loaded INTEGER,
    rows_rejected INTEGER,

    -- Performance
    transform_duration_seconds INTEGER,
    load_duration_seconds INTEGER,

    -- Error handling
    error_message TEXT,
    error_code VARCHAR(50),
    retry_attempt INTEGER DEFAULT 0,

    -- Metadata
    executed_by VARCHAR(100),
    execution_host VARCHAR(200),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'ETL pipeline execution log. Tracks performance, success/failure, and row counts for each pipeline run.';

-- =====================================================================================================================
-- VIEW: Pipeline Execution Summary
-- =====================================================================================================================

CREATE OR REPLACE VIEW vw_pipeline_execution_summary AS
SELECT
    p.pipeline_name,
    p.entity_type,
    p.enabled,
    COUNT(e.execution_id) AS total_executions,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_executions,
    SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_executions,
    ROUND(AVG(e.duration_seconds), 2) AS avg_duration_seconds,
    MAX(e.execution_end_timestamp) AS last_execution_timestamp,
    MAX(CASE WHEN e.status = 'SUCCESS' THEN e.execution_end_timestamp END) AS last_successful_execution
FROM metadata.etl_pipeline_config p
LEFT JOIN metadata.etl_execution_log e
    ON p.pipeline_name = e.pipeline_name
    AND e.execution_start_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY p.pipeline_name, p.entity_type, p.enabled
ORDER BY p.execution_order;

-- =====================================================================================================================
-- VIEW: Data Quality Scoring Summary
-- =====================================================================================================================

CREATE OR REPLACE VIEW vw_dq_scoring_summary AS
SELECT
    entity_type,
    COUNT(*) AS total_rules,
    SUM(points_if_met) AS max_possible_score,
    SUM(CASE WHEN field_importance = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_rules,
    SUM(CASE WHEN field_importance = 'HIGH' THEN 1 ELSE 0 END) AS high_importance_rules,
    SUM(CASE WHEN field_importance = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_importance_rules,
    SUM(CASE WHEN field_importance = 'CRITICAL' THEN points_if_met ELSE 0 END) AS critical_points,
    SUM(CASE WHEN active_flag = TRUE THEN 1 ELSE 0 END) AS active_rules
FROM metadata.dq_scoring_rules
GROUP BY entity_type
ORDER BY entity_type;

-- =====================================================================================================================
-- TESTING QUERIES
-- =====================================================================================================================

/*
-- View all configuration tables
SELECT 'SCD Config' AS table_name, COUNT(*) AS row_count FROM metadata.scd_type2_config
UNION ALL
SELECT 'Pipeline Config', COUNT(*) FROM metadata.etl_pipeline_config
UNION ALL
SELECT 'DQ Rules', COUNT(*) FROM metadata.dq_scoring_rules;

-- View SCD configurations
SELECT * FROM metadata.scd_type2_config WHERE active_flag = TRUE ORDER BY table_name;

-- View pipeline execution order
SELECT
    execution_order,
    parallel_execution_group,
    pipeline_name,
    entity_type,
    ARRAY_TO_STRING(depends_on_pipelines, ', ') AS dependencies,
    enabled
FROM metadata.etl_pipeline_config
ORDER BY execution_order, parallel_execution_group, pipeline_name;

-- View DQ scoring by entity
SELECT * FROM metadata.vw_dq_scoring_summary;

-- View DQ rules for specific entity
SELECT
    field_name,
    field_importance,
    points_if_met,
    rule_description
FROM metadata.dq_scoring_rules
WHERE entity_type = 'VETERAN'
  AND active_flag = TRUE
ORDER BY
    CASE field_importance
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END,
    points_if_met DESC;
*/

-- =====================================================================================================================
-- MAINTENANCE LOG
-- =====================================================================================================================
-- Date       | Author              | Change
-- -----------|---------------------|------------------------------------------------------------------------
-- 2025-11-20 | Data Engineering    | Initial creation - Metadata schema for ETL configuration
--            |                     | Tables: SCD config, pipeline config, DQ rules, execution log
--            |                     | Enables metadata-driven ETL framework
-- =====================================================================================================================
