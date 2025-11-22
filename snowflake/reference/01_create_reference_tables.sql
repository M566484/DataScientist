-- =====================================================
-- VES Data Pipeline - Reference Data & Crosswalk Mappings
-- =====================================================
-- Purpose: Map between OMS (legacy) and VEMS source systems
-- Pattern: Lookup tables for code translation and data harmonization
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- =====================================================
-- Create Reference Schema
-- =====================================================

CREATE SCHEMA IF NOT EXISTS REFERENCE
    COMMENT = 'Reference data and crosswalk mappings between source systems (OMS and VEMS)';

USE SCHEMA REFERENCE;

-- =====================================================
-- System of Record Configuration
-- =====================================================

CREATE OR REPLACE TABLE ref_system_of_record (
    entity_type VARCHAR(50) PRIMARY KEY,
    primary_source_system VARCHAR(50) NOT NULL,  -- OMS or VEMS
    fallback_source_system VARCHAR(50),
    reconciliation_rule VARCHAR(100),  -- PREFER_PRIMARY, MOST_RECENT, MERGE_FIELDS
    conflict_resolution VARCHAR(100),
    notes TEXT,
    effective_date DATE DEFAULT CURRENT_DATE(),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Defines which source system is authoritative for each entity type';

-- Populate system of record rules
INSERT INTO ref_system_of_record (entity_type, primary_source_system, fallback_source_system, reconciliation_rule, conflict_resolution) VALUES
('VETERAN', 'OMS', 'VEMS', 'PREFER_PRIMARY', 'Use OMS veteran_id if exists, else VEMS'),
('EVALUATOR', 'VEMS', 'OMS', 'PREFER_PRIMARY', 'VEMS has more current evaluator data'),
('FACILITY', 'OMS', 'VEMS', 'PREFER_PRIMARY', 'OMS is master for facilities'),
('EXAM_REQUEST', 'OMS', 'VEMS', 'MOST_RECENT', 'Use most recently updated record'),
('APPOINTMENT', 'VEMS', NULL, 'SINGLE_SOURCE', 'VEMS is only source for appointments'),
('EVALUATION', 'OMS', 'VEMS', 'MERGE_FIELDS', 'Merge clinical data from OMS, scheduling from VEMS'),
('QA_EVENT', 'OMS', NULL, 'SINGLE_SOURCE', 'OMS is only source for QA'),
('CLAIM', 'OMS', NULL, 'SINGLE_SOURCE', 'OMS is only source for claims');

-- =====================================================
-- Field-Level Mapping: OMS to Standard
-- =====================================================

CREATE OR REPLACE TABLE ref_field_mapping_oms (
    entity_type VARCHAR(50) NOT NULL,
    oms_field_name VARCHAR(100) NOT NULL,
    standard_field_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50),
    transformation_rule VARCHAR(500),
    notes TEXT,
    active_flag BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (entity_type, oms_field_name)
)
COMMENT = 'Maps OMS field names to standardized field names';

-- Populate OMS field mappings
INSERT INTO ref_field_mapping_oms (entity_type, oms_field_name, standard_field_name, data_type, transformation_rule) VALUES
-- Veteran mappings
('VETERAN', 'vet_ssn', 'veteran_ssn', 'VARCHAR', 'Direct mapping'),
('VETERAN', 'vet_first', 'first_name', 'VARCHAR', 'UPPER(TRIM(vet_first))'),
('VETERAN', 'vet_last', 'last_name', 'VARCHAR', 'UPPER(TRIM(vet_last))'),
('VETERAN', 'disability_pct', 'disability_rating', 'INTEGER', 'CAST(disability_pct AS INTEGER)'),
('VETERAN', 'email_addr', 'email', 'VARCHAR', 'LOWER(TRIM(email_addr))'),
('VETERAN', 'phone_num', 'phone_primary', 'VARCHAR', 'Direct mapping'),

-- Evaluator mappings
('EVALUATOR', 'provider_npi', 'evaluator_npi', 'VARCHAR', 'Direct mapping'),
('EVALUATOR', 'provider_first', 'first_name', 'VARCHAR', 'UPPER(TRIM(provider_first))'),
('EVALUATOR', 'provider_last', 'last_name', 'VARCHAR', 'UPPER(TRIM(provider_last))'),
('EVALUATOR', 'specialty_code', 'specialty', 'VARCHAR', 'Use ref_code_mapping_specialty'),

-- Exam Request mappings
('EXAM_REQUEST', 'request_num', 'exam_request_id', 'VARCHAR', 'Direct mapping'),
('EXAM_REQUEST', 'va_req_num', 'va_request_number', 'VARCHAR', 'Direct mapping'),
('EXAM_REQUEST', 'req_type_code', 'request_type', 'VARCHAR', 'Use ref_code_mapping_request_type'),
('EXAM_REQUEST', 'req_date', 'request_received_date', 'DATE', 'CAST(req_date AS DATE)');

-- =====================================================
-- Field-Level Mapping: VEMS to Standard
-- =====================================================

CREATE OR REPLACE TABLE ref_field_mapping_vems (
    entity_type VARCHAR(50) NOT NULL,
    vems_field_name VARCHAR(100) NOT NULL,
    standard_field_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50),
    transformation_rule VARCHAR(500),
    notes TEXT,
    active_flag BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (entity_type, vems_field_name)
)
COMMENT = 'Maps VEMS field names to standardized field names';

-- Populate VEMS field mappings
INSERT INTO ref_field_mapping_vems (entity_type, vems_field_name, standard_field_name, data_type, transformation_rule) VALUES
-- Veteran mappings
('VETERAN', 'veteran_ssn', 'veteran_ssn', 'VARCHAR', 'Direct mapping'),
('VETERAN', 'first_name', 'first_name', 'VARCHAR', 'UPPER(TRIM(first_name))'),
('VETERAN', 'last_name', 'last_name', 'VARCHAR', 'UPPER(TRIM(last_name))'),
('VETERAN', 'disability_rating', 'disability_rating', 'INTEGER', 'Direct mapping'),
('VETERAN', 'email', 'email', 'VARCHAR', 'LOWER(TRIM(email))'),
('VETERAN', 'phone', 'phone_primary', 'VARCHAR', 'Direct mapping'),

-- Evaluator mappings
('EVALUATOR', 'npi_number', 'evaluator_npi', 'VARCHAR', 'Direct mapping'),
('EVALUATOR', 'first_name', 'first_name', 'VARCHAR', 'UPPER(TRIM(first_name))'),
('EVALUATOR', 'last_name', 'last_name', 'VARCHAR', 'UPPER(TRIM(last_name))'),
('EVALUATOR', 'specialty_name', 'specialty', 'VARCHAR', 'Direct mapping (already standardized)'),

-- Appointment mappings (VEMS only)
('APPOINTMENT', 'appointment_id', 'appointment_id', 'VARCHAR', 'Direct mapping'),
('APPOINTMENT', 'appointment_datetime', 'appointment_date', 'DATE', 'CAST(appointment_datetime AS DATE)'),
('APPOINTMENT', 'appointment_datetime', 'appointment_time', 'TIME', 'CAST(appointment_datetime AS TIME)');

-- =====================================================
-- Code Value Mappings: Specialty
-- =====================================================

CREATE OR REPLACE TABLE ref_code_mapping_specialty (
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_value VARCHAR(100),
    standard_code VARCHAR(50) NOT NULL,
    standard_value VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    notes TEXT,
    active_flag BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (source_system, source_code)
)
COMMENT = 'Maps specialty codes between OMS, VEMS, and standard values';

-- Populate specialty mappings
INSERT INTO ref_code_mapping_specialty (source_system, source_code, source_value, standard_code, standard_value, category) VALUES
-- OMS specialty codes
('OMS', 'PSYCH', 'Psychiatry', 'PSYCHIATRY', 'PSYCHIATRY', 'MENTAL_HEALTH'),
('OMS', 'ORTHO', 'Orthopedics', 'ORTHOPEDICS', 'ORTHOPEDICS', 'MUSCULOSKELETAL'),
('OMS', 'NEURO', 'Neurology', 'NEUROLOGY', 'NEUROLOGY', 'NEUROLOGICAL'),
('OMS', 'CARD', 'Cardiology', 'CARDIOLOGY', 'CARDIOLOGY', 'CARDIOVASCULAR'),
('OMS', 'GEN', 'General Medicine', 'GENERAL_MEDICINE', 'GENERAL MEDICINE', 'GENERAL'),

-- VEMS specialty codes (already standardized, but include for completeness)
('VEMS', 'PSYCHIATRY', 'Psychiatry', 'PSYCHIATRY', 'PSYCHIATRY', 'MENTAL_HEALTH'),
('VEMS', 'ORTHOPEDICS', 'Orthopedics', 'ORTHOPEDICS', 'ORTHOPEDICS', 'MUSCULOSKELETAL'),
('VEMS', 'NEUROLOGY', 'Neurology', 'NEUROLOGY', 'NEUROLOGY', 'NEUROLOGICAL'),
('VEMS', 'CARDIOLOGY', 'Cardiology', 'CARDIOLOGY', 'CARDIOLOGY', 'CARDIOVASCULAR'),
('VEMS', 'GENERAL MEDICINE', 'General Medicine', 'GENERAL_MEDICINE', 'GENERAL MEDICINE', 'GENERAL');

-- =====================================================
-- Code Value Mappings: Request Type
-- =====================================================

CREATE OR REPLACE TABLE ref_code_mapping_request_type (
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_value VARCHAR(100),
    standard_code VARCHAR(50) NOT NULL,
    standard_value VARCHAR(100) NOT NULL,
    priority_level VARCHAR(20),
    default_sla_days INTEGER,
    notes TEXT,
    active_flag BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (source_system, source_code)
)
COMMENT = 'Maps request type codes between OMS, VEMS, and standard values';

-- Populate request type mappings
INSERT INTO ref_code_mapping_request_type (source_system, source_code, source_value, standard_code, standard_value, priority_level, default_sla_days) VALUES
-- OMS request type codes
('OMS', 'CP', 'C&P Exam', 'CP_EXAM', 'C&P EXAM', 'ROUTINE', 21),
('OMS', 'DBQ', 'DBQ Form', 'DBQ_FORM', 'DBQ FORM', 'ROUTINE', 21),
('OMS', 'REEX', 'Re-examination', 'REEXAM', 'RE-EXAMINATION', 'PRIORITY', 14),
('OMS', 'IME', 'Independent Medical Exam', 'IME', 'INDEPENDENT MEDICAL EXAM', 'URGENT', 7),

-- VEMS request type codes
('VEMS', 'C&P_EXAM', 'C&P Exam', 'CP_EXAM', 'C&P EXAM', 'ROUTINE', 21),
('VEMS', 'DBQ', 'DBQ Form', 'DBQ_FORM', 'DBQ FORM', 'ROUTINE', 21),
('VEMS', 'RE_EXAM', 'Re-examination', 'REEXAM', 'RE-EXAMINATION', 'PRIORITY', 14),
('VEMS', 'IME', 'Independent Medical Exam', 'IME', 'INDEPENDENT MEDICAL EXAM', 'URGENT', 7);

-- =====================================================
-- Code Value Mappings: Appointment Status
-- =====================================================

CREATE OR REPLACE TABLE ref_code_mapping_appointment_status (
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_value VARCHAR(100),
    standard_code VARCHAR(50) NOT NULL,
    standard_value VARCHAR(100) NOT NULL,
    status_category VARCHAR(50),
    notes TEXT,
    active_flag BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (source_system, source_code)
)
COMMENT = 'Maps appointment status codes between systems';

INSERT INTO ref_code_mapping_appointment_status (source_system, source_code, source_value, standard_code, standard_value, status_category) VALUES
-- OMS status codes
('OMS', 'SCH', 'Scheduled', 'SCHEDULED', 'SCHEDULED', 'ACTIVE'),
('OMS', 'CNF', 'Confirmed', 'CONFIRMED', 'CONFIRMED', 'ACTIVE'),
('OMS', 'CAN', 'Cancelled', 'CANCELLED', 'CANCELLED', 'INACTIVE'),
('OMS', 'NS', 'No Show', 'NO_SHOW', 'NO_SHOW', 'INACTIVE'),
('OMS', 'COMP', 'Completed', 'COMPLETED', 'COMPLETED', 'COMPLETED'),

-- VEMS status codes
('VEMS', 'SCHEDULED', 'Scheduled', 'SCHEDULED', 'SCHEDULED', 'ACTIVE'),
('VEMS', 'CONFIRMED', 'Confirmed', 'CONFIRMED', 'CONFIRMED', 'ACTIVE'),
('VEMS', 'CANCELLED', 'Cancelled', 'CANCELLED', 'CANCELLED', 'INACTIVE'),
('VEMS', 'NO_SHOW', 'No Show', 'NO_SHOW', 'NO_SHOW', 'INACTIVE'),
('VEMS', 'COMPLETED', 'Completed', 'COMPLETED', 'COMPLETED', 'COMPLETED');

-- =====================================================
-- Entity ID Crosswalk: Veterans
-- =====================================================

CREATE OR REPLACE TABLE ref_entity_crosswalk_veteran (
    master_veteran_id VARCHAR(50) PRIMARY KEY,
    oms_veteran_id VARCHAR(50),
    oms_ssn VARCHAR(11),
    vems_veteran_id VARCHAR(50),
    vems_ssn VARCHAR(11),
    va_file_number VARCHAR(50),
    match_confidence DECIMAL(5,2),  -- 0-100 confidence score
    match_method VARCHAR(50),  -- SSN_MATCH, NAME_DOB_MATCH, MANUAL
    primary_source_system VARCHAR(50),  -- OMS or VEMS
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Crosswalk mapping veteran IDs between OMS and VEMS systems';

-- =====================================================
-- Entity ID Crosswalk: Evaluators
-- =====================================================

CREATE OR REPLACE TABLE ref_entity_crosswalk_evaluator (
    master_evaluator_id VARCHAR(50) PRIMARY KEY,
    oms_evaluator_id VARCHAR(50),
    oms_provider_id VARCHAR(50),
    vems_evaluator_id VARCHAR(50),
    npi_number VARCHAR(10),  -- Common across systems
    license_number VARCHAR(50),
    match_confidence DECIMAL(5,2),
    match_method VARCHAR(50),  -- NPI_MATCH, LICENSE_MATCH, NAME_MATCH
    primary_source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Crosswalk mapping evaluator IDs between OMS and VEMS systems';

-- =====================================================
-- Entity ID Crosswalk: Facilities
-- =====================================================

CREATE OR REPLACE TABLE ref_entity_crosswalk_facility (
    master_facility_id VARCHAR(50) PRIMARY KEY,
    oms_facility_id VARCHAR(50),
    oms_facility_code VARCHAR(20),
    vems_facility_id VARCHAR(50),
    va_station_number VARCHAR(10),  -- Common identifier
    facility_name VARCHAR(255),
    match_confidence DECIMAL(5,2),
    match_method VARCHAR(50),
    primary_source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Crosswalk mapping facility IDs between OMS and VEMS systems';

-- =====================================================
-- Data Quality Rules by Source System
-- =====================================================

CREATE OR REPLACE TABLE ref_data_quality_rules (
    rule_id VARCHAR(50) PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    source_system VARCHAR(50),  -- NULL means applies to all
    field_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,  -- REQUIRED, RANGE, FORMAT, LOOKUP
    rule_expression VARCHAR(500) NOT NULL,
    error_severity VARCHAR(20),  -- ERROR, WARNING, INFO
    dq_score_impact INTEGER,  -- Points deducted if rule fails
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Data quality validation rules by entity and source system';

-- Populate DQ rules
INSERT INTO ref_data_quality_rules (rule_id, entity_type, source_system, field_name, rule_type, rule_expression, error_severity, dq_score_impact) VALUES
('VET_SSN_REQ', 'VETERAN', NULL, 'veteran_ssn', 'REQUIRED', 'veteran_ssn IS NOT NULL', 'ERROR', 20),
('VET_SSN_FMT', 'VETERAN', NULL, 'veteran_ssn', 'FORMAT', 'LENGTH(veteran_ssn) = 9', 'ERROR', 10),
('VET_NAME_REQ', 'VETERAN', NULL, 'last_name', 'REQUIRED', 'last_name IS NOT NULL', 'ERROR', 15),
('VET_RATING_RNG', 'VETERAN', NULL, 'disability_rating', 'RANGE', 'disability_rating BETWEEN 0 AND 100', 'WARNING', 10),
('EVAL_NPI_REQ', 'EVALUATOR', NULL, 'evaluator_npi', 'REQUIRED', 'evaluator_npi IS NOT NULL', 'ERROR', 20),
('EVAL_NPI_FMT', 'EVALUATOR', NULL, 'evaluator_npi', 'FORMAT', 'LENGTH(evaluator_npi) = 10', 'WARNING', 5),
('REQ_DATE_REQ', 'EXAM_REQUEST', NULL, 'request_received_date', 'REQUIRED', 'request_received_date IS NOT NULL', 'ERROR', 15);

-- =====================================================
-- Reconciliation Log
-- =====================================================

CREATE OR REPLACE TABLE ref_reconciliation_log (
    reconciliation_id INTEGER AUTOINCREMENT PRIMARY KEY,
    batch_id VARCHAR(50),
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(50) NOT NULL,
    conflict_type VARCHAR(100),  -- DUPLICATE, FIELD_MISMATCH, MISSING_IN_SYSTEM
    oms_value VARIANT,
    vems_value VARIANT,
    resolved_value VARIANT,
    resolution_method VARCHAR(100),
    resolution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_by VARCHAR(100)
)
COMMENT = 'Log of data reconciliation decisions between OMS and VEMS';

-- =====================================================
-- Verification Queries
-- =====================================================

-- Show all reference tables
SELECT
    table_name,
    row_count,
    comment
FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
WHERE table_schema = 'REFERENCE'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Show code mappings for specialty
SELECT * FROM ref_code_mapping_specialty WHERE active_flag = TRUE ORDER BY source_system, source_code;

-- Show system of record configuration
SELECT * FROM ref_system_of_record ORDER BY entity_type;
