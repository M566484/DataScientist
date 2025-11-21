-- =====================================================================================
-- METADATA-DRIVEN ORCHESTRATION FRAMEWORK
-- =====================================================================================
-- Purpose: Simplify task orchestration from 20+ individual tasks to 5 metadata-driven tasks
--
-- BEFORE: 20+ hardcoded tasks, complex dependencies, difficult to maintain
-- AFTER:  5 generic tasks + metadata configuration table
--
-- Benefits:
-- 1. Add new dimensions/facts without creating new tasks
-- 2. Easily change execution order via metadata
-- 3. Enable/disable loads without code changes
-- 4. Clear visibility into what runs and when
--
-- Author: Data Team
-- Date: 2025-11-21
-- =====================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA metadata;

-- =====================================================================================
-- DIMENSION LOAD CONFIGURATION (Define what dimensions to load and in what order)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS dimension_load_config (
    dimension_id INTEGER PRIMARY KEY,
    dimension_name VARCHAR(100) NOT NULL,
    dimension_table_name VARCHAR(100) NOT NULL, -- e.g., 'dim_veteran'
    load_procedure_name VARCHAR(200) NOT NULL,  -- e.g., 'sp_load_dim_veteran'
    load_order INTEGER NOT NULL,                -- Execution order (1, 2, 3...)
    depends_on_dimensions VARCHAR(500),          -- JSON array: ['dim_date']
    is_active BOOLEAN DEFAULT TRUE,
    enable_parallel BOOLEAN DEFAULT FALSE,       -- Can run in parallel with others?
    estimated_duration_minutes INTEGER,
    description VARCHAR(1000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate dimension load configuration
MERGE INTO dimension_load_config tgt
USING (
    SELECT * FROM VALUES
    (1, 'Date Dimension', 'dim_date', 'sp_load_dim_date', 1, '[]', TRUE, FALSE, 1, 'Date dimension - must load first'),
    (2, 'Veteran Dimension', 'dim_veteran', 'sp_load_dim_veteran', 2, '["dim_date"]', TRUE, TRUE, 10, 'Veteran master data with SCD Type 2'),
    (3, 'Evaluator Dimension', 'dim_evaluator', 'sp_load_dim_evaluator', 2, '["dim_date"]', TRUE, TRUE, 5, 'Evaluator master data with SCD Type 2'),
    (4, 'Facility Dimension', 'dim_facility', 'sp_load_dim_facility', 2, '["dim_date"]', TRUE, TRUE, 3, 'Facility master data with SCD Type 2'),
    (5, 'Evaluation Type Dimension', 'dim_evaluation_type', 'sp_load_dim_evaluation_type', 3, '["dim_date"]', TRUE, TRUE, 2, 'Evaluation type reference data'),
    (6, 'Medical Condition Dimension', 'dim_medical_condition', 'sp_load_dim_medical_condition', 3, '["dim_date"]', TRUE, TRUE, 2, 'Medical condition reference data'),
    (7, 'Claim Dimension', 'dim_claim', 'sp_load_dim_claim', 4, '["dim_date", "dim_veteran"]', TRUE, FALSE, 15, 'Claim master data with SCD Type 2'),
    (8, 'Appointment Dimension', 'dim_appointment', 'sp_load_dim_appointment', 3, '["dim_date"]', TRUE, TRUE, 2, 'Appointment reference data'),
    (9, 'Exam Request Type Dimension', 'dim_exam_request_types', 'sp_load_dim_exam_request_types', 3, '["dim_date"]', TRUE, TRUE, 2, 'Exam request type reference data')
) AS src (dimension_id, dimension_name, dimension_table_name, load_procedure_name, load_order, depends_on_dimensions, is_active, enable_parallel, estimated_duration_minutes, description)
ON tgt.dimension_id = src.dimension_id
WHEN MATCHED THEN UPDATE SET
    dimension_name = src.dimension_name,
    dimension_table_name = src.dimension_table_name,
    load_procedure_name = src.load_procedure_name,
    load_order = src.load_order,
    depends_on_dimensions = src.depends_on_dimensions,
    is_active = src.is_active,
    enable_parallel = src.enable_parallel,
    estimated_duration_minutes = src.estimated_duration_minutes,
    description = src.description,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    dimension_id, dimension_name, dimension_table_name, load_procedure_name, load_order,
    depends_on_dimensions, is_active, enable_parallel, estimated_duration_minutes, description
) VALUES (
    src.dimension_id, src.dimension_name, src.dimension_table_name, src.load_procedure_name, src.load_order,
    src.depends_on_dimensions, src.is_active, src.enable_parallel, src.estimated_duration_minutes, src.description
);

-- =====================================================================================
-- FACT LOAD CONFIGURATION (Define what facts to load and in what order)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS fact_load_config (
    fact_id INTEGER PRIMARY KEY,
    fact_name VARCHAR(100) NOT NULL,
    fact_table_name VARCHAR(100) NOT NULL,      -- e.g., 'fact_evaluation'
    load_procedure_name VARCHAR(200) NOT NULL,  -- e.g., 'sp_load_fact_evaluation'
    load_order INTEGER NOT NULL,
    depends_on_dimensions VARCHAR(500),          -- JSON array: ['dim_veteran', 'dim_evaluator']
    depends_on_facts VARCHAR(500),               -- JSON array: ['fact_exam_requests']
    is_active BOOLEAN DEFAULT TRUE,
    enable_parallel BOOLEAN DEFAULT FALSE,
    estimated_duration_minutes INTEGER,
    description VARCHAR(1000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate fact load configuration
MERGE INTO fact_load_config tgt
USING (
    SELECT * FROM VALUES
    (1, 'Exam Requests Fact', 'fact_exam_requests', 'sp_load_fact_exam_requests', 1,
     '["dim_date", "dim_veteran", "dim_evaluator", "dim_facility"]', '[]', TRUE, FALSE, 20,
     'Accumulating snapshot - exam request lifecycle'),
    (2, 'Evaluations Fact', 'fact_evaluation', 'sp_load_fact_evaluation', 2,
     '["dim_date", "dim_veteran", "dim_evaluator", "dim_facility", "dim_medical_condition"]', '["fact_exam_requests"]', TRUE, FALSE, 30,
     'Transaction fact - completed evaluations'),
    (3, 'Appointment Events Fact', 'fact_appointment_events', 'sp_load_fact_appointment_events', 1,
     '["dim_date", "dim_veteran", "dim_appointment"]', '[]', TRUE, TRUE, 15,
     'Transaction fact - appointment scheduling events'),
    (4, 'Evaluation QA Events Fact', 'fact_evaluation_qa_events', 'sp_load_fact_evaluation_qa_events', 3,
     '["dim_date", "dim_evaluator"]', '["fact_evaluation"]', TRUE, FALSE, 10,
     'Transaction fact - QA review workflow'),
    (5, 'Examiner Assignments Fact', 'fact_examiner_assignments', 'sp_load_fact_examiner_assignments', 2,
     '["dim_date", "dim_evaluator", "dim_facility"]', '["fact_exam_requests"]', TRUE, TRUE, 8,
     'Transaction fact - workload distribution'),
    (6, 'Claim Status Fact', 'fact_claim_status', 'sp_load_fact_claim_status', 4,
     '["dim_date", "dim_claim", "dim_veteran"]', '["fact_evaluation"]', TRUE, FALSE, 25,
     'Accumulating snapshot - claim lifecycle milestones'),
    (7, 'Appointments Scheduled Fact', 'fact_appointments_scheduled', 'sp_load_fact_appointments_scheduled', 1,
     '["dim_date", "dim_appointment", "dim_facility"]', '[]', TRUE, TRUE, 12,
     'Accumulating snapshot - appointment scheduling'),
    (8, 'Daily Snapshot Fact', 'fact_daily_snapshot', 'sp_load_fact_daily_snapshot', 5,
     '["dim_date", "dim_facility"]', '["fact_evaluation", "fact_appointment_events"]', TRUE, FALSE, 5,
     'Periodic snapshot - daily operational KPIs'),
    (9, 'Bottleneck Analysis Fact', 'fact_exam_processing_bottlenecks', 'sp_load_fact_exam_processing_bottlenecks', 6,
     '["dim_date"]', '["fact_exam_requests", "fact_evaluation"]', TRUE, FALSE, 10,
     'Analysis fact - bottleneck detection and measurement')
) AS src (fact_id, fact_name, fact_table_name, load_procedure_name, load_order, depends_on_dimensions, depends_on_facts, is_active, enable_parallel, estimated_duration_minutes, description)
ON tgt.fact_id = src.fact_id
WHEN MATCHED THEN UPDATE SET
    fact_name = src.fact_name,
    fact_table_name = src.fact_table_name,
    load_procedure_name = src.load_procedure_name,
    load_order = src.load_order,
    depends_on_dimensions = src.depends_on_dimensions,
    depends_on_facts = src.depends_on_facts,
    is_active = src.is_active,
    enable_parallel = src.enable_parallel,
    estimated_duration_minutes = src.estimated_duration_minutes,
    description = src.description,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    fact_id, fact_name, fact_table_name, load_procedure_name, load_order, depends_on_dimensions,
    depends_on_facts, is_active, enable_parallel, estimated_duration_minutes, description
) VALUES (
    src.fact_id, src.fact_name, src.fact_table_name, src.load_procedure_name, src.load_order, src.depends_on_dimensions,
    src.depends_on_facts, src.is_active, src.enable_parallel, src.estimated_duration_minutes, src.description
);

-- =====================================================================================
-- GENERIC DIMENSION LOADER (Replaces 9 individual dimension tasks)
-- =====================================================================================

CREATE OR REPLACE PROCEDURE sp_load_all_dimensions(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_dimension_cursor CURSOR FOR
        SELECT dimension_name, load_procedure_name, estimated_duration_minutes
        FROM IDENTIFIER(:v_dw_database || '.metadata.dimension_load_config')
        WHERE is_active = TRUE
        ORDER BY load_order, dimension_name;
    v_dimension_name VARCHAR;
    v_procedure_name VARCHAR;
    v_estimated_duration INTEGER;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_duration_minutes NUMBER(10,2);
    v_dimensions_loaded INTEGER DEFAULT 0;
    v_error_message VARCHAR;
BEGIN
    -- Loop through all active dimensions in order
    FOR dimension_record IN v_dimension_cursor DO
        v_dimension_name := dimension_record.dimension_name;
        v_procedure_name := dimension_record.load_procedure_name;
        v_estimated_duration := dimension_record.estimated_duration_minutes;
        v_start_time := CURRENT_TIMESTAMP();

        BEGIN
            -- Call the dimension load procedure
            CALL IDENTIFIER(:v_dw_database || '.warehouse.' || :v_procedure_name)(:p_batch_id);

            v_end_time := CURRENT_TIMESTAMP();
            v_duration_minutes := DATEDIFF(second, v_start_time, v_end_time) / 60.0;
            v_dimensions_loaded := v_dimensions_loaded + 1;

            -- Log success
            CALL sp_log_pipeline_execution(
                'DIMENSION_LOAD: ' || :v_dimension_name,
                'SUCCEEDED',
                :v_duration_minutes,
                NULL, -- Records processed (not tracked at this level)
                0,
                NULL,
                :p_batch_id
            );

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                v_end_time := CURRENT_TIMESTAMP();
                v_duration_minutes := DATEDIFF(second, v_start_time, v_end_time) / 60.0;

                -- Log failure
                CALL sp_log_pipeline_execution(
                    'DIMENSION_LOAD: ' || :v_dimension_name,
                    'FAILED',
                    :v_duration_minutes,
                    NULL,
                    NULL,
                    :v_error_message,
                    :p_batch_id
                );

                -- Decide whether to continue or stop based on configuration
                IF (fn_get_config_boolean('pipeline', 'enable_parallel_processing') = FALSE) THEN
                    RETURN 'Dimension load failed: ' || :v_dimension_name || ' - ' || :v_error_message;
                END IF;
        END;
    END FOR;

    RETURN 'Loaded ' || :v_dimensions_loaded || ' dimensions successfully';
END;
$$;

-- =====================================================================================
-- GENERIC FACT LOADER (Replaces 9+ individual fact tasks)
-- =====================================================================================

CREATE OR REPLACE PROCEDURE sp_load_all_facts(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_fact_cursor CURSOR FOR
        SELECT fact_name, load_procedure_name, estimated_duration_minutes
        FROM IDENTIFIER(:v_dw_database || '.metadata.fact_load_config')
        WHERE is_active = TRUE
        ORDER BY load_order, fact_name;
    v_fact_name VARCHAR;
    v_procedure_name VARCHAR;
    v_estimated_duration INTEGER;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_duration_minutes NUMBER(10,2);
    v_facts_loaded INTEGER DEFAULT 0;
    v_error_message VARCHAR;
BEGIN
    -- Loop through all active facts in order
    FOR fact_record IN v_fact_cursor DO
        v_fact_name := fact_record.fact_name;
        v_procedure_name := fact_record.load_procedure_name;
        v_estimated_duration := fact_record.estimated_duration_minutes;
        v_start_time := CURRENT_TIMESTAMP();

        BEGIN
            -- Call the fact load procedure
            CALL IDENTIFIER(:v_dw_database || '.warehouse.' || :v_procedure_name)(:p_batch_id);

            v_end_time := CURRENT_TIMESTAMP();
            v_duration_minutes := DATEDIFF(second, v_start_time, v_end_time) / 60.0;
            v_facts_loaded := v_facts_loaded + 1;

            -- Log success
            CALL sp_log_pipeline_execution(
                'FACT_LOAD: ' || :v_fact_name,
                'SUCCEEDED',
                :v_duration_minutes,
                NULL,
                0,
                NULL,
                :p_batch_id
            );

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                v_end_time := CURRENT_TIMESTAMP();
                v_duration_minutes := DATEDIFF(second, v_start_time, v_end_time) / 60.0;

                -- Log failure
                CALL sp_log_pipeline_execution(
                    'FACT_LOAD: ' || :v_fact_name,
                    'FAILED',
                    :v_duration_minutes,
                    NULL,
                    NULL,
                    :v_error_message,
                    :p_batch_id
                );

                -- Continue to next fact (log but don't stop)
                -- Can be changed via configuration
        END;
    END FOR;

    RETURN 'Loaded ' || :v_facts_loaded || ' facts successfully';
END;
$$;

-- =====================================================================================
-- SIMPLIFIED MASTER PIPELINE (5 main steps instead of 20+ tasks)
-- =====================================================================================

CREATE OR REPLACE PROCEDURE sp_master_pipeline_simplified()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_duration_minutes NUMBER(10,2);
    v_result VARCHAR;
BEGIN
    -- Generate batch ID
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_start_time := CURRENT_TIMESTAMP();

    -- STEP 1: Extract from ODS (if applicable)
    -- CALL sp_extract_ods_data(v_batch_id);

    -- STEP 2: Process staging layer
    -- CALL sp_process_staging_layer(v_batch_id);

    -- STEP 3: Load all dimensions (metadata-driven)
    CALL sp_load_all_dimensions(v_batch_id);

    -- STEP 4: Load all facts (metadata-driven)
    CALL sp_load_all_facts(v_batch_id);

    -- STEP 5: Run data quality checks
    -- CALL sp_run_dq_checks(v_batch_id);

    v_end_time := CURRENT_TIMESTAMP();
    v_duration_minutes := DATEDIFF(second, v_start_time, v_end_time) / 60.0;

    -- Log overall pipeline execution
    CALL sp_log_pipeline_execution(
        'MASTER_PIPELINE',
        'SUCCEEDED',
        v_duration_minutes,
        NULL,
        0,
        NULL,
        v_batch_id
    );

    RETURN 'Master pipeline completed successfully. Batch ID: ' || v_batch_id ||
           ' | Duration: ' || ROUND(v_duration_minutes, 2) || ' minutes';
END;
$$;

-- =====================================================================================
-- CONFIGURATION VIEWS - Easy visibility
-- =====================================================================================

-- View dimension load order and status
CREATE OR REPLACE VIEW vw_dimension_load_plan AS
SELECT
    load_order,
    dimension_name,
    dimension_table_name,
    load_procedure_name,
    estimated_duration_minutes,
    is_active,
    enable_parallel,
    depends_on_dimensions,
    description
FROM dimension_load_config
ORDER BY load_order, dimension_name;

-- View fact load order and status
CREATE OR REPLACE VIEW vw_fact_load_plan AS
SELECT
    load_order,
    fact_name,
    fact_table_name,
    load_procedure_name,
    estimated_duration_minutes,
    is_active,
    enable_parallel,
    depends_on_dimensions,
    depends_on_facts,
    description
FROM fact_load_config
ORDER BY load_order, fact_name;

-- View complete pipeline execution plan
CREATE OR REPLACE VIEW vw_pipeline_execution_plan AS
SELECT
    'DIMENSION' AS object_type,
    load_order AS execution_order,
    dimension_name AS object_name,
    estimated_duration_minutes,
    is_active
FROM dimension_load_config
UNION ALL
SELECT
    'FACT' AS object_type,
    load_order + 100 AS execution_order, -- Facts run after dimensions
    fact_name AS object_name,
    estimated_duration_minutes,
    is_active
FROM fact_load_config
ORDER BY execution_order, object_name;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- View the complete execution plan
SELECT * FROM vw_pipeline_execution_plan WHERE is_active = TRUE;

-- View dimension load order
SELECT * FROM vw_dimension_load_plan;

-- View fact load order
SELECT * FROM vw_fact_load_plan;

-- Run the simplified master pipeline (replaces 20+ tasks!)
CALL sp_master_pipeline_simplified();

-- Disable a specific dimension load (no code changes!)
UPDATE dimension_load_config
SET is_active = FALSE
WHERE dimension_name = 'Claim Dimension';

-- Change execution order (no code changes!)
UPDATE fact_load_config
SET load_order = 3
WHERE fact_name = 'Appointment Events Fact';

-- Add a new dimension (no new task creation needed!)
INSERT INTO dimension_load_config VALUES
(10, 'New Dimension', 'dim_new', 'sp_load_dim_new', 5, '["dim_date"]', TRUE, FALSE, 5, 'New dimension description');
*/

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

SELECT
    'Metadata-Driven Orchestration Deployed' AS status,
    (SELECT COUNT(*) FROM dimension_load_config WHERE is_active = TRUE) AS active_dimensions,
    (SELECT COUNT(*) FROM fact_load_config WHERE is_active = TRUE) AS active_facts,
    (SELECT SUM(estimated_duration_minutes) FROM dimension_load_config WHERE is_active = TRUE) AS estimated_dim_duration_min,
    (SELECT SUM(estimated_duration_minutes) FROM fact_load_config WHERE is_active = TRUE) AS estimated_fact_duration_min;
