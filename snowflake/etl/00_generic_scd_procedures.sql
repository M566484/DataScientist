-- =====================================================================================================================
-- GENERIC SCD TYPE 2 PROCEDURES
-- =====================================================================================================================
-- Purpose: Metadata-driven generic procedures for loading dimension tables with SCD Type 2 logic
-- Replaces: 6+ individual dimension load procedures (130 lines each = 780+ total lines)
-- Benefits:
--   - Single source of truth for SCD Type 2 logic
--   - Add new dimensions via metadata, not code
--   - Guaranteed consistent SCD implementation
--   - Easier to maintain and test
--
-- Dependencies:
--   - metadata.scd_type2_config table must exist
--   - Staging tables must have matching structure to target dimensions
--   - Target tables must have: effective_start_date, effective_end_date, is_current, updated_timestamp
--
-- Usage:
--   CALL sp_load_scd_type2_generic('dim_veterans', :batch_id);
--   CALL sp_load_scd_type2_generic('dim_evaluators', :batch_id);
-- =====================================================================================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

-- =====================================================================================================================
-- PROCEDURE: Generic SCD Type 2 Loader
-- =====================================================================================================================

CREATE OR REPLACE PROCEDURE sp_load_scd_type2_generic(
    p_table_name VARCHAR,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generic SCD Type 2 loader (SQL version). Loads any dimension table using configuration from metadata.scd_type2_config.'
AS
$$
DECLARE
    -- Configuration variables
    v_schema VARCHAR;
    v_staging_schema VARCHAR;
    v_staging_table VARCHAR;
    v_business_keys ARRAY;
    v_hash_column VARCHAR;
    v_surrogate_key VARCHAR;
    v_active_flag BOOLEAN;
    v_enabled BOOLEAN;

    -- Database and table references
    v_dw_database VARCHAR;
    v_target_table VARCHAR;
    v_source_table VARCHAR;

    -- Dynamic SQL components
    v_key_join VARCHAR;
    v_column_list VARCHAR;
    v_update_sql VARCHAR;
    v_insert_sql VARCHAR;

    -- Row counts
    v_rows_updated INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;

    -- Configuration result set
    config_rs RESULTSET;
BEGIN
    -- ================================================================================================================
    -- STEP 1: Get Configuration
    -- ================================================================================================================

    config_rs := (
        SELECT
            schema_name,
            staging_schema,
            staging_table,
            business_key_columns,
            hash_column,
            surrogate_key_column,
            active_flag,
            enabled
        FROM metadata.scd_type2_config
        WHERE table_name = :p_table_name
    );

    -- Check if configuration exists
    LET c1 CURSOR FOR config_rs;
    OPEN c1;
    FETCH c1 INTO v_schema, v_staging_schema, v_staging_table, v_business_keys,
                  v_hash_column, v_surrogate_key, v_active_flag, v_enabled;

    IF (SQLCODE <> 0) THEN
        RETURN 'ERROR: No configuration found for table ''' || :p_table_name || ''' in metadata.scd_type2_config';
    END IF;

    CLOSE c1;

    -- Check if enabled
    IF (NOT v_enabled) THEN
        RETURN 'SKIPPED: Table ''' || :p_table_name || ''' is disabled in metadata.scd_type2_config';
    END IF;

    IF (NOT v_active_flag) THEN
        RETURN 'SKIPPED: Table ''' || :p_table_name || ''' is inactive in metadata.scd_type2_config';
    END IF;

    -- Build database reference
    BEGIN
        v_dw_database := (SELECT fn_get_dw_database());
    EXCEPTION
        WHEN OTHER THEN
            v_dw_database := CURRENT_DATABASE();
    END;

    v_target_table := v_dw_database || '.' || v_schema || '.' || :p_table_name;
    v_source_table := v_dw_database || '.' || v_staging_schema || '.' || v_staging_table;

    -- ================================================================================================================
    -- STEP 2: Build Business Key Join Condition
    -- ================================================================================================================

    -- Convert array to join condition: ['col1', 'col2'] -> 'tgt.col1 = src.col1 AND tgt.col2 = src.col2'
    SELECT ARRAY_TO_STRING(
        ARRAY_AGG('tgt.' || value || ' = src.' || value),
        ' AND '
    )
    INTO v_key_join
    FROM TABLE(FLATTEN(INPUT => v_business_keys));

    -- ================================================================================================================
    -- STEP 3: End-Date Changed Records (SCD Type 2 Logic)
    -- ================================================================================================================

    v_update_sql := '
        UPDATE ' || v_target_table || ' tgt
        SET
            effective_end_date = CURRENT_TIMESTAMP(),
            is_current = FALSE,
            updated_timestamp = CURRENT_TIMESTAMP()
        FROM ' || v_source_table || ' src
        WHERE ' || v_key_join || '
          AND tgt.is_current = TRUE
          AND tgt.' || v_hash_column || ' != src.' || v_hash_column;

    EXECUTE IMMEDIATE v_update_sql;
    v_rows_updated := SQLROWCOUNT;

    -- ================================================================================================================
    -- STEP 4: Get Column List (Exclude Surrogate Key and Metadata Fields)
    -- ================================================================================================================

    SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position)
    INTO v_column_list
    FROM information_schema.columns
    WHERE table_schema = UPPER(v_staging_schema)
      AND table_name = UPPER(v_staging_table)
      AND table_catalog = v_dw_database
      AND column_name NOT IN (
          UPPER(v_surrogate_key),
          'EFFECTIVE_START_DATE',
          'EFFECTIVE_END_DATE',
          'IS_CURRENT',
          'CREATED_TIMESTAMP',
          'UPDATED_TIMESTAMP'
      );

    IF (v_column_list IS NULL OR TRIM(v_column_list) = '') THEN
        RETURN 'ERROR: No columns found in staging table ''' || v_staging_table || '''';
    END IF;

    -- ================================================================================================================
    -- STEP 5: Insert New and Changed Records
    -- ================================================================================================================

    v_insert_sql := '
        INSERT INTO ' || v_target_table || ' (
            ' || v_column_list || ',
            effective_start_date,
            effective_end_date,
            is_current,
            created_timestamp,
            updated_timestamp
        )
        SELECT
            ' || v_column_list || ',
            CURRENT_TIMESTAMP() AS effective_start_date,
            TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59'') AS effective_end_date,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS created_timestamp,
            CURRENT_TIMESTAMP() AS updated_timestamp
        FROM ' || v_source_table || ' src
        WHERE src.batch_id = ''' || :p_batch_id || '''
          AND (
              -- New record (doesn''t exist in target)
              NOT EXISTS (
                  SELECT 1
                  FROM ' || v_target_table || ' tgt
                  WHERE ' || v_key_join || '
              )
              OR
              -- Changed record (was just end-dated in step 3)
              EXISTS (
                  SELECT 1
                  FROM ' || v_target_table || ' tgt
                  WHERE ' || v_key_join || '
                    AND tgt.is_current = FALSE
                    AND tgt.effective_end_date >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
              )
          )';

    EXECUTE IMMEDIATE v_insert_sql;
    v_rows_inserted := SQLROWCOUNT;

    -- ================================================================================================================
    -- STEP 6: Return Results
    -- ================================================================================================================

    RETURN 'SUCCESS: Loaded ' || :p_table_name ||
           ' - Updated: ' || v_rows_updated ||
           ', Inserted: ' || v_rows_inserted ||
           ' (Batch: ' || :p_batch_id || ')';
END;
$$;

-- =====================================================================================================================
-- PROCEDURE: Batch Load Multiple Dimensions
-- =====================================================================================================================

CREATE OR REPLACE PROCEDURE sp_load_all_dimensions_scd2(
    p_batch_id VARCHAR
)
RETURNS TABLE (
    table_name VARCHAR,
    status VARCHAR,
    rows_updated NUMBER,
    rows_inserted NUMBER,
    duration_seconds NUMBER,
    error_message VARCHAR
)
LANGUAGE SQL
COMMENT = 'Loads all enabled dimension tables using generic SCD Type 2 procedure (SQL version). Returns results table with status for each dimension.'
AS
$$
DECLARE
    v_table_name VARCHAR;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration_seconds NUMBER(10,3);
    v_status VARCHAR DEFAULT 'SUCCESS';
    v_error_msg VARCHAR DEFAULT NULL;
    v_result_msg VARCHAR;
    v_rows_updated INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;

    -- Result set for configuration
    config_rs RESULTSET;
    c_config CURSOR FOR config_rs;
BEGIN
    -- Create temporary table to store results
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_load_results (
        table_name VARCHAR,
        status VARCHAR,
        rows_updated NUMBER,
        rows_inserted NUMBER,
        duration_seconds NUMBER(10,3),
        error_message VARCHAR
    );

    TRUNCATE TABLE temp_load_results;

    -- ================================================================================================================
    -- Get list of enabled dimension tables from configuration
    -- ================================================================================================================

    config_rs := (
        SELECT table_name
        FROM metadata.scd_type2_config
        WHERE active_flag = TRUE
          AND enabled = TRUE
        ORDER BY table_name
    );

    -- ================================================================================================================
    -- Load each dimension table
    -- ================================================================================================================

    OPEN c_config;
    FETCH c_config INTO v_table_name;

    WHILE (SQLCODE = 0) DO
        v_start_time := CURRENT_TIMESTAMP();
        v_status := 'SUCCESS';
        v_error_msg := NULL;
        v_rows_updated := 0;
        v_rows_inserted := 0;

        BEGIN
            -- Call generic SCD procedure
            CALL sp_load_scd_type2_generic(:v_table_name, :p_batch_id) INTO :v_result_msg;

            -- Parse result message to extract row counts
            -- Format: "SUCCESS: Loaded dim_veterans - Updated: 5, Inserted: 10 (Batch: ...)"
            BEGIN
                v_rows_updated := REGEXP_SUBSTR(:v_result_msg, 'Updated:\\s*(\\d+)', 1, 1, 'e', 1)::INTEGER;
            EXCEPTION
                WHEN OTHER THEN
                    v_rows_updated := 0;
            END;

            BEGIN
                v_rows_inserted := REGEXP_SUBSTR(:v_result_msg, 'Inserted:\\s*(\\d+)', 1, 1, 'e', 1)::INTEGER;
            EXCEPTION
                WHEN OTHER THEN
                    v_rows_inserted := 0;
            END;

        EXCEPTION
            WHEN OTHER THEN
                v_status := 'ERROR';
                v_error_msg := SQLERRM;
                v_rows_updated := 0;
                v_rows_inserted := 0;
        END;

        v_end_time := CURRENT_TIMESTAMP();
        v_duration_seconds := DATEDIFF(millisecond, v_start_time, v_end_time) / 1000.0;

        -- Insert result into temp table
        INSERT INTO temp_load_results VALUES (
            :v_table_name,
            :v_status,
            :v_rows_updated,
            :v_rows_inserted,
            :v_duration_seconds,
            :v_error_msg
        );

        FETCH c_config INTO v_table_name;
    END WHILE;

    CLOSE c_config;

    -- ================================================================================================================
    -- Return Results
    -- ================================================================================================================

    LET result_query RESULTSET := (
        SELECT
            table_name,
            status,
            rows_updated,
            rows_inserted,
            duration_seconds,
            error_message
        FROM temp_load_results
        ORDER BY
            CASE status
                WHEN 'ERROR' THEN 1
                WHEN 'SKIPPED' THEN 2
                ELSE 3
            END,
            table_name
    );

    RETURN TABLE(result_query);
END;
$$;

-- =====================================================================================================================
-- PROCEDURE: Validate SCD Type 2 Data Integrity
-- =====================================================================================================================

CREATE OR REPLACE PROCEDURE sp_validate_scd_type2_integrity(
    p_table_name VARCHAR
)
RETURNS TABLE (
    check_name VARCHAR,
    check_status VARCHAR,
    issue_count NUMBER,
    details VARCHAR
)
LANGUAGE SQL
COMMENT = 'Validates SCD Type 2 data integrity for a dimension table. Checks for overlapping dates, missing current records, etc.'
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_full_table_name VARCHAR;
    v_business_keys ARRAY;
    v_key_join VARCHAR;
BEGIN
    -- Get configuration
    SELECT schema_name || '.' || p_table_name, business_key_columns
    INTO v_full_table_name, v_business_keys
    FROM metadata.scd_type2_config
    WHERE table_name = :p_table_name;

    -- Build key join for grouping
    SELECT ARRAY_TO_STRING(v_business_keys, ', ')
    INTO v_key_join;

    -- Return validation checks
    LET validation_sql VARCHAR := '
        WITH checks AS (
            -- Check 1: Multiple current records for same business key
            SELECT
                ''Multiple Current Records'' AS check_name,
                CASE WHEN COUNT(*) > 0 THEN ''FAIL'' ELSE ''PASS'' END AS check_status,
                COUNT(*) AS issue_count,
                ''Found '' || COUNT(*) || '' business keys with multiple current records'' AS details
            FROM (
                SELECT ' || v_key_join || '
                FROM ' || v_dw_database || '.' || v_full_table_name || '
                WHERE is_current = TRUE
                GROUP BY ' || v_key_join || '
                HAVING COUNT(*) > 1
            )

            UNION ALL

            -- Check 2: Records with effective_end_date <= effective_start_date
            SELECT
                ''Invalid Date Range'' AS check_name,
                CASE WHEN COUNT(*) > 0 THEN ''FAIL'' ELSE ''PASS'' END AS check_status,
                COUNT(*) AS issue_count,
                ''Found '' || COUNT(*) || '' records with end_date <= start_date'' AS details
            FROM ' || v_dw_database || '.' || v_full_table_name || '
            WHERE effective_end_date <= effective_start_date

            UNION ALL

            -- Check 3: Current records with end_date not set to 9999-12-31
            SELECT
                ''Current Records End Date'' AS check_name,
                CASE WHEN COUNT(*) > 0 THEN ''FAIL'' ELSE ''PASS'' END AS check_status,
                COUNT(*) AS issue_count,
                ''Found '' || COUNT(*) || '' current records without proper end_date'' AS details
            FROM ' || v_dw_database || '.' || v_full_table_name || '
            WHERE is_current = TRUE
              AND effective_end_date != TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59'')

            UNION ALL

            -- Check 4: Non-current records with end_date = 9999-12-31
            SELECT
                ''Non-Current Records End Date'' AS check_name,
                CASE WHEN COUNT(*) > 0 THEN ''FAIL'' ELSE ''PASS'' END AS check_status,
                COUNT(*) AS issue_count,
                ''Found '' || COUNT(*) || '' non-current records with end_date = 9999'' AS details
            FROM ' || v_dw_database || '.' || v_full_table_name || '
            WHERE is_current = FALSE
              AND effective_end_date = TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59'')

            UNION ALL

            -- Check 5: Business keys with no current record
            SELECT
                ''Missing Current Records'' AS check_name,
                CASE WHEN COUNT(*) > 0 THEN ''WARN'' ELSE ''PASS'' END AS check_status,
                COUNT(*) AS issue_count,
                ''Found '' || COUNT(*) || '' business keys without a current record'' AS details
            FROM (
                SELECT ' || v_key_join || '
                FROM ' || v_dw_database || '.' || v_full_table_name || '
                GROUP BY ' || v_key_join || '
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) = 0
            )
        )
        SELECT * FROM checks
        ORDER BY
            CASE check_status
                WHEN ''FAIL'' THEN 1
                WHEN ''WARN'' THEN 2
                ELSE 3
            END,
            check_name
    ';

    RETURN TABLE(
        EXECUTE IMMEDIATE :validation_sql
    );
END;
$$;

-- =====================================================================================================================
-- TESTING QUERIES
-- =====================================================================================================================

/*
-- Test 1: Load a single dimension
CALL sp_load_scd_type2_generic('dim_veterans', 'TEST_BATCH_001');

-- Test 2: Load all dimensions
CALL sp_load_all_dimensions_scd2('TEST_BATCH_002');

-- Test 3: Validate data integrity
CALL sp_validate_scd_type2_integrity('dim_veterans');

-- Test 4: Compare old vs new procedure results
-- Run both procedures on same data and compare row counts
CALL sp_load_dim_veterans('TEST_BATCH_003');  -- Old procedure
SELECT COUNT(*) AS old_count FROM dim_veterans WHERE batch_id = 'TEST_BATCH_003';

TRUNCATE TABLE dim_veterans;

CALL sp_load_scd_type2_generic('dim_veterans', 'TEST_BATCH_003');  -- New generic procedure
SELECT COUNT(*) AS new_count FROM dim_veterans WHERE batch_id = 'TEST_BATCH_003';

-- Counts should match!
*/

-- =====================================================================================================================
-- USAGE EXAMPLES
-- =====================================================================================================================

/*
-- Example 1: Replace individual dimension load procedures

-- BEFORE: Individual procedures (780 lines total)
CALL sp_load_dim_veterans(:batch_id);
CALL sp_load_dim_evaluators(:batch_id);
CALL sp_load_dim_facilities(:batch_id);
CALL sp_load_dim_clinical_conditions(:batch_id);
CALL sp_load_dim_request_types(:batch_id);
CALL sp_load_dim_exam_locations(:batch_id);

-- AFTER: Generic procedure (call multiple times)
CALL sp_load_scd_type2_generic('dim_veterans', :batch_id);
CALL sp_load_scd_type2_generic('dim_evaluators', :batch_id);
CALL sp_load_scd_type2_generic('dim_facilities', :batch_id);
CALL sp_load_scd_type2_generic('dim_clinical_conditions', :batch_id);
CALL sp_load_scd_type2_generic('dim_request_types', :batch_id);
CALL sp_load_scd_type2_generic('dim_exam_locations', :batch_id);

-- OR: Batch load all at once
CALL sp_load_all_dimensions_scd2(:batch_id);


-- Example 2: Add new dimension (just add config, no new procedure!)

-- Step 1: Add configuration
INSERT INTO metadata.scd_type2_config (table_name, staging_table, business_key_columns, surrogate_key_column)
VALUES ('dim_appointments', 'stg_appointments', ARRAY_CONSTRUCT('appointment_id'), 'appointment_key');

-- Step 2: Load it!
CALL sp_load_scd_type2_generic('dim_appointments', :batch_id);
-- Done! No new procedure needed!


-- Example 3: Validate data integrity after load
CALL sp_validate_scd_type2_integrity('dim_veterans');
-- Check results for any FAIL or WARN status
*/

-- =====================================================================================================================
-- MAINTENANCE LOG
-- =====================================================================================================================
-- Date       | Author              | Change
-- -----------|---------------------|------------------------------------------------------------------------
-- 2025-11-20 | Data Engineering    | Initial creation - Generic SCD Type 2 procedures
--            |                     | Replaces 6+ individual dimension load procedures (780+ lines)
--            |                     | Includes batch loader and validation procedure
-- =====================================================================================================================
