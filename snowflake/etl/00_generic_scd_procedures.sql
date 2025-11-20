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

SET dw_database = (SELECT get_dw_database());
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
LANGUAGE JAVASCRIPT
COMMENT = 'Generic SCD Type 2 loader. Loads any dimension table using configuration from metadata.scd_type2_config.'
AS
$$
    // ================================================================================================================
    // STEP 1: Get Configuration
    // ================================================================================================================

    var config_sql = `
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
        WHERE table_name = ?
    `;

    var config_stmt = snowflake.createStatement({
        sqlText: config_sql,
        binds: [P_TABLE_NAME]
    });

    var config_result = config_stmt.execute();

    if (!config_result.next()) {
        return "ERROR: No configuration found for table '" + P_TABLE_NAME + "' in metadata.scd_type2_config";
    }

    // Check if enabled
    if (!config_result.getColumnValue('ENABLED')) {
        return "SKIPPED: Table '" + P_TABLE_NAME + "' is disabled in metadata.scd_type2_config";
    }

    if (!config_result.getColumnValue('ACTIVE_FLAG')) {
        return "SKIPPED: Table '" + P_TABLE_NAME + "' is inactive in metadata.scd_type2_config";
    }

    // Extract configuration
    var schema = config_result.getColumnValue('SCHEMA_NAME');
    var staging_schema = config_result.getColumnValue('STAGING_SCHEMA');
    var staging_table = config_result.getColumnValue('STAGING_TABLE');
    var business_keys = config_result.getColumnValue('BUSINESS_KEY_COLUMNS');
    var hash_column = config_result.getColumnValue('HASH_COLUMN');
    var surrogate_key = config_result.getColumnValue('SURROGATE_KEY_COLUMN');

    // Build database reference
    var dw_database = snowflake.execute({sqlText: "SELECT get_dw_database()"}).next() ?
                      snowflake.execute({sqlText: "SELECT get_dw_database()"}).getColumnValue(1) :
                      snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"}).next() ?
                      snowflake.execute({sqlText: "SELECT CURRENT_DATABASE()"}).getColumnValue(1) : null;

    var target_table = dw_database + '.' + schema + '.' + P_TABLE_NAME;
    var source_table = dw_database + '.' + staging_schema + '.' + staging_table;

    // ================================================================================================================
    // STEP 2: Build Business Key Join Condition
    // ================================================================================================================

    var key_conditions = [];
    for (var i = 0; i < business_keys.length; i++) {
        key_conditions.push('tgt.' + business_keys[i] + ' = src.' + business_keys[i]);
    }
    var key_join = key_conditions.join(' AND ');

    // ================================================================================================================
    // STEP 3: End-Date Changed Records (SCD Type 2 Logic)
    // ================================================================================================================

    var update_sql = `
        UPDATE ${target_table} tgt
        SET
            effective_end_date = CURRENT_TIMESTAMP(),
            is_current = FALSE,
            updated_timestamp = CURRENT_TIMESTAMP()
        FROM ${source_table} src
        WHERE ${key_join}
          AND tgt.is_current = TRUE
          AND tgt.${hash_column} != src.${hash_column}
    `;

    var update_stmt = snowflake.createStatement({sqlText: update_sql});
    update_stmt.execute();
    var rows_updated = update_stmt.getNumRowsAffected();

    // ================================================================================================================
    // STEP 4: Get Column List (Exclude Surrogate Key and Metadata Fields)
    // ================================================================================================================

    var columns_sql = `
        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS column_list
        FROM information_schema.columns
        WHERE table_schema = '${staging_schema}'
          AND table_name = UPPER('${staging_table}')
          AND table_catalog = '${dw_database}'
          AND column_name NOT IN (
              '${surrogate_key}',
              'EFFECTIVE_START_DATE',
              'EFFECTIVE_END_DATE',
              'IS_CURRENT',
              'CREATED_TIMESTAMP',
              'UPDATED_TIMESTAMP'
          )
    `;

    var cols_stmt = snowflake.createStatement({sqlText: columns_sql});
    var cols_result = cols_stmt.execute();

    if (!cols_result.next()) {
        return "ERROR: Could not determine column list for staging table '" + staging_table + "'";
    }

    var column_list = cols_result.getColumnValue('COLUMN_LIST');

    if (!column_list || column_list.trim() === '') {
        return "ERROR: No columns found in staging table '" + staging_table + "'";
    }

    // ================================================================================================================
    // STEP 5: Insert New and Changed Records
    // ================================================================================================================

    var insert_sql = `
        INSERT INTO ${target_table} (
            ${column_list},
            effective_start_date,
            effective_end_date,
            is_current,
            created_timestamp,
            updated_timestamp
        )
        SELECT
            ${column_list},
            CURRENT_TIMESTAMP() AS effective_start_date,
            TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS effective_end_date,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS created_timestamp,
            CURRENT_TIMESTAMP() AS updated_timestamp
        FROM ${source_table} src
        WHERE src.batch_id = ?
          AND (
              -- New record (doesn't exist in target)
              NOT EXISTS (
                  SELECT 1
                  FROM ${target_table} tgt
                  WHERE ${key_join}
              )
              OR
              -- Changed record (was just end-dated in step 3)
              EXISTS (
                  SELECT 1
                  FROM ${target_table} tgt
                  WHERE ${key_join}
                    AND tgt.is_current = FALSE
                    AND tgt.effective_end_date >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
              )
          )
    `;

    var insert_stmt = snowflake.createStatement({
        sqlText: insert_sql,
        binds: [P_BATCH_ID]
    });
    insert_stmt.execute();
    var rows_inserted = insert_stmt.getNumRowsAffected();

    // ================================================================================================================
    // STEP 6: Return Results
    // ================================================================================================================

    var result_message = 'SUCCESS: Loaded ' + P_TABLE_NAME +
                        ' - Updated: ' + rows_updated +
                        ', Inserted: ' + rows_inserted +
                        ' (Batch: ' + P_BATCH_ID + ')';

    return result_message;
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
LANGUAGE JAVASCRIPT
COMMENT = 'Loads all enabled dimension tables using generic SCD Type 2 procedure. Returns results table with status for each dimension.'
AS
$$
    // ================================================================================================================
    // Get list of enabled dimension tables from configuration
    // ================================================================================================================

    var config_sql = `
        SELECT table_name
        FROM metadata.scd_type2_config
        WHERE active_flag = TRUE
          AND enabled = TRUE
        ORDER BY table_name
    `;

    var config_stmt = snowflake.createStatement({sqlText: config_sql});
    var config_result = config_stmt.execute();

    var results = [];

    // ================================================================================================================
    // Load each dimension table
    // ================================================================================================================

    while (config_result.next()) {
        var table_name = config_result.getColumnValue('TABLE_NAME');
        var start_time = Date.now();
        var status = 'SUCCESS';
        var error_msg = null;
        var result_msg = '';

        try {
            // Call generic SCD procedure
            var load_stmt = snowflake.createStatement({
                sqlText: "CALL sp_load_scd_type2_generic(?, ?)",
                binds: [table_name, P_BATCH_ID]
            });

            var load_result = load_stmt.execute();
            if (load_result.next()) {
                result_msg = load_result.getColumnValue(1);
            }

            // Parse result message to extract row counts
            // Format: "SUCCESS: Loaded dim_veterans - Updated: 5, Inserted: 10 (Batch: ...)"
            var updated_match = result_msg.match(/Updated:\s*(\d+)/);
            var inserted_match = result_msg.match(/Inserted:\s*(\d+)/);

            var rows_updated = updated_match ? parseInt(updated_match[1]) : 0;
            var rows_inserted = inserted_match ? parseInt(inserted_match[1]) : 0;

        } catch (err) {
            status = 'ERROR';
            error_msg = err.message;
            rows_updated = 0;
            rows_inserted = 0;
        }

        var duration = (Date.now() - start_time) / 1000;

        results.push({
            TABLE_NAME: table_name,
            STATUS: status,
            ROWS_UPDATED: rows_updated,
            ROWS_INSERTED: rows_inserted,
            DURATION_SECONDS: duration,
            ERROR_MESSAGE: error_msg
        });
    }

    return results;
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
    v_dw_database VARCHAR DEFAULT (SELECT get_dw_database());
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
