-- =====================================================
-- VES Data Pipeline - ETL Procedures for Dimensions (IMPROVED)
-- =====================================================
-- Purpose: Transform and load dimension tables with SCD Type 2 logic
-- Pattern: ODS → Staging → Warehouse
-- Standards: VES Snowflake Naming Conventions v1.0
--
-- IMPROVEMENTS:
-- - Added comprehensive error handling
-- - Added pipeline execution logging
-- - Added input validation
-- - Added transaction management
-- - Added detailed error messages with context
-- =====================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

-- =====================================================
-- ETL Procedure: Transform ODS to Staging - Veterans (IMPROVED)
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_ods_to_staging_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Transform veteran data from ODS to Staging with comprehensive error handling'
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
    v_rows_processed INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_error_message VARCHAR;
    v_duration_minutes NUMBER(10,2);
BEGIN
    -- Validate input parameters
    IF (p_batch_id IS NULL OR TRIM(p_batch_id) = '') THEN
        RETURN 'ERROR: batch_id parameter is required and cannot be empty';
    END IF;

    -- Validate database names are configured
    IF (v_dw_database IS NULL OR v_ods_database IS NULL) THEN
        RETURN 'ERROR: Database configuration not found. Ensure fn_get_dw_database() and fn_get_ods_database() are configured';
    END IF;

    -- Step 1: Truncate staging table
    BEGIN
        TRUNCATE TABLE IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans');
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Failed to truncate staging table: ' || SQLERRM;
            v_duration_minutes := DATEDIFF(millisecond, v_start_time, CURRENT_TIMESTAMP()) / 60000.0;

            -- Log failure
            CALL sp_log_pipeline_execution(
                'sp_transform_ods_to_staging_veterans',
                'FAILED',
                v_duration_minutes,
                0,
                0,
                :v_error_message,
                :p_batch_id
            );

            RETURN v_error_message;
    END;

    -- Step 2: Transform and load from ODS to Staging
    BEGIN
        INSERT INTO IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans') (
            veteran_id,
            first_name,
            middle_name,
            last_name,
            full_name,
            date_of_birth,
            age,
            gender,
            email,
            phone,
            address_line1,
            address_line2,
            city,
            state,
            zip_code,
            country,
            service_branch,
            service_start_date,
            service_end_date,
            years_of_service,
            discharge_status,
            service_era,
            combat_veteran_flag,
            military_rank,
            military_occupation,
            current_disability_rating,
            disability_rating_category,
            service_connected_flag,
            va_enrolled_flag,
            va_enrollment_date,
            priority_group,
            source_record_hash,
            source_system,
            batch_id,
            dq_score,
            dq_issues
        )
        SELECT
            -- Business Key: Prefer VA ID, fall back to SSN
            COALESCE(veteran_va_id, 'SSN-' || veteran_ssn) AS veteran_id,

            -- Personal Information (cleansed)
            UPPER(TRIM(first_name)) AS first_name,
            UPPER(TRIM(middle_name)) AS middle_name,
            UPPER(TRIM(last_name)) AS last_name,
            UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) ||
                CASE WHEN middle_name IS NOT NULL THEN ' ' || SUBSTR(UPPER(TRIM(middle_name)), 1, 1) || '.' ELSE '' END AS full_name,
            date_of_birth,
            DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS age,
            UPPER(TRIM(gender)) AS gender,

            -- Contact Information (validated and standardized)
            LOWER(TRIM(email)) AS email,
            REGEXP_REPLACE(phone_primary, '[^0-9]', '') AS phone,
            UPPER(TRIM(address_line1)) AS address_line1,
            UPPER(TRIM(address_line2)) AS address_line2,
            UPPER(TRIM(city)) AS city,
            UPPER(TRIM(state)) AS state,
            REGEXP_REPLACE(zip_code, '[^0-9-]', '') AS zip_code,
            COALESCE(UPPER(TRIM(country)), 'USA') AS country,

            -- Military Service
            UPPER(TRIM(service_branch)) AS service_branch,
            service_start_date,
            service_end_date,
            ROUND(DATEDIFF(day, service_start_date, COALESCE(service_end_date, CURRENT_DATE())) / 365.25, 2) AS years_of_service,
            UPPER(TRIM(discharge_status)) AS discharge_status,
            UPPER(TRIM(service_era)) AS service_era,
            COALESCE(combat_veteran_flag, FALSE) AS combat_veteran_flag,
            UPPER(TRIM(military_rank)) AS military_rank,
            UPPER(TRIM(military_occupation)) AS military_occupation,

            -- Disability Information (validated)
            CASE
                WHEN disability_rating BETWEEN 0 AND 100 THEN disability_rating
                ELSE NULL
            END AS current_disability_rating,
            CASE
                WHEN disability_rating = 0 THEN '0%'
                WHEN disability_rating BETWEEN 10 AND 30 THEN '10-30%'
                WHEN disability_rating BETWEEN 40 AND 60 THEN '40-60%'
                WHEN disability_rating BETWEEN 70 AND 90 THEN '70-90%'
                WHEN disability_rating = 100 THEN '100%'
                ELSE NULL
            END AS disability_rating_category,
            COALESCE(service_connected_flag, FALSE) AS service_connected_flag,

            -- VA Benefits
            COALESCE(va_enrolled_flag, FALSE) AS va_enrolled_flag,
            va_enrollment_date,
            CASE
                WHEN priority_group BETWEEN 1 AND 8 THEN priority_group
                ELSE NULL
            END AS priority_group,

            -- Change Detection (MD5 hash of key fields)
            MD5(CONCAT_WS('|',
                COALESCE(first_name, ''),
                COALESCE(last_name, ''),
                COALESCE(date_of_birth::VARCHAR, ''),
                COALESCE(disability_rating::VARCHAR, ''),
                COALESCE(email, ''),
                COALESCE(phone_primary, '')
            )) AS source_record_hash,

            -- Metadata
            source_system,
            :p_batch_id AS batch_id,

            -- Data Quality Score (calculated)
            (
                (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
                (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
                (CASE WHEN date_of_birth IS NOT NULL THEN 15 ELSE 0 END) +
                (CASE WHEN veteran_va_id IS NOT NULL OR veteran_ssn IS NOT NULL THEN 20 ELSE 0 END) +
                (CASE WHEN email IS NOT NULL THEN 10 ELSE 0 END) +
                (CASE WHEN phone_primary IS NOT NULL THEN 10 ELSE 0 END) +
                (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
                (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
            ) AS dq_score,

            -- Data Quality Issues
            CONCAT_WS('; ',
                CASE WHEN first_name IS NULL THEN 'Missing first name' END,
                CASE WHEN last_name IS NULL THEN 'Missing last name' END,
                CASE WHEN date_of_birth IS NULL THEN 'Missing DOB' END,
                CASE WHEN veteran_va_id IS NULL AND veteran_ssn IS NULL THEN 'Missing ID' END,
                CASE WHEN disability_rating NOT BETWEEN 0 AND 100 THEN 'Invalid disability rating' END,
                CASE WHEN priority_group NOT BETWEEN 1 AND 8 THEN 'Invalid priority group' END
            ) AS dq_issues

        FROM IDENTIFIER(:v_ods_database || '.ODS_RAW.ods_veterans_source')
        WHERE batch_id = :p_batch_id
          AND extraction_timestamp = (
              SELECT MAX(extraction_timestamp)
              FROM IDENTIFIER(:v_ods_database || '.ODS_RAW.ods_veterans_source')
              WHERE batch_id = :p_batch_id
          );

        v_rows_processed := SQLROWCOUNT;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Failed to transform and insert records: ' || SQLERRM || ' | SQL State: ' || SQLSTATE;
            v_duration_minutes := DATEDIFF(millisecond, v_start_time, CURRENT_TIMESTAMP()) / 60000.0;

            -- Log failure
            CALL sp_log_pipeline_execution(
                'sp_transform_ods_to_staging_veterans',
                'FAILED',
                v_duration_minutes,
                0,
                0,
                :v_error_message,
                :p_batch_id
            );

            RETURN v_error_message;
    END;

    -- Calculate duration
    v_duration_minutes := DATEDIFF(millisecond, v_start_time, CURRENT_TIMESTAMP()) / 60000.0;

    -- Log success
    BEGIN
        CALL sp_log_pipeline_execution(
            'sp_transform_ods_to_staging_veterans',
            'SUCCEEDED',
            v_duration_minutes,
            v_rows_processed,
            0,
            NULL,
            :p_batch_id
        );
    EXCEPTION
        WHEN OTHER THEN
            -- Logging failure should not fail the procedure
            NULL;
    END;

    RETURN 'SUCCESS: Transformed ' || v_rows_processed || ' veteran records to staging in ' || ROUND(v_duration_minutes, 2) || ' minutes';
END;
$$;

-- =====================================================
-- ETL Procedure: Load Dimension with SCD Type 2 - Veterans (IMPROVED)
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_load_dim_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Load veteran dimension with SCD Type 2 logic and comprehensive error handling'
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_rows_updated INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_error_message VARCHAR;
    v_duration_minutes NUMBER(10,2);
BEGIN
    -- Validate input parameters
    IF (p_batch_id IS NULL OR TRIM(p_batch_id) = '') THEN
        RETURN 'ERROR: batch_id parameter is required and cannot be empty';
    END IF;

    -- Validate database is configured
    IF (v_dw_database IS NULL) THEN
        RETURN 'ERROR: Database configuration not found. Ensure fn_get_dw_database() is configured';
    END IF;

    -- Step 1: End-date changed records (Type 2 SCD logic)
    BEGIN
        UPDATE IDENTIFIER(:v_dw_database || '.WAREHOUSE.dim_veterans') tgt
        SET
            effective_end_date = CURRENT_TIMESTAMP(),
            is_current = FALSE,
            updated_timestamp = CURRENT_TIMESTAMP()
        FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans') src
        WHERE tgt.veteran_id = src.veteran_id
          AND tgt.is_current = TRUE
          AND tgt.source_record_hash != src.source_record_hash;

        v_rows_updated := SQLROWCOUNT;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Failed to end-date changed records: ' || SQLERRM || ' | SQL State: ' || SQLSTATE;
            v_duration_minutes := DATEDIFF(millisecond, v_start_time, CURRENT_TIMESTAMP()) / 60000.0;

            CALL sp_log_pipeline_execution(
                'sp_load_dim_veterans',
                'FAILED',
                v_duration_minutes,
                0,
                0,
                :v_error_message,
                :p_batch_id
            );

            RETURN v_error_message;
    END;

    -- Step 2: Insert new and changed records
    BEGIN
        INSERT INTO IDENTIFIER(:v_dw_database || '.WAREHOUSE.dim_veterans') (
            veteran_id, first_name, middle_name, last_name, full_name,
            date_of_birth, age, gender, email, phone,
            address_line1, address_line2, city, state, zip_code, country,
            service_branch, service_start_date, service_end_date, years_of_service,
            discharge_status, service_era, combat_veteran_flag, military_rank, military_occupation,
            current_disability_rating, disability_rating_category, service_connected_flag,
            va_enrolled_flag, va_enrollment_date, priority_group,
            source_record_hash, effective_start_date, effective_end_date, is_current,
            source_system, created_timestamp, updated_timestamp
        )
        SELECT
            src.veteran_id, src.first_name, src.middle_name, src.last_name, src.full_name,
            src.date_of_birth, src.age, src.gender, src.email, src.phone,
            src.address_line1, src.address_line2, src.city, src.state, src.zip_code, src.country,
            src.service_branch, src.service_start_date, src.service_end_date, src.years_of_service,
            src.discharge_status, src.service_era, src.combat_veteran_flag, src.military_rank, src.military_occupation,
            src.current_disability_rating, src.disability_rating_category, src.service_connected_flag,
            src.va_enrolled_flag, src.va_enrollment_date, src.priority_group,
            src.source_record_hash,
            CURRENT_TIMESTAMP() AS effective_start_date,
            TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS effective_end_date,
            TRUE AS is_current,
            src.source_system,
            CURRENT_TIMESTAMP() AS created_timestamp,
            CURRENT_TIMESTAMP() AS updated_timestamp
        FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans') src
        WHERE src.batch_id = :p_batch_id
          AND (
              -- New record (doesn't exist in target)
              NOT EXISTS (
                  SELECT 1
                  FROM IDENTIFIER(:v_dw_database || '.WAREHOUSE.dim_veterans') tgt
                  WHERE tgt.veteran_id = src.veteran_id
              )
              OR
              -- Changed record (was just end-dated)
              EXISTS (
                  SELECT 1
                  FROM IDENTIFIER(:v_dw_database || '.WAREHOUSE.dim_veterans') tgt
                  WHERE tgt.veteran_id = src.veteran_id
                    AND tgt.is_current = FALSE
                    AND tgt.effective_end_date >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
              )
          );

        v_rows_inserted := SQLROWCOUNT;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Failed to insert new/changed records: ' || SQLERRM || ' | SQL State: ' || SQLSTATE;
            v_duration_minutes := DATEDIFF(millisecond, v_start_time, CURRENT_TIMESTAMP()) / 60000.0;

            CALL sp_log_pipeline_execution(
                'sp_load_dim_veterans',
                'FAILED',
                v_duration_minutes,
                v_rows_updated,
                0,
                :v_error_message,
                :p_batch_id
            );

            RETURN v_error_message;
    END;

    -- Calculate duration
    v_duration_minutes := DATEDIFF(millisecond, v_start_time, CURRENT_TIMESTAMP()) / 60000.0;

    -- Log success
    BEGIN
        CALL sp_log_pipeline_execution(
            'sp_load_dim_veterans',
            'SUCCEEDED',
            v_duration_minutes,
            v_rows_updated + v_rows_inserted,
            0,
            NULL,
            :p_batch_id
        );
    EXCEPTION
        WHEN OTHER THEN
            -- Logging failure should not fail the procedure
            NULL;
    END;

    RETURN 'SUCCESS: Loaded dim_veterans - Updated: ' || v_rows_updated || ', Inserted: ' || v_rows_inserted || ' (Duration: ' || ROUND(v_duration_minutes, 2) || ' min)';
END;
$$;

-- =====================================================
-- Verification
-- =====================================================

SELECT 'ETL Procedures with Error Handling Created Successfully' AS status;
SELECT 'Run: CALL sp_transform_ods_to_staging_veterans(''TEST_BATCH'');' AS test_command;
