-- =====================================================
-- VES Data Pipeline - ETL Procedures for Dimensions
-- =====================================================
-- Purpose: Transform and load dimension tables with SCD Type 2 logic
-- Pattern: ODS → Staging → Warehouse
-- Standards: VES Snowflake Naming Conventions v1.0

USE DATABASE VETERAN_EVALUATION_DW;
USE SCHEMA WAREHOUSE;

-- =====================================================
-- ETL Procedure: Transform ODS to Staging - Veterans
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_ods_to_staging_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_veterans;

    -- Transform and load from ODS to Staging
    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_veterans (
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
        REGEXP_REPLACE(phone_primary, '[^0-9]', '') AS phone,  -- Remove non-numeric
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

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source
    WHERE batch_id = :p_batch_id
      AND extraction_timestamp = (
          SELECT MAX(extraction_timestamp)
          FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source
          WHERE batch_id = :p_batch_id
      );

    RETURN 'Transformed ' || SQLROWCOUNT || ' veteran records to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Load Dimension with SCD Type 2 - Veterans
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_load_dim_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_updated INTEGER;
    v_rows_inserted INTEGER;
BEGIN
    -- Step 1: End-date changed records (Type 2 SCD logic)
    UPDATE VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans tgt
    SET
        effective_end_date = CURRENT_TIMESTAMP(),
        is_current = FALSE,
        updated_timestamp = CURRENT_TIMESTAMP()
    FROM VETERAN_EVALUATION_DW.STAGING.stg_veterans src
    WHERE tgt.veteran_id = src.veteran_id
      AND tgt.is_current = TRUE
      AND tgt.source_record_hash != src.source_record_hash;  -- Detect changes

    v_rows_updated := SQLROWCOUNT;

    -- Step 2: Insert new versions for changed records
    INSERT INTO VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans (
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
        effective_start_date,
        effective_end_date,
        is_current,
        source_system,
        created_timestamp,
        updated_timestamp
    )
    SELECT
        src.veteran_id,
        src.first_name,
        src.middle_name,
        src.last_name,
        src.full_name,
        src.date_of_birth,
        src.age,
        src.gender,
        src.email,
        src.phone,
        src.address_line1,
        src.address_line2,
        src.city,
        src.state,
        src.zip_code,
        src.country,
        src.service_branch,
        src.service_start_date,
        src.service_end_date,
        src.years_of_service,
        src.discharge_status,
        src.service_era,
        src.combat_veteran_flag,
        src.military_rank,
        src.military_occupation,
        src.current_disability_rating,
        src.disability_rating_category,
        src.service_connected_flag,
        src.va_enrolled_flag,
        src.va_enrollment_date,
        src.priority_group,
        src.source_record_hash,
        CURRENT_TIMESTAMP() AS effective_start_date,
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS effective_end_date,
        TRUE AS is_current,
        src.source_system,
        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp
    FROM VETERAN_EVALUATION_DW.STAGING.stg_veterans src
    WHERE src.batch_id = :p_batch_id
      AND (
          -- New record (doesn't exist)
          NOT EXISTS (
              SELECT 1
              FROM VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans tgt
              WHERE tgt.veteran_id = src.veteran_id
          )
          OR
          -- Changed record (hash different from current)
          EXISTS (
              SELECT 1
              FROM VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans tgt
              WHERE tgt.veteran_id = src.veteran_id
                AND tgt.is_current = FALSE  -- Was just end-dated
                AND tgt.effective_end_date = CURRENT_TIMESTAMP()::DATE
          )
      );

    v_rows_inserted := SQLROWCOUNT;

    RETURN 'Updated ' || v_rows_updated || ' and inserted ' || v_rows_inserted || ' veteran records';
END;
$$;

-- =====================================================
-- ETL Procedure: Full Pipeline - Veterans
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_etl_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR;
BEGIN
    -- Step 1: Transform ODS to Staging
    CALL sp_transform_ods_to_staging_veterans(:p_batch_id);

    -- Step 2: Load Staging to Warehouse
    CALL sp_load_dim_veterans(:p_batch_id);

    RETURN 'Veterans ETL pipeline completed successfully';
EXCEPTION
    WHEN OTHER THEN
        -- Log error
        INSERT INTO VETERAN_EVALUATION_DW.ODS_RAW.ods_error_log (
            batch_id,
            source_table,
            error_type,
            error_message
        ) VALUES (
            :p_batch_id,
            'dim_veterans',
            'ETL_ERROR',
            SQLERRM
        );

        RETURN 'Error in veterans ETL: ' || SQLERRM;
END;
$$;

-- =====================================================
-- ETL Procedure: Transform ODS to Staging - Evaluators
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_ods_to_staging_evaluators(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_evaluators;

    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_evaluators (
        evaluator_id,
        first_name,
        last_name,
        full_name,
        specialty,
        sub_specialty,
        credentials,
        license_number,
        license_state,
        license_expiration_date,
        npi_number,
        employer_name,
        employment_type,
        hire_date,
        termination_date,
        years_of_experience,
        va_certified_flag,
        certification_date,
        board_certified_flag,
        average_evaluation_time_minutes,
        total_evaluations_completed,
        active_flag,
        source_record_hash,
        source_system,
        batch_id,
        dq_score,
        dq_issues
    )
    SELECT
        -- Business Key: Prefer NPI, fall back to license number
        COALESCE(evaluator_npi, license_number) AS evaluator_id,

        -- Personal Information
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS full_name,

        -- Professional Information
        UPPER(TRIM(specialty)) AS specialty,
        UPPER(TRIM(sub_specialty)) AS sub_specialty,
        UPPER(TRIM(credentials)) AS credentials,
        UPPER(TRIM(license_number)) AS license_number,
        UPPER(TRIM(license_state)) AS license_state,
        license_expiration_date,
        evaluator_npi AS npi_number,

        -- Employment
        UPPER(TRIM(employer_name)) AS employer_name,
        UPPER(TRIM(employment_type)) AS employment_type,
        hire_date,
        termination_date,

        -- Qualifications
        years_of_experience,
        COALESCE(va_certified_flag, FALSE) AS va_certified_flag,
        certification_date,
        COALESCE(board_certified_flag, FALSE) AS board_certified_flag,

        -- Performance
        average_evaluation_time_minutes,
        COALESCE(total_evaluations_completed, 0) AS total_evaluations_completed,

        -- Status
        COALESCE(active_flag, TRUE) AS active_flag,

        -- Change Detection
        MD5(CONCAT_WS('|',
            COALESCE(first_name, ''),
            COALESCE(last_name, ''),
            COALESCE(specialty, ''),
            COALESCE(license_number, ''),
            COALESCE(va_certified_flag::VARCHAR, ''),
            COALESCE(active_flag::VARCHAR, '')
        )) AS source_record_hash,

        -- Metadata
        source_system,
        :p_batch_id AS batch_id,

        -- Data Quality Score
        (
            (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN evaluator_npi IS NOT NULL OR license_number IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN specialty IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN credentials IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN license_expiration_date IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN va_certified_flag IS TRUE THEN 15 ELSE 0 END)
        ) AS dq_score,

        -- Data Quality Issues
        CONCAT_WS('; ',
            CASE WHEN first_name IS NULL THEN 'Missing first name' END,
            CASE WHEN last_name IS NULL THEN 'Missing last name' END,
            CASE WHEN evaluator_npi IS NULL AND license_number IS NULL THEN 'Missing ID' END,
            CASE WHEN specialty IS NULL THEN 'Missing specialty' END,
            CASE WHEN license_expiration_date < CURRENT_DATE() THEN 'Expired license' END
        ) AS dq_issues

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluators_source
    WHERE batch_id = :p_batch_id
      AND extraction_timestamp = (
          SELECT MAX(extraction_timestamp)
          FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluators_source
          WHERE batch_id = :p_batch_id
      );

    RETURN 'Transformed ' || SQLROWCOUNT || ' evaluator records to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Load Dimension with SCD Type 2 - Evaluators
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_load_dim_evaluators(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_updated INTEGER;
    v_rows_inserted INTEGER;
BEGIN
    -- End-date changed records
    UPDATE VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators tgt
    SET
        effective_end_date = CURRENT_TIMESTAMP(),
        is_current = FALSE,
        updated_timestamp = CURRENT_TIMESTAMP()
    FROM VETERAN_EVALUATION_DW.STAGING.stg_evaluators src
    WHERE tgt.evaluator_id = src.evaluator_id
      AND tgt.is_current = TRUE
      AND tgt.source_record_hash != src.source_record_hash;

    v_rows_updated := SQLROWCOUNT;

    -- Insert new/changed records
    INSERT INTO VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators (
        evaluator_id,
        first_name,
        last_name,
        full_name,
        specialty,
        sub_specialty,
        credentials,
        license_number,
        license_state,
        license_expiration_date,
        npi_number,
        employer_name,
        employment_type,
        hire_date,
        termination_date,
        years_of_experience,
        va_certified_flag,
        certification_date,
        board_certified_flag,
        average_evaluation_time_minutes,
        total_evaluations_completed,
        active_flag,
        source_record_hash,
        effective_start_date,
        effective_end_date,
        is_current,
        source_system,
        created_timestamp,
        updated_timestamp
    )
    SELECT
        src.*,
        CURRENT_TIMESTAMP() AS effective_start_date,
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS effective_end_date,
        TRUE AS is_current,
        src.source_system,
        CURRENT_TIMESTAMP() AS created_timestamp,
        CURRENT_TIMESTAMP() AS updated_timestamp
    FROM VETERAN_EVALUATION_DW.STAGING.stg_evaluators src
    WHERE src.batch_id = :p_batch_id
      AND (
          NOT EXISTS (
              SELECT 1 FROM VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators tgt
              WHERE tgt.evaluator_id = src.evaluator_id
          )
          OR EXISTS (
              SELECT 1 FROM VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators tgt
              WHERE tgt.evaluator_id = src.evaluator_id
                AND tgt.is_current = FALSE
                AND tgt.effective_end_date = CURRENT_TIMESTAMP()::DATE
          )
      );

    v_rows_inserted := SQLROWCOUNT;

    RETURN 'Updated ' || v_rows_updated || ' and inserted ' || v_rows_inserted || ' evaluator records';
END;
$$;

-- =====================================================
-- Master ETL Orchestration Procedure
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_etl_master_pipeline(
    p_extraction_type VARCHAR DEFAULT 'INCREMENTAL'  -- FULL or INCREMENTAL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_result VARCHAR;
BEGIN
    -- Generate batch ID
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- Create batch control record
    INSERT INTO VETERAN_EVALUATION_DW.ODS_RAW.ods_batch_control (
        batch_id,
        batch_name,
        source_system,
        extraction_type,
        batch_status
    ) VALUES (
        v_batch_id,
        'Master ETL Pipeline',
        'VES_OMS',
        :p_extraction_type,
        'RUNNING'
    );

    -- Execute dimension ETLs
    CALL sp_etl_veterans(v_batch_id);
    CALL sp_etl_evaluators(v_batch_id);

    -- Update batch status
    UPDATE VETERAN_EVALUATION_DW.ODS_RAW.ods_batch_control
    SET
        batch_status = 'COMPLETED',
        batch_end_timestamp = CURRENT_TIMESTAMP()
    WHERE batch_id = v_batch_id;

    RETURN 'Master ETL pipeline completed. Batch ID: ' || v_batch_id;
EXCEPTION
    WHEN OTHER THEN
        -- Update batch status to FAILED
        UPDATE VETERAN_EVALUATION_DW.ODS_RAW.ods_batch_control
        SET
            batch_status = 'FAILED',
            batch_end_timestamp = CURRENT_TIMESTAMP(),
            error_message = SQLERRM
        WHERE batch_id = v_batch_id;

        RETURN 'Master ETL pipeline failed: ' || SQLERRM;
END;
$$;

-- =====================================================
-- Usage Examples
-- =====================================================

-- Execute full ETL pipeline
-- CALL sp_etl_master_pipeline('FULL');

-- Execute individual dimension ETL
-- CALL sp_etl_veterans('BATCH_20250117_120000');
