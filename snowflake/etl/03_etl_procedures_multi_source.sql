-- =====================================================
-- VES Data Pipeline - Multi-Source ETL Procedures
-- =====================================================
-- Purpose: Transform ODS data from OMS and VEMS into staging
-- Pattern: ODS (OMS + VEMS) → Staging with reconciliation
-- Standards: VES Snowflake Naming Conventions v1.0

USE DATABASE VETERAN_EVALUATION_DW;
USE SCHEMA WAREHOUSE;

-- =====================================================
-- Helper Function: Get Standard Code Value
-- =====================================================

CREATE OR REPLACE FUNCTION fn_map_specialty_code(
    p_source_system VARCHAR,
    p_source_code VARCHAR
)
RETURNS VARCHAR
AS
$$
    SELECT standard_value
    FROM VETERAN_EVALUATION_DW.REFERENCE.ref_code_mapping_specialty
    WHERE source_system = p_source_system
      AND source_code = p_source_code
      AND active_flag = TRUE
    LIMIT 1
$$;

CREATE OR REPLACE FUNCTION fn_map_request_type_code(
    p_source_system VARCHAR,
    p_source_code VARCHAR
)
RETURNS VARCHAR
AS
$$
    SELECT standard_value
    FROM VETERAN_EVALUATION_DW.REFERENCE.ref_code_mapping_request_type
    WHERE source_system = p_source_system
      AND source_code = p_source_code
      AND active_flag = TRUE
    LIMIT 1
$$;

CREATE OR REPLACE FUNCTION fn_map_appointment_status_code(
    p_source_system VARCHAR,
    p_source_code VARCHAR
)
RETURNS VARCHAR
AS
$$
    SELECT standard_value
    FROM VETERAN_EVALUATION_DW.REFERENCE.ref_code_mapping_appointment_status
    WHERE source_system = p_source_system
      AND source_code = p_source_code
      AND active_flag = TRUE
    LIMIT 1
$$;

-- =====================================================
-- ETL Procedure: Build Entity Crosswalk - Veterans
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Match veterans between OMS and VEMS based on SSN
    MERGE INTO VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran tgt
    USING (
        SELECT
            COALESCE(oms.veteran_ssn, vems.veteran_ssn) AS master_veteran_id,
            oms.source_record_id AS oms_veteran_id,
            oms.veteran_ssn AS oms_ssn,
            vems.source_record_id AS vems_veteran_id,
            vems.veteran_ssn AS vems_ssn,
            COALESCE(oms.veteran_va_id, vems.veteran_va_id) AS va_file_number,
            CASE
                WHEN oms.veteran_ssn IS NOT NULL AND vems.veteran_ssn IS NOT NULL AND oms.veteran_ssn = vems.veteran_ssn THEN 100.00
                WHEN oms.veteran_ssn IS NOT NULL AND vems.veteran_ssn IS NULL THEN 90.00
                WHEN oms.veteran_ssn IS NULL AND vems.veteran_ssn IS NOT NULL THEN 90.00
                ELSE 0.00
            END AS match_confidence,
            CASE
                WHEN oms.veteran_ssn = vems.veteran_ssn THEN 'SSN_EXACT_MATCH'
                WHEN oms.veteran_ssn IS NOT NULL THEN 'SSN_OMS_ONLY'
                WHEN vems.veteran_ssn IS NOT NULL THEN 'SSN_VEMS_ONLY'
                ELSE 'NO_MATCH'
            END AS match_method,
            CASE
                WHEN oms.veteran_ssn IS NOT NULL THEN 'OMS'
                ELSE 'VEMS'
            END AS primary_source_system
        FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source oms
        FULL OUTER JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source vems
            ON oms.veteran_ssn = vems.veteran_ssn
            AND oms.source_system = 'OMS'
            AND vems.source_system = 'VEMS'
        WHERE oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id
    ) src
    ON tgt.master_veteran_id = src.master_veteran_id
    WHEN MATCHED THEN UPDATE SET
        oms_veteran_id = COALESCE(src.oms_veteran_id, tgt.oms_veteran_id),
        vems_veteran_id = COALESCE(src.vems_veteran_id, tgt.vems_veteran_id),
        match_confidence = src.match_confidence,
        updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        master_veteran_id,
        oms_veteran_id,
        oms_ssn,
        vems_veteran_id,
        vems_ssn,
        va_file_number,
        match_confidence,
        match_method,
        primary_source_system
    ) VALUES (
        src.master_veteran_id,
        src.oms_veteran_id,
        src.oms_ssn,
        src.vems_veteran_id,
        src.vems_ssn,
        src.va_file_number,
        src.match_confidence,
        src.match_method,
        src.primary_source_system
    );

    RETURN 'Crosswalk built for ' || SQLROWCOUNT || ' veterans';
END;
$$;

-- =====================================================
-- ETL Procedure: Build Entity Crosswalk - Evaluators
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_evaluators(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Match evaluators between OMS and VEMS based on NPI
    MERGE INTO VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_evaluator tgt
    USING (
        SELECT
            COALESCE(oms.evaluator_npi, vems.npi_number, oms.evaluator_license_number) AS master_evaluator_id,
            oms.source_record_id AS oms_evaluator_id,
            oms.evaluator_license_number AS oms_provider_id,
            vems.source_record_id AS vems_evaluator_id,
            COALESCE(oms.evaluator_npi, vems.npi_number) AS npi_number,
            COALESCE(oms.evaluator_license_number, vems.license_number) AS license_number,
            CASE
                WHEN oms.evaluator_npi IS NOT NULL AND vems.npi_number IS NOT NULL AND oms.evaluator_npi = vems.npi_number THEN 100.00
                WHEN oms.evaluator_npi IS NOT NULL AND vems.npi_number IS NULL THEN 85.00
                WHEN oms.evaluator_npi IS NULL AND vems.npi_number IS NOT NULL THEN 85.00
                ELSE 0.00
            END AS match_confidence,
            CASE
                WHEN oms.evaluator_npi = vems.npi_number THEN 'NPI_EXACT_MATCH'
                WHEN oms.evaluator_npi IS NOT NULL THEN 'NPI_OMS_ONLY'
                WHEN vems.npi_number IS NOT NULL THEN 'NPI_VEMS_ONLY'
                ELSE 'NO_MATCH'
            END AS match_method,
            CASE
                WHEN vems.npi_number IS NOT NULL THEN 'VEMS'  -- VEMS is primary for evaluators per ref_system_of_record
                ELSE 'OMS'
            END AS primary_source_system
        FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluators_source oms
        FULL OUTER JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluators_source vems
            ON oms.evaluator_npi = vems.npi_number
            AND oms.source_system = 'OMS'
            AND vems.source_system = 'VEMS'
        WHERE oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id
    ) src
    ON tgt.master_evaluator_id = src.master_evaluator_id
    WHEN MATCHED THEN UPDATE SET
        oms_evaluator_id = COALESCE(src.oms_evaluator_id, tgt.oms_evaluator_id),
        vems_evaluator_id = COALESCE(src.vems_evaluator_id, tgt.vems_evaluator_id),
        match_confidence = src.match_confidence,
        updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        master_evaluator_id,
        oms_evaluator_id,
        oms_provider_id,
        vems_evaluator_id,
        npi_number,
        license_number,
        match_confidence,
        match_method,
        primary_source_system
    ) VALUES (
        src.master_evaluator_id,
        src.oms_evaluator_id,
        src.oms_provider_id,
        src.vems_evaluator_id,
        src.npi_number,
        src.license_number,
        src.match_confidence,
        src.match_method,
        src.primary_source_system
    );

    RETURN 'Crosswalk built for ' || SQLROWCOUNT || ' evaluators';
END;
$$;

-- =====================================================
-- ETL Procedure: Transform Multi-Source ODS to Staging - Veterans
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_multisource_ods_to_staging_veterans(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- First, build crosswalk
    CALL sp_build_crosswalk_veterans(:p_batch_id);

    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_veterans;

    -- Transform and merge data from OMS and VEMS
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
    WITH combined_sources AS (
        SELECT
            xwalk.master_veteran_id,
            xwalk.primary_source_system,

            -- Get data from primary source (OMS preferred per ref_system_of_record)
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.first_name ELSE vems.first_name END AS first_name_primary,
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.last_name ELSE vems.last_name END AS last_name_primary,
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.middle_name ELSE vems.middle_name END AS middle_name_primary,
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.date_of_birth ELSE vems.date_of_birth END AS date_of_birth_primary,
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.gender ELSE vems.gender END AS gender_primary,

            -- Merge fields: Use most recent non-null value
            COALESCE(
                CASE WHEN vems.extraction_timestamp > oms.extraction_timestamp THEN vems.email ELSE NULL END,
                oms.email,
                vems.email
            ) AS email_merged,
            COALESCE(
                CASE WHEN vems.extraction_timestamp > oms.extraction_timestamp THEN vems.phone_primary ELSE NULL END,
                oms.phone_primary,
                vems.phone
            ) AS phone_merged,
            COALESCE(oms.address_line1, vems.address_line1) AS address_line1,
            COALESCE(oms.address_line2, vems.address_line2) AS address_line2,
            COALESCE(oms.city, vems.city) AS city,
            COALESCE(oms.state, vems.state) AS state,
            COALESCE(oms.zip_code, vems.zip_code) AS zip_code,
            COALESCE(oms.country, vems.country) AS country,

            -- Military service (prefer OMS as primary)
            COALESCE(oms.service_branch, vems.service_branch) AS service_branch,
            COALESCE(oms.service_start_date, vems.service_start_date) AS service_start_date,
            COALESCE(oms.service_end_date, vems.service_end_date) AS service_end_date,
            COALESCE(oms.discharge_status, vems.discharge_status) AS discharge_status,
            COALESCE(oms.service_era, vems.service_era) AS service_era,
            COALESCE(oms.combat_veteran_flag, vems.combat_veteran_flag, FALSE) AS combat_veteran_flag,
            COALESCE(oms.military_rank, vems.military_rank) AS military_rank,
            COALESCE(oms.military_occupation, vems.military_occupation) AS military_occupation,

            -- Disability info (prefer OMS as primary)
            COALESCE(oms.disability_rating, vems.disability_rating) AS disability_rating,
            COALESCE(oms.service_connected_flag, vems.service_connected_flag, FALSE) AS service_connected_flag,
            COALESCE(oms.va_enrolled_flag, vems.va_enrolled_flag, FALSE) AS va_enrolled_flag,
            COALESCE(oms.va_enrollment_date, vems.va_enrollment_date) AS va_enrollment_date,
            COALESCE(oms.priority_group, vems.priority_group) AS priority_group,

            -- Track conflicts for logging
            CASE
                WHEN oms.disability_rating IS NOT NULL AND vems.disability_rating IS NOT NULL
                     AND oms.disability_rating != vems.disability_rating
                THEN 'DISABILITY_RATING_MISMATCH'
                ELSE NULL
            END AS conflict_type,
            oms.disability_rating AS oms_disability_rating,
            vems.disability_rating AS vems_disability_rating,

            -- Metadata
            COALESCE(oms.source_system, vems.source_system) AS source_system,
            COALESCE(oms.batch_id, vems.batch_id) AS batch_id

        FROM VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran xwalk
        LEFT JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source oms
            ON xwalk.oms_veteran_id = oms.source_record_id
            AND oms.source_system = 'OMS'
            AND oms.batch_id = :p_batch_id
        LEFT JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source vems
            ON xwalk.vems_veteran_id = vems.source_record_id
            AND vems.source_system = 'VEMS'
            AND vems.batch_id = :p_batch_id
        WHERE (oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id)
    )
    SELECT
        master_veteran_id AS veteran_id,

        -- Personal Information (cleansed)
        UPPER(TRIM(first_name_primary)) AS first_name,
        UPPER(TRIM(middle_name_primary)) AS middle_name,
        UPPER(TRIM(last_name_primary)) AS last_name,
        UPPER(TRIM(last_name_primary)) || ', ' || UPPER(TRIM(first_name_primary)) ||
            CASE WHEN middle_name_primary IS NOT NULL THEN ' ' || SUBSTR(UPPER(TRIM(middle_name_primary)), 1, 1) || '.' ELSE '' END AS full_name,
        date_of_birth_primary AS date_of_birth,
        DATEDIFF(year, date_of_birth_primary, CURRENT_DATE()) AS age,
        UPPER(TRIM(gender_primary)) AS gender,

        -- Contact Information (validated and standardized)
        LOWER(TRIM(email_merged)) AS email,
        REGEXP_REPLACE(phone_merged, '[^0-9]', '') AS phone,
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
        combat_veteran_flag,
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
        service_connected_flag,

        -- VA Benefits
        va_enrolled_flag,
        va_enrollment_date,
        CASE
            WHEN priority_group BETWEEN 1 AND 8 THEN priority_group
            ELSE NULL
        END AS priority_group,

        -- Change Detection (MD5 hash of key fields)
        MD5(CONCAT_WS('|',
            COALESCE(first_name_primary, ''),
            COALESCE(last_name_primary, ''),
            COALESCE(date_of_birth_primary::VARCHAR, ''),
            COALESCE(disability_rating::VARCHAR, ''),
            COALESCE(email_merged, ''),
            COALESCE(phone_merged, '')
        )) AS source_record_hash,

        -- Metadata
        primary_source_system || '_MERGED' AS source_system,
        batch_id,

        -- Data Quality Score
        (
            (CASE WHEN first_name_primary IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN last_name_primary IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN date_of_birth_primary IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN master_veteran_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN email_merged IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN phone_merged IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
            (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
        ) AS dq_score,

        -- Data Quality Issues
        CONCAT_WS('; ',
            CASE WHEN first_name_primary IS NULL THEN 'Missing first name' END,
            CASE WHEN last_name_primary IS NULL THEN 'Missing last name' END,
            CASE WHEN date_of_birth_primary IS NULL THEN 'Missing DOB' END,
            CASE WHEN disability_rating NOT BETWEEN 0 AND 100 THEN 'Invalid disability rating' END,
            CASE WHEN priority_group NOT BETWEEN 1 AND 8 THEN 'Invalid priority group' END,
            CASE WHEN conflict_type IS NOT NULL THEN 'Data conflict: ' || conflict_type END
        ) AS dq_issues

    FROM combined_sources;

    -- Log conflicts
    INSERT INTO VETERAN_EVALUATION_DW.REFERENCE.ref_reconciliation_log (
        batch_id,
        entity_type,
        entity_id,
        conflict_type,
        oms_value,
        vems_value,
        resolved_value,
        resolution_method
    )
    SELECT
        :p_batch_id,
        'VETERAN',
        xwalk.master_veteran_id,
        'DISABILITY_RATING_MISMATCH',
        TO_VARIANT(oms.disability_rating),
        TO_VARIANT(vems.disability_rating),
        TO_VARIANT(COALESCE(oms.disability_rating, vems.disability_rating)),
        'PREFER_OMS'
    FROM VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran xwalk
    JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source oms
        ON xwalk.oms_veteran_id = oms.source_record_id
        AND oms.source_system = 'OMS'
    JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_veterans_source vems
        ON xwalk.vems_veteran_id = vems.source_record_id
        AND vems.source_system = 'VEMS'
    WHERE oms.disability_rating IS NOT NULL
      AND vems.disability_rating IS NOT NULL
      AND oms.disability_rating != vems.disability_rating
      AND oms.batch_id = :p_batch_id;

    RETURN 'Transformed ' || SQLROWCOUNT || ' veteran records from OMS and VEMS to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Transform Multi-Source ODS to Staging - Evaluators
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_multisource_ods_to_staging_evaluators(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- First, build crosswalk
    CALL sp_build_crosswalk_evaluators(:p_batch_id);

    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_evaluators;

    -- Transform and merge data from OMS and VEMS (prefer VEMS per ref_system_of_record)
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
    WITH combined_sources AS (
        SELECT
            xwalk.master_evaluator_id,
            xwalk.primary_source_system,

            -- Personal (prefer VEMS as primary source)
            CASE WHEN xwalk.primary_source_system = 'VEMS' THEN vems.first_name ELSE oms.first_name END AS first_name_primary,
            CASE WHEN xwalk.primary_source_system = 'VEMS' THEN vems.last_name ELSE oms.last_name END AS last_name_primary,

            -- Professional (prefer VEMS, which has more current data)
            COALESCE(vems.specialty_name, fn_map_specialty_code('OMS', oms.specialty_code)) AS specialty,
            COALESCE(vems.sub_specialty, oms.sub_specialty) AS sub_specialty,
            COALESCE(vems.credentials, oms.credentials) AS credentials,
            COALESCE(oms.license_number, vems.license_number) AS license_number,
            COALESCE(oms.license_state, vems.license_state) AS license_state,
            COALESCE(oms.license_expiration_date, vems.license_expiration_date) AS license_expiration_date,
            COALESCE(vems.npi_number, oms.evaluator_npi) AS npi_number,

            -- Employment (prefer VEMS)
            COALESCE(vems.employer_name, oms.employer_name) AS employer_name,
            COALESCE(vems.employment_type, oms.employment_type) AS employment_type,
            COALESCE(vems.hire_date, oms.hire_date) AS hire_date,
            COALESCE(vems.termination_date, oms.termination_date) AS termination_date,

            -- Qualifications
            COALESCE(vems.years_of_experience, oms.years_of_experience) AS years_of_experience,
            COALESCE(vems.va_certified_flag, oms.va_certified_flag, FALSE) AS va_certified_flag,
            COALESCE(vems.certification_date, oms.certification_date) AS certification_date,
            COALESCE(vems.board_certified_flag, oms.board_certified_flag, FALSE) AS board_certified_flag,

            -- Performance
            COALESCE(vems.average_evaluation_time_minutes, oms.average_evaluation_time_minutes) AS average_evaluation_time_minutes,
            COALESCE(vems.total_evaluations_completed, oms.total_evaluations_completed, 0) AS total_evaluations_completed,

            -- Status (prefer VEMS)
            COALESCE(vems.active_flag, oms.active_flag, TRUE) AS active_flag,

            -- Metadata
            COALESCE(vems.source_system, oms.source_system) AS source_system,
            COALESCE(vems.batch_id, oms.batch_id) AS batch_id

        FROM VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_evaluator xwalk
        LEFT JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluators_source oms
            ON xwalk.oms_evaluator_id = oms.source_record_id
            AND oms.source_system = 'OMS'
            AND oms.batch_id = :p_batch_id
        LEFT JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluators_source vems
            ON xwalk.vems_evaluator_id = vems.source_record_id
            AND vems.source_system = 'VEMS'
            AND vems.batch_id = :p_batch_id
        WHERE (oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id)
    )
    SELECT
        master_evaluator_id AS evaluator_id,

        -- Personal Information
        UPPER(TRIM(first_name_primary)) AS first_name,
        UPPER(TRIM(last_name_primary)) AS last_name,
        UPPER(TRIM(last_name_primary)) || ', ' || UPPER(TRIM(first_name_primary)) AS full_name,

        -- Professional Information
        UPPER(TRIM(specialty)) AS specialty,
        UPPER(TRIM(sub_specialty)) AS sub_specialty,
        UPPER(TRIM(credentials)) AS credentials,
        UPPER(TRIM(license_number)) AS license_number,
        UPPER(TRIM(license_state)) AS license_state,
        license_expiration_date,
        npi_number,

        -- Employment
        UPPER(TRIM(employer_name)) AS employer_name,
        UPPER(TRIM(employment_type)) AS employment_type,
        hire_date,
        termination_date,

        -- Qualifications
        years_of_experience,
        va_certified_flag,
        certification_date,
        board_certified_flag,

        -- Performance
        average_evaluation_time_minutes,
        total_evaluations_completed,

        -- Status
        active_flag,

        -- Change Detection
        MD5(CONCAT_WS('|',
            COALESCE(first_name_primary, ''),
            COALESCE(last_name_primary, ''),
            COALESCE(specialty, ''),
            COALESCE(license_number, ''),
            COALESCE(va_certified_flag::VARCHAR, ''),
            COALESCE(active_flag::VARCHAR, '')
        )) AS source_record_hash,

        -- Metadata
        primary_source_system || '_MERGED' AS source_system,
        batch_id,

        -- Data Quality Score
        (
            (CASE WHEN first_name_primary IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN last_name_primary IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN npi_number IS NOT NULL OR license_number IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN specialty IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN credentials IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN license_expiration_date IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN va_certified_flag IS TRUE THEN 15 ELSE 0 END)
        ) AS dq_score,

        -- Data Quality Issues
        CONCAT_WS('; ',
            CASE WHEN first_name_primary IS NULL THEN 'Missing first name' END,
            CASE WHEN last_name_primary IS NULL THEN 'Missing last name' END,
            CASE WHEN npi_number IS NULL AND license_number IS NULL THEN 'Missing ID' END,
            CASE WHEN specialty IS NULL THEN 'Missing specialty' END,
            CASE WHEN license_expiration_date < CURRENT_DATE() THEN 'Expired license' END
        ) AS dq_issues

    FROM combined_sources;

    RETURN 'Transformed ' || SQLROWCOUNT || ' evaluator records from OMS and VEMS to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Build Entity Crosswalk - Facilities
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_facilities(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Match facilities between OMS and VEMS based on facility ID and name
    MERGE INTO VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_facility tgt
    USING (
        SELECT
            COALESCE(oms.facility_id, vems.facility_id) AS master_facility_id,
            oms.source_record_id AS oms_facility_id,
            oms.facility_id AS oms_facility_code,
            vems.source_record_id AS vems_facility_id,
            vems.facility_id AS vems_facility_code,
            COALESCE(oms.facility_name, vems.facility_name) AS facility_name,
            CASE
                WHEN oms.facility_id IS NOT NULL AND vems.facility_id IS NOT NULL AND oms.facility_id = vems.facility_id THEN 100.00
                WHEN UPPER(TRIM(oms.facility_name)) = UPPER(TRIM(vems.facility_name)) THEN 95.00
                WHEN oms.facility_id IS NOT NULL AND vems.facility_id IS NULL THEN 90.00
                WHEN oms.facility_id IS NULL AND vems.facility_id IS NOT NULL THEN 90.00
                ELSE 0.00
            END AS match_confidence,
            CASE
                WHEN oms.facility_id = vems.facility_id THEN 'FACILITY_ID_EXACT_MATCH'
                WHEN UPPER(TRIM(oms.facility_name)) = UPPER(TRIM(vems.facility_name)) THEN 'NAME_MATCH'
                WHEN oms.facility_id IS NOT NULL THEN 'FACILITY_ID_OMS_ONLY'
                WHEN vems.facility_id IS NOT NULL THEN 'FACILITY_ID_VEMS_ONLY'
                ELSE 'NO_MATCH'
            END AS match_method,
            CASE
                WHEN oms.facility_id IS NOT NULL THEN 'OMS'  -- OMS is primary for facilities
                ELSE 'VEMS'
            END AS primary_source_system
        FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_facilities_source oms
        FULL OUTER JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_facilities_source vems
            ON (oms.facility_id = vems.facility_id OR UPPER(TRIM(oms.facility_name)) = UPPER(TRIM(vems.facility_name)))
            AND oms.source_system = 'OMS'
            AND vems.source_system = 'VEMS'
        WHERE oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id
    ) src
    ON tgt.master_facility_id = src.master_facility_id
    WHEN MATCHED THEN UPDATE SET
        oms_facility_id = COALESCE(src.oms_facility_id, tgt.oms_facility_id),
        vems_facility_id = COALESCE(src.vems_facility_id, tgt.vems_facility_id),
        match_confidence = src.match_confidence,
        updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        master_facility_id,
        oms_facility_id,
        oms_facility_code,
        vems_facility_id,
        vems_facility_code,
        facility_name,
        match_confidence,
        match_method,
        primary_source_system
    ) VALUES (
        src.master_facility_id,
        src.oms_facility_id,
        src.oms_facility_code,
        src.vems_facility_id,
        src.vems_facility_code,
        src.facility_name,
        src.match_confidence,
        src.match_method,
        src.primary_source_system
    );

    RETURN 'Crosswalk built for ' || SQLROWCOUNT || ' facilities';
END;
$$;

-- =====================================================
-- ETL Procedure: Transform Multi-Source ODS to Staging - Facilities
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_multisource_ods_to_staging_facilities(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- First, build crosswalk
    CALL sp_build_crosswalk_facilities(:p_batch_id);

    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_facilities;

    -- Transform and merge data from OMS and VEMS
    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_facilities (
        facility_id,
        facility_name,
        facility_type,
        parent_organization,
        address_line1,
        address_line2,
        city,
        state,
        zip_code,
        country,
        phone,
        fax,
        email,
        timezone,
        operating_hours,
        capacity,
        active_flag,
        source_record_hash,
        source_system,
        batch_id,
        dq_score,
        dq_issues
    )
    WITH combined_sources AS (
        SELECT
            xwalk.master_facility_id,
            xwalk.primary_source_system,

            -- Primary fields (prefer OMS per ref_system_of_record)
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.facility_name ELSE vems.facility_name END AS facility_name_primary,
            CASE WHEN xwalk.primary_source_system = 'OMS' THEN oms.facility_type ELSE vems.facility_type END AS facility_type_primary,

            -- Merged fields
            COALESCE(oms.parent_organization, vems.parent_organization) AS parent_organization,
            COALESCE(oms.address_line1, vems.address_line1) AS address_line1,
            COALESCE(oms.address_line2, vems.address_line2) AS address_line2,
            COALESCE(oms.city, vems.city) AS city,
            COALESCE(oms.state, vems.state) AS state,
            COALESCE(oms.zip_code, vems.zip_code) AS zip_code,
            COALESCE(oms.country, vems.country) AS country,
            COALESCE(
                CASE WHEN vems.extraction_timestamp > oms.extraction_timestamp THEN vems.phone ELSE NULL END,
                oms.phone,
                vems.phone
            ) AS phone_merged,
            COALESCE(oms.fax, vems.fax) AS fax,
            COALESCE(
                CASE WHEN vems.extraction_timestamp > oms.extraction_timestamp THEN vems.email ELSE NULL END,
                oms.email,
                vems.email
            ) AS email_merged,
            COALESCE(vems.timezone, oms.timezone) AS timezone,
            COALESCE(vems.operating_hours, oms.operating_hours) AS operating_hours,
            COALESCE(vems.capacity, oms.capacity) AS capacity,
            COALESCE(vems.active_flag, oms.active_flag, TRUE) AS active_flag,

            -- Metadata
            COALESCE(oms.source_system, vems.source_system) AS source_system,
            COALESCE(oms.batch_id, vems.batch_id) AS batch_id

        FROM VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_facility xwalk
        LEFT JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_facilities_source oms
            ON xwalk.oms_facility_id = oms.source_record_id
            AND oms.source_system = 'OMS'
            AND oms.batch_id = :p_batch_id
        LEFT JOIN VETERAN_EVALUATION_DW.ODS_RAW.ods_facilities_source vems
            ON xwalk.vems_facility_id = vems.source_record_id
            AND vems.source_system = 'VEMS'
            AND vems.batch_id = :p_batch_id
        WHERE (oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id)
    )
    SELECT
        master_facility_id AS facility_id,

        -- Facility Information
        UPPER(TRIM(facility_name_primary)) AS facility_name,
        UPPER(TRIM(facility_type_primary)) AS facility_type,
        UPPER(TRIM(parent_organization)) AS parent_organization,

        -- Address
        UPPER(TRIM(address_line1)) AS address_line1,
        UPPER(TRIM(address_line2)) AS address_line2,
        UPPER(TRIM(city)) AS city,
        UPPER(TRIM(state)) AS state,
        REGEXP_REPLACE(zip_code, '[^0-9-]', '') AS zip_code,
        COALESCE(UPPER(TRIM(country)), 'USA') AS country,

        -- Contact
        REGEXP_REPLACE(phone_merged, '[^0-9]', '') AS phone,
        REGEXP_REPLACE(fax, '[^0-9]', '') AS fax,
        LOWER(TRIM(email_merged)) AS email,

        -- Operations
        UPPER(TRIM(timezone)) AS timezone,
        operating_hours,
        capacity,
        active_flag,

        -- Change Detection
        MD5(CONCAT_WS('|',
            COALESCE(facility_name_primary, ''),
            COALESCE(facility_type_primary, ''),
            COALESCE(address_line1, ''),
            COALESCE(city, ''),
            COALESCE(state, ''),
            COALESCE(active_flag::VARCHAR, '')
        )) AS source_record_hash,

        -- Metadata
        primary_source_system || '_MERGED' AS source_system,
        batch_id,

        -- Data Quality Score
        (
            (CASE WHEN facility_name_primary IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN facility_type_primary IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN city IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN state IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN phone_merged IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN email_merged IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN timezone IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN active_flag IS TRUE THEN 5 ELSE 0 END)
        ) AS dq_score,

        -- Data Quality Issues
        CONCAT_WS('; ',
            CASE WHEN facility_name_primary IS NULL THEN 'Missing facility name' END,
            CASE WHEN facility_type_primary IS NULL THEN 'Missing facility type' END,
            CASE WHEN state IS NULL THEN 'Missing state' END,
            CASE WHEN phone_merged IS NULL THEN 'Missing phone' END
        ) AS dq_issues

    FROM combined_sources;

    RETURN 'Transformed ' || SQLROWCOUNT || ' facility records from OMS and VEMS to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Transform Multi-Source ODS to Staging - Exam Requests
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_multisource_ods_to_staging_exam_requests(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_fact_exam_requests;

    -- Transform exam requests (combine OMS and VEMS using veteran and evaluator crosswalks)
    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_fact_exam_requests (
        exam_request_id,
        veteran_id,
        evaluator_id,
        facility_id,
        request_type,
        request_date,
        specialty_required,
        priority_level,
        status,
        source_record_hash,
        source_system,
        batch_id,
        dq_score,
        dq_issues
    )
    SELECT
        COALESCE(src.exam_request_id, src.source_record_id) AS exam_request_id,

        -- Map to master IDs using crosswalks
        COALESCE(vet_xwalk.master_veteran_id, src.veteran_id) AS veteran_id,
        COALESCE(eval_xwalk.master_evaluator_id, src.assigned_evaluator_id) AS evaluator_id,
        COALESCE(fac_xwalk.master_facility_id, src.facility_id) AS facility_id,

        -- Standardize codes
        fn_map_request_type_code(src.source_system, src.request_type) AS request_type,
        src.request_date,
        fn_map_specialty_code(src.source_system, src.specialty_required) AS specialty_required,
        UPPER(TRIM(src.priority_level)) AS priority_level,
        UPPER(TRIM(src.status)) AS status,

        -- Change Detection
        MD5(CONCAT_WS('|',
            COALESCE(src.exam_request_id, ''),
            COALESCE(src.veteran_id, ''),
            COALESCE(src.status, ''),
            COALESCE(src.request_date::VARCHAR, '')
        )) AS source_record_hash,

        -- Metadata
        src.source_system,
        src.batch_id,

        -- Data Quality
        (
            (CASE WHEN src.exam_request_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.veteran_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.request_date IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.request_type IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN src.specialty_required IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN src.status IS NOT NULL THEN 10 ELSE 0 END)
        ) AS dq_score,

        CONCAT_WS('; ',
            CASE WHEN src.exam_request_id IS NULL THEN 'Missing request ID' END,
            CASE WHEN src.veteran_id IS NULL THEN 'Missing veteran ID' END,
            CASE WHEN src.request_date IS NULL THEN 'Missing request date' END
        ) AS dq_issues

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_exam_requests_source src
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran vet_xwalk
        ON (src.veteran_id = vet_xwalk.oms_veteran_id AND src.source_system = 'OMS')
        OR (src.veteran_id = vet_xwalk.vems_veteran_id AND src.source_system = 'VEMS')
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_evaluator eval_xwalk
        ON (src.assigned_evaluator_id = eval_xwalk.oms_evaluator_id AND src.source_system = 'OMS')
        OR (src.assigned_evaluator_id = eval_xwalk.vems_evaluator_id AND src.source_system = 'VEMS')
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_facility fac_xwalk
        ON (src.facility_id = fac_xwalk.oms_facility_id AND src.source_system = 'OMS')
        OR (src.facility_id = fac_xwalk.vems_facility_id AND src.source_system = 'VEMS')
    WHERE src.batch_id = :p_batch_id;

    RETURN 'Transformed ' || SQLROWCOUNT || ' exam request records to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Transform Multi-Source ODS to Staging - Evaluations
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_multisource_ods_to_staging_evaluations(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_fact_evaluations;

    -- Transform evaluations (combine OMS and VEMS using crosswalks)
    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_fact_evaluations (
        evaluation_id,
        veteran_id,
        evaluator_id,
        facility_id,
        evaluation_date,
        evaluation_type,
        specialty,
        primary_diagnosis,
        report_submitted_date,
        source_record_hash,
        source_system,
        batch_id,
        dq_score,
        dq_issues
    )
    SELECT
        COALESCE(src.evaluation_id, src.source_record_id) AS evaluation_id,

        -- Map to master IDs using crosswalks
        COALESCE(vet_xwalk.master_veteran_id, src.veteran_id) AS veteran_id,
        COALESCE(eval_xwalk.master_evaluator_id, src.evaluator_id) AS evaluator_id,
        COALESCE(fac_xwalk.master_facility_id, src.facility_id) AS facility_id,

        src.evaluation_date,
        UPPER(TRIM(src.evaluation_type)) AS evaluation_type,
        fn_map_specialty_code(src.source_system, src.specialty) AS specialty,
        UPPER(TRIM(src.primary_diagnosis)) AS primary_diagnosis,
        src.report_submitted_date,

        -- Change Detection
        MD5(CONCAT_WS('|',
            COALESCE(src.evaluation_id, ''),
            COALESCE(src.veteran_id, ''),
            COALESCE(src.evaluation_date::VARCHAR, ''),
            COALESCE(src.report_submitted_date::VARCHAR, '')
        )) AS source_record_hash,

        -- Metadata
        src.source_system,
        src.batch_id,

        -- Data Quality
        (
            (CASE WHEN src.evaluation_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.veteran_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.evaluator_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.evaluation_date IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.evaluation_type IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN src.report_submitted_date IS NOT NULL THEN 10 ELSE 0 END)
        ) AS dq_score,

        CONCAT_WS('; ',
            CASE WHEN src.evaluation_id IS NULL THEN 'Missing evaluation ID' END,
            CASE WHEN src.veteran_id IS NULL THEN 'Missing veteran ID' END,
            CASE WHEN src.evaluator_id IS NULL THEN 'Missing evaluator ID' END,
            CASE WHEN src.evaluation_date IS NULL THEN 'Missing evaluation date' END
        ) AS dq_issues

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluations_source src
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran vet_xwalk
        ON (src.veteran_id = vet_xwalk.oms_veteran_id AND src.source_system = 'OMS')
        OR (src.veteran_id = vet_xwalk.vems_veteran_id AND src.source_system = 'VEMS')
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_evaluator eval_xwalk
        ON (src.evaluator_id = eval_xwalk.oms_evaluator_id AND src.source_system = 'OMS')
        OR (src.evaluator_id = eval_xwalk.vems_evaluator_id AND src.source_system = 'VEMS')
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_facility fac_xwalk
        ON (src.facility_id = fac_xwalk.oms_facility_id AND src.source_system = 'OMS')
        OR (src.facility_id = fac_xwalk.vems_facility_id AND src.source_system = 'VEMS')
    WHERE src.batch_id = :p_batch_id;

    RETURN 'Transformed ' || SQLROWCOUNT || ' evaluation records to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Transform Multi-Source ODS to Staging - Appointments
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_multisource_ods_to_staging_appointments(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate staging table
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_fact_appointment_events;

    -- Transform appointments (VEMS is primary source per ref_system_of_record, with OMS fallback)
    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_fact_appointment_events (
        appointment_id,
        veteran_id,
        evaluator_id,
        facility_id,
        scheduled_datetime,
        appointment_status,
        event_date,
        event_type,
        source_record_hash,
        source_system,
        batch_id,
        dq_score,
        dq_issues
    )
    SELECT
        COALESCE(src.appointment_id, src.source_record_id) AS appointment_id,

        -- Map to master IDs using crosswalks
        COALESCE(vet_xwalk.master_veteran_id, src.veteran_id) AS veteran_id,
        COALESCE(eval_xwalk.master_evaluator_id, src.evaluator_id) AS evaluator_id,
        COALESCE(fac_xwalk.master_facility_id, src.facility_id) AS facility_id,

        src.scheduled_datetime,
        fn_map_appointment_status_code(src.source_system, src.appointment_status) AS appointment_status,
        COALESCE(src.event_date, src.scheduled_datetime::DATE) AS event_date,
        UPPER(TRIM(src.event_type)) AS event_type,

        -- Change Detection
        MD5(CONCAT_WS('|',
            COALESCE(src.appointment_id, ''),
            COALESCE(src.veteran_id, ''),
            COALESCE(src.scheduled_datetime::VARCHAR, ''),
            COALESCE(src.appointment_status, '')
        )) AS source_record_hash,

        -- Metadata
        src.source_system,
        src.batch_id,

        -- Data Quality
        (
            (CASE WHEN src.appointment_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.veteran_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.scheduled_datetime IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN src.appointment_status IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN src.evaluator_id IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN src.facility_id IS NOT NULL THEN 10 ELSE 0 END)
        ) AS dq_score,

        CONCAT_WS('; ',
            CASE WHEN src.appointment_id IS NULL THEN 'Missing appointment ID' END,
            CASE WHEN src.veteran_id IS NULL THEN 'Missing veteran ID' END,
            CASE WHEN src.scheduled_datetime IS NULL THEN 'Missing scheduled datetime' END
        ) AS dq_issues

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_appointments_source src
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran vet_xwalk
        ON (src.veteran_id = vet_xwalk.oms_veteran_id AND src.source_system = 'OMS')
        OR (src.veteran_id = vet_xwalk.vems_veteran_id AND src.source_system = 'VEMS')
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_evaluator eval_xwalk
        ON (src.evaluator_id = eval_xwalk.oms_evaluator_id AND src.source_system = 'OMS')
        OR (src.evaluator_id = eval_xwalk.vems_evaluator_id AND src.source_system = 'VEMS')
    LEFT JOIN VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_facility fac_xwalk
        ON (src.facility_id = fac_xwalk.oms_facility_id AND src.source_system = 'OMS')
        OR (src.facility_id = fac_xwalk.vems_facility_id AND src.source_system = 'VEMS')
    WHERE src.batch_id = :p_batch_id;

    RETURN 'Transformed ' || SQLROWCOUNT || ' appointment records to staging';
END;
$$;

-- =====================================================
-- Master ETL Orchestration Procedure - Multi-Source
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_etl_master_pipeline_multisource(
    p_extraction_type VARCHAR DEFAULT 'INCREMENTAL'
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
        'Master ETL Pipeline - Multi-Source (OMS + VEMS)',
        'OMS_VEMS_MERGED',
        :p_extraction_type,
        'RUNNING'
    );

    -- Execute dimension ETLs with multi-source transformation
    CALL sp_transform_multisource_ods_to_staging_veterans(v_batch_id);
    CALL sp_load_dim_veterans(v_batch_id);

    CALL sp_transform_multisource_ods_to_staging_evaluators(v_batch_id);
    CALL sp_load_dim_evaluators(v_batch_id);

    CALL sp_transform_multisource_ods_to_staging_facilities(v_batch_id);
    CALL sp_load_dim_facilities(v_batch_id);

    -- Execute fact ETLs with multi-source transformation
    CALL sp_transform_multisource_ods_to_staging_exam_requests(v_batch_id);
    CALL sp_etl_exam_requests(v_batch_id);

    CALL sp_transform_multisource_ods_to_staging_evaluations(v_batch_id);
    CALL sp_etl_evaluations(v_batch_id);

    CALL sp_transform_multisource_ods_to_staging_appointments(v_batch_id);
    CALL sp_etl_appointment_events(v_batch_id);

    -- Update batch status
    UPDATE VETERAN_EVALUATION_DW.ODS_RAW.ods_batch_control
    SET
        batch_status = 'COMPLETED',
        batch_end_timestamp = CURRENT_TIMESTAMP()
    WHERE batch_id = v_batch_id;

    RETURN 'Master ETL pipeline (multi-source) completed. Batch ID: ' || v_batch_id;
EXCEPTION
    WHEN OTHER THEN
        -- Update batch status to FAILED
        UPDATE VETERAN_EVALUATION_DW.ODS_RAW.ods_batch_control
        SET
            batch_status = 'FAILED',
            batch_end_timestamp = CURRENT_TIMESTAMP(),
            error_message = SQLERRM
        WHERE batch_id = v_batch_id;

        RETURN 'Master ETL pipeline (multi-source) failed: ' || SQLERRM;
END;
$$;

-- =====================================================
-- Usage Examples
-- =====================================================

-- Execute multi-source ETL pipeline
-- CALL sp_etl_master_pipeline_multisource('FULL');

-- Build crosswalk manually
-- CALL sp_build_crosswalk_veterans('BATCH_20250117_120000');
-- CALL sp_build_crosswalk_evaluators('BATCH_20250117_120000');

-- Check reconciliation log
-- SELECT * FROM VETERAN_EVALUATION_DW.REFERENCE.ref_reconciliation_log ORDER BY reconciliation_timestamp DESC LIMIT 100;

-- View crosswalk match confidence
-- SELECT
--     primary_source_system,
--     match_method,
--     COUNT(*) AS count,
--     AVG(match_confidence) AS avg_confidence
-- FROM VETERAN_EVALUATION_DW.REFERENCE.ref_entity_crosswalk_veteran
-- GROUP BY primary_source_system, match_method
-- ORDER BY avg_confidence DESC;
