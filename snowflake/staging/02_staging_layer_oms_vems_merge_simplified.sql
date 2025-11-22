-- =====================================================================================
-- STAGING LAYER: SIMPLIFIED STEP-BY-STEP OMS/VEMS INTEGRATION
-- =====================================================================================
-- Purpose: Process ODS data from OMS and VEMS into unified staging tables
--
-- This script provides a SIMPLE, LINEAR execution path for staging layer processing.
-- Each step is independent and can be run/validated separately.
--
-- EXECUTION FLOW:
--   STEP 1: Build entity crosswalks (match veterans/evaluators/facilities)
--   STEP 2: Merge and transform entities (create unified staging records)
--   STEP 3: Merge and transform facts (exam requests, evaluations, appointments)
--   STEP 4: Validate data quality and log conflicts
--
-- Author: Data Team
-- Date: 2025-11-17
-- =====================================================================================

-- =====================================================================================
-- STEP 1: BUILD ENTITY CROSSWALKS (MATCHING)
-- =====================================================================================
-- Purpose: Match entities between OMS and VEMS to create master IDs
-- Duration: ~2-5 minutes depending on data volume

-- -----------------------------------------------------------------------------------------
-- STEP 1A: Match Veterans by SSN
-- -----------------------------------------------------------------------------------------
-- Logic: Full outer join on SSN to find matches and orphans
-- Result: ref_entity_crosswalk_veteran table with master_veteran_id for each record

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_veterans_simple(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    -- Clear existing crosswalk for this batch
    DELETE FROM IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_veteran')
    WHERE batch_id = :p_batch_id;

    -- Build crosswalk using FULL OUTER JOIN on SSN
    INSERT INTO IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_veteran') (
        batch_id,
        master_veteran_id,
        oms_veteran_id,
        vems_veteran_id,
        match_confidence,
        match_method,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,

        -- Master ID: Use SSN as the universal identifier
        COALESCE(oms.veteran_ssn, vems.veteran_ssn) AS master_veteran_id,

        -- Source IDs
        oms.source_record_id AS oms_veteran_id,
        vems.source_record_id AS vems_veteran_id,

        -- Match confidence: 100% if in both systems, 90% if in one
        CASE
            WHEN oms.veteran_ssn IS NOT NULL AND vems.veteran_ssn IS NOT NULL THEN 100.00
            WHEN oms.veteran_ssn IS NOT NULL THEN 90.00
            WHEN vems.veteran_ssn IS NOT NULL THEN 90.00
        END AS match_confidence,

        -- Match method for auditability
        CASE
            WHEN oms.veteran_ssn = vems.veteran_ssn THEN 'SSN_EXACT_MATCH'
            WHEN oms.veteran_ssn IS NOT NULL THEN 'SSN_OMS_ONLY'
            ELSE 'SSN_VEMS_ONLY'
        END AS match_method,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_ods_database || '.ODS.ods_veterans_source') oms
    FULL OUTER JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_veterans_source') vems
        ON oms.veteran_ssn = vems.veteran_ssn
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
    WHERE oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id;

    RETURN 'Veteran crosswalk built: ' || SQLROWCOUNT || ' records';
END;
$$;

-- -----------------------------------------------------------------------------------------
-- STEP 1B: Match Evaluators by NPI
-- -----------------------------------------------------------------------------------------
-- Logic: Full outer join on NPI (National Provider Identifier)
-- Result: ref_entity_crosswalk_evaluator table with master_evaluator_id

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_evaluators_simple(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    DELETE FROM IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_evaluator')
    WHERE batch_id = :p_batch_id;

    INSERT INTO IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_evaluator') (
        batch_id,
        master_evaluator_id,
        oms_evaluator_id,
        vems_evaluator_id,
        match_confidence,
        match_method,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,

        -- Master ID: Use NPI as the universal identifier
        COALESCE(oms.evaluator_npi, vems.evaluator_npi) AS master_evaluator_id,

        oms.source_record_id AS oms_evaluator_id,
        vems.source_record_id AS vems_evaluator_id,

        CASE
            WHEN oms.evaluator_npi IS NOT NULL AND vems.evaluator_npi IS NOT NULL THEN 100.00
            WHEN oms.evaluator_npi IS NOT NULL THEN 90.00
            WHEN vems.evaluator_npi IS NOT NULL THEN 90.00
        END AS match_confidence,

        CASE
            WHEN oms.evaluator_npi = vems.evaluator_npi THEN 'NPI_EXACT_MATCH'
            WHEN oms.evaluator_npi IS NOT NULL THEN 'NPI_OMS_ONLY'
            ELSE 'NPI_VEMS_ONLY'
        END AS match_method,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_ods_database || '.ODS.ods_evaluators_source') oms
    FULL OUTER JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_evaluators_source') vems
        ON oms.evaluator_npi = vems.evaluator_npi
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
    WHERE oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id;

    RETURN 'Evaluator crosswalk built: ' || SQLROWCOUNT || ' records';
END;
$$;

-- -----------------------------------------------------------------------------------------
-- STEP 1C: Match Facilities by Facility ID
-- -----------------------------------------------------------------------------------------
-- Logic: Full outer join on facility_id
-- Result: ref_entity_crosswalk_facility table with master_facility_id

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_facilities_simple(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    DELETE FROM IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_facility')
    WHERE batch_id = :p_batch_id;

    INSERT INTO IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_facility') (
        batch_id,
        master_facility_id,
        oms_facility_id,
        vems_facility_id,
        match_confidence,
        match_method,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,

        -- Master ID: Use facility_id as the universal identifier
        COALESCE(oms.facility_id, vems.facility_id) AS master_facility_id,

        oms.source_record_id AS oms_facility_id,
        vems.source_record_id AS vems_facility_id,

        CASE
            WHEN oms.facility_id IS NOT NULL AND vems.facility_id IS NOT NULL THEN 100.00
            WHEN oms.facility_id IS NOT NULL THEN 90.00
            WHEN vems.facility_id IS NOT NULL THEN 90.00
        END AS match_confidence,

        CASE
            WHEN oms.facility_id = vems.facility_id THEN 'FACILITY_ID_EXACT_MATCH'
            WHEN oms.facility_id IS NOT NULL THEN 'FACILITY_ID_OMS_ONLY'
            ELSE 'FACILITY_ID_VEMS_ONLY'
        END AS match_method,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_ods_database || '.ODS.ods_facilities_source') oms
    FULL OUTER JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_facilities_source') vems
        ON oms.facility_id = vems.facility_id
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
    WHERE oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id;

    RETURN 'Facility crosswalk built: ' || SQLROWCOUNT || ' records';
END;
$$;

-- =====================================================================================
-- STEP 2: MERGE AND TRANSFORM ENTITIES
-- =====================================================================================
-- Purpose: Create unified staging records by merging OMS and VEMS data
-- Uses: Crosswalks from Step 1, System-of-Record rules, Code mappings

-- -----------------------------------------------------------------------------------------
-- STEP 2A: Merge Veterans into Staging
-- -----------------------------------------------------------------------------------------
-- Logic:
--   1. Join crosswalk to get OMS and VEMS records for each master veteran
--   2. Apply system-of-record rules (OMS is primary for veterans)
--   3. Map codes using UDFs
--   4. Calculate data quality score
--   5. Log conflicts

CREATE OR REPLACE PROCEDURE sp_merge_veterans_to_staging(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    -- Clear staging for this batch
    DELETE FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans')
    WHERE batch_id = :p_batch_id;

    -- Merge OMS and VEMS data into staging
    INSERT INTO IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans') (
        batch_id,
        master_veteran_id,
        veteran_ssn,
        first_name,
        last_name,
        date_of_birth,
        gender,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        disability_rating,
        branch_of_service,
        discharge_date,

        -- Metadata
        source_system,
        oms_veteran_id,
        vems_veteran_id,
        match_confidence,
        dq_score,
        dq_issues,
        conflict_type,
        oms_value,
        vems_value,
        resolution_method,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,
        xwalk.master_veteran_id,

        -- ============================================================
        -- FIELD MERGING LOGIC
        -- Rule: OMS is system-of-record for veterans (per ref_system_of_record)
        -- Exception: Use VEMS if OMS value is NULL
        -- ============================================================

        -- SSN: Should be same in both (used for matching), prefer OMS
        COALESCE(oms.veteran_ssn, vems.veteran_ssn) AS veteran_ssn,

        -- Name fields: OMS is primary, standardize to uppercase
        UPPER(TRIM(COALESCE(oms.first_name, vems.first_name))) AS first_name,
        UPPER(TRIM(COALESCE(oms.last_name, vems.last_name))) AS last_name,

        -- DOB: OMS is primary
        COALESCE(oms.date_of_birth, vems.date_of_birth) AS date_of_birth,

        -- Gender: OMS is primary
        COALESCE(oms.gender, vems.gender) AS gender,

        -- Contact info: Use most recent (VEMS preferred for contact as it's more current)
        LOWER(TRIM(COALESCE(vems.email, oms.email))) AS email,
        COALESCE(vems.phone, oms.phone) AS phone,

        -- Address: VEMS preferred (more current)
        COALESCE(vems.address, oms.address) AS address,
        COALESCE(vems.city, oms.city) AS city,
        COALESCE(vems.state, oms.state) AS state,
        COALESCE(vems.zip_code, oms.zip_code) AS zip_code,

        -- Disability rating: OMS is authoritative
        -- Log conflict if different
        COALESCE(oms.disability_rating, vems.disability_rating) AS disability_rating,

        -- Military info: OMS is primary
        COALESCE(oms.branch_of_service, vems.branch_of_service) AS branch_of_service,
        COALESCE(oms.discharge_date, vems.discharge_date) AS discharge_date,

        -- ============================================================
        -- METADATA
        -- ============================================================

        CASE
            WHEN xwalk.match_method = 'SSN_EXACT_MATCH' THEN 'OMS_VEMS_MERGED'
            WHEN xwalk.match_method = 'SSN_OMS_ONLY' THEN 'OMS'
            ELSE 'VEMS'
        END AS source_system,

        xwalk.oms_veteran_id,
        xwalk.vems_veteran_id,
        xwalk.match_confidence,

        -- ============================================================
        -- DATA QUALITY SCORE (0-100)
        -- ============================================================
        (
            (CASE WHEN COALESCE(oms.first_name, vems.first_name) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.last_name, vems.last_name) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.date_of_birth, vems.date_of_birth) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN xwalk.master_veteran_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.email, oms.email) IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.phone, oms.phone) IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.state, oms.state) IS NOT NULL THEN 5 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.disability_rating, vems.disability_rating) BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
        ) AS dq_score,

        -- ============================================================
        -- DATA QUALITY ISSUES
        -- ============================================================
        NULLIF(CONCAT_WS('; ',
            CASE WHEN COALESCE(oms.first_name, vems.first_name) IS NULL THEN 'Missing first name' END,
            CASE WHEN COALESCE(oms.last_name, vems.last_name) IS NULL THEN 'Missing last name' END,
            CASE WHEN COALESCE(oms.date_of_birth, vems.date_of_birth) IS NULL THEN 'Missing DOB' END,
            CASE WHEN COALESCE(oms.disability_rating, vems.disability_rating) NOT BETWEEN 0 AND 100
                THEN 'Invalid disability rating' END,
            CASE WHEN oms.disability_rating IS NOT NULL AND vems.disability_rating IS NOT NULL
                 AND oms.disability_rating != vems.disability_rating
                THEN 'Disability rating mismatch' END
        ), '') AS dq_issues,

        -- ============================================================
        -- CONFLICT DETECTION AND LOGGING
        -- ============================================================

        CASE
            WHEN oms.disability_rating IS NOT NULL AND vems.disability_rating IS NOT NULL
                 AND oms.disability_rating != vems.disability_rating
            THEN 'DISABILITY_RATING_MISMATCH'
            WHEN oms.date_of_birth IS NOT NULL AND vems.date_of_birth IS NOT NULL
                 AND oms.date_of_birth != vems.date_of_birth
            THEN 'DOB_MISMATCH'
            ELSE NULL
        END AS conflict_type,

        -- Store conflicting OMS value
        CASE
            WHEN oms.disability_rating IS NOT NULL AND vems.disability_rating IS NOT NULL
                 AND oms.disability_rating != vems.disability_rating
            THEN TO_VARIANT(oms.disability_rating)
            WHEN oms.date_of_birth IS NOT NULL AND vems.date_of_birth IS NOT NULL
                 AND oms.date_of_birth != vems.date_of_birth
            THEN TO_VARIANT(oms.date_of_birth)
            ELSE NULL
        END AS oms_value,

        -- Store conflicting VEMS value
        CASE
            WHEN oms.disability_rating IS NOT NULL AND vems.disability_rating IS NOT NULL
                 AND oms.disability_rating != vems.disability_rating
            THEN TO_VARIANT(vems.disability_rating)
            WHEN oms.date_of_birth IS NOT NULL AND vems.date_of_birth IS NOT NULL
                 AND oms.date_of_birth != vems.date_of_birth
            THEN TO_VARIANT(vems.date_of_birth)
            ELSE NULL
        END AS vems_value,

        -- Resolution method
        CASE
            WHEN oms.disability_rating IS NOT NULL AND vems.disability_rating IS NOT NULL
                 AND oms.disability_rating != vems.disability_rating
            THEN 'PREFER_OMS'
            WHEN oms.date_of_birth IS NOT NULL AND vems.date_of_birth IS NOT NULL
                 AND oms.date_of_birth != vems.date_of_birth
            THEN 'PREFER_OMS'
            ELSE NULL
        END AS resolution_method,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_veteran') xwalk
    LEFT JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_veterans_source') oms
        ON xwalk.oms_veteran_id = oms.source_record_id
        AND oms.source_system = 'OMS'
    LEFT JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_veterans_source') vems
        ON xwalk.vems_veteran_id = vems.source_record_id
        AND vems.source_system = 'VEMS'
    WHERE xwalk.batch_id = :p_batch_id;

    -- Log conflicts to reconciliation log
    INSERT INTO IDENTIFIER(:v_dw_database || '.REFERENCE.ref_reconciliation_log') (
        batch_id, entity_type, entity_id, conflict_type,
        oms_value, vems_value, resolved_value, resolution_method, resolution_timestamp
    )
    SELECT
        batch_id,
        'VETERAN' AS entity_type,
        master_veteran_id AS entity_id,
        conflict_type,
        oms_value,
        vems_value,
        oms_value AS resolved_value, -- OMS wins per system-of-record rules
        resolution_method,
        CURRENT_TIMESTAMP() AS resolution_timestamp
    FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_veterans')
    WHERE batch_id = :p_batch_id
      AND conflict_type IS NOT NULL;

    RETURN 'Veterans merged to staging: ' || SQLROWCOUNT || ' records';
END;
$$;

-- -----------------------------------------------------------------------------------------
-- STEP 2B: Merge Evaluators into Staging
-- -----------------------------------------------------------------------------------------
-- Logic: Similar to veterans, but VEMS is system-of-record for evaluators

CREATE OR REPLACE PROCEDURE sp_merge_evaluators_to_staging(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    DELETE FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_evaluators')
    WHERE batch_id = :p_batch_id;

    INSERT INTO IDENTIFIER(:v_dw_database || '.STAGING.stg_evaluators') (
        batch_id,
        master_evaluator_id,
        evaluator_npi,
        first_name,
        last_name,
        credential,
        specialty,
        license_number,
        license_state,
        email,
        phone,
        active_flag,

        source_system,
        oms_evaluator_id,
        vems_evaluator_id,
        match_confidence,
        dq_score,
        dq_issues,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,
        xwalk.master_evaluator_id,

        -- NPI: Should be same in both
        COALESCE(vems.evaluator_npi, oms.evaluator_npi) AS evaluator_npi,

        -- ============================================================
        -- FIELD MERGING: VEMS is system-of-record for evaluators
        -- (Provider info is most current in VEMS)
        -- ============================================================

        UPPER(TRIM(COALESCE(vems.first_name, oms.first_name))) AS first_name,
        UPPER(TRIM(COALESCE(vems.last_name, oms.last_name))) AS last_name,
        COALESCE(vems.credential, oms.credential) AS credential,

        -- Specialty: Use code mapping UDF to standardize
        IDENTIFIER(:v_dw_database || '.REFERENCE.fn_map_specialty_code')(
            COALESCE(vems.source_system, oms.source_system),
            COALESCE(vems.specialty_code, oms.specialty_code)
        ) AS specialty,

        COALESCE(vems.license_number, oms.license_number) AS license_number,
        COALESCE(vems.license_state, oms.license_state) AS license_state,
        LOWER(TRIM(COALESCE(vems.email, oms.email))) AS email,
        COALESCE(vems.phone, oms.phone) AS phone,
        COALESCE(vems.active_flag, oms.active_flag, TRUE) AS active_flag,

        CASE
            WHEN xwalk.match_method = 'NPI_EXACT_MATCH' THEN 'OMS_VEMS_MERGED'
            WHEN xwalk.match_method = 'NPI_VEMS_ONLY' THEN 'VEMS'
            ELSE 'OMS'
        END AS source_system,

        xwalk.oms_evaluator_id,
        xwalk.vems_evaluator_id,
        xwalk.match_confidence,

        -- Data quality score
        (
            (CASE WHEN COALESCE(vems.evaluator_npi, oms.evaluator_npi) IS NOT NULL THEN 25 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.first_name, oms.first_name) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.last_name, oms.last_name) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.license_number, oms.license_number) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.specialty_code, oms.specialty_code) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.email, oms.email) IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN COALESCE(vems.active_flag, oms.active_flag) IS NOT NULL THEN 5 ELSE 0 END)
        ) AS dq_score,

        NULLIF(CONCAT_WS('; ',
            CASE WHEN COALESCE(vems.evaluator_npi, oms.evaluator_npi) IS NULL THEN 'Missing NPI' END,
            CASE WHEN COALESCE(vems.first_name, oms.first_name) IS NULL THEN 'Missing first name' END,
            CASE WHEN COALESCE(vems.last_name, oms.last_name) IS NULL THEN 'Missing last name' END,
            CASE WHEN COALESCE(vems.license_number, oms.license_number) IS NULL THEN 'Missing license' END
        ), '') AS dq_issues,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_evaluator') xwalk
    LEFT JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_evaluators_source') oms
        ON xwalk.oms_evaluator_id = oms.source_record_id AND oms.source_system = 'OMS'
    LEFT JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_evaluators_source') vems
        ON xwalk.vems_evaluator_id = vems.source_record_id AND vems.source_system = 'VEMS'
    WHERE xwalk.batch_id = :p_batch_id;

    RETURN 'Evaluators merged to staging: ' || SQLROWCOUNT || ' records';
END;
$$;

-- -----------------------------------------------------------------------------------------
-- STEP 2C: Merge Facilities into Staging
-- -----------------------------------------------------------------------------------------
-- Logic: OMS is system-of-record for facilities

CREATE OR REPLACE PROCEDURE sp_merge_facilities_to_staging(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    DELETE FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_facilities')
    WHERE batch_id = :p_batch_id;

    INSERT INTO IDENTIFIER(:v_dw_database || '.STAGING.stg_facilities') (
        batch_id,
        master_facility_id,
        facility_id,
        facility_name,
        facility_type,
        address,
        city,
        state,
        zip_code,
        phone,
        capacity,
        active_flag,

        source_system,
        oms_facility_id,
        vems_facility_id,
        match_confidence,
        dq_score,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,
        xwalk.master_facility_id,

        COALESCE(oms.facility_id, vems.facility_id) AS facility_id,

        -- OMS is system-of-record for facilities
        UPPER(TRIM(COALESCE(oms.facility_name, vems.facility_name))) AS facility_name,
        COALESCE(oms.facility_type, vems.facility_type) AS facility_type,
        COALESCE(oms.address, vems.address) AS address,
        COALESCE(oms.city, vems.city) AS city,
        COALESCE(oms.state, vems.state) AS state,
        COALESCE(oms.zip_code, vems.zip_code) AS zip_code,
        COALESCE(oms.phone, vems.phone) AS phone,
        COALESCE(oms.capacity, vems.capacity) AS capacity,
        COALESCE(oms.active_flag, vems.active_flag, TRUE) AS active_flag,

        CASE
            WHEN xwalk.match_method = 'FACILITY_ID_EXACT_MATCH' THEN 'OMS_VEMS_MERGED'
            WHEN xwalk.match_method = 'FACILITY_ID_OMS_ONLY' THEN 'OMS'
            ELSE 'VEMS'
        END AS source_system,

        xwalk.oms_facility_id,
        xwalk.vems_facility_id,
        xwalk.match_confidence,

        (
            (CASE WHEN COALESCE(oms.facility_id, vems.facility_id) IS NOT NULL THEN 30 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.facility_name, vems.facility_name) IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.state, vems.state) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.city, vems.city) IS NOT NULL THEN 10 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.capacity, vems.capacity) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.active_flag, vems.active_flag) IS NOT NULL THEN 10 ELSE 0 END)
        ) AS dq_score,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_facility') xwalk
    LEFT JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_facilities_source') oms
        ON xwalk.oms_facility_id = oms.source_record_id AND oms.source_system = 'OMS'
    LEFT JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_facilities_source') vems
        ON xwalk.vems_facility_id = vems.source_record_id AND vems.source_system = 'VEMS'
    WHERE xwalk.batch_id = :p_batch_id;

    RETURN 'Facilities merged to staging: ' || SQLROWCOUNT || ' records';
END;
$$;

-- =====================================================================================
-- STEP 3: MERGE AND TRANSFORM FACTS
-- =====================================================================================
-- Purpose: Process transactional data (exam requests, evaluations, appointments)
-- Uses: Master IDs from crosswalks, code mappings

-- -----------------------------------------------------------------------------------------
-- STEP 3A: Merge Exam Requests into Staging
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sp_merge_exam_requests_to_staging(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
    v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
BEGIN
    DELETE FROM IDENTIFIER(:v_dw_database || '.STAGING.stg_fact_exam_requests')
    WHERE batch_id = :p_batch_id;

    INSERT INTO IDENTIFIER(:v_dw_database || '.STAGING.stg_fact_exam_requests') (
        batch_id,
        exam_request_id,
        master_veteran_id,
        master_facility_id,
        request_type,
        request_date,
        priority_level,
        sla_days,
        current_status,
        completion_date,

        source_system,
        source_record_id,
        dq_score,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,

        -- Exam request ID from source
        COALESCE(oms.exam_request_id, vems.exam_request_id) AS exam_request_id,

        -- Link to master veteran ID
        vet_xwalk.master_veteran_id,

        -- Link to master facility ID
        fac_xwalk.master_facility_id,

        -- Map request type code to standard value
        IDENTIFIER(:v_dw_database || '.REFERENCE.fn_map_request_type_code')(
            COALESCE(oms.source_system, vems.source_system),
            COALESCE(oms.request_type_code, vems.request_type_code)
        ) AS request_type,

        COALESCE(oms.request_date, vems.request_date) AS request_date,
        COALESCE(oms.priority_level, vems.priority_level) AS priority_level,
        COALESCE(oms.sla_days, vems.sla_days) AS sla_days,
        COALESCE(oms.current_status, vems.current_status) AS current_status,
        COALESCE(oms.completion_date, vems.completion_date) AS completion_date,

        COALESCE(oms.source_system, vems.source_system) AS source_system,
        COALESCE(oms.source_record_id, vems.source_record_id) AS source_record_id,

        (
            (CASE WHEN vet_xwalk.master_veteran_id IS NOT NULL THEN 30 ELSE 0 END) +
            (CASE WHEN fac_xwalk.master_facility_id IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.request_date, vems.request_date) IS NOT NULL THEN 20 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.request_type_code, vems.request_type_code) IS NOT NULL THEN 15 ELSE 0 END) +
            (CASE WHEN COALESCE(oms.sla_days, vems.sla_days) IS NOT NULL THEN 15 ELSE 0 END)
        ) AS dq_score,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM IDENTIFIER(:v_ods_database || '.ODS.ods_exam_requests_source') oms
    FULL OUTER JOIN IDENTIFIER(:v_ods_database || '.ODS.ods_exam_requests_source') vems
        ON oms.exam_request_id = vems.exam_request_id
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
    LEFT JOIN IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_veteran') vet_xwalk
        ON COALESCE(oms.veteran_ssn, vems.veteran_ssn) = vet_xwalk.master_veteran_id
    LEFT JOIN IDENTIFIER(:v_dw_database || '.REFERENCE.ref_entity_crosswalk_facility') fac_xwalk
        ON COALESCE(oms.facility_id, vems.facility_id) = fac_xwalk.master_facility_id
    WHERE (oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id);

    RETURN 'Exam requests merged to staging: ' || SQLROWCOUNT || ' records';
END;
$$;

-- =====================================================================================
-- STEP 4: MASTER ORCHESTRATION PROCEDURE
-- =====================================================================================
-- Purpose: Run all staging layer steps in correct order
-- Usage: CALL sp_staging_layer_master('BATCH_20251117_001');

CREATE OR REPLACE PROCEDURE sp_staging_layer_master(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_summary VARCHAR;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();

    -- ============================================================
    -- STEP 1: BUILD CROSSWALKS (ENTITY MATCHING)
    -- ============================================================

    CALL sp_build_crosswalk_veterans_simple(:p_batch_id);
    v_summary := 'Step 1A: ' || SQLSTATE || '\n';

    CALL sp_build_crosswalk_evaluators_simple(:p_batch_id);
    v_summary := v_summary || 'Step 1B: ' || SQLSTATE || '\n';

    CALL sp_build_crosswalk_facilities_simple(:p_batch_id);
    v_summary := v_summary || 'Step 1C: ' || SQLSTATE || '\n';

    -- ============================================================
    -- STEP 2: MERGE ENTITIES
    -- ============================================================

    CALL sp_merge_veterans_to_staging(:p_batch_id);
    v_summary := v_summary || 'Step 2A: ' || SQLSTATE || '\n';

    CALL sp_merge_evaluators_to_staging(:p_batch_id);
    v_summary := v_summary || 'Step 2B: ' || SQLSTATE || '\n';

    CALL sp_merge_facilities_to_staging(:p_batch_id);
    v_summary := v_summary || 'Step 2C: ' || SQLSTATE || '\n';

    -- ============================================================
    -- STEP 3: MERGE FACTS
    -- ============================================================

    CALL sp_merge_exam_requests_to_staging(:p_batch_id);
    v_summary := v_summary || 'Step 3A: ' || SQLSTATE || '\n';

    v_end_time := CURRENT_TIMESTAMP();

    v_summary := v_summary || 'Total duration: ' ||
        DATEDIFF(second, v_start_time, v_end_time) || ' seconds';

    RETURN v_summary;
END;
$$;

-- =====================================================================================
-- USAGE EXAMPLE
-- =====================================================================================
/*

-- Execute the entire staging layer processing for a batch
CALL sp_staging_layer_master('BATCH_20251117_001');

-- Or run steps individually for debugging:
CALL sp_build_crosswalk_veterans_simple('BATCH_20251117_001');
CALL sp_build_crosswalk_evaluators_simple('BATCH_20251117_001');
CALL sp_build_crosswalk_facilities_simple('BATCH_20251117_001');
CALL sp_merge_veterans_to_staging('BATCH_20251117_001');
CALL sp_merge_evaluators_to_staging('BATCH_20251117_001');
CALL sp_merge_facilities_to_staging('BATCH_20251117_001');
CALL sp_merge_exam_requests_to_staging('BATCH_20251117_001');

*/
