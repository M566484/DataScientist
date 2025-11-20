-- =====================================================
-- VES Data Pipeline - Dynamic Tables Approach
-- =====================================================
-- Purpose: Modern, declarative staging layer using Snowflake Dynamic Tables
-- Pattern: Self-refreshing tables with automatic CDC and dependency management
-- Standards: VES Snowflake Naming Conventions v1.0
--
-- BENEFITS OF DYNAMIC TABLES:
--   - Automatic refresh when source data changes (no manual orchestration)
--   - Built-in incremental processing (efficient CDC)
--   - Declarative SQL (no procedural code)
--   - Automatic dependency management (DAG-based)
--   - Target lag SLA (control freshness)
--
-- ARCHITECTURE:
--   Layer 1: ODS_RAW (source tables)
--         ↓
--   Layer 2A: Dynamic Crosswalks (entity matching)
--         ↓
--   Layer 2B: Dynamic Staging Tables (merged entities)
--         ↓
--   Layer 3: Warehouse (dimensional model)
--
-- Author: Data Team
-- Date: 2025-11-20
-- =====================================================

SET dw_database = (SELECT get_dw_database());
SET ods_database = (SELECT get_ods_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA REFERENCE;

-- =====================================================
-- DYNAMIC TABLE 1: Facility Crosswalk
-- =====================================================
-- Purpose: Match facilities between OMS and VEMS
-- Refresh: Every 15 minutes
-- Dependencies: ods_facilities_source (both OMS and VEMS)
-- Match Key: facility_id
--
-- Rolling Window: 7 days (configurable)
-- Incremental: AUTO mode (Snowflake decides based on query)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dt_crosswalk_facilities
    TARGET_LAG = '15 minutes'
    WAREHOUSE = compute_wh
    REFRESH_MODE = AUTO
    COMMENT = 'Dynamic crosswalk for facility matching between OMS and VEMS'
AS
WITH oms_latest AS (
    -- Get latest OMS facility records (7-day rolling window)
    SELECT
        source_record_id,
        facility_id,
        facility_name,
        state,
        ingestion_timestamp,
        batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id
            ORDER BY ingestion_timestamp DESC
        ) as rn
    FROM IDENTIFIER($ods_database || '.ODS_RAW.ods_facilities_source')
    WHERE source_system = 'OMS'
      AND ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    QUALIFY rn = 1  -- Keep only the latest record per facility
),
vems_latest AS (
    -- Get latest VEMS facility records (7-day rolling window)
    SELECT
        source_record_id,
        facility_id,
        facility_name,
        state,
        ingestion_timestamp,
        batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id
            ORDER BY ingestion_timestamp DESC
        ) as rn
    FROM IDENTIFIER($ods_database || '.ODS_RAW.ods_facilities_source')
    WHERE source_system = 'VEMS'
      AND ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    QUALIFY rn = 1
),
matched_facilities AS (
    -- Full outer join to find matches and orphans
    SELECT
        -- Master ID: Use facility_id as universal identifier
        COALESCE(o.facility_id, v.facility_id) as master_facility_id,

        -- Source IDs
        o.source_record_id as oms_facility_id,
        v.source_record_id as vems_facility_id,

        -- Match metadata
        CASE
            WHEN o.facility_id IS NOT NULL AND v.facility_id IS NOT NULL THEN 100.00
            WHEN o.facility_id IS NOT NULL THEN 90.00
            WHEN v.facility_id IS NOT NULL THEN 90.00
        END as match_confidence,

        CASE
            WHEN o.facility_id = v.facility_id THEN 'FACILITY_ID_EXACT_MATCH'
            WHEN o.facility_id IS NOT NULL THEN 'FACILITY_ID_OMS_ONLY'
            ELSE 'FACILITY_ID_VEMS_ONLY'
        END as match_method,

        -- Facility metadata for validation
        COALESCE(o.facility_name, v.facility_name) as facility_name,
        COALESCE(o.state, v.state) as state,

        -- Track which source is primary
        CASE
            WHEN o.facility_id IS NOT NULL AND v.facility_id IS NOT NULL THEN 'BOTH'
            WHEN o.facility_id IS NOT NULL THEN 'OMS_ONLY'
            ELSE 'VEMS_ONLY'
        END as source_systems,

        -- Batch IDs for traceability
        o.batch_id as oms_batch_id,
        v.batch_id as vems_batch_id,

        -- Timestamps
        GREATEST(
            COALESCE(o.ingestion_timestamp, '1900-01-01'::TIMESTAMP),
            COALESCE(v.ingestion_timestamp, '1900-01-01'::TIMESTAMP)
        ) as last_updated,

        CURRENT_TIMESTAMP() as crosswalk_created_timestamp

    FROM oms_latest o
    FULL OUTER JOIN vems_latest v
        ON o.facility_id = v.facility_id
)
SELECT * FROM matched_facilities;

-- =====================================================
-- DYNAMIC TABLE 2: Facility Staging
-- =====================================================
-- Purpose: Merged facility records from OMS and VEMS
-- Refresh: Every 30 minutes (depends on crosswalk)
-- Dependencies: dt_crosswalk_facilities, ods_facilities_source
-- System-of-Record: OMS is primary for facilities
-- =====================================================

USE SCHEMA STAGING;

CREATE OR REPLACE DYNAMIC TABLE dt_stg_facilities
    TARGET_LAG = '30 minutes'
    WAREHOUSE = compute_wh
    REFRESH_MODE = AUTO
    COMMENT = 'Dynamic staging table for facilities with OMS/VEMS merge logic'
AS
WITH oms_latest AS (
    SELECT *
    FROM IDENTIFIER($ods_database || '.ODS_RAW.ods_facilities_source')
    WHERE source_system = 'OMS'
      AND ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_record_id
        ORDER BY ingestion_timestamp DESC
    ) = 1
),
vems_latest AS (
    SELECT *
    FROM IDENTIFIER($ods_database || '.ODS_RAW.ods_facilities_source')
    WHERE source_system = 'VEMS'
      AND ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_record_id
        ORDER BY ingestion_timestamp DESC
    ) = 1
),
merged_facilities AS (
    SELECT
        -- Business Keys
        xwalk.master_facility_id,
        COALESCE(o.facility_id, v.facility_id) as facility_id,

        -- ============================================================
        -- FIELD MERGING LOGIC
        -- System-of-Record: OMS is primary for facilities
        -- Fallback: Use VEMS if OMS value is NULL
        -- ============================================================

        -- Basic Information (OMS primary)
        UPPER(TRIM(COALESCE(o.facility_name, v.facility_name))) as facility_name,
        COALESCE(o.facility_type, v.facility_type) as facility_type,

        -- Location Information (OMS primary)
        COALESCE(o.address, v.address) as address_line1,
        COALESCE(o.address2, v.address2) as address_line2,
        COALESCE(o.city, v.city) as city,
        COALESCE(o.state, v.state) as state,
        COALESCE(o.zip_code, v.zip_code) as zip_code,
        COALESCE(o.county, v.county) as county,

        -- Derived: Full address
        CONCAT_WS(', ',
            NULLIF(COALESCE(o.address, v.address), ''),
            NULLIF(COALESCE(o.city, v.city), ''),
            NULLIF(COALESCE(o.state, v.state), ''),
            NULLIF(COALESCE(o.zip_code, v.zip_code), '')
        ) as full_address,

        -- VA Organization (OMS primary)
        COALESCE(o.visn_code, v.visn_code) as visn_code,
        COALESCE(o.visn_name, v.visn_name) as visn_name,
        COALESCE(o.parent_facility_id, v.parent_facility_id) as parent_facility_id,

        -- Contact Information (OMS primary)
        COALESCE(o.phone, v.phone) as phone,
        COALESCE(o.fax, v.fax) as fax,
        LOWER(TRIM(COALESCE(o.email, v.email))) as email,
        COALESCE(o.website, v.website) as website,

        -- Operational Details (OMS primary)
        COALESCE(o.active_flag, v.active_flag, TRUE) as active_flag,
        COALESCE(o.operating_hours, v.operating_hours) as operating_hours,
        COALESCE(o.weekend_hours, v.weekend_hours) as weekend_hours,
        COALESCE(o.capacity, v.capacity) as capacity,

        -- ============================================================
        -- CHANGE DETECTION
        -- ============================================================
        MD5(
            CONCAT_WS('||',
                COALESCE(o.facility_name, v.facility_name),
                COALESCE(o.address, v.address),
                COALESCE(o.phone, v.phone),
                COALESCE(o.active_flag, v.active_flag)
            )
        ) as source_record_hash,

        -- ============================================================
        -- METADATA
        -- ============================================================
        CASE
            WHEN xwalk.match_method = 'FACILITY_ID_EXACT_MATCH' THEN 'OMS_VEMS_MERGED'
            WHEN xwalk.match_method = 'FACILITY_ID_OMS_ONLY' THEN 'OMS'
            ELSE 'VEMS'
        END as source_system,

        xwalk.oms_facility_id,
        xwalk.vems_facility_id,
        xwalk.match_confidence,
        xwalk.match_method,

        -- ============================================================
        -- DATA QUALITY SCORING (0-100)
        -- ============================================================
        (
            CASE WHEN COALESCE(o.facility_id, v.facility_id) IS NOT NULL THEN 30 ELSE 0 END +
            CASE WHEN COALESCE(o.facility_name, v.facility_name) IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN COALESCE(o.state, v.state) IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN COALESCE(o.city, v.city) IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN COALESCE(o.phone, v.phone) IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN COALESCE(o.capacity, v.capacity) IS NOT NULL THEN 5 ELSE 0 END +
            CASE WHEN COALESCE(o.active_flag, v.active_flag) IS NOT NULL THEN 10 ELSE 0 END
        ) as dq_score,

        -- ============================================================
        -- DATA QUALITY ISSUES
        -- ============================================================
        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(
                IFF(COALESCE(o.facility_id, v.facility_id) IS NULL, 'MISSING_FACILITY_ID', NULL),
                IFF(COALESCE(o.facility_name, v.facility_name) IS NULL, 'MISSING_FACILITY_NAME', NULL),
                IFF(COALESCE(o.state, v.state) IS NULL, 'MISSING_STATE', NULL),
                IFF(COALESCE(o.city, v.city) IS NULL, 'MISSING_CITY', NULL),
                IFF(COALESCE(o.phone, v.phone) IS NULL, 'MISSING_PHONE', NULL)
            ), '; '
        ) as dq_issues,

        -- ============================================================
        -- CONFLICT DETECTION
        -- ============================================================
        CASE
            WHEN o.facility_name IS NOT NULL AND v.facility_name IS NOT NULL
                 AND UPPER(TRIM(o.facility_name)) != UPPER(TRIM(v.facility_name))
            THEN 'FACILITY_NAME_MISMATCH'
            WHEN o.state IS NOT NULL AND v.state IS NOT NULL
                 AND o.state != v.state
            THEN 'STATE_MISMATCH'
            ELSE NULL
        END as conflict_type,

        -- Preserve conflicting values for audit
        CASE
            WHEN o.facility_name IS NOT NULL AND v.facility_name IS NOT NULL
                 AND UPPER(TRIM(o.facility_name)) != UPPER(TRIM(v.facility_name))
            THEN OBJECT_CONSTRUCT('facility_name', o.facility_name, 'state', o.state)
            ELSE NULL
        END as oms_values,

        CASE
            WHEN o.facility_name IS NOT NULL AND v.facility_name IS NOT NULL
                 AND UPPER(TRIM(o.facility_name)) != UPPER(TRIM(v.facility_name))
            THEN OBJECT_CONSTRUCT('facility_name', v.facility_name, 'state', v.state)
            ELSE NULL
        END as vems_values,

        -- Resolution method
        IFF(conflict_type IS NOT NULL, 'PREFER_OMS', NULL) as resolution_method,

        -- Timestamps
        xwalk.last_updated,
        CURRENT_TIMESTAMP() as staging_processed_timestamp

    FROM IDENTIFIER($dw_database || '.REFERENCE.dt_crosswalk_facilities') xwalk
    LEFT JOIN oms_latest o
        ON xwalk.oms_facility_id = o.source_record_id
    LEFT JOIN vems_latest v
        ON xwalk.vems_facility_id = v.source_record_id
)
SELECT * FROM merged_facilities;

-- =====================================================
-- DYNAMIC TABLE 3: Appointment Events Fact
-- =====================================================
-- Purpose: Extract appointment event facts with master IDs
-- Refresh: Every 20 minutes
-- Dependencies: dt_crosswalk_facilities, crosswalks for veterans/evaluators
-- System-of-Record: VEMS (appointments only exist in VEMS)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dt_stg_fact_appointment_events
    TARGET_LAG = '20 minutes'
    WAREHOUSE = compute_wh
    REFRESH_MODE = AUTO
    COMMENT = 'Dynamic staging table for appointment events from VEMS'
AS
WITH vems_appointments AS (
    -- Get latest VEMS appointment events (7-day rolling window)
    SELECT *
    FROM IDENTIFIER($ods_database || '.ODS_RAW.ods_appointment_events_source')
    WHERE source_system = 'VEMS'
      AND ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY ingestion_timestamp DESC
    ) = 1
),
enriched_events AS (
    SELECT
        -- Degenerate Dimensions
        apt.event_id,
        apt.appointment_id,
        apt.exam_request_id,

        -- Dimension Keys (resolved via crosswalks)
        vet_xwalk.master_veteran_id,
        eval_xwalk.master_evaluator_id,
        fac_xwalk.master_facility_id,

        -- Date Keys
        CAST(apt.event_timestamp AS DATE) as event_date,
        apt.event_timestamp,

        -- Event Details
        apt.event_type,  -- SCHEDULED, CONFIRMED, RESCHEDULED, CANCELLED, NO_SHOW, COMPLETED
        apt.event_status,

        -- Derive event sequence number (order of events for this appointment)
        ROW_NUMBER() OVER (
            PARTITION BY apt.appointment_id
            ORDER BY apt.event_timestamp
        ) as event_sequence_number,

        -- Rescheduling
        apt.previous_appointment_id,
        apt.new_appointment_id,
        apt.rescheduling_reason,

        -- Cancellation
        apt.cancellation_reason,
        apt.cancelled_by,

        -- No-Show
        IFF(apt.event_type = 'NO_SHOW', TRUE, FALSE) as no_show_flag,
        apt.no_show_reason,

        -- Location
        apt.appointment_location_type,
        apt.appointment_location,

        -- Scheduling
        apt.scheduled_date,
        apt.scheduled_time,

        -- Completion
        apt.completed_timestamp,
        apt.duration_minutes,

        -- Veteran Communication
        apt.veteran_notified_flag,
        apt.veteran_notification_date,
        apt.veteran_confirmed_flag,

        -- ============================================================
        -- DATA QUALITY SCORING
        -- ============================================================
        (
            CASE WHEN apt.event_id IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN apt.appointment_id IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN vet_xwalk.master_veteran_id IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN apt.event_type IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN apt.event_timestamp IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN apt.scheduled_date IS NOT NULL THEN 10 ELSE 0 END
        ) as dq_score,

        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(
                IFF(apt.event_id IS NULL, 'MISSING_EVENT_ID', NULL),
                IFF(apt.appointment_id IS NULL, 'MISSING_APPOINTMENT_ID', NULL),
                IFF(vet_xwalk.master_veteran_id IS NULL, 'MISSING_VETERAN_LINK', NULL),
                IFF(apt.event_type IS NULL, 'MISSING_EVENT_TYPE', NULL)
            ), '; '
        ) as dq_issues,

        -- Metadata
        'VEMS' as source_system,
        apt.batch_id,
        apt.ingestion_timestamp as source_loaded_timestamp,
        CURRENT_TIMESTAMP() as staging_processed_timestamp

    FROM vems_appointments apt

    -- Join to crosswalks to get master IDs
    LEFT JOIN IDENTIFIER($dw_database || '.REFERENCE.dt_crosswalk_veterans') vet_xwalk
        ON apt.veteran_ssn = vet_xwalk.master_veteran_id

    LEFT JOIN IDENTIFIER($dw_database || '.REFERENCE.dt_crosswalk_evaluators') eval_xwalk
        ON apt.evaluator_npi = eval_xwalk.master_evaluator_id

    LEFT JOIN IDENTIFIER($dw_database || '.REFERENCE.dt_crosswalk_facilities') fac_xwalk
        ON apt.facility_id = fac_xwalk.master_facility_id
)
SELECT * FROM enriched_events;

-- =====================================================
-- DYNAMIC TABLE 4: QA Events Fact
-- =====================================================
-- Purpose: Extract QA event facts with master IDs
-- Refresh: Every 30 minutes
-- Dependencies: Crosswalks for evaluators
-- System-of-Record: OMS (QA events only exist in OMS)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dt_stg_fact_qa_events
    TARGET_LAG = '30 minutes'
    WAREHOUSE = compute_wh
    REFRESH_MODE = AUTO
    COMMENT = 'Dynamic staging table for QA events from OMS'
AS
WITH oms_qa_events AS (
    -- Get latest OMS QA events (7-day rolling window)
    SELECT *
    FROM IDENTIFIER($ods_database || '.ODS_RAW.ods_qa_events_source')
    WHERE source_system = 'OMS'
      AND ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY qa_event_id
        ORDER BY ingestion_timestamp DESC
    ) = 1
),
enriched_qa_events AS (
    SELECT
        -- Degenerate Dimensions
        qa.qa_event_id,
        qa.evaluation_id,

        -- Dimension Keys
        reviewer_xwalk.master_evaluator_id as qa_reviewer_id,

        -- Date Keys
        CAST(qa.event_timestamp AS DATE) as event_date,
        qa.event_timestamp,

        -- Event Details
        qa.event_type,  -- REVIEW_STARTED, DEFICIENCY_FOUND, CLARIFICATION_REQUESTED, APPROVED, REJECTED
        qa.event_status,
        qa.qa_cycle_number,

        -- Review Details
        qa.review_outcome,
        qa.overall_quality_score,
        qa.completeness_score,
        qa.accuracy_score,

        -- Derived quality metrics
        ROUND((qa.completeness_score + qa.accuracy_score) / 2.0, 2) as clarity_score,
        qa.nexus_quality_score,

        -- Deficiencies
        IFF(qa.deficiency_count > 0, TRUE, FALSE) as deficiency_found_flag,
        qa.deficiency_count,

        -- Derive deficiency severity based on count
        CASE
            WHEN qa.deficiency_count = 0 THEN NULL
            WHEN qa.deficiency_count <= 2 THEN 'MINOR'
            WHEN qa.deficiency_count <= 5 THEN 'MODERATE'
            ELSE 'MAJOR'
        END as deficiency_severity,

        qa.deficiency_category,
        qa.deficiency_description,

        -- Clarification
        qa.clarification_requested_flag,
        qa.clarification_type,
        qa.clarification_description,
        qa.clarification_due_date,

        -- Approval
        qa.approved_flag,
        qa.approved_timestamp,

        -- First pass approval (QA cycle 1 + approved)
        IFF(qa.qa_cycle_number = 1 AND qa.approved_flag = TRUE, TRUE, FALSE) as first_pass_approval_flag,

        -- Timing (Derived)
        ROUND(
            DATEDIFF(second, qa.review_started_timestamp, qa.event_timestamp) / 3600.0,
            2
        ) as review_duration_hours,

        ROUND(
            DATEDIFF(second, qa.evaluation_submitted_timestamp, qa.event_timestamp) / 3600.0,
            2
        ) as turnaround_time_hours,

        DATEDIFF(day, qa.evaluation_submitted_timestamp, qa.event_timestamp) as days_in_qa_at_event,

        -- ============================================================
        -- DATA QUALITY SCORING
        -- ============================================================
        (
            CASE WHEN qa.qa_event_id IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN qa.evaluation_id IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN reviewer_xwalk.master_evaluator_id IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN qa.event_type IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN qa.review_outcome IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN qa.overall_quality_score IS NOT NULL THEN 15 ELSE 0 END
        ) as dq_score,

        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(
                IFF(qa.qa_event_id IS NULL, 'MISSING_QA_EVENT_ID', NULL),
                IFF(qa.evaluation_id IS NULL, 'MISSING_EVALUATION_ID', NULL),
                IFF(reviewer_xwalk.master_evaluator_id IS NULL, 'MISSING_REVIEWER_LINK', NULL),
                IFF(qa.event_type IS NULL, 'MISSING_EVENT_TYPE', NULL)
            ), '; '
        ) as dq_issues,

        -- Metadata
        'OMS' as source_system,
        qa.batch_id,
        qa.ingestion_timestamp as source_loaded_timestamp,
        CURRENT_TIMESTAMP() as staging_processed_timestamp

    FROM oms_qa_events qa

    -- Join to evaluator crosswalk to get master reviewer ID
    LEFT JOIN IDENTIFIER($dw_database || '.REFERENCE.dt_crosswalk_evaluators') reviewer_xwalk
        ON qa.reviewer_npi = reviewer_xwalk.master_evaluator_id
)
SELECT * FROM enriched_qa_events;

-- =====================================================
-- DYNAMIC TABLE 5: Data Quality Summary Dashboard
-- =====================================================
-- Purpose: Real-time data quality metrics across all staging tables
-- Refresh: Every 10 minutes
-- Dependencies: All dynamic staging tables
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE dt_vw_staging_dq_summary
    TARGET_LAG = '10 minutes'
    WAREHOUSE = compute_wh
    REFRESH_MODE = FULL  -- Use FULL since we're using UNION ALL
    COMMENT = 'Real-time data quality dashboard for dynamic staging tables'
AS
WITH facilities_dq AS (
    SELECT
        'dt_stg_facilities' as table_name,
        'Facilities' as entity_name,
        COUNT(*) as total_records,
        ROUND(AVG(dq_score), 2) as avg_dq_score,
        MIN(dq_score) as min_dq_score,
        MAX(dq_score) as max_dq_score,
        SUM(CASE WHEN dq_score >= 90 THEN 1 ELSE 0 END) as excellent_records,
        SUM(CASE WHEN dq_score >= 80 AND dq_score < 90 THEN 1 ELSE 0 END) as good_records,
        SUM(CASE WHEN dq_score >= 70 AND dq_score < 80 THEN 1 ELSE 0 END) as acceptable_records,
        SUM(CASE WHEN dq_score < 70 THEN 1 ELSE 0 END) as poor_records,
        SUM(CASE WHEN conflict_type IS NOT NULL THEN 1 ELSE 0 END) as records_with_conflicts,
        MAX(staging_processed_timestamp) as last_refresh_timestamp
    FROM IDENTIFIER($dw_database || '.STAGING.dt_stg_facilities')
),
appointment_events_dq AS (
    SELECT
        'dt_stg_fact_appointment_events' as table_name,
        'Appointment Events' as entity_name,
        COUNT(*) as total_records,
        ROUND(AVG(dq_score), 2) as avg_dq_score,
        MIN(dq_score) as min_dq_score,
        MAX(dq_score) as max_dq_score,
        SUM(CASE WHEN dq_score >= 90 THEN 1 ELSE 0 END) as excellent_records,
        SUM(CASE WHEN dq_score >= 80 AND dq_score < 90 THEN 1 ELSE 0 END) as good_records,
        SUM(CASE WHEN dq_score >= 70 AND dq_score < 80 THEN 1 ELSE 0 END) as acceptable_records,
        SUM(CASE WHEN dq_score < 70 THEN 1 ELSE 0 END) as poor_records,
        0 as records_with_conflicts,  -- No conflicts for fact tables
        MAX(staging_processed_timestamp) as last_refresh_timestamp
    FROM IDENTIFIER($dw_database || '.STAGING.dt_stg_fact_appointment_events')
),
qa_events_dq AS (
    SELECT
        'dt_stg_fact_qa_events' as table_name,
        'QA Events' as entity_name,
        COUNT(*) as total_records,
        ROUND(AVG(dq_score), 2) as avg_dq_score,
        MIN(dq_score) as min_dq_score,
        MAX(dq_score) as max_dq_score,
        SUM(CASE WHEN dq_score >= 90 THEN 1 ELSE 0 END) as excellent_records,
        SUM(CASE WHEN dq_score >= 80 AND dq_score < 90 THEN 1 ELSE 0 END) as good_records,
        SUM(CASE WHEN dq_score >= 70 AND dq_score < 80 THEN 1 ELSE 0 END) as acceptable_records,
        SUM(CASE WHEN dq_score < 70 THEN 1 ELSE 0 END) as poor_records,
        0 as records_with_conflicts,
        MAX(staging_processed_timestamp) as last_refresh_timestamp
    FROM IDENTIFIER($dw_database || '.STAGING.dt_stg_fact_qa_events')
),
combined AS (
    SELECT * FROM facilities_dq
    UNION ALL
    SELECT * FROM appointment_events_dq
    UNION ALL
    SELECT * FROM qa_events_dq
)
SELECT
    *,
    -- Overall health indicator
    CASE
        WHEN avg_dq_score >= 90 THEN 'EXCELLENT'
        WHEN avg_dq_score >= 80 THEN 'GOOD'
        WHEN avg_dq_score >= 70 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as health_status,

    -- Percentage calculations
    ROUND((excellent_records::DECIMAL / NULLIF(total_records, 0)) * 100, 2) as pct_excellent,
    ROUND((poor_records::DECIMAL / NULLIF(total_records, 0)) * 100, 2) as pct_poor
FROM combined
ORDER BY table_name;

-- =====================================================
-- REGULAR VIEW: Latest Facilities (No Batch ID)
-- =====================================================
-- Purpose: Simple interface for downstream consumers
-- Pattern: Hide dynamic table complexity, expose clean API
-- =====================================================

CREATE OR REPLACE VIEW vw_latest_facilities AS
SELECT
    master_facility_id,
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    zip_code,
    phone,
    active_flag,
    dq_score,
    source_system,
    last_updated
FROM dt_stg_facilities
WHERE active_flag = TRUE
ORDER BY facility_name;

-- =====================================================
-- VERIFICATION & MONITORING
-- =====================================================

-- Check Dynamic Table status
SELECT
    name as dynamic_table_name,
    database_name,
    schema_name,
    target_lag,
    refresh_mode,
    warehouse_name,
    scheduling_state,
    last_refresh_start_time,
    last_refresh_end_time,
    DATEDIFF(second, last_refresh_start_time, last_refresh_end_time) as last_refresh_seconds,
    next_refresh_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = $dw_database
  AND schema_name IN ('REFERENCE', 'STAGING')
ORDER BY schema_name, name;

-- Check Dynamic Table refresh history
SELECT
    name as dynamic_table_name,
    state,
    refresh_start_time,
    refresh_end_time,
    DATEDIFF(second, refresh_start_time, refresh_end_time) as duration_seconds,
    refresh_action,  -- INCREMENTAL or FULL
    completion_target
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE database_name = $dw_database
  AND schema_name IN ('REFERENCE', 'STAGING')
ORDER BY refresh_start_time DESC
LIMIT 20;

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

/*

-- 1. Query the data quality dashboard
SELECT * FROM dt_vw_staging_dq_summary;

-- 2. Query facilities with high quality
SELECT * FROM dt_stg_facilities
WHERE dq_score >= 90
ORDER BY staging_processed_timestamp DESC;

-- 3. Find facilities with conflicts
SELECT
    master_facility_id,
    facility_name,
    conflict_type,
    oms_values,
    vems_values,
    resolution_method
FROM dt_stg_facilities
WHERE conflict_type IS NOT NULL;

-- 4. Check appointment events for today
SELECT * FROM dt_stg_fact_appointment_events
WHERE event_date = CURRENT_DATE()
ORDER BY event_timestamp DESC;

-- 5. Monitor QA review metrics
SELECT
    event_date,
    COUNT(*) as total_qa_events,
    SUM(CASE WHEN approved_flag THEN 1 ELSE 0 END) as approved_count,
    SUM(CASE WHEN first_pass_approval_flag THEN 1 ELSE 0 END) as first_pass_approvals,
    ROUND(AVG(overall_quality_score), 2) as avg_quality_score,
    ROUND(AVG(review_duration_hours), 2) as avg_review_hours
FROM dt_stg_fact_qa_events
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY event_date
ORDER BY event_date DESC;

-- 6. Force refresh a dynamic table (if needed)
ALTER DYNAMIC TABLE dt_stg_facilities REFRESH;

-- 7. Pause/Resume a dynamic table
ALTER DYNAMIC TABLE dt_stg_facilities SUSPEND;
ALTER DYNAMIC TABLE dt_stg_facilities RESUME;

-- 8. Change target lag
ALTER DYNAMIC TABLE dt_stg_facilities SET TARGET_LAG = '15 minutes';

*/
