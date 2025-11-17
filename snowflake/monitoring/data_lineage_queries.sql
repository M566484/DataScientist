-- =====================================================
-- VES Data Lineage Monitoring Queries
-- =====================================================
-- Purpose: Track data lineage from source to warehouse
-- Usage: Run these queries to investigate data issues
-- Standards: VES Snowflake Naming Conventions v1.0

USE DATABASE VETERAN_EVALUATION_DW;

-- =====================================================
-- QUERY 1: Complete Lineage Trace for a Veteran
-- =====================================================
-- Purpose: Trace a single veteran through all pipeline layers
-- Usage: Replace '123-45-6789' with the veteran SSN/ID

CREATE OR REPLACE VIEW vw_veteran_lineage_trace AS
WITH warehouse_current AS (
    SELECT
        veteran_id,
        veteran_sk,
        first_name,
        last_name,
        disability_rating,
        source_record_id,
        effective_date,
        expiration_date,
        is_current
    FROM WAREHOUSE.dim_veterans
    WHERE is_current = TRUE
),
crosswalk AS (
    SELECT
        master_veteran_id,
        oms_veteran_id,
        oms_ssn,
        vems_veteran_id,
        vems_ssn,
        match_method,
        match_confidence,
        primary_source_system,
        created_timestamp as crosswalk_created,
        updated_timestamp as crosswalk_updated
    FROM REFERENCE.ref_entity_crosswalk_veteran
),
oms_latest AS (
    SELECT
        source_record_id,
        veteran_ssn,
        first_name,
        last_name,
        disability_rating as oms_disability_rating,
        extraction_timestamp as oms_extracted_at,
        batch_id as oms_batch_id,
        ROW_NUMBER() OVER (PARTITION BY veteran_ssn ORDER BY extraction_timestamp DESC) as rn
    FROM ODS_RAW.ods_veterans_source
    WHERE source_system = 'OMS'
),
vems_latest AS (
    SELECT
        source_record_id,
        veteran_ssn,
        first_name,
        last_name,
        disability_rating as vems_disability_rating,
        extraction_timestamp as vems_extracted_at,
        batch_id as vems_batch_id,
        ROW_NUMBER() OVER (PARTITION BY veteran_ssn ORDER BY extraction_timestamp DESC) as rn
    FROM ODS_RAW.ods_veterans_source
    WHERE source_system = 'VEMS'
),
conflicts AS (
    SELECT
        entity_id,
        conflict_type,
        oms_value,
        vems_value,
        resolved_value,
        resolution_method,
        reconciliation_timestamp,
        ROW_NUMBER() OVER (PARTITION BY entity_id, conflict_type ORDER BY reconciliation_timestamp DESC) as rn
    FROM REFERENCE.ref_reconciliation_log
    WHERE entity_type = 'VETERAN'
)
SELECT
    -- Current Warehouse State
    w.veteran_id,
    w.veteran_sk as current_warehouse_sk,
    w.first_name as warehouse_first_name,
    w.last_name as warehouse_last_name,
    w.disability_rating as warehouse_disability_rating,
    w.effective_date as warehouse_effective_date,

    -- Crosswalk Information
    x.match_method,
    x.match_confidence,
    x.primary_source_system,
    x.crosswalk_created,
    x.crosswalk_updated,

    -- OMS Source
    x.oms_veteran_id,
    o.oms_disability_rating,
    o.oms_extracted_at,
    o.oms_batch_id,

    -- VEMS Source
    x.vems_veteran_id,
    v.vems_disability_rating,
    v.vems_extracted_at,
    v.vems_batch_id,

    -- Conflict Information
    c.conflict_type as latest_conflict_type,
    c.oms_value as conflict_oms_value,
    c.vems_value as conflict_vems_value,
    c.resolved_value as conflict_resolved_value,
    c.resolution_method,
    c.reconciliation_timestamp as latest_conflict_timestamp,

    -- Data Freshness
    DATEDIFF(day, o.oms_extracted_at, CURRENT_TIMESTAMP()) as oms_days_old,
    DATEDIFF(day, v.vems_extracted_at, CURRENT_TIMESTAMP()) as vems_days_old,

    -- Quality Flags
    CASE
        WHEN o.oms_disability_rating IS NOT NULL
             AND v.vems_disability_rating IS NOT NULL
             AND o.oms_disability_rating != v.vems_disability_rating
        THEN TRUE
        ELSE FALSE
    END as has_current_conflict

FROM warehouse_current w
LEFT JOIN crosswalk x ON w.veteran_id = x.master_veteran_id
LEFT JOIN oms_latest o ON x.oms_veteran_id = o.source_record_id AND o.rn = 1
LEFT JOIN vems_latest v ON x.vems_veteran_id = v.source_record_id AND v.rn = 1
LEFT JOIN conflicts c ON x.master_veteran_id = c.entity_id AND c.rn = 1;

-- Usage Example:
-- SELECT * FROM vw_veteran_lineage_trace WHERE veteran_id = '123-45-6789';

-- =====================================================
-- QUERY 2: Source System Comparison
-- =====================================================
-- Purpose: Compare values between OMS and VEMS for a veteran
-- Identifies discrepancies and data quality issues

CREATE OR REPLACE PROCEDURE sp_compare_source_systems(
    p_veteran_id VARCHAR
)
RETURNS TABLE (
    field_name VARCHAR,
    oms_value VARCHAR,
    vems_value VARCHAR,
    match_status VARCHAR,
    warehouse_value VARCHAR,
    source_used VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH oms_data AS (
            SELECT
                first_name,
                last_name,
                disability_rating,
                email,
                phone_primary as phone,
                address_line1,
                city,
                state
            FROM ODS_RAW.ods_veterans_source
            WHERE veteran_ssn = :p_veteran_id
              AND source_system = 'OMS'
            ORDER BY extraction_timestamp DESC
            LIMIT 1
        ),
        vems_data AS (
            SELECT
                first_name,
                last_name,
                disability_rating,
                email,
                phone,
                address_line1,
                city,
                state
            FROM ODS_RAW.ods_veterans_source
            WHERE veteran_ssn = :p_veteran_id
              AND source_system = 'VEMS'
            ORDER BY extraction_timestamp DESC
            LIMIT 1
        ),
        warehouse_data AS (
            SELECT
                first_name,
                last_name,
                current_disability_rating,
                email,
                phone,
                address_line1,
                city,
                state
            FROM WAREHOUSE.dim_veterans
            WHERE veteran_id = :p_veteran_id
              AND is_current = TRUE
        ),
        system_of_record AS (
            SELECT primary_source_system
            FROM REFERENCE.ref_system_of_record
            WHERE entity_type = 'VETERAN'
        )
        SELECT
            field_name,
            oms_value::VARCHAR as oms_value,
            vems_value::VARCHAR as vems_value,
            CASE
                WHEN oms_value = vems_value THEN 'MATCH'
                WHEN oms_value IS NULL THEN 'MISSING_IN_OMS'
                WHEN vems_value IS NULL THEN 'MISSING_IN_VEMS'
                ELSE 'MISMATCH'
            END as match_status,
            warehouse_value::VARCHAR as warehouse_value,
            CASE
                WHEN warehouse_value = oms_value THEN 'OMS'
                WHEN warehouse_value = vems_value THEN 'VEMS'
                WHEN warehouse_value IS NOT NULL THEN 'MERGED'
                ELSE 'NONE'
            END as source_used
        FROM (
            SELECT 'first_name' as field_name, o.first_name as oms_value, v.first_name as vems_value, w.first_name as warehouse_value FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'last_name', o.last_name, v.last_name, w.last_name FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'disability_rating', o.disability_rating::VARCHAR, v.disability_rating::VARCHAR, w.current_disability_rating::VARCHAR FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'email', o.email, v.email, w.email FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'phone', o.phone, v.phone, w.phone FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'address_line1', o.address_line1, v.address_line1, w.address_line1 FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'city', o.city, v.city, w.city FROM oms_data o, vems_data v, warehouse_data w UNION ALL
            SELECT 'state', o.state, v.state, w.state FROM oms_data o, vems_data v, warehouse_data w
        )
    );
    RETURN TABLE(res);
END;
$$;

-- Usage Example:
-- CALL sp_compare_source_systems('123-45-6789');

-- =====================================================
-- QUERY 3: Historical Value Changes
-- =====================================================
-- Purpose: Show how a veteran's data changed over time
-- Uses SCD Type 2 history

CREATE OR REPLACE VIEW vw_veteran_history AS
SELECT
    veteran_id,
    veteran_sk,
    first_name,
    last_name,
    current_disability_rating,
    effective_date,
    expiration_date,
    is_current,
    LEAD(current_disability_rating) OVER (PARTITION BY veteran_id ORDER BY effective_date DESC) as previous_disability_rating,
    LEAD(effective_date) OVER (PARTITION BY veteran_id ORDER BY effective_date DESC) as previous_effective_date,
    DATEDIFF(day, effective_date, COALESCE(expiration_date, CURRENT_DATE())) as days_active,
    source_record_id
FROM WAREHOUSE.dim_veterans
ORDER BY veteran_id, effective_date DESC;

-- Usage Example:
-- SELECT * FROM vw_veteran_history WHERE veteran_id = '123-45-6789';

-- =====================================================
-- QUERY 4: Match Quality Report
-- =====================================================
-- Purpose: Assess quality of OMS/VEMS matching
-- Identifies low-confidence matches that need review

CREATE OR REPLACE VIEW vw_match_quality_report AS
SELECT
    primary_source_system,
    match_method,
    COUNT(*) as match_count,
    AVG(match_confidence) as avg_confidence,
    MIN(match_confidence) as min_confidence,
    MAX(match_confidence) as max_confidence,
    SUM(CASE WHEN match_confidence = 100 THEN 1 ELSE 0 END) as perfect_matches,
    SUM(CASE WHEN match_confidence < 100 THEN 1 ELSE 0 END) as fuzzy_matches,
    SUM(CASE WHEN match_confidence < 90 THEN 1 ELSE 0 END) as low_confidence_matches
FROM REFERENCE.ref_entity_crosswalk_veteran
GROUP BY primary_source_system, match_method
ORDER BY avg_confidence DESC;

-- Usage Example:
-- SELECT * FROM vw_match_quality_report;

-- Find specific low-confidence matches
CREATE OR REPLACE VIEW vw_low_confidence_matches AS
SELECT
    master_veteran_id,
    oms_veteran_id,
    vems_veteran_id,
    match_method,
    match_confidence,
    primary_source_system,
    created_timestamp
FROM REFERENCE.ref_entity_crosswalk_veteran
WHERE match_confidence < 95
ORDER BY match_confidence ASC;

-- Usage Example:
-- SELECT * FROM vw_low_confidence_matches LIMIT 100;

-- =====================================================
-- QUERY 5: Conflict Analysis
-- =====================================================
-- Purpose: Analyze data conflicts between OMS and VEMS
-- Helps identify systematic data quality issues

CREATE OR REPLACE VIEW vw_conflict_summary AS
SELECT
    entity_type,
    conflict_type,
    COUNT(*) as conflict_count,
    COUNT(DISTINCT entity_id) as affected_entities,
    MIN(reconciliation_timestamp) as first_occurrence,
    MAX(reconciliation_timestamp) as last_occurrence,
    DATEDIFF(day, MIN(reconciliation_timestamp), MAX(reconciliation_timestamp)) as days_span
FROM REFERENCE.ref_reconciliation_log
GROUP BY entity_type, conflict_type
ORDER BY conflict_count DESC;

-- Usage Example:
-- SELECT * FROM vw_conflict_summary;

-- Detailed conflict view for veterans
CREATE OR REPLACE VIEW vw_veteran_conflicts AS
SELECT
    entity_id as veteran_id,
    conflict_type,
    oms_value,
    vems_value,
    resolved_value,
    resolution_method,
    reconciliation_timestamp,
    batch_id
FROM REFERENCE.ref_reconciliation_log
WHERE entity_type = 'VETERAN'
ORDER BY reconciliation_timestamp DESC;

-- Usage Example:
-- SELECT * FROM vw_veteran_conflicts WHERE veteran_id = '123-45-6789';

-- =====================================================
-- QUERY 6: ETL Batch Lineage
-- =====================================================
-- Purpose: Track which batches loaded which data
-- Essential for rollback and troubleshooting

CREATE OR REPLACE VIEW vw_batch_lineage AS
SELECT
    b.batch_id,
    b.batch_name,
    b.source_system,
    b.extraction_type,
    b.batch_start_timestamp,
    b.batch_end_timestamp,
    b.batch_status,
    b.records_extracted,
    b.records_loaded,
    b.records_rejected,
    DATEDIFF(minute, b.batch_start_timestamp, b.batch_end_timestamp) as duration_minutes,

    -- Count records in ODS from this batch
    (SELECT COUNT(*) FROM ODS_RAW.ods_veterans_source WHERE batch_id = b.batch_id) as ods_veteran_count,
    (SELECT COUNT(*) FROM ODS_RAW.ods_evaluators_source WHERE batch_id = b.batch_id) as ods_evaluator_count,
    (SELECT COUNT(*) FROM ODS_RAW.ods_exam_requests_source WHERE batch_id = b.batch_id) as ods_exam_request_count,

    -- Error count
    (SELECT COUNT(*) FROM ODS_RAW.ods_error_log WHERE batch_id = b.batch_id) as error_count

FROM ODS_RAW.ods_batch_control b
ORDER BY b.batch_start_timestamp DESC;

-- Usage Example:
-- SELECT * FROM vw_batch_lineage WHERE batch_date >= CURRENT_DATE - 7;

-- =====================================================
-- QUERY 7: Data Freshness Report
-- =====================================================
-- Purpose: Identify stale data from source systems
-- Alerts when data hasn't been updated recently

CREATE OR REPLACE VIEW vw_data_freshness AS
WITH oms_freshness AS (
    SELECT
        'OMS' as source_system,
        'VETERANS' as entity_type,
        MAX(extraction_timestamp) as last_extraction,
        COUNT(*) as record_count,
        DATEDIFF(hour, MAX(extraction_timestamp), CURRENT_TIMESTAMP()) as hours_since_last_update
    FROM ODS_RAW.ods_veterans_source
    WHERE source_system = 'OMS'

    UNION ALL

    SELECT
        'OMS',
        'EVALUATORS',
        MAX(extraction_timestamp),
        COUNT(*),
        DATEDIFF(hour, MAX(extraction_timestamp), CURRENT_TIMESTAMP())
    FROM ODS_RAW.ods_evaluators_source
    WHERE source_system = 'OMS'

    UNION ALL

    SELECT
        'OMS',
        'EXAM_REQUESTS',
        MAX(extraction_timestamp),
        COUNT(*),
        DATEDIFF(hour, MAX(extraction_timestamp), CURRENT_TIMESTAMP())
    FROM ODS_RAW.ods_exam_requests_source
    WHERE source_system = 'OMS'
),
vems_freshness AS (
    SELECT
        'VEMS' as source_system,
        'VETERANS' as entity_type,
        MAX(extraction_timestamp) as last_extraction,
        COUNT(*) as record_count,
        DATEDIFF(hour, MAX(extraction_timestamp), CURRENT_TIMESTAMP()) as hours_since_last_update
    FROM ODS_RAW.ods_veterans_source
    WHERE source_system = 'VEMS'

    UNION ALL

    SELECT
        'VEMS',
        'EVALUATORS',
        MAX(extraction_timestamp),
        COUNT(*),
        DATEDIFF(hour, MAX(extraction_timestamp), CURRENT_TIMESTAMP())
    FROM ODS_RAW.ods_evaluators_source
    WHERE source_system = 'VEMS'

    UNION ALL

    SELECT
        'VEMS',
        'APPOINTMENTS',
        MAX(extraction_timestamp),
        COUNT(*),
        DATEDIFF(hour, MAX(extraction_timestamp), CURRENT_TIMESTAMP())
    FROM ODS_RAW.ods_appointments_source
    WHERE source_system = 'VEMS'
)
SELECT
    source_system,
    entity_type,
    last_extraction,
    record_count,
    hours_since_last_update,
    CASE
        WHEN hours_since_last_update <= 24 THEN 'FRESH'
        WHEN hours_since_last_update <= 48 THEN 'ACCEPTABLE'
        WHEN hours_since_last_update <= 168 THEN 'STALE'
        ELSE 'CRITICAL'
    END as freshness_status
FROM oms_freshness
UNION ALL
SELECT * FROM vems_freshness
ORDER BY hours_since_last_update DESC;

-- Usage Example:
-- SELECT * FROM vw_data_freshness;

-- =====================================================
-- QUERY 8: Error Analysis
-- =====================================================
-- Purpose: Investigate ETL failures and data quality issues

CREATE OR REPLACE VIEW vw_error_summary AS
SELECT
    error_type,
    source_table,
    COUNT(*) as error_count,
    COUNT(DISTINCT batch_id) as affected_batches,
    MIN(error_timestamp) as first_occurrence,
    MAX(error_timestamp) as last_occurrence,
    -- Sample error messages
    ARRAY_AGG(DISTINCT LEFT(error_message, 100)) WITHIN GROUP (ORDER BY error_timestamp DESC) as sample_errors
FROM ODS_RAW.ods_error_log
WHERE error_timestamp >= CURRENT_DATE - 7
GROUP BY error_type, source_table
ORDER BY error_count DESC;

-- Usage Example:
-- SELECT * FROM vw_error_summary;

-- Detailed error log
-- SELECT * FROM ODS_RAW.ods_error_log WHERE batch_id = 'BATCH_20250117_120000';

-- =====================================================
-- QUERY 9: End-to-End Pipeline Health Check
-- =====================================================
-- Purpose: Quick health check of entire data pipeline
-- Run this daily to ensure pipeline is functioning

CREATE OR REPLACE PROCEDURE sp_pipeline_health_check()
RETURNS TABLE (
    check_name VARCHAR,
    status VARCHAR,
    metric_value VARCHAR,
    details VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH health_metrics AS (
            -- Check 1: Recent successful batch
            SELECT
                'Last Successful Batch' as check_name,
                CASE
                    WHEN DATEDIFF(hour, MAX(batch_end_timestamp), CURRENT_TIMESTAMP()) <= 24 THEN 'HEALTHY'
                    WHEN DATEDIFF(hour, MAX(batch_end_timestamp), CURRENT_TIMESTAMP()) <= 48 THEN 'WARNING'
                    ELSE 'CRITICAL'
                END as status,
                TO_VARCHAR(MAX(batch_end_timestamp), 'YYYY-MM-DD HH24:MI:SS') as metric_value,
                DATEDIFF(hour, MAX(batch_end_timestamp), CURRENT_TIMESTAMP())::VARCHAR || ' hours ago' as details
            FROM ODS_RAW.ods_batch_control
            WHERE batch_status = 'COMPLETED'

            UNION ALL

            -- Check 2: Failed batches in last 24 hours
            SELECT
                'Failed Batches (24h)',
                CASE
                    WHEN COUNT(*) = 0 THEN 'HEALTHY'
                    WHEN COUNT(*) <= 2 THEN 'WARNING'
                    ELSE 'CRITICAL'
                END,
                COUNT(*)::VARCHAR,
                'Review batch control table'
            FROM ODS_RAW.ods_batch_control
            WHERE batch_status = 'FAILED'
              AND batch_start_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'

            UNION ALL

            -- Check 3: Data errors in last 24 hours
            SELECT
                'Data Errors (24h)',
                CASE
                    WHEN COUNT(*) = 0 THEN 'HEALTHY'
                    WHEN COUNT(*) <= 10 THEN 'WARNING'
                    ELSE 'CRITICAL'
                END,
                COUNT(*)::VARCHAR,
                'Review error log'
            FROM ODS_RAW.ods_error_log
            WHERE error_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'

            UNION ALL

            -- Check 4: Low confidence matches
            SELECT
                'Low Confidence Matches',
                CASE
                    WHEN COUNT(*) = 0 THEN 'HEALTHY'
                    WHEN COUNT(*) <= 50 THEN 'WARNING'
                    ELSE 'CRITICAL'
                END,
                COUNT(*)::VARCHAR,
                'Review entity crosswalks'
            FROM REFERENCE.ref_entity_crosswalk_veteran
            WHERE match_confidence < 95

            UNION ALL

            -- Check 5: Unresolved conflicts
            SELECT
                'Active Conflicts',
                CASE
                    WHEN COUNT(*) <= 10 THEN 'HEALTHY'
                    WHEN COUNT(*) <= 50 THEN 'WARNING'
                    ELSE 'CRITICAL'
                END,
                COUNT(*)::VARCHAR,
                'Review reconciliation log'
            FROM REFERENCE.ref_reconciliation_log
            WHERE reconciliation_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '7 days'

            UNION ALL

            -- Check 6: Warehouse record count
            SELECT
                'Active Veterans in Warehouse',
                CASE
                    WHEN COUNT(*) > 0 THEN 'HEALTHY'
                    ELSE 'CRITICAL'
                END,
                COUNT(*)::VARCHAR,
                'dim_veterans.is_current = TRUE'
            FROM WAREHOUSE.dim_veterans
            WHERE is_current = TRUE
        )
        SELECT * FROM health_metrics
    );
    RETURN TABLE(res);
END;
$$;

-- Usage Example:
-- CALL sp_pipeline_health_check();

-- =====================================================
-- QUERY 10: Complete Lineage Documentation Report
-- =====================================================
-- Purpose: Generate full lineage report for a veteran
-- Use for compliance and audit purposes

CREATE OR REPLACE PROCEDURE sp_generate_lineage_report(
    p_veteran_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    report_text VARCHAR DEFAULT '';
BEGIN
    -- This would generate a comprehensive text report
    -- For brevity, returning summary

    RETURN 'Lineage report generated for veteran: ' || p_veteran_id ||
           '. Query vw_veteran_lineage_trace, vw_veteran_history, and vw_veteran_conflicts for details.';
END;
$$;

-- =====================================================
-- MONITORING RECOMMENDATIONS
-- =====================================================

-- Daily Tasks:
-- 1. CALL sp_pipeline_health_check();
-- 2. SELECT * FROM vw_data_freshness;
-- 3. SELECT * FROM vw_error_summary;

-- Weekly Tasks:
-- 1. SELECT * FROM vw_match_quality_report;
-- 2. SELECT * FROM vw_conflict_summary;
-- 3. SELECT * FROM vw_batch_lineage WHERE batch_date >= CURRENT_DATE - 7;

-- On-Demand (Investigation):
-- 1. SELECT * FROM vw_veteran_lineage_trace WHERE veteran_id = '<ID>';
-- 2. CALL sp_compare_source_systems('<ID>');
-- 3. SELECT * FROM vw_veteran_history WHERE veteran_id = '<ID>';
-- 4. SELECT * FROM vw_veteran_conflicts WHERE veteran_id = '<ID>';

-- =====================================================
-- ALERTS (Set up in Snowflake or external monitoring)
-- =====================================================

-- Alert 1: No successful batch in 24 hours
-- Alert 2: Error count > 100 in last hour
-- Alert 3: Failed batch detected
-- Alert 4: Low confidence matches > 100
-- Alert 5: Data staleness > 48 hours
