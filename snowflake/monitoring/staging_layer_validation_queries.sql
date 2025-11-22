-- =====================================================================================
-- STAGING LAYER VALIDATION & MONITORING QUERIES
-- =====================================================================================
-- Purpose: Validate staging layer processing and monitor data quality
-- Usage: Run these queries after each staging batch to verify results
--
-- Query Categories:
--   1. Crosswalk Validation (Are entities matching correctly?)
--   2. Data Quality Validation (Is merged data high quality?)
--   3. Conflict Detection (What conflicts occurred?)
--   4. Volume Reconciliation (Did we process all records?)
--   5. System-of-Record Validation (Are rules being applied correctly?)
--
-- Author: Data Team
-- Date: 2025-11-17
-- =====================================================================================

SET dw_database = (SELECT fn_get_dw_database());
SET ods_database = (SELECT fn_get_ods_database());

-- =====================================================================================
-- 1. CROSSWALK VALIDATION QUERIES
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 1.1: Crosswalk Match Summary
-- Purpose: High-level view of how well entities matched between OMS and VEMS
-- -----------------------------------------------------------------------------
SELECT
    'VETERANS' AS entity_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN match_method = 'SSN_EXACT_MATCH' THEN 1 ELSE 0 END) AS exact_matches,
    SUM(CASE WHEN match_method = 'SSN_OMS_ONLY' THEN 1 ELSE 0 END) AS oms_only,
    SUM(CASE WHEN match_method = 'SSN_VEMS_ONLY' THEN 1 ELSE 0 END) AS vems_only,
    ROUND(SUM(CASE WHEN match_method = 'SSN_EXACT_MATCH' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS exact_match_pct,
    ROUND(AVG(match_confidence), 2) AS avg_confidence
FROM IDENTIFIER($dw_database || '.REFERENCE.ref_entity_crosswalk_veteran')
WHERE batch_id = 'BATCH_20251117_001' -- CHANGE THIS TO YOUR BATCH ID

UNION ALL

SELECT
    'EVALUATORS' AS entity_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN match_method = 'NPI_EXACT_MATCH' THEN 1 ELSE 0 END) AS exact_matches,
    SUM(CASE WHEN match_method = 'NPI_OMS_ONLY' THEN 1 ELSE 0 END) AS oms_only,
    SUM(CASE WHEN match_method = 'NPI_VEMS_ONLY' THEN 1 ELSE 0 END) AS vems_only,
    ROUND(SUM(CASE WHEN match_method = 'NPI_EXACT_MATCH' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS exact_match_pct,
    ROUND(AVG(match_confidence), 2) AS avg_confidence
FROM IDENTIFIER($dw_database || '.REFERENCE.ref_entity_crosswalk_evaluator')
WHERE batch_id = 'BATCH_20251117_001'

UNION ALL

SELECT
    'FACILITIES' AS entity_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN match_method = 'FACILITY_ID_EXACT_MATCH' THEN 1 ELSE 0 END) AS exact_matches,
    SUM(CASE WHEN match_method = 'FACILITY_ID_OMS_ONLY' THEN 1 ELSE 0 END) AS oms_only,
    SUM(CASE WHEN match_method = 'FACILITY_ID_VEMS_ONLY' THEN 1 ELSE 0 END) AS vems_only,
    ROUND(SUM(CASE WHEN match_method = 'FACILITY_ID_EXACT_MATCH' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS exact_match_pct,
    ROUND(AVG(match_confidence), 2) AS avg_confidence
FROM IDENTIFIER($dw_database || '.REFERENCE.ref_entity_crosswalk_facility')
WHERE batch_id = 'BATCH_20251117_001';

/*
EXPECTED RESULTS:
- exact_match_pct should be 70-90% for most entities
- avg_confidence should be > 95
- If exact_match_pct is low (<50%), investigate SSN/NPI data quality in source systems
*/

-- -----------------------------------------------------------------------------
-- Query 1.2: Low Confidence Matches
-- Purpose: Identify entities with low match confidence for manual review
-- -----------------------------------------------------------------------------
SELECT
    'VETERAN' AS entity_type,
    master_veteran_id AS master_id,
    oms_veteran_id,
    vems_veteran_id,
    match_confidence,
    match_method
FROM IDENTIFIER($dw_database || '.REFERENCE.ref_entity_crosswalk_veteran')
WHERE batch_id = 'BATCH_20251117_001'
  AND match_confidence < 95  -- Flag anything not an exact match
ORDER BY match_confidence

LIMIT 100;

/*
ACTION ITEMS:
- Review records with match_confidence < 95
- If OMS_ONLY or VEMS_ONLY, verify the entity truly exists in only one system
- Consider manual matching for critical low-confidence records
*/

-- =====================================================================================
-- 2. DATA QUALITY VALIDATION QUERIES
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 2.1: Data Quality Score Distribution
-- Purpose: Understand overall quality of merged data
-- -----------------------------------------------------------------------------
SELECT
    'VETERANS' AS entity_type,
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        WHEN dq_score >= 60 THEN 'Poor (60-69)'
        ELSE 'Critical (<60)'
    END AS dq_category,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(dq_score), 2) AS avg_dq_score
FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans')
WHERE batch_id = 'BATCH_20251117_001'
GROUP BY
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        WHEN dq_score >= 60 THEN 'Poor (60-69)'
        ELSE 'Critical (<60)'
    END

UNION ALL

SELECT
    'EVALUATORS' AS entity_type,
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        WHEN dq_score >= 60 THEN 'Poor (60-69)'
        ELSE 'Critical (<60)'
    END AS dq_category,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(dq_score), 2) AS avg_dq_score
FROM IDENTIFIER($dw_database || '.STAGING.stg_evaluators')
WHERE batch_id = 'BATCH_20251117_001'
GROUP BY
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        WHEN dq_score >= 60 THEN 'Poor (60-69)'
        ELSE 'Critical (<60)'
    END

ORDER BY entity_type, avg_dq_score DESC;

/*
EXPECTED RESULTS:
- >70% of records should be in "Excellent" or "Good" categories
- <5% should be in "Critical" category
- If many records are "Critical", review dq_issues column to identify root cause
*/

-- -----------------------------------------------------------------------------
-- Query 2.2: Common Data Quality Issues
-- Purpose: Identify most frequent DQ issues
-- -----------------------------------------------------------------------------
SELECT
    'VETERANS' AS entity_type,
    TRIM(value) AS dq_issue,
    COUNT(*) AS occurrence_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans') WHERE batch_id = 'BATCH_20251117_001'), 2) AS pct_of_total
FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans'),
     LATERAL SPLIT_TO_TABLE(dq_issues, ';')
WHERE batch_id = 'BATCH_20251117_001'
  AND dq_issues IS NOT NULL
GROUP BY TRIM(value)

UNION ALL

SELECT
    'EVALUATORS' AS entity_type,
    TRIM(value) AS dq_issue,
    COUNT(*) AS occurrence_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM IDENTIFIER($dw_database || '.STAGING.stg_evaluators') WHERE batch_id = 'BATCH_20251117_001'), 2) AS pct_of_total
FROM IDENTIFIER($dw_database || '.STAGING.stg_evaluators'),
     LATERAL SPLIT_TO_TABLE(dq_issues, ';')
WHERE batch_id = 'BATCH_20251117_001'
  AND dq_issues IS NOT NULL
GROUP BY TRIM(value)

ORDER BY entity_type, occurrence_count DESC
LIMIT 50;

/*
ACTION ITEMS:
- Focus on top 3-5 most common issues
- For "Missing <field>" issues, investigate source system data
- For "Invalid <field>" issues, review validation rules
*/

-- -----------------------------------------------------------------------------
-- Query 2.3: Records with Critical Data Quality Issues
-- Purpose: List specific records needing manual review
-- -----------------------------------------------------------------------------
SELECT
    master_veteran_id,
    veteran_ssn,
    first_name,
    last_name,
    dq_score,
    dq_issues,
    source_system,
    match_confidence
FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans')
WHERE batch_id = 'BATCH_20251117_001'
  AND dq_score < 60  -- Critical quality threshold
ORDER BY dq_score
LIMIT 100;

/*
USE THIS FOR:
- Manual data cleanup
- Identifying source system data quality problems
- Prioritizing data stewardship efforts
*/

-- =====================================================================================
-- 3. CONFLICT DETECTION & RESOLUTION VALIDATION
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 3.1: Conflict Summary
-- Purpose: Overview of conflicts detected and resolved
-- -----------------------------------------------------------------------------
SELECT
    entity_type,
    conflict_type,
    COUNT(*) AS conflict_count,
    COUNT(DISTINCT entity_id) AS affected_entities,
    resolution_method,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY entity_type), 2) AS pct_of_entity_conflicts
FROM IDENTIFIER($dw_database || '.REFERENCE.ref_reconciliation_log')
WHERE batch_id = 'BATCH_20251117_001'
GROUP BY entity_type, conflict_type, resolution_method
ORDER BY entity_type, conflict_count DESC;

/*
EXPECTED CONFLICTS:
- DISABILITY_RATING_MISMATCH: Common, OMS is authoritative
- DOB_MISMATCH: Rare, investigate if >1%
- Any new conflict types should be investigated
*/

-- -----------------------------------------------------------------------------
-- Query 3.2: Detailed Conflict Log
-- Purpose: Review specific conflicts for validation
-- -----------------------------------------------------------------------------
SELECT
    entity_type,
    entity_id,
    conflict_type,
    oms_value,
    vems_value,
    resolved_value,
    resolution_method,
    resolution_timestamp
FROM IDENTIFIER($dw_database || '.REFERENCE.ref_reconciliation_log')
WHERE batch_id = 'BATCH_20251117_001'
ORDER BY entity_type, conflict_type, entity_id
LIMIT 100;

/*
VALIDATION CHECKS:
- Verify resolved_value matches expected system-of-record
- For DISABILITY_RATING_MISMATCH, resolved_value should = oms_value
- For DOB_MISMATCH, investigate why dates differ (data entry error?)
*/

-- -----------------------------------------------------------------------------
-- Query 3.3: Conflicted Veterans Detail
-- Purpose: See full veteran records that had conflicts
-- -----------------------------------------------------------------------------
SELECT
    v.master_veteran_id,
    v.first_name,
    v.last_name,
    v.disability_rating,
    v.conflict_type,
    v.oms_value AS oms_disability_rating,
    v.vems_value AS vems_disability_rating,
    v.resolution_method,
    v.dq_score
FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans') v
WHERE v.batch_id = 'BATCH_20251117_001'
  AND v.conflict_type IS NOT NULL
ORDER BY v.conflict_type, v.master_veteran_id
LIMIT 100;

-- =====================================================================================
-- 4. VOLUME RECONCILIATION QUERIES
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 4.1: Source to Staging Volume Reconciliation
-- Purpose: Verify all ODS records made it to staging
-- -----------------------------------------------------------------------------
WITH ods_counts AS (
    SELECT
        'VETERANS' AS entity_type,
        source_system,
        COUNT(*) AS ods_count
    FROM IDENTIFIER($ods_database || '.ODS.ods_veterans_source')
    WHERE batch_id = 'BATCH_20251117_001'
    GROUP BY source_system

    UNION ALL

    SELECT
        'EVALUATORS' AS entity_type,
        source_system,
        COUNT(*) AS ods_count
    FROM IDENTIFIER($ods_database || '.ODS.ods_evaluators_source')
    WHERE batch_id = 'BATCH_20251117_001'
    GROUP BY source_system

    UNION ALL

    SELECT
        'FACILITIES' AS entity_type,
        source_system,
        COUNT(*) AS ods_count
    FROM IDENTIFIER($ods_database || '.ODS.ods_facilities_source')
    WHERE batch_id = 'BATCH_20251117_001'
    GROUP BY source_system
),
staging_counts AS (
    SELECT
        'VETERANS' AS entity_type,
        source_system,
        COUNT(*) AS staging_count
    FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans')
    WHERE batch_id = 'BATCH_20251117_001'
    GROUP BY source_system

    UNION ALL

    SELECT
        'EVALUATORS' AS entity_type,
        source_system,
        COUNT(*) AS staging_count
    FROM IDENTIFIER($dw_database || '.STAGING.stg_evaluators')
    WHERE batch_id = 'BATCH_20251117_001'
    GROUP BY source_system

    UNION ALL

    SELECT
        'FACILITIES' AS entity_type,
        source_system,
        COUNT(*) AS staging_count
    FROM IDENTIFIER($dw_database || '.STAGING.stg_facilities')
    WHERE batch_id = 'BATCH_20251117_001'
    GROUP BY source_system
)
SELECT
    o.entity_type,
    o.source_system,
    o.ods_count,
    COALESCE(s.staging_count, 0) AS staging_count,
    o.ods_count - COALESCE(s.staging_count, 0) AS records_lost,
    CASE
        WHEN o.ods_count = COALESCE(s.staging_count, 0) THEN '✓ PASS'
        ELSE '✗ FAIL - Records missing'
    END AS validation_status
FROM ods_counts o
LEFT JOIN staging_counts s
    ON o.entity_type = s.entity_type
    AND o.source_system = s.source_system
ORDER BY o.entity_type, o.source_system;

/*
EXPECTED RESULTS:
- For OMS_VEMS_MERGED: staging_count may be less than sum of OMS + VEMS (de-duplication)
- For individual systems: ods_count should equal staging_count
- records_lost should be 0 or minimal (<1%)
*/

-- -----------------------------------------------------------------------------
-- Query 4.2: Staging Layer Record Counts by Source System
-- Purpose: Summary of merged records
-- -----------------------------------------------------------------------------
SELECT
    'VETERANS' AS entity_type,
    source_system,
    COUNT(*) AS record_count,
    ROUND(AVG(match_confidence), 2) AS avg_match_confidence,
    ROUND(AVG(dq_score), 2) AS avg_dq_score
FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans')
WHERE batch_id = 'BATCH_20251117_001'
GROUP BY source_system

UNION ALL

SELECT
    'EVALUATORS' AS entity_type,
    source_system,
    COUNT(*) AS record_count,
    ROUND(AVG(match_confidence), 2) AS avg_match_confidence,
    ROUND(AVG(dq_score), 2) AS avg_dq_score
FROM IDENTIFIER($dw_database || '.STAGING.stg_evaluators')
WHERE batch_id = 'BATCH_20251117_001'
GROUP BY source_system

UNION ALL

SELECT
    'FACILITIES' AS entity_type,
    source_system,
    COUNT(*) AS record_count,
    ROUND(AVG(match_confidence), 2) AS avg_match_confidence,
    ROUND(AVG(dq_score), 2) AS avg_dq_score
FROM IDENTIFIER($dw_database || '.STAGING.stg_facilities')
WHERE batch_id = 'BATCH_20251117_001'
GROUP BY source_system

ORDER BY entity_type, source_system;

-- =====================================================================================
-- 5. SYSTEM-OF-RECORD VALIDATION QUERIES
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 5.1: Verify System-of-Record Rules Applied Correctly
-- Purpose: Validate that OMS/VEMS data is merged per configured rules
-- -----------------------------------------------------------------------------

-- Veterans: OMS should be system-of-record
-- Check: When both OMS and VEMS have a disability_rating, OMS value should win
WITH conflict_check AS (
    SELECT
        v.master_veteran_id,
        v.disability_rating AS staging_value,
        oms.disability_rating AS oms_value,
        vems.disability_rating AS vems_value,
        CASE
            WHEN oms.disability_rating IS NOT NULL THEN oms.disability_rating
            ELSE vems.disability_rating
        END AS expected_value
    FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans') v
    LEFT JOIN IDENTIFIER($ods_database || '.ODS.ods_veterans_source') oms
        ON v.oms_veteran_id = oms.source_record_id AND oms.source_system = 'OMS'
    LEFT JOIN IDENTIFIER($ods_database || '.ODS.ods_veterans_source') vems
        ON v.vems_veteran_id = vems.source_record_id AND vems.source_system = 'VEMS'
    WHERE v.batch_id = 'BATCH_20251117_001'
      AND oms.disability_rating IS NOT NULL
      AND vems.disability_rating IS NOT NULL
)
SELECT
    COUNT(*) AS total_conflicts,
    SUM(CASE WHEN staging_value = expected_value THEN 1 ELSE 0 END) AS correctly_resolved,
    SUM(CASE WHEN staging_value != expected_value THEN 1 ELSE 0 END) AS incorrectly_resolved,
    CASE
        WHEN SUM(CASE WHEN staging_value != expected_value THEN 1 ELSE 0 END) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - System-of-record rules not applied correctly'
    END AS validation_status
FROM conflict_check;

/*
EXPECTED: incorrectly_resolved should be 0
IF FAIL: Review sp_merge_veterans_to_staging procedure logic
*/

-- Evaluators: VEMS should be system-of-record
-- Check: When both OMS and VEMS have a phone, VEMS value should win
WITH evaluator_check AS (
    SELECT
        e.master_evaluator_id,
        e.phone AS staging_value,
        oms.phone AS oms_value,
        vems.phone AS vems_value,
        CASE
            WHEN vems.phone IS NOT NULL THEN vems.phone
            ELSE oms.phone
        END AS expected_value
    FROM IDENTIFIER($dw_database || '.STAGING.stg_evaluators') e
    LEFT JOIN IDENTIFIER($ods_database || '.ODS.ods_evaluators_source') oms
        ON e.oms_evaluator_id = oms.source_record_id AND oms.source_system = 'OMS'
    LEFT JOIN IDENTIFIER($ods_database || '.ODS.ods_evaluators_source') vems
        ON e.vems_evaluator_id = vems.source_record_id AND vems.source_system = 'VEMS'
    WHERE e.batch_id = 'BATCH_20251117_001'
      AND oms.phone IS NOT NULL
      AND vems.phone IS NOT NULL
)
SELECT
    COUNT(*) AS total_conflicts,
    SUM(CASE WHEN staging_value = expected_value THEN 1 ELSE 0 END) AS correctly_resolved,
    SUM(CASE WHEN staging_value != expected_value THEN 1 ELSE 0 END) AS incorrectly_resolved,
    CASE
        WHEN SUM(CASE WHEN staging_value != expected_value THEN 1 ELSE 0 END) = 0 THEN '✓ PASS'
        ELSE '✗ FAIL - System-of-record rules not applied correctly'
    END AS validation_status
FROM evaluator_check;

-- =====================================================================================
-- MASTER VALIDATION DASHBOARD
-- =====================================================================================

-- -----------------------------------------------------------------------------
-- Query 6: Master Validation Dashboard (Run this first!)
-- Purpose: One-stop view of overall staging layer health
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_staging_validation_dashboard AS
WITH crosswalk_stats AS (
    SELECT
        'Veteran Crosswalk' AS check_name,
        COUNT(*) AS value,
        'crosswalks built' AS unit,
        CASE WHEN COUNT(*) > 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
    FROM IDENTIFIER($dw_database || '.REFERENCE.ref_entity_crosswalk_veteran')
    WHERE batch_id = (SELECT MAX(batch_id) FROM IDENTIFIER($dw_database || '.REFERENCE.ref_entity_crosswalk_veteran'))
),
staging_dq AS (
    SELECT
        'Veteran Data Quality' AS check_name,
        ROUND(AVG(dq_score), 2) AS value,
        'avg DQ score' AS unit,
        CASE WHEN AVG(dq_score) >= 80 THEN '✓ PASS' ELSE '✗ WARN' END AS status
    FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans')
    WHERE batch_id = (SELECT MAX(batch_id) FROM IDENTIFIER($dw_database || '.STAGING.stg_veterans'))
),
conflict_stats AS (
    SELECT
        'Conflicts Detected' AS check_name,
        COUNT(*) AS value,
        'conflicts logged' AS unit,
        CASE WHEN COUNT(*) < 1000 THEN '✓ PASS' ELSE '⚠ REVIEW' END AS status
    FROM IDENTIFIER($dw_database || '.REFERENCE.ref_reconciliation_log')
    WHERE batch_id = (SELECT MAX(batch_id) FROM IDENTIFIER($dw_database || '.REFERENCE.ref_reconciliation_log'))
)
SELECT * FROM crosswalk_stats
UNION ALL
SELECT * FROM staging_dq
UNION ALL
SELECT * FROM conflict_stats;

-- Run the dashboard
SELECT * FROM vw_staging_validation_dashboard;

-- =====================================================================================
-- TROUBLESHOOTING GUIDE
-- =====================================================================================

/*
COMMON ISSUES AND SOLUTIONS:

1. LOW MATCH RATES (<50%):
   - Check that SSN/NPI fields are populated in ODS
   - Verify batch_id is consistent across all procedures
   - Run: SELECT COUNT(*), source_system FROM ods_veterans_source GROUP BY source_system

2. HIGH CONFLICT RATES (>10%):
   - Review ref_system_of_record table for correct rules
   - Check if OMS/VEMS have different business logic
   - May be expected if systems are out of sync

3. LOW DQ SCORES (<70 avg):
   - Identify common issues with Query 2.2
   - Coordinate with source system teams to improve data
   - Adjust DQ scoring weights if needed

4. RECORDS LOST (ods_count != staging_count):
   - Check for NULL join keys (SSN, NPI, Facility ID)
   - Verify crosswalk procedures ran successfully
   - Review error logs in stored procedures

5. SYSTEM-OF-RECORD RULES NOT APPLIED:
   - Review COALESCE logic in merge procedures
   - Verify ref_system_of_record table is populated
   - Check that correct source_system values are in ODS

VALIDATION CHECKLIST:
□ Query 1.1: Crosswalk match rates >70%
□ Query 2.1: DQ scores >80% in "Excellent/Good"
□ Query 3.1: Conflicts logged and resolved
□ Query 4.1: No records lost (PASS status)
□ Query 5.1: System-of-record rules applied (PASS status)
□ Query 6: Master dashboard shows all green

*/
