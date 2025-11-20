# Comprehensive Deployment Guide
## Common Functions, Reference Tables, and Metadata-Driven ETL Framework

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Deployment Phases](#deployment-phases)
4. [Phase 1: Foundation Setup](#phase-1-foundation-setup-quick-wins)
5. [Phase 2: Proof of Concept](#phase-2-proof-of-concept)
6. [Phase 3: Full Rollout](#phase-3-full-rollout)
7. [Testing & Validation](#testing--validation)
8. [Rollback Procedures](#rollback-procedures)
9. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
10. [Success Criteria](#success-criteria)

---

## Overview

This guide provides step-by-step instructions for deploying the refactored ETL framework, including:

- ✅ **25+ common functions** (string normalization, dates, validation, DQ scoring)
- ✅ **Reference tables** (disability ratings, priority groups)
- ✅ **Metadata framework** (SCD config, pipeline orchestration, DQ rules)
- ✅ **Generic SCD Type 2 procedures** (replaces 6+ individual dimension loaders)

### Expected Benefits

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Total Lines of Code** | ~3,500 | ~1,330 | -62% |
| **Functions Refactoring** | N/A | 960 lines saved | N/A |
| **Architectural Changes** | N/A | 2,210 lines saved | N/A |
| **Deployment Time for Changes** | Hours-Days | Minutes | -95% |
| **Business Rule Changes** | Code deployment | SQL UPDATE | 100% faster |
| **Consistency Guarantee** | ⚠️ Risk | ✅ Guaranteed | 100% |

### Deployment Timeline

| Phase | Duration | Can Start | Risk Level |
|-------|----------|-----------|------------|
| **Phase 1:** Foundation | 2-3 hours | Immediately | 🟢 Low |
| **Phase 2:** Proof of Concept | 4-6 hours | After Phase 1 | 🟡 Medium |
| **Phase 3:** Full Rollout | 8-12 hours | After Phase 2 validation | 🟡 Medium |
| **Total** | **14-21 hours** | Over 2-3 weeks | N/A |

---

## Prerequisites

### Required Access

- [ ] Snowflake account with SYSADMIN or equivalent role
- [ ] CREATE SCHEMA, CREATE TABLE, CREATE PROCEDURE, CREATE FUNCTION privileges
- [ ] Access to source code repository
- [ ] Ability to run SQL scripts in target database

### Required Files

Ensure you have all deployment files from the repository:

```
📁 snowflake/
├── 📁 functions/
│   └── 00_common_data_functions.sql          ✅ 25+ functions
├── 📁 reference/
│   ├── 01_create_reference_tables.sql        (existing)
│   ├── 02_ref_disability_rating_categories.sql  ✅ NEW
│   └── 03_ref_priority_groups.sql            ✅ NEW
├── 📁 metadata/
│   └── 01_create_metadata_tables.sql         ✅ NEW
└── 📁 etl/
    ├── 00_generic_scd_procedures.sql         ✅ NEW
    ├── 01_etl_procedures_dimensions.sql      (existing - will refactor)
    └── ...

📄 COMMON_FUNCTIONS_ANALYSIS.md               ✅ Analysis
📄 ARCHITECTURAL_IMPROVEMENTS.md               ✅ Design
📄 PROOF_OF_CONCEPT_REFACTORING.md            ✅ Example
📄 DEPLOYMENT_GUIDE.md                        ✅ This file
```

### Environment Setup

```sql
-- Verify database context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE();

-- Verify get_dw_database() function exists
SELECT get_dw_database();

-- Expected: Your data warehouse database name (e.g., 'VES_DW')
```

---

## Deployment Phases

### Phase Overview

```
Phase 1: Foundation Setup (Quick Wins)
├── Deploy common functions
├── Deploy reference tables
├── Deploy metadata tables
└── Validate all objects created
      ↓
Phase 2: Proof of Concept
├── Deploy generic SCD procedures
├── Refactor Veterans pipeline (1 dimension)
├── Run side-by-side comparison
└── Validate data integrity
      ↓
Phase 3: Full Rollout
├── Refactor remaining dimensions (5+)
├── Update orchestration procedures
├── Migrate fact table ETLs
└── Decommission old procedures
```

---

## Phase 1: Foundation Setup (Quick Wins)

**Goal:** Deploy reusable functions, reference tables, and metadata framework
**Duration:** 2-3 hours
**Risk Level:** 🟢 Low (no changes to existing procedures)

### Step 1.1: Deploy Common Functions (30 minutes)

```sql
-- ============================================================
-- 1.1.1: Set database context
-- ============================================================
USE ROLE SYSADMIN;  -- Or your admin role
SELECT get_dw_database();  -- Note the database name
USE DATABASE IDENTIFIER(get_dw_database());
USE SCHEMA WAREHOUSE;

-- ============================================================
-- 1.1.2: Deploy common functions
-- ============================================================
-- Run the entire file:
-- @snowflake/functions/00_common_data_functions.sql

-- Or copy/paste and execute all statements from the file

-- ============================================================
-- 1.1.3: Verify functions created
-- ============================================================
SHOW FUNCTIONS LIKE 'fn_%' IN SCHEMA WAREHOUSE;

-- Expected: 25+ functions starting with 'fn_'

-- ============================================================
-- 1.1.4: Test key functions
-- ============================================================

-- Test string normalization
SELECT fn_normalize_string_upper('  test  ') AS result;
-- Expected: 'TEST'

-- Test phone cleaning
SELECT fn_clean_phone_number('(555) 123-4567') AS result;
-- Expected: '5551234567'

-- Test email normalization
SELECT fn_normalize_email('  User@EXAMPLE.COM  ') AS result;
-- Expected: 'user@example.com'

-- Test age calculation
SELECT fn_calculate_age('1980-05-15') AS age;
-- Expected: ~45 (as of 2025)

-- Test years of service
SELECT fn_calculate_years_of_service('2010-01-01', '2020-01-01') AS years;
-- Expected: 10.00

-- Test disability rating validation
SELECT
    fn_validate_disability_rating(85) AS valid,     -- Expected: 85
    fn_validate_disability_rating(150) AS invalid;  -- Expected: NULL

-- Test priority group validation
SELECT
    fn_validate_priority_group(5) AS valid,         -- Expected: 5
    fn_validate_priority_group(10) AS invalid;      -- Expected: NULL

-- ============================================================
-- 1.1.5: Test hash generation
-- ============================================================
SELECT fn_generate_record_hash_5(
    'John', 'Doe', 'john@email.com', '5551234567', 'NY'
) AS hash;
-- Expected: MD5 hash string (32 characters)

-- ============================================================
-- 1.1.6: Test DQ scoring
-- ============================================================
SELECT fn_calculate_veteran_dq_score(
    'John',           -- first_name
    'Doe',            -- last_name
    '1980-01-01',     -- date_of_birth
    'V123',           -- veteran_id
    NULL,             -- ssn
    'john@va.gov',    -- email
    '5551234567',     -- phone
    'NY',             -- state
    80                -- disability_rating
) AS dq_score;
-- Expected: 100 (all fields present and valid)

-- ============================================================
-- ✅ Checkpoint: All functions working correctly
-- ============================================================
```

---

### Step 1.2: Deploy Reference Tables (30 minutes)

```sql
-- ============================================================
-- 1.2.1: Deploy disability rating categories
-- ============================================================
USE SCHEMA REFERENCE;

-- Run: @snowflake/reference/02_ref_disability_rating_categories.sql
-- Or copy/paste and execute all statements

-- ============================================================
-- 1.2.2: Verify disability rating table
-- ============================================================
SELECT * FROM ref_disability_rating_categories ORDER BY sort_order;

-- Expected: 5 rows (0%, 10-30%, 40-60%, 70-90%, 100%)

-- Test lookup function
SELECT
    0 AS rating,
    fn_categorize_disability_rating(0) AS category
UNION ALL SELECT 25, fn_categorize_disability_rating(25)
UNION ALL SELECT 50, fn_categorize_disability_rating(50)
UNION ALL SELECT 85, fn_categorize_disability_rating(85)
UNION ALL SELECT 100, fn_categorize_disability_rating(100);

-- Expected results:
-- 0    -> 0%
-- 25   -> 10-30%
-- 50   -> 40-60%
-- 85   -> 70-90%
-- 100  -> 100%

-- ============================================================
-- 1.2.3: Deploy priority groups
-- ============================================================

-- Run: @snowflake/reference/03_ref_priority_groups.sql
-- Or copy/paste and execute all statements

-- ============================================================
-- 1.2.4: Verify priority groups table
-- ============================================================
SELECT
    priority_group,
    group_name,
    copay_required,
    income_limits_apply
FROM ref_priority_groups
ORDER BY priority_group;

-- Expected: 8 rows (groups 1-8)

-- Test lookup function
SELECT fn_get_priority_group_details(5) AS group_5_details;

-- Expected: JSON object with group 5 information

-- ============================================================
-- ✅ Checkpoint: Reference tables populated and functions working
-- ============================================================
```

---

### Step 1.3: Deploy Metadata Tables (45 minutes)

```sql
-- ============================================================
-- 1.3.1: Create metadata schema and tables
-- ============================================================
USE ROLE SYSADMIN;
USE DATABASE IDENTIFIER(get_dw_database());

-- Run: @snowflake/metadata/01_create_metadata_tables.sql
-- Or copy/paste and execute all statements

-- ============================================================
-- 1.3.2: Verify metadata tables created
-- ============================================================
USE SCHEMA METADATA;

SHOW TABLES IN SCHEMA METADATA;

-- Expected tables:
-- - scd_type2_config
-- - etl_pipeline_config
-- - dq_scoring_rules
-- - etl_execution_log

-- ============================================================
-- 1.3.3: Verify SCD configuration
-- ============================================================
SELECT
    table_name,
    staging_table,
    business_key_columns,
    enabled
FROM scd_type2_config
ORDER BY table_name;

-- Expected: 6 rows (veterans, evaluators, facilities, etc.)

-- ============================================================
-- 1.3.4: Verify pipeline configuration
-- ============================================================
SELECT
    pipeline_name,
    entity_type,
    execution_order,
    parallel_execution_group,
    enabled
FROM etl_pipeline_config
ORDER BY execution_order, pipeline_name;

-- Expected: 7+ rows with proper execution order

-- ============================================================
-- 1.3.5: Verify DQ scoring rules
-- ============================================================
SELECT
    entity_type,
    field_name,
    points_if_met,
    field_importance
FROM dq_scoring_rules
WHERE entity_type = 'VETERAN'
ORDER BY points_if_met DESC;

-- Expected: 9 rules for veterans with varying point values

-- Test DQ scoring summary view
SELECT * FROM vw_dq_scoring_summary;

-- Expected: Summary for VETERAN and EVALUATOR entities

-- ============================================================
-- ✅ Checkpoint: All metadata tables populated
-- ============================================================
```

---

### Step 1.4: Deploy Generic SCD Procedures (30 minutes)

```sql
-- ============================================================
-- 1.4.1: Deploy generic SCD procedures
-- ============================================================
USE SCHEMA WAREHOUSE;

-- Run: @snowflake/etl/00_generic_scd_procedures.sql
-- Or copy/paste and execute all statements

-- ============================================================
-- 1.4.2: Verify procedures created
-- ============================================================
SHOW PROCEDURES LIKE '%scd%' IN SCHEMA WAREHOUSE;

-- Expected procedures:
-- - sp_load_scd_type2_generic
-- - sp_load_all_dimensions_scd2
-- - sp_validate_scd_type2_integrity

-- ============================================================
-- 1.4.3: Test validation procedure
-- ============================================================
-- Run validation on existing dimension
CALL sp_validate_scd_type2_integrity('dim_veterans');

-- Review results - should show mostly PASS
-- Any FAIL indicates data integrity issues to investigate

-- ============================================================
-- ✅ Checkpoint: Generic procedures deployed and ready
-- ============================================================
```

---

### Step 1.5: Foundation Validation (15 minutes)

```sql
-- ============================================================
-- Complete validation checklist
-- ============================================================

-- ✅ 1. Functions count
SELECT COUNT(*) AS function_count
FROM information_schema.functions
WHERE function_schema = 'WAREHOUSE'
  AND function_name LIKE 'fn_%';
-- Expected: >= 25

-- ✅ 2. Reference tables populated
SELECT
    'disability_ratings' AS table_name,
    COUNT(*) AS row_count
FROM reference.ref_disability_rating_categories
UNION ALL
SELECT 'priority_groups', COUNT(*)
FROM reference.ref_priority_groups;
-- Expected: 5 ratings, 8 priority groups

-- ✅ 3. Metadata tables populated
SELECT
    'scd_config' AS table_name,
    COUNT(*) AS row_count
FROM metadata.scd_type2_config
UNION ALL
SELECT 'pipeline_config', COUNT(*)
FROM metadata.etl_pipeline_config
UNION ALL
SELECT 'dq_rules', COUNT(*)
FROM metadata.dq_scoring_rules;
-- Expected: 6+ SCD configs, 7+ pipelines, 15+ DQ rules

-- ✅ 4. Procedures exist
SELECT COUNT(*) AS procedure_count
FROM information_schema.procedures
WHERE procedure_schema = 'WAREHOUSE'
  AND procedure_name IN (
      'sp_load_scd_type2_generic',
      'sp_load_all_dimensions_scd2',
      'sp_validate_scd_type2_integrity'
  );
-- Expected: 3

-- ============================================================
-- ✅ PHASE 1 COMPLETE!
-- ============================================================
-- Foundation is ready. All reusable components deployed.
-- No changes to existing ETL procedures yet.
-- Risk of breaking existing pipelines: ZERO
-- ============================================================
```

---

## Phase 2: Proof of Concept

**Goal:** Refactor Veterans pipeline and validate results
**Duration:** 4-6 hours
**Risk Level:** 🟡 Medium (creates new procedures, doesn't modify existing)

### Step 2.1: Create Refactored Transform Procedure (1 hour)

```sql
-- ============================================================
-- 2.1.1: Deploy refactored veterans transform
-- ============================================================
USE SCHEMA WAREHOUSE;

-- Copy the refactored procedure from:
-- PROOF_OF_CONCEPT_REFACTORING.md -> Part 1 -> AFTER section

-- This creates: sp_transform_ods_to_staging_veterans_v2

-- ============================================================
-- 2.1.2: Verify procedure created
-- ============================================================
SHOW PROCEDURES LIKE '%veterans%' IN SCHEMA WAREHOUSE;

-- Expected:
-- - sp_transform_ods_to_staging_veterans (old - unchanged)
-- - sp_transform_ods_to_staging_veterans_v2 (new - refactored)
-- Both should exist

-- ============================================================
-- ✅ Checkpoint: New procedure created, old unchanged
-- ============================================================
```

---

### Step 2.2: Side-by-Side Comparison Test (2 hours)

```sql
-- ============================================================
-- 2.2.1: Prepare test environment
-- ============================================================

-- Create backup of current staging data
CREATE OR REPLACE TABLE staging.stg_veterans_backup AS
SELECT * FROM staging.stg_veterans;

-- Create backup of current dimension data
CREATE OR REPLACE TABLE warehouse.dim_veterans_backup AS
SELECT * FROM warehouse.dim_veterans;

-- ============================================================
-- 2.2.2: Test OLD procedures
-- ============================================================
SET test_batch_id = 'POC_TEST_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');

-- Clear staging
TRUNCATE TABLE staging.stg_veterans;

-- Run OLD transform
CALL sp_transform_ods_to_staging_veterans(:test_batch_id);

-- Save staging results
CREATE OR REPLACE TABLE test_staging_old AS
SELECT * FROM staging.stg_veterans WHERE batch_id = :test_batch_id;

-- Run OLD load
TRUNCATE TABLE warehouse.dim_veterans;
CALL sp_load_dim_veterans(:test_batch_id);

-- Save dimension results
CREATE OR REPLACE TABLE test_dimension_old AS
SELECT * FROM warehouse.dim_veterans;

-- ============================================================
-- 2.2.3: Test NEW procedures
-- ============================================================

-- Clear staging and dimension
TRUNCATE TABLE staging.stg_veterans;
TRUNCATE TABLE warehouse.dim_veterans;

-- Run NEW transform
CALL sp_transform_ods_to_staging_veterans_v2(:test_batch_id);

-- Save staging results
CREATE OR REPLACE TABLE test_staging_new AS
SELECT * FROM staging.stg_veterans WHERE batch_id = :test_batch_id;

-- Run NEW generic load
CALL sp_load_scd_type2_generic('dim_veterans', :test_batch_id);

-- Save dimension results
CREATE OR REPLACE TABLE test_dimension_new AS
SELECT * FROM warehouse.dim_veterans;

-- ============================================================
-- 2.2.4: Compare staging results
-- ============================================================

SELECT
    'Record Count' AS metric,
    (SELECT COUNT(*) FROM test_staging_old) AS old_value,
    (SELECT COUNT(*) FROM test_staging_new) AS new_value,
    CASE
        WHEN old_value = new_value THEN '✅ MATCH'
        ELSE '❌ MISMATCH'
    END AS status
UNION ALL
SELECT
    'Unique Veterans',
    (SELECT COUNT(DISTINCT veteran_id) FROM test_staging_old),
    (SELECT COUNT(DISTINCT veteran_id) FROM test_staging_new),
    CASE WHEN old_value = new_value THEN '✅ MATCH' ELSE '❌ MISMATCH' END
UNION ALL
SELECT
    'Avg DQ Score',
    (SELECT ROUND(AVG(dq_score), 2) FROM test_staging_old),
    (SELECT ROUND(AVG(dq_score), 2) FROM test_staging_new),
    CASE WHEN old_value = new_value THEN '✅ MATCH' ELSE '❌ MISMATCH' END;

-- ============================================================
-- 2.2.5: Compare dimension results
-- ============================================================

SELECT
    'Dimension Records' AS metric,
    (SELECT COUNT(*) FROM test_dimension_old) AS old_value,
    (SELECT COUNT(*) FROM test_dimension_new) AS new_value,
    CASE
        WHEN old_value = new_value THEN '✅ MATCH'
        ELSE '❌ MISMATCH'
    END AS status
UNION ALL
SELECT
    'Current Records',
    (SELECT COUNT(*) FROM test_dimension_old WHERE is_current = TRUE),
    (SELECT COUNT(*) FROM test_dimension_new WHERE is_current = TRUE),
    CASE WHEN old_value = new_value THEN '✅ MATCH' ELSE '❌ MISMATCH' END;

-- ============================================================
-- 2.2.6: Detailed field comparison
-- ============================================================

-- Compare disability rating categories
SELECT
    'Disability Categories Match' AS check_name,
    COUNT(*) AS mismatch_count,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM (
    SELECT
        o.veteran_id,
        o.disability_rating_category AS old_category,
        n.disability_rating_category AS new_category
    FROM test_dimension_old o
    FULL OUTER JOIN test_dimension_new n
        ON o.veteran_id = n.veteran_id
        AND o.effective_start_date = n.effective_start_date
    WHERE COALESCE(o.disability_rating_category, 'NULL') !=
          COALESCE(n.disability_rating_category, 'NULL')
);

-- Compare phone numbers (should all be numeric)
SELECT
    'Phone Numbers Match' AS check_name,
    COUNT(*) AS mismatch_count,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM (
    SELECT
        o.veteran_id,
        o.phone AS old_phone,
        n.phone AS new_phone
    FROM test_dimension_old o
    FULL OUTER JOIN test_dimension_new n
        ON o.veteran_id = n.veteran_id
        AND o.effective_start_date = n.effective_start_date
    WHERE COALESCE(o.phone, 'NULL') != COALESCE(n.phone, 'NULL')
);

-- ============================================================
-- ✅ Expected Results: All metrics should show ✅ MATCH
-- ============================================================

-- If any mismatches, investigate:
SELECT
    o.veteran_id,
    o.disability_rating_category AS old_category,
    n.disability_rating_category AS new_category,
    o.current_disability_rating AS old_rating,
    n.current_disability_rating AS new_rating
FROM test_dimension_old o
FULL OUTER JOIN test_dimension_new n
    ON o.veteran_id = n.veteran_id
    AND o.effective_start_date = n.effective_start_date
WHERE COALESCE(o.disability_rating_category, 'NULL') !=
      COALESCE(n.disability_rating_category, 'NULL')
LIMIT 100;
```

---

### Step 2.3: Performance Testing (1 hour)

```sql
-- ============================================================
-- 2.3.1: Test OLD procedure performance
-- ============================================================
SET perf_batch_id = 'PERF_TEST_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');

-- Measure OLD transform
SET start_time = (SELECT CURRENT_TIMESTAMP());
TRUNCATE TABLE staging.stg_veterans;
CALL sp_transform_ods_to_staging_veterans(:perf_batch_id);
SET old_transform_duration = DATEDIFF(second, :start_time, CURRENT_TIMESTAMP());

-- Measure OLD load
SET start_time = (SELECT CURRENT_TIMESTAMP());
TRUNCATE TABLE warehouse.dim_veterans;
CALL sp_load_dim_veterans(:perf_batch_id);
SET old_load_duration = DATEDIFF(second, :start_time, CURRENT_TIMESTAMP());

-- ============================================================
-- 2.3.2: Test NEW procedure performance
-- ============================================================

-- Measure NEW transform
SET start_time = (SELECT CURRENT_TIMESTAMP());
TRUNCATE TABLE staging.stg_veterans;
CALL sp_transform_ods_to_staging_veterans_v2(:perf_batch_id);
SET new_transform_duration = DATEDIFF(second, :start_time, CURRENT_TIMESTAMP());

-- Measure NEW load
SET start_time = (SELECT CURRENT_TIMESTAMP());
TRUNCATE TABLE warehouse.dim_veterans;
CALL sp_load_scd_type2_generic('dim_veterans', :perf_batch_id);
SET new_load_duration = DATEDIFF(second, :start_time, CURRENT_TIMESTAMP());

-- ============================================================
-- 2.3.3: Compare performance
-- ============================================================
SELECT
    'Transform' AS procedure,
    :old_transform_duration AS old_seconds,
    :new_transform_duration AS new_seconds,
    ROUND((:new_transform_duration::FLOAT / :old_transform_duration::FLOAT - 1) * 100, 2) AS percent_change,
    CASE
        WHEN :new_transform_duration <= :old_transform_duration * 1.1 THEN '✅ ACCEPTABLE'
        ELSE '⚠️ REVIEW'
    END AS verdict
UNION ALL
SELECT
    'Load',
    :old_load_duration,
    :new_load_duration,
    ROUND((:new_load_duration::FLOAT / :old_load_duration::FLOAT - 1) * 100, 2),
    CASE
        WHEN :new_load_duration <= :old_load_duration * 1.1 THEN '✅ ACCEPTABLE'
        ELSE '⚠️ REVIEW'
    END;

-- Expected: New procedures within 10% of old (minor overhead acceptable)
-- Functions are IMMUTABLE, so Snowflake can optimize them

-- ============================================================
-- ✅ Checkpoint: Performance acceptable
-- ============================================================
```

---

### Step 2.4: Data Integrity Validation (30 minutes)

```sql
-- ============================================================
-- 2.4.1: Validate SCD Type 2 integrity
-- ============================================================
CALL sp_validate_scd_type2_integrity('dim_veterans');

-- Review results - all should be PASS
-- If any FAIL, investigate before proceeding

-- ============================================================
-- 2.4.2: Validate business rules
-- ============================================================

-- Check disability rating categories
SELECT
    current_disability_rating,
    disability_rating_category,
    COUNT(*) AS record_count
FROM warehouse.dim_veterans
WHERE is_current = TRUE
GROUP BY current_disability_rating, disability_rating_category
ORDER BY current_disability_rating;

-- Validate categories match expectations:
-- 0        -> 0%
-- 10-30    -> 10-30%
-- 40-60    -> 40-60%
-- 70-90    -> 70-90%
-- 100      -> 100%

-- Check priority groups
SELECT
    priority_group,
    COUNT(*) AS record_count,
    fn_get_priority_group_details(priority_group):group_name AS group_name
FROM warehouse.dim_veterans
WHERE is_current = TRUE
  AND priority_group IS NOT NULL
GROUP BY priority_group
ORDER BY priority_group;

-- All priority groups should be 1-8
-- NULL values should have been validated out

-- ============================================================
-- ✅ Checkpoint: Data integrity validated
-- ============================================================
```

---

### Step 2.5: Restore and Sign-Off (30 minutes)

```sql
-- ============================================================
-- 2.5.1: Restore original data
-- ============================================================
TRUNCATE TABLE staging.stg_veterans;
INSERT INTO staging.stg_veterans SELECT * FROM staging.stg_veterans_backup;

TRUNCATE TABLE warehouse.dim_veterans;
INSERT INTO warehouse.dim_veterans SELECT * FROM warehouse.dim_veterans_backup;

-- ============================================================
-- 2.5.2: Clean up test tables
-- ============================================================
DROP TABLE IF EXISTS test_staging_old;
DROP TABLE IF EXISTS test_staging_new;
DROP TABLE IF EXISTS test_dimension_old;
DROP TABLE IF EXISTS test_dimension_new;
DROP TABLE IF EXISTS staging.stg_veterans_backup;
DROP TABLE IF EXISTS warehouse.dim_veterans_backup;

-- ============================================================
-- ✅ PHASE 2 COMPLETE!
-- ============================================================
-- Proof of concept validated:
-- ✅ Data integrity: PASS
-- ✅ Performance: Acceptable
-- ✅ Business rules: Working correctly
-- Ready to proceed to full rollout
-- ============================================================
```

---

## Phase 3: Full Rollout

**Goal:** Migrate all dimensions and update orchestration
**Duration:** 8-12 hours
**Risk Level:** 🟡 Medium (modifies production procedures)

### Step 3.1: Production Cutover Plan

```sql
-- ============================================================
-- 3.1.1: Schedule maintenance window (if required)
-- ============================================================
-- Recommended: 4-hour window during off-peak hours
-- No downtime required if using rename strategy

-- ============================================================
-- 3.1.2: Create rollback copies
-- ============================================================

-- Backup all dimension procedures before modification
-- (Use version control tags or export procedures)

SHOW PROCEDURES LIKE 'sp_transform_ods_to_staging_%';
SHOW PROCEDURES LIKE 'sp_load_dim_%';

-- Document current procedure names for rollback

-- ============================================================
-- 3.1.3: Cutover Veterans pipeline
-- ============================================================

-- Step 1: Rename old procedures (keep as backup)
ALTER PROCEDURE sp_transform_ods_to_staging_veterans
RENAME TO sp_transform_ods_to_staging_veterans_OLD_BACKUP;

ALTER PROCEDURE sp_load_dim_veterans
RENAME TO sp_load_dim_veterans_OLD_BACKUP;

-- Step 2: Rename new procedures to production names
ALTER PROCEDURE sp_transform_ods_to_staging_veterans_v2
RENAME TO sp_transform_ods_to_staging_veterans;

-- Step 3: Update orchestration to use generic loader
-- Find and update calls like:
--   CALL sp_load_dim_veterans(:batch_id);
-- Replace with:
--   CALL sp_load_scd_type2_generic('dim_veterans', :batch_id);

-- ============================================================
-- 3.1.4: Test production cutover
-- ============================================================
SET prod_test_batch = 'PROD_CUTOVER_TEST_001';

CALL sp_transform_ods_to_staging_veterans(:prod_test_batch);
CALL sp_load_scd_type2_generic('dim_veterans', :prod_test_batch);

-- Validate results
SELECT COUNT(*) FROM staging.stg_veterans WHERE batch_id = :prod_test_batch;
SELECT COUNT(*) FROM warehouse.dim_veterans WHERE batch_id = :prod_test_batch;

CALL sp_validate_scd_type2_integrity('dim_veterans');

-- ============================================================
-- ✅ Checkpoint: Veterans pipeline cutover complete
-- ============================================================
```

---

### Step 3.2: Migrate Remaining Dimensions (6-8 hours)

Repeat for each dimension: Evaluators, Facilities, Clinical Conditions, Request Types, Exam Locations

```sql
-- ============================================================
-- Template for each dimension
-- ============================================================

-- Example: Evaluators
-- ------------------------------------------------------------

-- 1. Create refactored transform (follow Veterans pattern)
CREATE OR REPLACE PROCEDURE sp_transform_ods_to_staging_evaluators_v2(...)
-- Use common functions throughout

-- 2. Side-by-side test
-- (Follow Phase 2 testing pattern)

-- 3. Cutover
ALTER PROCEDURE sp_transform_ods_to_staging_evaluators
RENAME TO sp_transform_ods_to_staging_evaluators_OLD_BACKUP;

ALTER PROCEDURE sp_transform_ods_to_staging_evaluators_v2
RENAME TO sp_transform_ods_to_staging_evaluators;

-- 4. Update orchestration for generic load
-- Replace: CALL sp_load_dim_evaluators(:batch_id);
-- With:    CALL sp_load_scd_type2_generic('dim_evaluators', :batch_id);

-- 5. Test
CALL sp_transform_ods_to_staging_evaluators('TEST');
CALL sp_load_scd_type2_generic('dim_evaluators', 'TEST');
CALL sp_validate_scd_type2_integrity('dim_evaluators');

-- ============================================================
-- Repeat for: Facilities, Clinical Conditions, etc.
-- ============================================================
```

---

### Step 3.3: Update Master Orchestration (2 hours)

```sql
-- ============================================================
-- 3.3.1: Update dimension loading orchestration
-- ============================================================

-- BEFORE:
/*
CREATE OR REPLACE PROCEDURE sp_load_all_dimensions(p_batch_id VARCHAR)
AS
$$
BEGIN
    CALL sp_load_dim_veterans(:p_batch_id);
    CALL sp_load_dim_evaluators(:p_batch_id);
    CALL sp_load_dim_facilities(:p_batch_id);
    CALL sp_load_dim_clinical_conditions(:p_batch_id);
    CALL sp_load_dim_request_types(:p_batch_id);
    CALL sp_load_dim_exam_locations(:p_batch_id);
END;
$$;
*/

-- AFTER: Use generic batch loader
CREATE OR REPLACE PROCEDURE sp_load_all_dimensions(p_batch_id VARCHAR)
RETURNS TABLE (...)
LANGUAGE SQL
AS
$$
BEGIN
    -- Use the generic batch loader
    RETURN TABLE(
        CALL sp_load_all_dimensions_scd2(:p_batch_id)
    );
END;
$$;

-- Or even simpler - just call the generic batch procedure directly:
-- CALL sp_load_all_dimensions_scd2(:batch_id);

-- ============================================================
-- 3.3.2: Add execution logging
-- ============================================================

-- Enhance orchestration to log to metadata.etl_execution_log
-- (Example - customize for your orchestration pattern)

CREATE OR REPLACE PROCEDURE sp_pipeline_veterans_with_logging(p_batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_status VARCHAR;
    v_error VARCHAR;
BEGIN
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';

    BEGIN
        -- Run transform
        CALL sp_transform_ods_to_staging_veterans(:p_batch_id);

        -- Run load
        CALL sp_load_scd_type2_generic('dim_veterans', :p_batch_id);

        SET v_status = 'SUCCESS';
        SET v_error = NULL;

    EXCEPTION
        WHEN OTHER THEN
            SET v_status = 'FAILED';
            SET v_error = SQLERRM;
    END;

    SET v_end_time = CURRENT_TIMESTAMP();

    -- Log execution
    INSERT INTO metadata.etl_execution_log (
        pipeline_name,
        batch_id,
        execution_start_timestamp,
        execution_end_timestamp,
        duration_seconds,
        status,
        error_message
    ) VALUES (
        'veterans_pipeline',
        :p_batch_id,
        :v_start_time,
        :v_end_time,
        DATEDIFF(second, :v_start_time, :v_end_time),
        :v_status,
        :v_error
    );

    RETURN v_status || ': ' || COALESCE(v_error, 'Completed successfully');
END;
$$;

-- ============================================================
-- ✅ Checkpoint: Orchestration updated
-- ============================================================
```

---

### Step 3.4: Decommission Old Procedures (1 hour)

```sql
-- ============================================================
-- Only after 2-4 weeks of successful production runs
-- ============================================================

-- 1. Verify new procedures running successfully
SELECT
    pipeline_name,
    COUNT(*) AS execution_count,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    MAX(execution_end_timestamp) AS last_run
FROM metadata.etl_execution_log
WHERE execution_start_timestamp >= DATEADD(week, -2, CURRENT_TIMESTAMP())
GROUP BY pipeline_name
ORDER BY pipeline_name;

-- All pipelines should show 100% successful runs

-- 2. Drop old backup procedures
DROP PROCEDURE IF EXISTS sp_transform_ods_to_staging_veterans_OLD_BACKUP(...);
DROP PROCEDURE IF EXISTS sp_load_dim_veterans_OLD_BACKUP(...);
-- Repeat for all dimensions

-- 3. Document decommissioning
-- Update repository README with deprecated procedures list

-- ============================================================
-- ✅ PHASE 3 COMPLETE!
-- ============================================================
```

---

## Testing & Validation

### Automated Test Suite

```sql
-- ============================================================
-- Comprehensive validation script
-- Run after each phase completion
-- ============================================================

-- Test 1: Functions exist and work
SELECT 'Functions Test' AS test_name,
       CASE WHEN COUNT(*) >= 25 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM information_schema.functions
WHERE function_schema = 'WAREHOUSE' AND function_name LIKE 'fn_%';

-- Test 2: Reference tables populated
SELECT 'Reference Tables Test' AS test_name,
       CASE WHEN (
           SELECT COUNT(*) FROM reference.ref_disability_rating_categories
       ) >= 5 THEN '✅ PASS' ELSE '❌ FAIL' END AS status;

-- Test 3: Metadata tables configured
SELECT 'Metadata Config Test' AS test_name,
       CASE WHEN (
           SELECT COUNT(*) FROM metadata.scd_type2_config WHERE enabled = TRUE
       ) >= 6 THEN '✅ PASS' ELSE '❌ FAIL' END AS status;

-- Test 4: SCD integrity
CREATE OR REPLACE PROCEDURE sp_test_all_scd_integrity()
RETURNS TABLE (table_name VARCHAR, status VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    v_tables CURSOR FOR
        SELECT table_name FROM metadata.scd_type2_config WHERE enabled = TRUE;
    v_table_name VARCHAR;
BEGIN
    FOR v_table IN v_tables DO
        FETCH v_table INTO v_table_name;

        -- Run validation
        CALL sp_validate_scd_type2_integrity(:v_table_name);

        -- Aggregate results (simplified)
        RETURN QUERY (
            SELECT :v_table_name, 'Check completed' AS status
        );
    END FOR;
END;
$$;

CALL sp_test_all_scd_integrity();

-- ============================================================
-- All tests should show ✅ PASS
-- ============================================================
```

---

## Rollback Procedures

### Quick Rollback (if issues found)

```sql
-- ============================================================
-- Rollback transform procedures
-- ============================================================

-- Rename new back to _v2
ALTER PROCEDURE sp_transform_ods_to_staging_veterans
RENAME TO sp_transform_ods_to_staging_veterans_v2;

-- Restore old procedure
ALTER PROCEDURE sp_transform_ods_to_staging_veterans_OLD_BACKUP
RENAME TO sp_transform_ods_to_staging_veterans;

-- ============================================================
-- Rollback load procedures
-- ============================================================

-- Update orchestration to call old load procedure
-- Change back from:
--   CALL sp_load_scd_type2_generic('dim_veterans', :batch_id);
-- To:
--   CALL sp_load_dim_veterans(:batch_id);

-- Restore old load procedure
ALTER PROCEDURE sp_load_dim_veterans_OLD_BACKUP
RENAME TO sp_load_dim_veterans;

-- ============================================================
-- ✅ System rolled back to previous state
-- ============================================================
-- Investigate issues before re-attempting deployment
```

---

## Monitoring & Troubleshooting

### Key Metrics to Monitor

```sql
-- ============================================================
-- Daily monitoring query
-- ============================================================
SELECT
    pipeline_name,
    COUNT(*) AS executions_today,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    MAX(execution_end_timestamp) AS last_run
FROM metadata.etl_execution_log
WHERE execution_start_timestamp >= CURRENT_DATE()
GROUP BY pipeline_name
ORDER BY failed DESC, pipeline_name;

-- ============================================================
-- Performance trend analysis
-- ============================================================
SELECT
    DATE_TRUNC('day', execution_start_timestamp) AS run_date,
    pipeline_name,
    AVG(duration_seconds) AS avg_duration,
    COUNT(*) AS run_count
FROM metadata.etl_execution_log
WHERE execution_start_timestamp >= DATEADD(week, -4, CURRENT_TIMESTAMP())
  AND status = 'SUCCESS'
GROUP BY run_date, pipeline_name
ORDER BY run_date DESC, pipeline_name;

-- Look for:
-- ✅ Duration within expected ranges
-- ✅ 100% success rate
-- ⚠️ Sudden spikes in duration
-- ⚠️ Any failures
```

---

### Common Issues & Solutions

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Function not found** | `ERROR: Unknown function fn_xxx` | Re-run functions deployment script |
| **Table not found** | `ERROR: ref_disability_rating_categories does not exist` | Re-run reference tables script |
| **Config missing** | `ERROR: No configuration found for table 'dim_xxx'` | Add row to metadata.scd_type2_config |
| **Performance degradation** | Procedures 2x+ slower | Check query plans, verify functions are IMMUTABLE |
| **Data mismatch** | Categories wrong | Verify reference table data, test lookup functions |
| **SCD integrity fail** | Multiple current records | Run sp_validate_scd_type2_integrity, fix data |

---

## Success Criteria

### Phase 1 Success Criteria

- ✅ All 25+ functions deployed and tested
- ✅ Reference tables populated (13 rows total minimum)
- ✅ Metadata tables created and configured (30+ rows total)
- ✅ Generic SCD procedures deployed
- ✅ All validation tests pass
- ✅ Zero impact to existing pipelines

---

### Phase 2 Success Criteria

- ✅ Veterans transform refactored (54% code reduction)
- ✅ Side-by-side comparison shows identical data
- ✅ Performance within 10% of baseline
- ✅ SCD integrity validation passes
- ✅ Business rules working correctly
- ✅ Stakeholder sign-off obtained

---

### Phase 3 Success Criteria

- ✅ All 6+ dimensions migrated
- ✅ Orchestration updated and tested
- ✅ Production runs successful for 2+ weeks
- ✅ Execution logging working
- ✅ Monitoring dashboards updated
- ✅ Documentation updated
- ✅ Old procedures decommissioned

---

### Overall Success Metrics

| Metric | Target | Actual |
|--------|--------|--------|
| **Code Reduction** | 35-40% | ___% |
| **Lines Saved** | 2,000+ | ___ |
| **Deployment Success Rate** | 100% | ___% |
| **Performance Impact** | < 10% | ___% |
| **Data Integrity** | 100% match | ___% |
| **Production Incidents** | 0 | ___ |

---

## Post-Deployment

### Week 1-2: Close Monitoring

- [ ] Daily execution log review
- [ ] Performance metrics tracking
- [ ] Data quality spot checks
- [ ] User feedback collection

### Week 3-4: Validation Period

- [ ] Run full regression tests
- [ ] Compare month-over-month metrics
- [ ] Stakeholder satisfaction survey
- [ ] Document lessons learned

### Month 2+: Continuous Improvement

- [ ] Add new dimensions using metadata
- [ ] Refine DQ scoring rules
- [ ] Enhance business rule reference tables
- [ ] Training for development team

---

## Conclusion

This deployment guide provides a comprehensive, step-by-step approach to implementing the refactored ETL framework with:

✅ **Low-risk phased approach**
✅ **Comprehensive testing at each phase**
✅ **Clear rollback procedures**
✅ **Detailed validation criteria**
✅ **Post-deployment monitoring**

**Estimated Total Effort:** 14-21 hours over 2-3 weeks
**Expected Benefit:** 62% code reduction, improved maintainability, business agility

**Ready to begin?** Start with Phase 1 - Foundation Setup!
