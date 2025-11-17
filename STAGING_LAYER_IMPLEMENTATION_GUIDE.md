---
# Staging Layer Implementation Guide
## OMS + VEMS Data Integration - Step-by-Step Execution

**Purpose:** This guide walks you through implementing the staging layer that merges OMS (legacy) and VEMS (modern) data.

**Time Required:** 2-4 hours for initial setup, 30-60 minutes per batch thereafter

**Difficulty:** Intermediate (made simple with this guide!)

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [What You're Building](#what-youre-building)
3. [Step-by-Step Execution](#step-by-step-execution)
4. [Validation Checkpoints](#validation-checkpoints)
5. [Troubleshooting](#troubleshooting)
6. [Production Deployment](#production-deployment)

---

## Prerequisites

### ✅ Required Tables Must Exist

Before starting, verify these tables exist:

```sql
-- Check ODS tables
SELECT COUNT(*) FROM datascience.ods.ods_veterans_source;
SELECT COUNT(*) FROM datascience.ods.ods_evaluators_source;
SELECT COUNT(*) FROM datascience.ods.ods_facilities_source;
SELECT COUNT(*) FROM datascience.ods.ods_exam_requests_source;

-- Check reference tables
SELECT COUNT(*) FROM datascience.reference.ref_entity_crosswalk_veteran;
SELECT COUNT(*) FROM datascience.reference.ref_entity_crosswalk_evaluator;
SELECT COUNT(*) FROM datascience.reference.ref_entity_crosswalk_facility;
SELECT COUNT(*) FROM datascience.reference.ref_system_of_record;
SELECT COUNT(*) FROM datascience.reference.ref_code_mapping_specialty;
SELECT COUNT(*) FROM datascience.reference.ref_reconciliation_log;

-- Check staging tables
SELECT COUNT(*) FROM datascience.staging.stg_veterans;
SELECT COUNT(*) FROM datascience.staging.stg_evaluators;
SELECT COUNT(*) FROM datascience.staging.stg_facilities;
SELECT COUNT(*) FROM datascience.staging.stg_fact_exam_requests;
```

**If any tables are missing:**
```sql
-- Create ODS tables
\@snowflake/ods/01_create_ods_tables.sql

-- Create reference tables
\@snowflake/reference/01_create_reference_tables.sql

-- Create staging tables
\@snowflake/staging/01_create_staging_tables.sql
```

### ✅ Required UDF Functions Must Exist

```sql
-- Check if code mapping functions exist
SHOW FUNCTIONS LIKE 'fn_map_specialty_code' IN SCHEMA datascience.reference;
SHOW FUNCTIONS LIKE 'fn_map_request_type_code' IN SCHEMA datascience.reference;
SHOW FUNCTIONS LIKE 'fn_map_appointment_status_code' IN SCHEMA datascience.reference;
```

**If functions are missing:**
```sql
\@snowflake/reference/02_create_reference_udfs.sql
```

### ✅ Sample Data Loaded (For Testing)

If you're testing first (recommended!), you need sample ODS data:

```sql
-- Create a test batch
INSERT INTO datascience.ods.ods_veterans_source (
    batch_id, source_system, source_record_id, veteran_ssn, first_name, last_name,
    date_of_birth, gender, email, phone, disability_rating, extraction_timestamp
)
VALUES
    -- OMS records
    ('TEST_BATCH_001', 'OMS', 'OMS_VET_001', '123456789', 'JOHN', 'DOE', '1975-06-15', 'M', 'john.doe@email.com', '555-1234', 80, CURRENT_TIMESTAMP()),
    ('TEST_BATCH_001', 'OMS', 'OMS_VET_002', '987654321', 'JANE', 'SMITH', '1982-03-22', 'F', 'jane.smith@email.com', '555-5678', 60, CURRENT_TIMESTAMP()),
    -- VEMS records (matching OMS veterans)
    ('TEST_BATCH_001', 'VEMS', 'VEMS_VET_001', '123456789', 'JOHN', 'DOE', '1975-06-15', 'M', 'john.updated@email.com', '555-9999', 70, CURRENT_TIMESTAMP()),
    ('TEST_BATCH_001', 'VEMS', 'VEMS_VET_003', '111222333', 'BOB', 'JOHNSON', '1990-11-10', 'M', 'bob.j@email.com', '555-3333', 90, CURRENT_TIMESTAMP());

-- Verify data loaded
SELECT source_system, COUNT(*)
FROM datascience.ods.ods_veterans_source
WHERE batch_id = 'TEST_BATCH_001'
GROUP BY source_system;
-- Should show: OMS (2 records), VEMS (2 records)
```

---

## What You're Building

### The Big Picture

```
┌────────────────────────────────────────────────────────────────┐
│  BEFORE: Two separate systems                                  │
│                                                                 │
│  OMS (Legacy)          VEMS (Modern)                           │
│  ├─ 50,000 veterans    ├─ 45,000 veterans                      │
│  ├─ 200 evaluators     ├─ 180 evaluators                       │
│  └─ 30 facilities      └─ 28 facilities                        │
│                                                                 │
│  Problem: Duplicates! Same person in both systems              │
└────────────────────────────────────────────────────────────────┘

                          ↓ STAGING LAYER MERGES ↓

┌────────────────────────────────────────────────────────────────┐
│  AFTER: One unified view                                        │
│                                                                 │
│  STAGING (Merged)                                              │
│  ├─ 52,000 unique veterans   (De-duplicated)                  │
│  ├─ 205 unique evaluators     (De-duplicated)                  │
│  └─ 31 unique facilities      (De-duplicated)                  │
│                                                                 │
│  Each record has:                                              │
│  ✓ Master ID (universal identifier)                            │
│  ✓ Best data from both systems (system-of-record rules)       │
│  ✓ Data quality score (90-100 = excellent)                    │
│  ✓ Conflict log (if OMS/VEMS disagreed)                       │
└────────────────────────────────────────────────────────────────┘
```

### How Matching Works

**Example: Matching Veterans**

```
OMS Record:                    VEMS Record:
├─ SSN: 123-45-6789           ├─ SSN: 123-45-6789
├─ Name: JOHN DOE             ├─ Name: JOHN DOE
├─ Disability: 80%            ├─ Disability: 70%  ← CONFLICT!
└─ Email: old@email.com       └─ Email: new@email.com

                  ↓ MERGE LOGIC ↓

Staging Record:
├─ Master ID: 123-45-6789 (SSN as universal ID)
├─ Name: JOHN DOE (same in both)
├─ Disability: 80% (OMS wins - system-of-record)
├─ Email: new@email.com (VEMS is newer for contact info)
├─ Match Confidence: 100% (exact SSN match)
├─ Conflict Logged: Yes (disability rating mismatch)
└─ DQ Score: 95 (excellent quality)
```

### System-of-Record Rules

| Entity | Primary Source | Rationale | Example |
|--------|---------------|-----------|---------|
| **Veterans** | OMS | Complete historical records | Disability rating from OMS |
| **Evaluators** | VEMS | Most current provider info | Phone number from VEMS |
| **Facilities** | OMS | Master facility data | Facility name from OMS |
| **Contact Info** | VEMS | More recently updated | Email from VEMS |

---

## Step-by-Step Execution

### Phase 1: Setup (One-Time)

#### Step 1: Deploy Staging Layer Procedures

```sql
-- Deploy the simplified staging layer procedures
\@snowflake/staging/02_staging_layer_oms_vems_merge_simplified.sql

-- Verify procedures created
SHOW PROCEDURES LIKE 'sp_%' IN SCHEMA datascience.staging;
```

**Expected Output:** Should see 8 procedures:
- `sp_build_crosswalk_veterans_simple`
- `sp_build_crosswalk_evaluators_simple`
- `sp_build_crosswalk_facilities_simple`
- `sp_merge_veterans_to_staging`
- `sp_merge_evaluators_to_staging`
- `sp_merge_facilities_to_staging`
- `sp_merge_exam_requests_to_staging`
- `sp_staging_layer_master`

#### Step 2: Deploy Validation Queries

```sql
\@snowflake/monitoring/staging_layer_validation_queries.sql

-- Verify validation view created
SELECT * FROM vw_staging_validation_dashboard LIMIT 1;
```

---

### Phase 2: First Execution (Testing)

**Batch ID:** Use format `TEST_BATCH_YYYYMMDD_###` for testing

#### Step 3: Build Entity Crosswalks (Matching)

This is where OMS and VEMS records get matched to create master IDs.

```sql
-- Step 3A: Match veterans by SSN
CALL sp_build_crosswalk_veterans_simple('TEST_BATCH_20251117_001');

-- Step 3B: Match evaluators by NPI
CALL sp_build_crosswalk_evaluators_simple('TEST_BATCH_20251117_001');

-- Step 3C: Match facilities by facility ID
CALL sp_build_crosswalk_facilities_simple('TEST_BATCH_20251117_001');
```

**⏱ Duration:** 30 seconds - 2 minutes

**✅ CHECKPOINT 1: Validate Crosswalks**

```sql
-- Run validation query from staging_layer_validation_queries.sql
-- Query 1.1: Crosswalk Match Summary

SELECT
    'VETERANS' AS entity_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN match_method = 'SSN_EXACT_MATCH' THEN 1 ELSE 0 END) AS exact_matches,
    SUM(CASE WHEN match_method = 'SSN_OMS_ONLY' THEN 1 ELSE 0 END) AS oms_only,
    SUM(CASE WHEN match_method = 'SSN_VEMS_ONLY' THEN 1 ELSE 0 END) AS vems_only,
    ROUND(AVG(match_confidence), 2) AS avg_confidence
FROM datascience.reference.ref_entity_crosswalk_veteran
WHERE batch_id = 'TEST_BATCH_20251117_001';
```

**Expected Results:**
```
entity_type  total_records  exact_matches  oms_only  vems_only  avg_confidence
-----------  -------------  -------------  --------  ---------  --------------
VETERANS     4              1              1         2          95.00
```

**What This Means:**
- **total_records = 4:** Found 4 unique veterans across both systems
- **exact_matches = 1:** 1 veteran found in BOTH OMS and VEMS (matched by SSN)
- **oms_only = 1:** 1 veteran exists ONLY in OMS
- **vems_only = 2:** 2 veterans exist ONLY in VEMS
- **avg_confidence = 95%:** High confidence in matches

**✓ If your numbers look similar, proceed to Step 4!**

**✗ If you see 0 records:** Check that ODS data was loaded with correct batch_id

---

#### Step 4: Merge Entities to Staging

This is where we combine OMS and VEMS data into unified records.

```sql
-- Step 4A: Merge veterans
CALL sp_merge_veterans_to_staging('TEST_BATCH_20251117_001');

-- Step 4B: Merge evaluators
CALL sp_merge_evaluators_to_staging('TEST_BATCH_20251117_001');

-- Step 4C: Merge facilities
CALL sp_merge_facilities_to_staging('TEST_BATCH_20251117_001');
```

**⏱ Duration:** 1-3 minutes

**✅ CHECKPOINT 2: Validate Merged Data**

```sql
-- Check veteran merge results
SELECT
    master_veteran_id,
    first_name,
    last_name,
    disability_rating,
    source_system,
    match_confidence,
    dq_score,
    conflict_type
FROM datascience.staging.stg_veterans
WHERE batch_id = 'TEST_BATCH_20251117_001'
ORDER BY master_veteran_id;
```

**Expected Results:**
```
master_veteran_id  first_name  last_name  disability_rating  source_system     match_confidence  dq_score  conflict_type
-----------------  ----------  ---------  -----------------  ---------------  ----------------  --------  --------------------
111222333          BOB         JOHNSON    90                 VEMS              90.00            95        NULL
123456789          JOHN        DOE        80                 OMS_VEMS_MERGED   100.00           100       DISABILITY_RATING_MISMATCH
987654321          JANE        SMITH      60                 OMS               90.00            95        NULL
```

**What This Means:**
- **BOB JOHNSON:** Only in VEMS, disability = 90% from VEMS
- **JOHN DOE:** In BOTH systems (merged), disability = 80% from OMS (system-of-record), email = new@email.com from VEMS (more current)
- **JANE SMITH:** Only in OMS, disability = 60% from OMS
- **Conflict detected:** John Doe had different disability ratings (OMS=80%, VEMS=70%), resolved to OMS value

**✓ If you see merged records with good DQ scores (>80), proceed to Step 5!**

---

#### Step 5: Check Data Quality Scores

```sql
-- Query 2.1: Data Quality Score Distribution
SELECT
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        ELSE 'Poor (<70)'
    END AS dq_category,
    COUNT(*) AS record_count,
    ROUND(AVG(dq_score), 2) AS avg_dq_score
FROM datascience.staging.stg_veterans
WHERE batch_id = 'TEST_BATCH_20251117_001'
GROUP BY
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        ELSE 'Poor (<70)'
    END;
```

**Expected Results:**
```
dq_category         record_count  avg_dq_score
------------------  ------------  ------------
Excellent (90-100)  4             96.25
```

**✓ If >70% of records are "Excellent" or "Good", you're ready for production!**

---

#### Step 6: Review Conflicts

```sql
-- Query 3.1: Conflict Summary
SELECT
    entity_type,
    conflict_type,
    COUNT(*) AS conflict_count,
    resolution_method
FROM datascience.reference.ref_reconciliation_log
WHERE batch_id = 'TEST_BATCH_20251117_001'
GROUP BY entity_type, conflict_type, resolution_method;
```

**Expected Results:**
```
entity_type  conflict_type               conflict_count  resolution_method
-----------  --------------------------  --------------  -----------------
VETERAN      DISABILITY_RATING_MISMATCH  1               PREFER_OMS
```

**What This Means:**
- 1 conflict was detected (John Doe's disability rating)
- Resolved using "PREFER_OMS" rule (OMS is system-of-record for veterans)

**✓ Conflicts are EXPECTED and NORMAL. The important part is they're logged and resolved per your rules.**

---

### Phase 3: Production Deployment

#### Step 7: Run Complete Pipeline (All Steps at Once)

Once testing is successful, use the master orchestration procedure:

```sql
-- Run entire staging layer in one call
CALL sp_staging_layer_master('PROD_BATCH_20251117_001');
```

**⏱ Duration:** 5-10 minutes for production volumes

**Output:**
```
Step 1A: Veteran crosswalk built: 52,000 records
Step 1B: Evaluator crosswalk built: 205 records
Step 1C: Facility crosswalk built: 31 records
Step 2A: Veterans merged to staging: 52,000 records
Step 2B: Evaluators merged to staging: 205 records
Step 2C: Facilities merged to staging: 31 records
Step 3A: Exam requests merged to staging: 125,000 records
Total duration: 342 seconds
```

---

#### Step 8: Run Master Validation Dashboard

```sql
SELECT * FROM vw_staging_validation_dashboard;
```

**Expected Results:**
```
check_name               value     unit             status
-----------------------  --------  ---------------  --------
Veteran Crosswalk        52000     crosswalks built ✓ PASS
Veteran Data Quality     87.5      avg DQ score     ✓ PASS
Conflicts Detected       234       conflicts logged ✓ PASS
```

**✓ All checks should show "PASS" or "WARN". No "FAIL" statuses.**

---

#### Step 9: Volume Reconciliation

```sql
-- Query 4.1: Source to Staging Volume Reconciliation
-- Verify all ODS records made it to staging

WITH ods_counts AS (
    SELECT source_system, COUNT(*) AS ods_count
    FROM datascience.ods.ods_veterans_source
    WHERE batch_id = 'PROD_BATCH_20251117_001'
    GROUP BY source_system
),
staging_counts AS (
    SELECT source_system, COUNT(*) AS staging_count
    FROM datascience.staging.stg_veterans
    WHERE batch_id = 'PROD_BATCH_20251117_001'
    GROUP BY source_system
)
SELECT
    o.source_system,
    o.ods_count,
    COALESCE(s.staging_count, 0) AS staging_count,
    o.ods_count - COALESCE(s.staging_count, 0) AS records_lost,
    CASE
        WHEN o.ods_count = COALESCE(s.staging_count, 0) THEN '✓ PASS'
        ELSE '✗ FAIL - Records missing'
    END AS validation_status
FROM ods_counts o
LEFT JOIN staging_counts s ON o.source_system = s.source_system;
```

**Expected Results:**
```
source_system  ods_count  staging_count  records_lost  validation_status
-------------  ---------  -------------  ------------  -----------------
OMS            50000      50000          0             ✓ PASS
VEMS           45000      45000          0             ✓ PASS
```

**✓ records_lost should be 0 or close to 0 (<1%)**

---

## Validation Checkpoints

### Quick Validation Checklist

After each batch run, check:

```sql
-- ✓ Checkpoint 1: Crosswalks built
SELECT COUNT(*) FROM datascience.reference.ref_entity_crosswalk_veteran WHERE batch_id = 'YOUR_BATCH_ID';
-- Should be > 0

-- ✓ Checkpoint 2: Staging records created
SELECT COUNT(*) FROM datascience.staging.stg_veterans WHERE batch_id = 'YOUR_BATCH_ID';
-- Should be > 0

-- ✓ Checkpoint 3: High data quality
SELECT AVG(dq_score) FROM datascience.staging.stg_veterans WHERE batch_id = 'YOUR_BATCH_ID';
-- Should be > 80

-- ✓ Checkpoint 4: Conflicts logged
SELECT COUNT(*) FROM datascience.reference.ref_reconciliation_log WHERE batch_id = 'YOUR_BATCH_ID';
-- Can be 0 or higher (conflicts are OK!)

-- ✓ Checkpoint 5: No records lost
-- Run Query 4.1 from validation queries
```

### Red Flags (When to Investigate)

| Issue | Red Flag | Action |
|-------|----------|--------|
| Match rate | <50% exact matches | Check SSN/NPI data quality in ODS |
| DQ score | <70 average | Review common DQ issues (Query 2.2) |
| Records lost | >5% missing | Verify join keys (SSN, NPI, facility_id) |
| Conflicts | >20% of records | Expected if systems are out of sync |

---

## Troubleshooting

### Problem 1: "No crosswalks built (COUNT = 0)"

**Symptoms:**
```sql
SELECT COUNT(*) FROM ref_entity_crosswalk_veteran WHERE batch_id = 'YOUR_BATCH_ID';
-- Returns: 0
```

**Causes & Solutions:**

1. **ODS data not loaded**
   ```sql
   -- Check if ODS has data for your batch
   SELECT COUNT(*) FROM ods_veterans_source WHERE batch_id = 'YOUR_BATCH_ID';
   -- If 0, load ODS data first
   ```

2. **Wrong batch_id**
   ```sql
   -- Find latest batch_id
   SELECT DISTINCT batch_id FROM ods_veterans_source ORDER BY batch_id DESC LIMIT 5;
   -- Use the correct batch_id
   ```

3. **source_system not set correctly**
   ```sql
   -- Check source_system values
   SELECT DISTINCT source_system FROM ods_veterans_source;
   -- Should be 'OMS' and 'VEMS' (case-sensitive!)
   ```

---

### Problem 2: "Low match confidence (<50%)"

**Symptoms:**
```sql
SELECT AVG(match_confidence) FROM ref_entity_crosswalk_veteran WHERE batch_id = 'YOUR_BATCH_ID';
-- Returns: 45.00 (low!)
```

**Causes & Solutions:**

1. **Most records in only one system**
   ```sql
   -- Check distribution
   SELECT match_method, COUNT(*)
   FROM ref_entity_crosswalk_veteran
   WHERE batch_id = 'YOUR_BATCH_ID'
   GROUP BY match_method;
   ```
   - If mostly OMS_ONLY or VEMS_ONLY: Systems may not have overlapping data yet
   - **Expected during transition period** from OMS to VEMS

2. **SSN/NPI not populated**
   ```sql
   -- Check for NULLs in join keys
   SELECT COUNT(*) FROM ods_veterans_source WHERE veteran_ssn IS NULL;
   -- Should be 0 or very low
   ```

---

### Problem 3: "Low DQ scores (<70 average)"

**Symptoms:**
```sql
SELECT AVG(dq_score) FROM stg_veterans WHERE batch_id = 'YOUR_BATCH_ID';
-- Returns: 62.00 (low!)
```

**Causes & Solutions:**

1. **Find common issues**
   ```sql
   -- Run Query 2.2: Common Data Quality Issues
   SELECT TRIM(value) AS dq_issue, COUNT(*)
   FROM stg_veterans, LATERAL SPLIT_TO_TABLE(dq_issues, ';')
   WHERE batch_id = 'YOUR_BATCH_ID'
   GROUP BY TRIM(value)
   ORDER BY COUNT(*) DESC
   LIMIT 10;
   ```

2. **Common DQ Issues:**
   - "Missing email" → Not critical, but lowers score
   - "Missing DOB" → Critical, investigate source data
   - "Invalid disability rating" → Check source validation rules

3. **Adjust DQ scoring weights** (if needed)
   - Edit `sp_merge_veterans_to_staging`
   - Modify the DQ score calculation section

---

### Problem 4: "Records lost in staging"

**Symptoms:**
```sql
-- ODS has 1000 records, but staging only has 950
```

**Causes & Solutions:**

1. **NULL join keys (SSN, NPI)**
   ```sql
   -- Find veterans with NULL SSN
   SELECT COUNT(*) FROM ods_veterans_source
   WHERE veteran_ssn IS NULL AND batch_id = 'YOUR_BATCH_ID';
   ```
   - **Solution:** Fix source data or add fuzzy matching logic

2. **Crosswalk didn't build**
   ```sql
   -- Check crosswalk count
   SELECT COUNT(*) FROM ref_entity_crosswalk_veteran WHERE batch_id = 'YOUR_BATCH_ID';
   ```
   - **Solution:** Re-run crosswalk procedures

---

### Problem 5: "System-of-record rules not working"

**Symptoms:**
```sql
-- VEMS value used instead of OMS for veterans (should be OMS!)
```

**Causes & Solutions:**

1. **Check system-of-record configuration**
   ```sql
   SELECT * FROM ref_system_of_record WHERE entity_type = 'VETERAN';
   -- Should show: primary_source = 'OMS'
   ```

2. **Review COALESCE order in merge procedures**
   ```sql
   -- In sp_merge_veterans_to_staging, disability_rating should be:
   COALESCE(oms.disability_rating, vems.disability_rating)
   -- NOT:
   COALESCE(vems.disability_rating, oms.disability_rating)  -- WRONG!
   ```

3. **Run validation query**
   ```sql
   -- Query 5.1: Verify System-of-Record Rules Applied Correctly
   -- From staging_layer_validation_queries.sql
   ```

---

## Production Deployment

### Scheduling the Staging Layer

**Daily Batch Processing:**

```sql
-- Option 1: Manual daily execution
CALL sp_staging_layer_master('PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001');

-- Option 2: Scheduled task (Snowflake)
CREATE TASK staging_layer_daily_task
  WAREHOUSE = etl_wh
  SCHEDULE = 'USING CRON 0 2 * * * America/New_York' -- 2 AM daily
AS
  CALL sp_staging_layer_master('PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001');

-- Start the task
ALTER TASK staging_layer_daily_task RESUME;
```

### Monitoring

**Daily Validation Queries (Run After Each Batch):**

```sql
-- 1. Check master dashboard
SELECT * FROM vw_staging_validation_dashboard;

-- 2. Review conflicts
SELECT entity_type, conflict_type, COUNT(*)
FROM ref_reconciliation_log
WHERE batch_id = 'TODAY_BATCH_ID'
GROUP BY entity_type, conflict_type;

-- 3. Check DQ scores
SELECT AVG(dq_score) FROM stg_veterans WHERE batch_id = 'TODAY_BATCH_ID';
SELECT AVG(dq_score) FROM stg_evaluators WHERE batch_id = 'TODAY_BATCH_ID';

-- 4. Volume reconciliation
-- Run Query 4.1 from validation queries
```

### Alert Thresholds

Set up alerts for:

| Metric | Threshold | Severity |
|--------|-----------|----------|
| DQ score avg | <75 | WARNING |
| DQ score avg | <60 | CRITICAL |
| Match rate | <40% | WARNING |
| Records lost | >5% | CRITICAL |
| Processing time | >30 min | WARNING |

---

## Quick Reference Card

### Essential Commands

```sql
-- RUN COMPLETE STAGING LAYER
CALL sp_staging_layer_master('YOUR_BATCH_ID');

-- VALIDATE RESULTS
SELECT * FROM vw_staging_validation_dashboard;

-- CHECK DATA QUALITY
SELECT AVG(dq_score) FROM stg_veterans WHERE batch_id = 'YOUR_BATCH_ID';

-- REVIEW CONFLICTS
SELECT * FROM ref_reconciliation_log WHERE batch_id = 'YOUR_BATCH_ID' LIMIT 100;

-- VOLUME CHECK
SELECT source_system, COUNT(*) FROM stg_veterans WHERE batch_id = 'YOUR_BATCH_ID' GROUP BY source_system;
```

### File Locations

- **Procedures:** `snowflake/staging/02_staging_layer_oms_vems_merge_simplified.sql`
- **Validation Queries:** `snowflake/monitoring/staging_layer_validation_queries.sql`
- **This Guide:** `STAGING_LAYER_IMPLEMENTATION_GUIDE.md`
- **Interactive Guide:** `VES_Multi_Source_Integration_Guide.html`

---

## Success Criteria

Your staging layer is working correctly when:

✅ **Crosswalk match rate:** >70% exact matches
✅ **Data quality average:** >80 DQ score
✅ **Conflicts:** Logged and resolved per system-of-record rules
✅ **Volume reconciliation:** <1% records lost
✅ **Processing time:** <30 minutes for production volumes
✅ **Validation dashboard:** All checks show "PASS"

---

## Getting Help

### Documentation

1. **This guide** - Step-by-step execution
2. **Interactive HTML guide** - `VES_Multi_Source_Integration_Guide.html` (architecture overview)
3. **Validation queries** - `staging_layer_validation_queries.sql` (all validation queries)
4. **Architecture doc** - `DATA_PIPELINE_ARCHITECTURE.md` (system design)

### Troubleshooting Process

1. Run validation queries to identify the issue
2. Check the troubleshooting section above for common problems
3. Review procedure logs and error messages
4. Test with small sample data first
5. Escalate if needed with validation query results

---

## You've Got This! 💪

**Remember:**
- Start with test data first (small batch)
- Validate at each checkpoint
- Conflicts are NORMAL and EXPECTED
- DQ scores >80 = you're doing great
- The procedures handle the complex logic for you

**Next Steps After Staging Layer:**
1. Load staging data to warehouse dimensions
2. Build fact tables from staging
3. Set up automated monitoring
4. Train team on conflict resolution

---

**Last Updated:** 2025-11-17
**Version:** 1.0
**Author:** Data Team
