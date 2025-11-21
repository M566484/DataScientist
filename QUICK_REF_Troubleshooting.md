# Quick Reference: Troubleshooting
**Top 15 Issues & Solutions** | 2-Minute Fixes | Last Updated: 2025-11-21

---

## 🔍 Quick Diagnosis

```sql
-- Run this FIRST to see what's wrong
SELECT * FROM vw_golden_signals_dashboard;
SELECT * FROM vw_alerts_active;
```

---

## Issue #1: Pipeline Didn't Run

**Symptoms:** Data is stale, freshness signal is 🔴

**Quick Fix:**
```sql
-- Check if tasks are running
SHOW TASKS IN DATABASE IDENTIFIER(fn_get_dw_database());

-- Resume suspended tasks
ALTER TASK task_daily_ods_extraction RESUME;

-- Manually run pipeline
CALL sp_master_pipeline_simplified();
```

**Root Causes:**
- Task was manually suspended
- Task failed due to error (check execution history)
- Warehouse was suspended/unavailable

---

## Issue #2: Tests Failing

**Symptoms:** Red flags in test results

**Quick Fix:**
```sql
-- See which tests failed
SELECT * FROM vw_failed_tests;

-- Re-run tests to confirm
CALL sp_run_etl_tests('ALL', 'CRITICAL', NULL);
```

**Common Failures:**
- **Orphan records** → Re-run dimension load before fact load
- **Null business keys** → Check source data quality
- **Duplicate records** → Investigate staging layer merge logic

---

## Issue #3: High Credit Usage

**Symptoms:** Cost signal is 🔴 or 🟡

**Quick Fix:**
```sql
-- Find expensive queries
SELECT
    query_text,
    total_elapsed_time/1000 AS seconds,
    credits_used_cloud_services
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY credits_used_cloud_services DESC
LIMIT 10;

-- Check warehouse auto-suspend
SHOW WAREHOUSES;

-- Suspend idle warehouse
ALTER WAREHOUSE <warehouse_name> SUSPEND;
```

---

## Issue #4: Slow Queries

**Symptoms:** Latency signal is 🔴 or 🟡

**Quick Fix:**
```sql
-- Find slow queries
SELECT
    query_id,
    query_text,
    total_elapsed_time/1000 AS seconds
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC
LIMIT 10;

-- Check if clustering is enabled
SHOW TABLES IN SCHEMA warehouse LIKE 'fact_%';

-- Manually recluster (if needed)
ALTER TABLE warehouse.fact_evaluation RESUME RECLUSTER;
```

---

## Issue #5: Data Quality Score Low

**Symptoms:** Quality signal is 🔴 or 🟡

**Quick Fix:**
```sql
-- Identify bad tables
SELECT
    table_name,
    dq_score,
    critical_issues,
    warning_issues
FROM data_quality_scores
WHERE check_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND dq_score < fn_get_config_number('quality', 'min_dq_score_critical')
ORDER BY dq_score ASC;

-- Review source data
SELECT * FROM staging.stg_veterans
WHERE dq_score < 80
LIMIT 100;
```

---

## Issue #6: Duplicate Records in Dimensions

**Symptoms:** Test "No duplicate current records" failing

**Quick Fix:**
```sql
-- Find duplicates
SELECT
    veteran_id,
    COUNT(*) AS duplicate_count
FROM warehouse.dim_veteran
WHERE is_current = TRUE
GROUP BY veteran_id
HAVING COUNT(*) > 1;

-- Fix: End-date old records
UPDATE warehouse.dim_veteran
SET is_current = FALSE,
    effective_end_date = CURRENT_TIMESTAMP()
WHERE veteran_sk IN (
    -- Keep only the most recent record per veteran_id
    SELECT veteran_sk FROM (
        SELECT veteran_sk,
               ROW_NUMBER() OVER (PARTITION BY veteran_id ORDER BY effective_start_date DESC) AS rn
        FROM warehouse.dim_veteran
        WHERE is_current = TRUE
    ) WHERE rn > 1
);
```

---

## Issue #7: Orphan Records in Facts

**Symptoms:** Test "No orphan veterans" failing

**Quick Fix:**
```sql
-- Find orphans
SELECT COUNT(*) FROM warehouse.fact_evaluation f
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.dim_veteran v
    WHERE f.veteran_sk = v.veteran_sk
);

-- Fix: Re-run dimension load, then fact load
CALL sp_load_all_dimensions('MANUAL_FIX');
CALL sp_load_all_facts('MANUAL_FIX');
```

---

## Issue #8: Configuration Value Not Working

**Symptoms:** Changed config but behavior didn't change

**Quick Fix:**
```sql
-- Verify configuration is active
SELECT * FROM vw_active_configurations
WHERE config_category = 'pipeline'
  AND config_key = 'default_batch_size';

-- Check if function is using it
SELECT fn_get_config('pipeline', 'default_batch_size');

-- Update with audit trail
CALL sp_update_configuration(
    'pipeline',
    'default_batch_size',
    '15000',
    'Updated for testing'
);
```

---

## Issue #9: Task Keeps Failing

**Symptoms:** Error signal shows repeated failures

**Quick Fix:**
```sql
-- Get task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_NAME_HERE',
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;

-- Suspend task temporarily
ALTER TASK task_name SUSPEND;

-- Fix underlying issue, then resume
ALTER TASK task_name RESUME;
```

---

## Issue #10: Data Not Loading from ODS to Staging

**Symptoms:** Staging tables empty or not updating

**Quick Fix:**
```sql
-- Check ODS data exists
SELECT COUNT(*) FROM IDENTIFIER(fn_get_ods_database() || '.ODS.ods_veterans_source')
WHERE batch_id = (SELECT MAX(batch_id) FROM IDENTIFIER(fn_get_ods_database() || '.ODS.ods_veterans_source'));

-- Check staging transformation
SELECT COUNT(*) FROM staging.stg_veterans
WHERE batch_id = (SELECT MAX(batch_id) FROM staging.stg_veterans);

-- Manually run staging process
CALL sp_transform_ods_to_staging_veterans('MANUAL_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI'));
```

---

## Issue #11: Warehouse Running Out of Storage

**Symptoms:** Storage alerts or quota exceeded

**Quick Fix:**
```sql
-- Check storage usage
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    ROUND(BYTES/1024/1024/1024, 2) AS SIZE_GB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('staging', 'warehouse', 'marts')
ORDER BY BYTES DESC
LIMIT 20;

-- Clean up staging (if retention period exceeded)
DELETE FROM staging.stg_veterans
WHERE loaded_timestamp < DATEADD(day, -7, CURRENT_TIMESTAMP());

-- Drop old backups/clones
DROP SCHEMA IF EXISTS warehouse_backup_old;
```

---

## Issue #12: Time Travel Needed (Recover Deleted Data)

**Symptoms:** Accidental DELETE/UPDATE

**Quick Fix:**
```sql
-- View table as it was 2 hours ago
SELECT * FROM warehouse.dim_veteran
AT (OFFSET => -7200) -- 7200 seconds = 2 hours
WHERE veteran_id = 'VETERAN_123';

-- Restore table to 2 hours ago
CREATE OR REPLACE TABLE warehouse.dim_veteran
CLONE warehouse.dim_veteran AT (OFFSET => -7200);

-- Or restore specific records
INSERT INTO warehouse.dim_veteran
SELECT * FROM warehouse.dim_veteran
AT (OFFSET => -7200)
WHERE veteran_id IN ('VET_1', 'VET_2');
```

---

## Issue #13: Multi-Source Conflicts

**Symptoms:** OMS vs VEMS data mismatches

**Quick Fix:**
```sql
-- Check conflict log
SELECT * FROM REFERENCE.ref_reconciliation_log
WHERE reconciliation_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY reconciliation_timestamp DESC;

-- Review system of record configuration
SELECT * FROM REFERENCE.ref_system_of_record;

-- Update preferred source if needed
UPDATE REFERENCE.ref_system_of_record
SET primary_source_system = 'VEMS'
WHERE entity_type = 'VETERAN';
```

---

## Issue #14: Pipeline Takes Too Long

**Symptoms:** Latency > 2 hours

**Quick Fix:**
```sql
-- Identify slowest steps
SELECT
    pipeline_name,
    duration_minutes,
    execution_start_time
FROM pipeline_execution_history
WHERE execution_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY duration_minutes DESC;

-- Check individual dimension/fact load times
SELECT * FROM vw_dimension_load_plan
ORDER BY estimated_duration_minutes DESC;

-- Enable parallel loading (if not already)
CALL sp_update_configuration(
    'pipeline',
    'enable_parallel_processing',
    'TRUE',
    'Speed up pipeline'
);
```

---

## Issue #15: Can't Connect to Snowflake

**Symptoms:** Connection timeout or authentication error

**Quick Fix:**
1. **Check network**: Ping snowflake URL
2. **Verify credentials**: Username/password correct?
3. **Check account name**: `<account>.snowflakecomputing.com`
4. **MFA issues**: Regenerate MFA if needed
5. **IP whitelisting**: Check if your IP is allowed

```bash
# Test connection with SnowSQL
snowsql -a <account> -u <username>

# If MFA:
snowsql -a <account> -u <username> --authenticator externalbrowser
```

---

## 🚨 Emergency Procedures

### CRITICAL: Complete Pipeline Failure

```sql
-- 1. Suspend all tasks immediately
ALTER TASK task_daily_ods_extraction SUSPEND;

-- 2. Create emergency backup
CREATE SCHEMA warehouse_emergency_backup_<TIMESTAMP>
CLONE warehouse;

-- 3. Check what's broken
SELECT * FROM vw_golden_signals_dashboard;
SELECT * FROM vw_failed_tests;

-- 4. Review errors
SELECT * FROM pipeline_execution_history
WHERE execution_status = 'FAILED'
ORDER BY execution_start_time DESC
LIMIT 10;

-- 5. Fix and test
CALL sp_run_etl_tests('ALL', 'CRITICAL', NULL);

-- 6. Resume when ready
ALTER TASK task_daily_ods_extraction RESUME;
```

---

## 📞 Escalation Path

| Severity | Response Time | Action |
|----------|---------------|--------|
| **SEV-1 (Critical)** | 15 min | Call on-call engineer immediately |
| **SEV-2 (High)** | 1 hour | Create incident ticket, notify team |
| **SEV-3 (Medium)** | 4 hours | Check playbook, ask in team chat |
| **SEV-4 (Low)** | Next day | Document and schedule for sprint |

---

## 🎯 Prevention Checklist

Before deploying changes:
- ✅ Run tests: `CALL sp_run_etl_tests('ALL', 'CRITICAL', NULL);`
- ✅ Check Golden Signals: `SELECT * FROM vw_golden_signals_dashboard;`
- ✅ Create backup: `CREATE SCHEMA backup_<date> CLONE warehouse;`
- ✅ Test in DEV first
- ✅ Document changes in configuration audit

---

**Pro Tip:** 90% of issues are caught by:
1. Morning health check (5 min daily)
2. Running tests before deployment
3. Monitoring Golden Signals dashboard

**Keep this guide bookmarked! 📌**
