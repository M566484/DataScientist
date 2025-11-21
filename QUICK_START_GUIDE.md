# VES Data Warehouse - Quick Start Guide
**Get productive in 15 minutes** | Last Updated: 2025-11-21

---

## 📌 Morning Health Check (5 minutes - Do this every day at 9 AM)

```sql
-- 1. Connect to Snowflake
USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA metadata;

-- 2. Check overall system health (ONE query!)
SELECT * FROM vw_golden_signals_dashboard;
```

**What you're looking for:**
- ✅ **All green** (`🟢 GOOD`) = You're done!
- 🟡 **Any yellow** (`🟡 WARNING`) = Monitor closely, review details
- 🔴 **Any red** (`🔴 CRITICAL`) = Take action immediately (see Troubleshooting below)

---

## 🚀 Common Daily Tasks

### Task 1: Run the Data Pipeline Manually

```sql
-- Generate a new batch
CALL sp_master_pipeline_simplified();
```

### Task 2: Check Pipeline Status

```sql
-- View recent pipeline runs
SELECT
    pipeline_name,
    execution_status,
    duration_minutes,
    execution_start_time,
    error_message
FROM pipeline_execution_history
WHERE execution_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY execution_start_time DESC
LIMIT 20;
```

### Task 3: Check Data Freshness

```sql
-- When was data last updated?
SELECT
    'dim_veteran' AS table_name,
    MAX(updated_timestamp) AS last_update,
    DATEDIFF(hour, MAX(updated_timestamp), CURRENT_TIMESTAMP()) AS hours_ago
FROM warehouse.dim_veteran WHERE is_current = TRUE
UNION ALL
SELECT
    'fact_evaluation',
    MAX(created_timestamp),
    DATEDIFF(hour, MAX(created_timestamp), CURRENT_TIMESTAMP())
FROM warehouse.fact_evaluation;
```

### Task 4: Run Data Quality Checks

```sql
-- Run all critical tests
CALL sp_run_etl_tests('ALL', 'CRITICAL', NULL);

-- View any failures
SELECT * FROM vw_failed_tests;
```

### Task 5: Check Credit Usage (Cost Monitoring)

```sql
-- How many credits used today?
SELECT
    SUM(credits_used) AS credits_today,
    SUM(credits_used) * 30 AS projected_monthly
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP());
```

---

## 🔥 Troubleshooting Quick Reference

### Problem: Pipeline Failed

**Quick Fix:**
```sql
-- 1. Find the error
SELECT * FROM pipeline_execution_history
WHERE execution_status = 'FAILED'
ORDER BY execution_start_time DESC
LIMIT 5;

-- 2. Check error details
SELECT error_message FROM pipeline_execution_history
WHERE execution_id = <failed_execution_id>;

-- 3. Retry the pipeline
CALL sp_master_pipeline_simplified();
```

### Problem: Data Quality Score Low

**Quick Fix:**
```sql
-- 1. Identify problematic tables
SELECT
    table_name,
    dq_score,
    critical_issues,
    warning_issues
FROM data_quality_scores
WHERE check_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND dq_score < 80
ORDER BY dq_score ASC;

-- 2. Investigate specific issues
-- (Review the table with lowest score)
```

### Problem: Data is Stale (Not Fresh)

**Quick Fix:**
```sql
-- 1. Check task status
SHOW TASKS IN DATABASE IDENTIFIER(fn_get_dw_database());

-- 2. Resume tasks if suspended
ALTER TASK task_daily_ods_extraction RESUME;

-- 3. Manually trigger pipeline
CALL sp_master_pipeline_simplified();
```

### Problem: High Credit Usage

**Quick Fix:**
```sql
-- 1. Find expensive queries
SELECT
    query_id,
    query_text,
    total_elapsed_time/1000 AS seconds,
    credits_used_cloud_services
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
  AND credits_used_cloud_services > 1
ORDER BY credits_used_cloud_services DESC
LIMIT 10;

-- 2. Check warehouse sizes
SHOW WAREHOUSES;

-- 3. Suspend idle warehouses
ALTER WAREHOUSE <warehouse_name> SUSPEND;
```

---

## 📊 Useful Views (Your Go-To Dashboards)

| View Name | Purpose | Example Query |
|-----------|---------|---------------|
| `vw_golden_signals_dashboard` | Overall health (5 metrics) | `SELECT * FROM vw_golden_signals_dashboard;` |
| `vw_alerts_active` | Current alerts needing attention | `SELECT * FROM vw_alerts_active;` |
| `vw_latest_test_results` | Test execution results | `SELECT * FROM vw_latest_test_results;` |
| `vw_dimension_load_plan` | Dimension execution order | `SELECT * FROM vw_dimension_load_plan;` |
| `vw_fact_load_plan` | Fact execution order | `SELECT * FROM vw_fact_load_plan;` |
| `vw_active_configurations` | System configurations | `SELECT * FROM vw_active_configurations;` |

---

## ⚙️ Configuration Changes (No Code Required!)

### Change Pipeline Batch Size

```sql
CALL sp_update_configuration(
    'pipeline',
    'default_batch_size',
    '20000',  -- New value
    'Increased for performance'  -- Reason
);
```

### Change Data Quality Threshold

```sql
CALL sp_update_configuration(
    'quality',
    'min_dq_score_critical',
    '90',  -- New value
    'Stricter quality standards'
);
```

### Disable a Dimension Load

```sql
UPDATE dimension_load_config
SET is_active = FALSE
WHERE dimension_name = 'Claim Dimension';
```

### Change Email Alert Recipients

```sql
CALL sp_update_configuration(
    'alerting',
    'critical_alert_recipients',
    'newteam@company.com',
    'Updated team email'
);
```

---

## 📚 Where to Find More Help

| Topic | Document |
|-------|----------|
| **Detailed troubleshooting** | `TROUBLESHOOTING_PLAYBOOK.md` |
| **Daily operations** | `STANDARD_OPERATING_PROCEDURES.md` |
| **Architecture deep dive** | `DATA_PIPELINE_ARCHITECTURE.md` |
| **Developer onboarding** | `DEVELOPER_ONBOARDING_GUIDE.md` |
| **Performance tuning** | `PERFORMANCE_OPTIMIZATION_GUIDE.md` |

---

## 🎯 Key Principles for Maintainability

1. **Configuration over code** - Change behavior via `system_configuration` table
2. **Metadata-driven** - Add new dimensions/facts via config tables
3. **Test everything** - Run tests before deploying changes
4. **Monitor the Golden Signals** - Ignore noise, focus on 5 metrics
5. **Document changes** - Use `sp_update_configuration()` for audit trail

---

## Emergency Contact

**Critical Issues (SEV-1):**
- Data loss
- Pipeline completely down
- Security breach

**Action:** Escalate immediately to on-call engineer

**Non-Critical Issues (SEV-2/3):**
- Check `TROUBLESHOOTING_PLAYBOOK.md` first
- Review `vw_alerts_active` for guidance
- Run tests: `CALL sp_run_etl_tests('ALL', 'CRITICAL', NULL);`

---

**Remember:**
- ✅ **Morning health check**: 5 minutes daily
- ✅ **Test before deploying**: Prevent regressions
- ✅ **Use configuration tables**: Avoid hardcoding
- ✅ **Monitor Golden Signals**: Focus on what matters

**You've got this! 🚀**
