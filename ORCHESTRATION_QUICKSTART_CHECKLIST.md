# Orchestration Quick-Start Checklist
## Get Your VES Data Pipeline Automated in 30 Minutes

**Use this checklist** to set up automated task orchestration and notifications for your VES data pipeline.

---

## ⏱️ Time Estimate

- **Prerequisites:** 10 minutes (one-time setup by admin)
- **Task Creation:** 10 minutes (copy-paste from SQL file)
- **Testing:** 5 minutes (manual execution test)
- **Activation:** 5 minutes (resume tasks)

**Total:** 30 minutes

---

## 📋 Prerequisites Checklist

### Step 1: Get ACCOUNTADMIN Access (One-Time)

**You need ACCOUNTADMIN role to:**
- Create notification integrations
- Grant integration usage to your role

**Ask your Snowflake admin to run:**

```sql
USE ROLE ACCOUNTADMIN;

-- Create email notification integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_notification_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'data-team@yourcompany.com',    -- ← CHANGE THIS
    'ops-team@yourcompany.com'      -- ← CHANGE THIS
  );

-- Grant usage to data engineering role
GRANT USAGE ON INTEGRATION email_notification_int TO ROLE data_engineer;  -- ← CHANGE ROLE NAME

-- Verify
SHOW NOTIFICATION INTEGRATIONS;
```

**✓ Checkpoint:** You should see `email_notification_int` in the list.

---

### Step 2: Create Dedicated Warehouses (5 minutes)

```sql
USE ROLE sysadmin;  -- Or your admin role

-- Warehouse for most ETL tasks
CREATE WAREHOUSE IF NOT EXISTS etl_task_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60          -- Suspend after 1 minute idle
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for automated ETL tasks';

-- Warehouse for heavy processing (large fact tables)
CREATE WAREHOUSE IF NOT EXISTS etl_heavy_task_wh
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Large warehouse for data-intensive tasks';

-- Grant usage to your role
GRANT USAGE ON WAREHOUSE etl_task_wh TO ROLE data_engineer;       -- ← CHANGE ROLE
GRANT USAGE ON WAREHOUSE etl_heavy_task_wh TO ROLE data_engineer; -- ← CHANGE ROLE

-- Verify
SHOW WAREHOUSES LIKE 'etl_%';
```

**✓ Checkpoint:** Both warehouses should appear with `auto_suspend = 60`.

---

### Step 3: Create Metadata Tracking Table (2 minutes)

```sql
USE DATABASE VESDW_PRD;  -- ← CHANGE to your database
USE SCHEMA metadata;       -- ← CHANGE to your schema

CREATE TABLE IF NOT EXISTS etl_task_log (
    log_id NUMBER AUTOINCREMENT PRIMARY KEY,
    task_name VARCHAR(200),
    batch_id VARCHAR(100),
    status VARCHAR(20),
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    records_processed NUMBER,
    error_message VARCHAR(5000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify
SELECT COUNT(*) FROM etl_task_log;  -- Should return 0 (empty table)
```

**✓ Checkpoint:** Table created successfully.

---

### Step 4: Create Email Alert Procedure (2 minutes)

```sql
CREATE OR REPLACE PROCEDURE sp_send_email_alert(
    p_subject VARCHAR,
    p_message VARCHAR,
    p_recipients ARRAY
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$SEND_EMAIL(
        'email_notification_int',
        :p_recipients,
        :p_subject,
        :p_message,
        'text/html'
    );
    RETURN 'Email sent successfully to ' || ARRAY_SIZE(:p_recipients) || ' recipients';
END;
$$;

-- Test email
CALL sp_send_email_alert(
    'Test Alert - VES Data Pipeline',
    '<h2>Test Email</h2><p>If you receive this, email notifications are working!</p>',
    ARRAY_CONSTRUCT('YOUR_EMAIL@company.com')  -- ← CHANGE THIS
);
```

**✓ Checkpoint:** Check your inbox for test email.

**⚠️ If email doesn't arrive:**
- Check spam folder
- Verify email is in ALLOWED_RECIPIENTS list
- Check Snowflake email comes from `notifications@snowflake.net` (whitelist if needed)

---

## 📦 Task Creation Checklist

### Step 5: Deploy Orchestration SQL (5 minutes)

**Option 1: Run entire file**

```sql
-- In Snowflake UI or SnowSQL
\@snowflake/orchestration/01_snowflake_native_orchestration.sql
```

**Option 2: Copy-paste tasks individually**

Open `snowflake/orchestration/01_snowflake_native_orchestration.sql` and copy-paste:

- [ ] Task 3A: `task_daily_ods_extraction` (root task)
- [ ] Task 3B: `task_daily_staging_layer`
- [ ] Task 3C: `task_daily_dimensions`
- [ ] Task 3D: `task_daily_facts`
- [ ] Task 3E: `task_daily_dq_validation`
- [ ] Task 6A: `task_hourly_monitoring`

**✓ Checkpoint:** Verify all tasks created:

```sql
SHOW TASKS;
-- Should show 6 tasks, all in 'suspended' state
```

---

### Step 6: Customize Task Logic (5 minutes)

**⚠️ IMPORTANT:** The SQL file contains placeholder logic. Update these sections:

#### Task 3A: ODS Extraction

```sql
-- TODO: Replace with actual extraction logic
-- Current: Assumes data loaded by external process (Mulesoft)

-- Add your logic here:
-- CALL your_ods_extraction_procedure(:v_batch_id);
```

#### Task 3C: Dimensions

```sql
-- TODO: Replace with actual dimension loading procedures
-- Current: Only loads dim_veteran as example

-- Add procedures for other dimensions:
-- CALL sp_load_dim_evaluator(:v_batch_id);
-- CALL sp_load_dim_facility(:v_batch_id);
-- etc.
```

#### Task 3D: Facts

```sql
-- TODO: Replace with actual fact loading procedures
-- Current: Only loads fact_exam_requests as example

-- Add procedures for other facts:
-- CALL sp_load_fact_evaluations(:v_batch_id);
-- CALL sp_load_fact_appointments(:v_batch_id);
-- CALL sp_load_fact_bottlenecks(:v_batch_id);
```

**✓ Checkpoint:** Task logic matches your procedures.

---

## 🧪 Testing Checklist

### Step 7: Manual Test Execution (5 minutes)

**Test individual task:**

```sql
-- Execute root task (won't trigger children since tasks are suspended)
EXECUTE TASK task_daily_ods_extraction;

-- Check execution status
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(second, scheduled_time, completed_time) AS duration_sec,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = 'task_daily_ods_extraction'
ORDER BY scheduled_time DESC
LIMIT 1;
```

**Expected Results:**
- `state = 'SUCCEEDED'` ✓
- `error_message = NULL` ✓
- `duration_sec < 600` (less than 10 minutes) ✓

**If Failed:**
- Check `error_message` column
- Review query using `query_id`: `SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE query_id = '<query_id>'`
- Fix issue, recreate task, test again

**✓ Checkpoint:** Root task executes successfully.

---

### Step 8: Test Email Notifications (2 minutes)

**Trigger a test alert:**

```sql
-- Manually call alert procedure
CALL sp_send_email_alert(
    'Pipeline Test Alert',
    '<h2>Test Alert from Task</h2><p>This is a test from task execution.</p>',
    ARRAY_CONSTRUCT('YOUR_EMAIL@company.com')  -- ← CHANGE THIS
);
```

**✓ Checkpoint:** Email received successfully.

---

## 🚀 Activation Checklist

### Step 9: Resume Tasks (2 minutes)

**⚠️ CRITICAL:** Resume in **REVERSE dependency order** (children first, then parents).

```sql
-- Resume child tasks first
ALTER TASK task_daily_dq_validation RESUME;      -- ← Level 5 (deepest child)
ALTER TASK task_daily_facts RESUME;              -- ← Level 4
ALTER TASK task_daily_dimensions RESUME;         -- ← Level 3
ALTER TASK task_daily_staging_layer RESUME;     -- ← Level 2
ALTER TASK task_daily_ods_extraction RESUME;     -- ← Level 1 (root) - RESUME LAST

-- Resume monitoring task
ALTER TASK task_hourly_monitoring RESUME;        -- ← Independent task
```

**Verify all tasks are active:**

```sql
SHOW TASKS;
-- Check 'state' column: should be 'started' for all tasks
```

**✓ Checkpoint:** All tasks show `state = 'started'`.

---

### Step 10: Monitor First Scheduled Run

**When will tasks run?**

```sql
-- Check next scheduled run
SELECT
    name,
    state,
    schedule,
    next_scheduled_time
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'task_daily_ods_extraction',
    RECURSIVE => TRUE
))
ORDER BY name;
```

**Root task schedule:** Daily at 1:00 AM EST

**Don't want to wait?** Execute manually to trigger full DAG:

```sql
-- This will trigger root task AND all dependent child tasks
EXECUTE TASK task_daily_ods_extraction;
```

**Monitor execution:**

```sql
-- Check all task runs
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(second, scheduled_time, completed_time) AS duration_sec
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;
```

**Expected execution order:**
1. `task_daily_ods_extraction` (runs first)
2. `task_daily_staging_layer` (waits for #1)
3. `task_daily_dimensions` (waits for #2)
4. `task_daily_facts` (waits for #3)
5. `task_daily_dq_validation` (waits for #4)

**✓ Checkpoint:** All tasks execute successfully in correct order.

---

## 📊 Monitoring Checklist

### Step 11: Set Up Monitoring Queries (3 minutes)

**Create monitoring views:**

```sql
-- View failed tasks
CREATE OR REPLACE VIEW vw_failed_tasks_24h AS
SELECT
    name AS task_name,
    state,
    scheduled_time,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE state = 'FAILED'
  AND scheduled_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

-- View task success rate
CREATE OR REPLACE VIEW vw_task_success_rate AS
SELECT
    name AS task_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful_runs,
    ROUND(SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY name
ORDER BY success_rate_pct;
```

**Daily monitoring routine:**

```sql
-- Run these queries daily to check pipeline health

-- 1. Any failed tasks?
SELECT * FROM vw_failed_tasks_24h;
-- Expected: 0 rows

-- 2. Success rates OK?
SELECT * FROM vw_task_success_rate;
-- Expected: success_rate_pct = 100% for all tasks

-- 3. Check custom logs
SELECT * FROM VESDW_PRD.metadata.etl_task_log
WHERE start_time >= CURRENT_DATE()
ORDER BY start_time DESC;
-- Expected: status = 'SUCCESS' for all
```

**✓ Checkpoint:** Monitoring views created and returning expected results.

---

## ✅ Final Verification

### Step 12: Complete Validation (5 minutes)

**Run this validation query:**

```sql
WITH task_validation AS (
    SELECT
        'Tasks Created' AS check_name,
        COUNT(*) AS actual_value,
        6 AS expected_value,
        CASE WHEN COUNT(*) = 6 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
    FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
        TASK_NAME => 'task_daily_ods_extraction',
        RECURSIVE => TRUE
    ))

    UNION ALL

    SELECT
        'Tasks Active',
        SUM(CASE WHEN state = 'started' THEN 1 ELSE 0 END),
        6,
        CASE WHEN SUM(CASE WHEN state = 'started' THEN 1 ELSE 0 END) = 6 THEN '✓ PASS' ELSE '✗ FAIL' END
    FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
        TASK_NAME => 'task_daily_ods_extraction',
        RECURSIVE => TRUE
    ))

    UNION ALL

    SELECT
        'Email Integration',
        COUNT(*),
        1,
        CASE WHEN COUNT(*) >= 1 THEN '✓ PASS' ELSE '✗ FAIL' END
    FROM TABLE(INFORMATION_SCHEMA.INTEGRATIONS)
    WHERE integration_type = 'NOTIFICATION'

    UNION ALL

    SELECT
        'Warehouses Created',
        COUNT(*),
        2,
        CASE WHEN COUNT(*) = 2 THEN '✓ PASS' ELSE '✗ FAIL' END
    FROM INFORMATION_SCHEMA.WAREHOUSES
    WHERE warehouse_name IN ('ETL_TASK_WH', 'ETL_HEAVY_TASK_WH')
)
SELECT * FROM task_validation;
```

**Expected Output:**

```
check_name           actual_value  expected_value  status
-------------------  ------------  --------------  --------
Tasks Created        6             6               ✓ PASS
Tasks Active         6             6               ✓ PASS
Email Integration    1             1               ✓ PASS
Warehouses Created   2             2               ✓ PASS
```

**✓ All checks PASS?** You're ready for production! 🎉

---

## 🎯 Success Criteria

Your orchestration is successfully set up when:

- [x] All 6 tasks created and in `started` state
- [x] Email notifications working (test email received)
- [x] Manual task execution succeeds
- [x] Task dependencies working (child tasks run after parent succeeds)
- [x] Monitoring views showing expected results
- [x] Warehouses auto-suspending after 60 seconds

---

## 📅 Daily Operations

### Morning Routine (5 minutes)

```sql
-- 1. Check for failed tasks
SELECT * FROM vw_failed_tasks_24h;

-- 2. Review yesterday's batch
SELECT * FROM VESDW_PRD.metadata.etl_task_log
WHERE start_time >= CURRENT_DATE() - 1
ORDER BY start_time;

-- 3. Check warehouse costs
SELECT
    warehouse_name,
    SUM(credits_used) AS credits_yesterday
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => CURRENT_DATE() - 1,
    DATE_RANGE_END => CURRENT_DATE()
))
WHERE warehouse_name IN ('ETL_TASK_WH', 'ETL_HEAVY_TASK_WH')
GROUP BY warehouse_name;
```

### Weekly Review (10 minutes)

```sql
-- Success rate over last week
SELECT * FROM vw_task_success_rate;

-- Average duration per task
SELECT
    name,
    AVG(DATEDIFF(minute, scheduled_time, completed_time)) AS avg_duration_min,
    MAX(DATEDIFF(minute, scheduled_time, completed_time)) AS max_duration_min
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND state = 'SUCCEEDED'
GROUP BY name;
```

---

## 🆘 Quick Troubleshooting

### Task Not Running

```sql
-- Check if task is suspended
SHOW TASKS;
-- Fix: ALTER TASK <task_name> RESUME;
```

### Task Failed

```sql
-- Get error details
SELECT error_message FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = '<task_name>' ORDER BY scheduled_time DESC LIMIT 1;
```

### Email Not Received

```sql
-- Verify integration
SHOW NOTIFICATION INTEGRATIONS;

-- Test send
CALL sp_send_email_alert('Test', 'Test message', ARRAY_CONSTRUCT('your@email.com'));
```

---

## 📚 Reference Documentation

- **Full Orchestration SQL:** `snowflake/orchestration/01_snowflake_native_orchestration.sql`
- **Detailed Guide:** `SNOWFLAKE_ORCHESTRATION_GUIDE.md`
- **Snowflake Docs:** https://docs.snowflake.com/en/user-guide/tasks-intro

---

## 🎉 You're Done!

Your VES data pipeline is now:
- ✅ Fully automated (runs daily at 1 AM)
- ✅ Self-monitoring (hourly checks + email alerts)
- ✅ Cost-optimized (auto-suspend warehouses)
- ✅ Production-ready (error handling + logging)

**Next Steps:**
1. Monitor first week of automated runs
2. Adjust warehouse sizes if needed (up for speed, down for cost)
3. Customize email alerts based on your team's preferences
4. Add more validation checks as needed

---

**Last Updated:** 2025-11-17
**Version:** 1.0
