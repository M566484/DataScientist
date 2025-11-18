# VES Data Warehouse - Troubleshooting Playbook
## Quick Solutions to Common Issues

**Purpose:** Fast resolution guide for common data warehouse issues
**Audience:** All Data Team Members, On-Call Engineers
**Format:** Problem → Diagnosis → Solution

**Version:** 1.0
**Last Updated:** 2025-11-17
**Author:** Data Team

---

## Table of Contents

1. [Pipeline Failures](#pipeline-failures)
2. [Data Quality Issues](#data-quality-issues)
3. [Performance Problems](#performance-problems)
4. [Task & Scheduling Issues](#task--scheduling-issues)
5. [Data Load Errors](#data-load-errors)
6. [Query Errors](#query-errors)
7. [Access & Permission Issues](#access--permission-issues)
8. [Cost & Resource Issues](#cost--resource-issues)
9. [Monitoring & Alerting](#monitoring--alerting)
10. [When All Else Fails](#when-all-else-fails)

---

## Pipeline Failures

### Problem: Daily Pipeline Didn't Run

**Symptoms:**
- No new data in fact tables
- Last update timestamp > 24 hours old
- Users complaining about stale dashboards

**Diagnosis:**

```sql
-- Check task execution history
SELECT
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    error_message,
    error_code
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_DAILY_ODS_EXTRACTION'
))
ORDER BY scheduled_time DESC
LIMIT 5;
```

**Common Causes & Solutions:**

#### Cause 1: Task is Suspended

**Check:**
```sql
SHOW TASKS LIKE 'task_daily_ods_extraction';
-- Look at 'state' column: should be 'started'
```

**Solution:**
```sql
ALTER TASK task_daily_ods_extraction RESUME;
```

#### Cause 2: Warehouse is Suspended/Unavailable

**Check:**
```sql
SHOW WAREHOUSES LIKE 'etl_task_wh';
-- Look at 'state' column
```

**Solution:**
```sql
-- Resume warehouse
ALTER WAREHOUSE etl_task_wh RESUME;

-- Or recreate if deleted
CREATE WAREHOUSE etl_task_wh
    WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- Reassign to task
ALTER TASK task_daily_ods_extraction
    SET WAREHOUSE = etl_task_wh;
```

#### Cause 3: Dependency Task Failed

**Check:**
```sql
-- View all tasks in dependency order
SELECT
    name,
    state,
    predecessors
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'TASK_DAILY_ODS_EXTRACTION'
));
```

**Solution:**
```sql
-- Manually execute failed predecessor
EXECUTE TASK task_prerequisite_name;

-- Wait for completion, then execute main task
EXECUTE TASK task_daily_ods_extraction;
```

#### Cause 4: Source Data Unavailable

**Check:**
```sql
-- Verify ODS has data for today
SELECT
    COUNT(*) AS row_count,
    MAX(created_timestamp) AS latest_data
FROM VESODS_PRDDATA_PRD.VEMS_CORE.exam_requests
WHERE created_date = CURRENT_DATE();
```

**Solution:**
- If row_count = 0: Contact upstream team, source system issue
- If latest_data old: Wait for source refresh, or manually trigger source extract

---

### Problem: Task Running But Not Completing

**Symptoms:**
- Task shows state = 'EXECUTING' for hours
- Warehouse is running but no progress

**Diagnosis:**

```sql
-- Check currently running queries for this task
SELECT
    query_id,
    query_text,
    start_time,
    DATEDIFF(minute, start_time, CURRENT_TIMESTAMP()) AS running_minutes,
    bytes_scanned,
    percentage_scanned_from_cache
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE warehouse_name = 'ETL_TASK_WH'
  AND execution_status = 'RUNNING'
ORDER BY start_time;
```

**Solutions:**

#### Solution 1: Query is Large (Normal)

- If `running_minutes` < 60 and making progress: Wait
- Monitor `bytes_scanned` - if increasing, query is working

#### Solution 2: Query is Stuck

**Indicators:**
- No change in `bytes_scanned` for >10 minutes
- `running_minutes` > 120

**Action:**
```sql
-- Kill the stuck query
SELECT SYSTEM$CANCEL_QUERY('<query_id>');

-- Suspend and resume task to reset
ALTER TASK task_daily_ods_extraction SUSPEND;
-- Wait 30 seconds
ALTER TASK task_daily_ods_extraction RESUME;

-- Manually execute if needed
EXECUTE TASK task_daily_ods_extraction;
```

#### Solution 3: Warehouse Too Small

**Indicators:**
- Query spilling to disk (check query profile)
- `bytes_spilled_to_local_storage` > 0

**Action:**
```sql
-- Temporarily increase warehouse size
ALTER WAREHOUSE etl_task_wh SET WAREHOUSE_SIZE = LARGE;

-- Re-run task
EXECUTE TASK task_daily_ods_extraction;

-- After completion, resize back down
ALTER WAREHOUSE etl_task_wh SET WAREHOUSE_SIZE = MEDIUM;
```

---

## Data Quality Issues

### Problem: Data Quality Check Failing

**Symptoms:**
- Email alert: "Data Quality Failures Detected"
- Dashboard shows red status for table
- `vw_data_quality_summary` shows failures

**Diagnosis:**

```sql
-- Get failure details
SELECT
    check_name,
    table_name,
    check_type,
    severity,
    expected_value,
    actual_value,
    variance_pct
FROM VESDW_PRD.metadata.data_quality_checks
WHERE check_status = 'FAIL'
  AND check_timestamp >= CURRENT_DATE()
ORDER BY severity DESC;
```

**Common Scenarios:**

#### Scenario 1: NULL Values in Critical Columns

**Example:** `veteran_ssn IS NULL` check failing

**Investigate:**
```sql
SELECT
    COUNT(*) AS total_records,
    COUNT(CASE WHEN veteran_ssn IS NULL THEN 1 END) AS null_ssns,
    ROUND(COUNT(CASE WHEN veteran_ssn IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS null_pct
FROM VESDW_PRD.staging.stg_veterans
WHERE batch_id = (SELECT MAX(batch_id) FROM VESDW_PRD.staging.stg_veterans);

-- Sample bad records
SELECT *
FROM VESDW_PRD.staging.stg_veterans
WHERE veteran_ssn IS NULL
  AND batch_id = (SELECT MAX(batch_id) FROM VESDW_PRD.staging.stg_veterans)
LIMIT 20;
```

**Solutions:**
```sql
-- Option A: Fix in source system (best)
-- Contact source system owner

-- Option B: Filter out bad records
DELETE FROM VESDW_PRD.staging.stg_veterans
WHERE veteran_ssn IS NULL
  AND batch_id = '<current_batch>';

-- Log rejected records
INSERT INTO VESDW_PRD.metadata.rejected_records (...)
SELECT ... FROM bad_records;

-- Option C: Default/derive value (if business rule allows)
UPDATE VESDW_PRD.staging.stg_veterans
SET veteran_ssn = 'UNKNOWN_' || LPAD(veteran_id::VARCHAR, 9, '0')
WHERE veteran_ssn IS NULL
  AND batch_id = '<current_batch>';
```

#### Scenario 2: Data Out of Valid Range

**Example:** `disability_rating` outside 0-100 range

**Investigate:**
```sql
SELECT
    disability_rating,
    COUNT(*) AS occurrence_count
FROM VESDW_PRD.staging.stg_veterans
WHERE disability_rating NOT BETWEEN 0 AND 100
GROUP BY disability_rating
ORDER BY occurrence_count DESC;
```

**Solutions:**
```sql
-- Cap at valid range
UPDATE VESDW_PRD.staging.stg_veterans
SET disability_rating = CASE
    WHEN disability_rating < 0 THEN 0
    WHEN disability_rating > 100 THEN 100
    ELSE disability_rating
END
WHERE disability_rating NOT BETWEEN 0 AND 100;

-- Or reject if data quality is critical
DELETE FROM VESDW_PRD.staging.stg_veterans
WHERE disability_rating NOT BETWEEN 0 AND 100;
```

#### Scenario 3: Referential Integrity Violation

**Example:** `veteran_dim_sk` doesn't exist in `dim_veteran`

**Investigate:**
```sql
-- Find orphaned records
SELECT
    er.exam_request_sk,
    er.veteran_dim_sk,
    er.source_exam_request_id
FROM VESDW_PRD.warehouse.fact_exam_requests er
LEFT JOIN VESDW_PRD.warehouse.dim_veteran v
    ON er.veteran_dim_sk = v.veteran_sk
WHERE v.veteran_sk IS NULL
LIMIT 100;
```

**Solutions:**
```sql
-- Reload dimension first
CALL sp_build_dim_veteran();

-- Then reload facts
CALL sp_load_fact_exam_requests('<batch_id>');

-- Or create placeholder dimension record
INSERT INTO VESDW_PRD.warehouse.dim_veteran (
    veteran_sk,
    veteran_ssn,
    first_name,
    last_name,
    is_current
)
VALUES (
    -1,  -- Unknown surrogate key
    'UNKNOWN',
    'Unknown',
    'Veteran',
    TRUE
);
```

---

## Performance Problems

### Problem: Query Taking Too Long

**Symptoms:**
- User complaint: "Dashboard won't load"
- Query running for >1 minute
- Warehouse credits usage spiking

**Diagnosis:**

```sql
-- Find the slow query
SELECT
    query_id,
    query_text,
    total_elapsed_time / 1000 AS duration_seconds,
    bytes_scanned / (1024*1024*1024) AS gb_scanned,
    partitions_scanned,
    partitions_total,
    bytes_spilled_to_local_storage,
    bytes_spilled_to_remote_storage
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE user_name = '<user>'
  AND start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC
LIMIT 5;
```

**Quick Fixes:**

#### Fix 1: Add WHERE Clause

**Problem:** Full table scan

```sql
-- Bad
SELECT * FROM fact_exam_requests;

-- Good
SELECT * FROM fact_exam_requests
WHERE request_date >= CURRENT_DATE() - 30;
```

#### Fix 2: Select Only Needed Columns

```sql
-- Bad
SELECT * FROM fact_exam_requests WHERE ...;

-- Good
SELECT
    exam_request_sk,
    veteran_dim_sk,
    exam_status
FROM fact_exam_requests
WHERE ...;
```

#### Fix 3: Use Materialized View

**Check if MV exists:**
```sql
SHOW MATERIALIZED VIEWS LIKE '%<topic>%';
```

**Create MV if needed:**
```sql
CREATE MATERIALIZED VIEW mv_daily_exam_summary AS
SELECT
    request_date,
    exam_status,
    COUNT(*) AS exam_count,
    AVG(cycle_time_days) AS avg_cycle_time
FROM fact_exam_requests
GROUP BY request_date, exam_status;

-- Query MV instead
SELECT * FROM mv_daily_exam_summary
WHERE request_date >= CURRENT_DATE() - 30;
```

#### Fix 4: Add Clustering

**For large tables (>1M rows):**
```sql
-- Add clustering on frequently filtered columns
ALTER TABLE fact_exam_requests
    CLUSTER BY (request_date, exam_status);

-- Wait for automatic reclustering or force
ALTER TABLE fact_exam_requests RECLUSTER;
```

#### Fix 5: Increase Warehouse Size (Temporary)

```sql
-- For one-time large query
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = LARGE;

-- Run query
<your query here>

-- Resize back down
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = SMALL;
```

---

### Problem: Dashboard Loading Slow

**Symptoms:**
- Dashboard taking >10 seconds to load
- Multiple users complaining
- Queries hitting same tables

**Solutions:**

#### Solution 1: Create Dedicated Warehouse

```sql
-- Create dashboard-specific warehouse
CREATE WAREHOUSE dashboard_wh
    WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 60  -- 1 minute
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5  -- Scale for concurrency
    SCALING_POLICY = STANDARD;

-- Update dashboard connection to use dashboard_wh
```

#### Solution 2: Enable Query Result Caching

**Already enabled by default, but verify:**
```sql
SHOW PARAMETERS LIKE 'use_cached_result' IN ACCOUNT;
-- Should show TRUE
```

#### Solution 3: Pre-Aggregate Data

```sql
-- Create summary tables refreshed hourly
CREATE TABLE dashboard_summary_hourly AS
SELECT
    date_hour,
    exam_status,
    COUNT(*) AS exam_count,
    AVG(cycle_time) AS avg_cycle_time
FROM fact_exam_requests
GROUP BY date_hour, exam_status;

-- Refresh hourly via task
CREATE TASK refresh_dashboard_summary
    WAREHOUSE = dashboard_wh
    SCHEDULE = '60 MINUTE'
AS
    -- Truncate and reload last 7 days
    DELETE FROM dashboard_summary_hourly
    WHERE date_hour >= CURRENT_TIMESTAMP() - INTERVAL '7 days';

    INSERT INTO dashboard_summary_hourly
    SELECT ...;
```

---

## Task & Scheduling Issues

### Problem: Task Skipped Execution

**Symptoms:**
- Expected task run didn't happen
- Gap in data for specific day
- Task history shows "SKIPPED"

**Diagnosis:**

```sql
SELECT
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    return_value
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_DAILY_ODS_EXTRACTION',
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -3, CURRENT_TIMESTAMP())
))
WHERE state = 'SKIPPED'
ORDER BY scheduled_time DESC;
```

**Cause & Solution:**

**Cause:** `WHEN` condition not met

**Example:**
```sql
CREATE TASK my_task
    ...
    WHEN SYSTEM$STREAM_HAS_DATA('my_stream')  -- No data in stream
AS ...
```

**Solution:**
```sql
-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('my_stream');  -- Returns FALSE

-- Check stream contents
SELECT COUNT(*) FROM my_stream;  -- Returns 0

-- Manually trigger upstream process to populate stream
EXECUTE TASK task_that_populates_stream;
```

---

### Problem: Task Running at Wrong Time

**Symptoms:**
- Task running during business hours (should be off-hours)
- Task not running at expected time

**Diagnosis:**

```sql
SHOW TASKS LIKE 'task_daily_ods_extraction';
-- Check 'schedule' column
```

**Solution:**

```sql
-- Update schedule (CRON syntax)
ALTER TASK task_daily_ods_extraction
    SET SCHEDULE = 'USING CRON 0 2 * * * America/New_York';
    -- Runs at 2 AM EST daily

-- Common CRON patterns:
-- Every day at 2 AM EST:  0 2 * * * America/New_York
-- Every hour:             0 * * * * America/New_York
-- Every 15 minutes:       */15 * * * * America/New_York
-- Monday at 8 AM:         0 8 * * 1 America/New_York
```

---

## Data Load Errors

### Problem: COPY INTO Failed

**Symptoms:**
- Error: "File not found"
- Error: "Number of columns in file does not match"
- Error: "Invalid UTF8 character"

**Diagnosis:**

```sql
-- Check copy history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STG_VETERANS',
    START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())
))
WHERE status = 'LOAD_FAILED'
ORDER BY last_load_time DESC;
```

**Common Errors & Solutions:**

#### Error: "File not found in stage"

**Check:**
```sql
-- List files in stage
LIST @my_stage/path/;
```

**Solution:**
- Verify file uploaded to correct path
- Check file name (case-sensitive!)
- Verify stage credentials still valid

#### Error: "Number of columns mismatch"

**Check:**
```sql
-- Validate file structure
SELECT $1, $2, $3, ... $10
FROM @my_stage/problem_file.csv
(FILE_FORMAT => 'csv_format')
LIMIT 10;
```

**Solution:**
```sql
-- Option A: Update table to match file
ALTER TABLE stg_veterans ADD COLUMN new_column VARCHAR;

-- Option B: Update file format to skip extra columns
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = CSV
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;  -- Ignore extra columns
```

#### Error: "Invalid UTF8 character"

**Solution:**
```sql
-- Use encoding parameter
COPY INTO stg_veterans
FROM @my_stage/file.csv
FILE_FORMAT = (
    TYPE = CSV
    ENCODING = 'ISO-8859-1'  -- Or 'WINDOWS-1252'
);

-- Or skip bad characters
COPY INTO stg_veterans
FROM @my_stage/file.csv
FILE_FORMAT = (TYPE = CSV)
ON_ERROR = CONTINUE  -- Skip bad rows
VALIDATION_MODE = RETURN_ERRORS;
```

---

## Query Errors

### Problem: "SQL compilation error"

**Symptoms:**
- Query won't run
- Error message about syntax

**Common Scenarios:**

#### Error: "Object does not exist"

**Example:** `Table 'FACT_EXAM_REQUESTS' does not exist`

**Check:**
```sql
-- Verify table exists
SHOW TABLES LIKE 'fact_exam_requests' IN SCHEMA warehouse;

-- Check current context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();
```

**Solution:**
```sql
-- Use fully qualified name
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests;

-- Or set context
USE SCHEMA VESDW_PRD.warehouse;
SELECT * FROM fact_exam_requests;
```

#### Error: "Invalid identifier"

**Example:** Column name has spaces or special characters

**Solution:**
```sql
-- Bad
SELECT Last Name FROM veterans;

-- Good
SELECT "Last Name" FROM veterans;  -- Use double quotes

-- Better: Rename column without spaces
ALTER TABLE veterans RENAME COLUMN "Last Name" TO last_name;
```

---

### Problem: "Timeout" or "Statement reached its timeout"

**Diagnosis:**
```sql
-- Check statement timeout setting
SHOW PARAMETERS LIKE 'statement_timeout_in_seconds';
```

**Solutions:**

```sql
-- Option 1: Increase timeout for session
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;  -- 1 hour

-- Option 2: Optimize query (better long-term)
-- See Performance Problems section

-- Option 3: Break into smaller chunks
-- Instead of:
DELETE FROM large_table WHERE created_date < '2020-01-01';

-- Do:
DELETE FROM large_table
WHERE created_date >= '2020-01-01' AND created_date < '2020-02-01';

DELETE FROM large_table
WHERE created_date >= '2020-02-01' AND created_date < '2020-03-01';
-- ... etc
```

---

## Access & Permission Issues

### Problem: "Insufficient privileges"

**Symptoms:**
- Error: "SQL access control error: Insufficient privileges to operate on table"
- User can't query table or run procedure

**Diagnosis:**

```sql
-- Check user's current role
SELECT CURRENT_ROLE();

-- Check grants for role
SHOW GRANTS TO ROLE DATA_ANALYST;

-- Check what roles user has
SHOW GRANTS TO USER john_doe;
```

**Solutions:**

#### Solution 1: User Not Using Correct Role

```sql
-- Switch to correct role
USE ROLE DATA_ANALYST;

-- Or in connection string
snowsql -a ABC12345 -u john_doe -r DATA_ANALYST
```

#### Solution 2: Role Doesn't Have Permission

```sql
-- Grant necessary permissions
USE ROLE ACCOUNTADMIN;  -- Or role with grant privileges

GRANT USAGE ON DATABASE VESDW_PRD TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA VESDW_PRD.warehouse TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA VESDW_PRD.warehouse TO ROLE DATA_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA VESDW_PRD.warehouse TO ROLE DATA_ANALYST;
```

#### Solution 3: User Doesn't Have Role

```sql
-- Grant role to user
GRANT ROLE DATA_ANALYST TO USER john_doe;
```

---

## Cost & Resource Issues

### Problem: Unexpected High Costs

**Symptoms:**
- Monthly bill much higher than expected
- Budget alert triggered
- Credit usage spiking

**Diagnosis:**

```sql
-- Check credit usage by warehouse (last 7 days)
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits,
    ROUND(SUM(credits_used) * 2.50, 2) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- Find long-running warehouses
SELECT
    warehouse_name,
    start_time,
    end_time,
    DATEDIFF(hour, start_time, end_time) AS hours_running,
    credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE DATEDIFF(hour, start_time, end_time) > 2  -- Running >2 hours
ORDER BY credits_used DESC;
```

**Common Causes & Solutions:**

#### Cause 1: Warehouse Left Running

**Check:**
```sql
SHOW WAREHOUSES;
-- Look for 'state' = 'STARTED' when should be 'SUSPENDED'
```

**Solution:**
```sql
-- Suspend warehouse
ALTER WAREHOUSE analytics_wh SUSPEND;

-- Set auto-suspend (if not already set)
ALTER WAREHOUSE analytics_wh SET AUTO_SUSPEND = 300;  -- 5 minutes
```

#### Cause 2: Warehouse Oversized

**Solution:**
```sql
-- Check if warehouse can be downsized
-- Review query performance with smaller warehouse

ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = SMALL;
-- Test for 1 week, monitor performance
```

#### Cause 3: Excessive Clustering Costs

**Check:**
```sql
-- Review automatic clustering history
SELECT
    table_name,
    SUM(credits_used) AS clustering_credits
FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    DATE_RANGE_START => DATEADD(day, -30, CURRENT_TIMESTAMP())
))
GROUP BY table_name
ORDER BY clustering_credits DESC;
```

**Solution:**
```sql
-- Pause automatic clustering on low-priority tables
ALTER TABLE <table> SUSPEND RECLUSTER;

-- Or remove clustering from tables that don't need it
ALTER TABLE <table> DROP CLUSTERING KEY;
```

---

## Monitoring & Alerting

### Problem: Not Receiving Alerts

**Symptoms:**
- Pipeline failed but no email received
- Quality check failed but no notification

**Diagnosis:**

```sql
-- Check notification integrations
SHOW NOTIFICATION INTEGRATIONS;

-- Test email notification
CALL sp_send_email_alert(
    'Test Alert',
    '<h1>This is a test</h1>',
    ARRAY_CONSTRUCT('your-email@company.com')
);
```

**Solutions:**

#### Solution 1: Notification Integration Not Enabled

```sql
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_notification_int
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('your-team@company.com');

-- Grant usage
GRANT USAGE ON INTEGRATION email_notification_int TO ROLE DATA_ENGINEER;
```

#### Solution 2: Email in Spam

- Check spam/junk folder
- Add `notifications@snowflake.net` to safe senders

#### Solution 3: Task Sending Alert is Suspended

```sql
-- Check monitoring tasks
SHOW TASKS LIKE 'task_hourly_health_check';

-- Resume if suspended
ALTER TASK task_hourly_health_check RESUME;
ALTER TASK task_daily_quality_report RESUME;
```

---

## When All Else Fails

### Escalation Checklist

**Before escalating:**
- [ ] Reviewed this troubleshooting guide
- [ ] Checked query history for errors
- [ ] Checked task execution history
- [ ] Reviewed recent deployments
- [ ] Searched Confluence/documentation
- [ ] Asked in Slack #ves-data-team

**If still stuck:**

1. **Gather Information:**
   - Exact error message
   - Query ID or task name
   - Steps to reproduce
   - When did it start failing?
   - What changed recently?

2. **Document:**
   - Create Jira ticket with `incident` label
   - Include all gathered information
   - Add screenshots if helpful

3. **Escalate:**
   - Slack: Tag @data-team-lead
   - If after hours: PagerDuty
   - If security issue: Also tag @security

4. **Snowflake Support:**
   - For platform issues
   - Create case: https://community.snowflake.com
   - Include account ID, query ID, error message
   - Response time: <4 hours for Priority 2

### Emergency "Break Glass" Procedures

**Complete System Down (SEV-1):**

1. **Immediately:**
   ```
   @channel SEV-1 INCIDENT
   Issue: [Description]
   Impact: [All users | Specific team | etc]
   Incident Channel: #incident-YYYYMMDD-HHmm
   ```

2. **If Database Unavailable:**
   - See: [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md)
   - Consider activating DR site

3. **If Data Corrupted:**
   ```sql
   -- Use Time Travel to recover
   CREATE TABLE fact_exam_requests_backup CLONE fact_exam_requests
       AT(TIMESTAMP => '<timestamp_before_corruption>');

   -- Verify backup
   SELECT COUNT(*) FROM fact_exam_requests_backup;

   -- Restore
   DROP TABLE fact_exam_requests;
   ALTER TABLE fact_exam_requests_backup RENAME TO fact_exam_requests;
   ```

---

## Appendix: Useful Queries

### Quick Diagnostics

```sql
-- Current running queries
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE execution_status = 'RUNNING'
ORDER BY start_time;

-- Recent failures
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE execution_status = 'FAILED'
  AND start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;

-- Credit usage today
SELECT
    warehouse_name,
    SUM(credits_used) AS credits_today
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= CURRENT_DATE()
GROUP BY warehouse_name;

-- Data freshness check
SELECT
    'fact_exam_requests' AS table_name,
    MAX(created_timestamp) AS last_update,
    DATEDIFF(hour, MAX(created_timestamp), CURRENT_TIMESTAMP()) AS hours_old
FROM VESDW_PRD.warehouse.fact_exam_requests;

-- Task health
SELECT
    name,
    state,
    schedule,
    last_committed_on
FROM TABLE(INFORMATION_SCHEMA.TASKS)
WHERE schema_name = 'WAREHOUSE'
ORDER BY name;
```

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Maintained By:** Data Team
**Feedback:** Submit improvements via PR

**Remember:** When in doubt, ask! The team is here to help. 🚀

---

**END OF TROUBLESHOOTING PLAYBOOK**
