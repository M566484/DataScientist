---
# Snowflake Native Orchestration Guide
## Automate Your Data Pipeline with Tasks, Streams, and Notifications

**Purpose:** This guide shows you how to automate the VES data pipeline using Snowflake's native orchestration features—no external tools required!

**What You'll Learn:**
- Schedule ETL pipelines using Snowflake Tasks
- Create task dependencies (DAG orchestration)
- Set up email and Slack notifications
- Monitor task execution and handle errors
- Use Streams for incremental processing
- Implement retry logic and circuit breakers

---

## Table of Contents

1. [Why Snowflake Native Orchestration?](#why-snowflake-native-orchestration)
2. [Architecture Overview](#architecture-overview)
3. [Setup Guide](#setup-guide)
4. [Task Orchestration Deep Dive](#task-orchestration-deep-dive)
5. [Notification Setup](#notification-setup)
6. [Monitoring and Alerting](#monitoring-and-alerting)
7. [Streams for Incremental Processing](#streams-for-incremental-processing)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [Cost Optimization](#cost-optimization)

---

## Why Snowflake Native Orchestration?

### ✅ Benefits

| Feature | Benefit |
|---------|---------|
| **No External Dependencies** | Everything runs in Snowflake—no Airflow, Luigi, or other orchestrators needed |
| **Simple Deployment** | Just SQL—no Python/Java code, no servers to manage |
| **Native Integration** | Direct access to Snowflake metadata, warehouses, and data |
| **Cost Efficient** | Warehouses auto-suspend when idle; pay only for execution time |
| **Built-in Monitoring** | Task history, error logs, and performance metrics included |
| **Serverless** | Snowflake manages infrastructure, scaling, and failover |

### 🎯 Use Cases

**Perfect For:**
- ✓ Daily/hourly batch processing
- ✓ ETL pipelines with dependencies (staging → warehouse → marts)
- ✓ Automated data quality checks
- ✓ Incremental data loads
- ✓ Scheduled reports and notifications

**Not Ideal For:**
- ✗ Real-time streaming (< 1 minute latency)
- ✗ Complex conditional logic requiring external systems
- ✗ Integration with non-Snowflake systems requiring custom APIs

---

## Architecture Overview

### VES Data Pipeline Task DAG

```
┌─────────────────────────────────────────────────────────────────┐
│  DAILY PIPELINE (Runs at 1:00 AM EST)                           │
└─────────────────────────────────────────────────────────────────┘

 [Root Task]
 task_daily_ods_extraction
 ├─ Extract OMS data → ODS
 ├─ Extract VEMS data → ODS
 └─ Validate data arrival
        │
        │ ✓ Success (triggers child task)
        ▼
 task_daily_staging_layer
 ├─ Build entity crosswalks
 ├─ Merge OMS/VEMS data
 ├─ Calculate DQ scores
 └─ Log conflicts
        │
        │ ✓ Success
        ▼
 task_daily_dimensions
 ├─ Load dim_veteran (SCD Type 2)
 ├─ Load dim_evaluator
 ├─ Load dim_facility
 └─ Load other dimensions
        │
        │ ✓ Success
        ▼
 task_daily_facts
 ├─ Load fact_exam_requests
 ├─ Load fact_evaluations
 ├─ Load fact_appointments
 └─ Load fact_bottlenecks
        │
        │ ✓ Success
        ▼
 task_daily_dq_validation
 ├─ Run validation queries
 ├─ Check SLA breaches
 ├─ Generate summary report
 └─ Send daily email

┌─────────────────────────────────────────────────────────────────┐
│  MONITORING TASK (Runs hourly)                                  │
└─────────────────────────────────────────────────────────────────┘

 task_hourly_monitoring
 ├─ Check for failed tasks
 ├─ Identify long-running tasks
 ├─ Send alerts if issues detected
 └─ Log to metadata table
```

### Task Dependencies Explained

**Parent-Child Relationships:**
- `task_daily_ods_extraction` (ROOT) → No dependencies, runs on schedule
- `task_daily_staging_layer` → Runs AFTER `task_daily_ods_extraction` succeeds
- `task_daily_dimensions` → Runs AFTER `task_daily_staging_layer` succeeds
- `task_daily_facts` → Runs AFTER `task_daily_dimensions` succeeds
- `task_daily_dq_validation` → Runs AFTER `task_daily_facts` succeeds

**Key Concept:** If a parent task fails, child tasks do NOT run.

---

## Setup Guide

### Phase 1: Prerequisites (5 minutes)

#### Step 1: Create Notification Integration

```sql
-- Email notifications (requires ACCOUNTADMIN)
CREATE NOTIFICATION INTEGRATION email_notification_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'data-team@company.com',
    'ops-team@company.com'
  );

-- Grant usage to your role
GRANT USAGE ON INTEGRATION email_notification_int TO ROLE data_engineer;
```

**Note:** Only ACCOUNTADMIN can create notification integrations. If you don't have ACCOUNTADMIN, ask your admin to run this once.

#### Step 2: Create Dedicated Warehouses

```sql
-- ETL task warehouse (auto-suspend after 1 minute)
CREATE WAREHOUSE etl_task_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Heavy processing warehouse (for large fact tables)
CREATE WAREHOUSE etl_heavy_task_wh
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

**Cost Impact:**
- Medium warehouse: ~$4/hour (but only runs during task execution)
- Large warehouse: ~$8/hour
- With auto-suspend = 60, typical daily cost: $5-15

#### Step 3: Create Metadata Tracking Table

```sql
CREATE TABLE VESDW_PRD.metadata.etl_task_log (
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
```

---

### Phase 2: Create Tasks (10 minutes)

#### Task Creation Order

1. Create ALL tasks first (in SUSPENDED state)
2. Resume tasks in REVERSE dependency order (children first)

**Why?** Snowflake requires child tasks to be active before parent tasks can be resumed.

#### Example: Create Root Task

```sql
CREATE OR REPLACE TASK task_daily_ods_extraction
  WAREHOUSE = etl_task_wh
  SCHEDULE = 'USING CRON 0 1 * * * America/New_York'  -- 1 AM EST daily
AS
DECLARE
  v_batch_id VARCHAR;
BEGIN
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  -- Your ETL logic here
  CALL sp_staging_layer_master(:v_batch_id);

  -- Log success
  INSERT INTO VESDW_PRD.metadata.etl_task_log (task_name, batch_id, status, start_time)
  VALUES ('task_daily_ods_extraction', :v_batch_id, 'SUCCESS', CURRENT_TIMESTAMP());

EXCEPTION
  WHEN OTHER THEN
    -- Send alert on failure
    CALL sp_send_email_alert(
      'CRITICAL: ODS Extraction Failed',
      '<p>Error: ' || SQLERRM || '</p>',
      ARRAY_CONSTRUCT('ops-team@company.com')
    );
    RAISE;
END;
```

#### Example: Create Dependent Task

```sql
CREATE OR REPLACE TASK task_daily_staging_layer
  WAREHOUSE = etl_task_wh
  AFTER task_daily_ods_extraction  -- ← Dependency declared here
AS
DECLARE
  v_batch_id VARCHAR;
BEGIN
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  -- Run staging layer
  CALL sp_staging_layer_master(:v_batch_id);
END;
```

**Complete task creation script:** See `snowflake/orchestration/01_snowflake_native_orchestration.sql`

---

### Phase 3: Activate Tasks (2 minutes)

**⚠️ CRITICAL:** Resume tasks in **REVERSE dependency order!**

```sql
-- Resume child tasks first
ALTER TASK task_daily_dq_validation RESUME;      -- ← Deepest child first
ALTER TASK task_daily_facts RESUME;
ALTER TASK task_daily_dimensions RESUME;
ALTER TASK task_daily_staging_layer RESUME;
ALTER TASK task_daily_ods_extraction RESUME;     -- ← Root task LAST

-- Resume monitoring task
ALTER TASK task_hourly_monitoring RESUME;
```

**Verify tasks are active:**

```sql
SHOW TASKS;
-- Look for state = 'started' (not 'suspended')
```

---

### Phase 4: Test Execution (5 minutes)

#### Manual Test (Without Waiting for Schedule)

```sql
-- Execute root task immediately
EXECUTE TASK task_daily_ods_extraction;

-- This will trigger all downstream tasks automatically
```

#### Monitor Execution

```sql
-- Check task history
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(second, scheduled_time, completed_time) AS duration_sec,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;
```

**Expected States:**
- `SCHEDULED` → Task queued to run
- `EXECUTING` → Task currently running
- `SUCCEEDED` → Task completed successfully
- `FAILED` → Task failed (check error_message)
- `SKIPPED` → Parent task failed, so this task didn't run

---

## Task Orchestration Deep Dive

### Schedule Syntax

#### CRON Schedule (Recommended)

```sql
-- Daily at 1:00 AM EST
SCHEDULE = 'USING CRON 0 1 * * * America/New_York'

-- Every 6 hours
SCHEDULE = 'USING CRON 0 */6 * * * America/New_York'

-- Weekdays at 9 AM EST
SCHEDULE = 'USING CRON 0 9 * * 1-5 America/New_York'

-- First day of month at midnight
SCHEDULE = 'USING CRON 0 0 1 * * America/New_York'
```

**CRON Format:** `minute hour day month day_of_week timezone`

#### Interval Schedule (Simple)

```sql
-- Every 60 minutes
SCHEDULE = '60 MINUTE'

-- Every 12 hours
SCHEDULE = '720 MINUTE'
```

**Note:** CRON is more flexible for complex schedules.

### Task Dependencies

#### Linear Dependency (Simple Pipeline)

```sql
CREATE TASK task_a SCHEDULE = '60 MINUTE' AS ...;
CREATE TASK task_b AFTER task_a AS ...;
CREATE TASK task_c AFTER task_b AS ...;

-- Execution: task_a → task_b → task_c
```

#### Multiple Parents (Fan-In)

```sql
CREATE TASK task_a SCHEDULE = '60 MINUTE' AS ...;
CREATE TASK task_b SCHEDULE = '60 MINUTE' AS ...;
CREATE TASK task_c AFTER task_a, task_b AS ...;  -- Waits for BOTH

-- Execution: Both task_a AND task_b must succeed before task_c runs
```

#### Multiple Children (Fan-Out)

```sql
CREATE TASK task_a SCHEDULE = '60 MINUTE' AS ...;
CREATE TASK task_b AFTER task_a AS ...;
CREATE TASK task_c AFTER task_a AS ...;

-- Execution: task_a → (task_b AND task_c run in parallel)
```

#### Complex DAG

```sql
CREATE TASK task_root SCHEDULE = '60 MINUTE' AS ...;
CREATE TASK task_staging AFTER task_root AS ...;
CREATE TASK task_dim_1 AFTER task_staging AS ...;
CREATE TASK task_dim_2 AFTER task_staging AS ...;
CREATE TASK task_fact AFTER task_dim_1, task_dim_2 AS ...;

-- Execution:
--   task_root
--       ↓
--   task_staging
--       ↓
--   task_dim_1 ← → task_dim_2  (parallel)
--       ↓           ↓
--       └─→ task_fact ←┘  (waits for both)
```

### Error Handling

#### Automatic Retry on Transient Errors

Snowflake automatically retries tasks for transient errors (network issues, warehouse startup, etc.):
- Default: 3 retries
- Retry delay: Exponential backoff

#### Manual Error Handling

```sql
CREATE TASK my_task
  WAREHOUSE = etl_task_wh
  SCHEDULE = '60 MINUTE'
AS
DECLARE
  v_retry_count INT DEFAULT 0;
  v_max_retries INT DEFAULT 3;
BEGIN
  WHILE (v_retry_count < v_max_retries) DO
    BEGIN
      -- Your ETL logic here
      CALL sp_staging_layer_master('BATCH_001');

      -- Exit loop if successful
      v_retry_count := v_max_retries;

    EXCEPTION
      WHEN OTHER THEN
        v_retry_count := v_retry_count + 1;

        IF (v_retry_count >= v_max_retries) THEN
          -- Send alert after final retry
          CALL sp_send_email_alert(
            'CRITICAL: Task Failed After ' || :v_max_retries || ' Retries',
            '<p>Error: ' || SQLERRM || '</p>',
            ARRAY_CONSTRUCT('ops-team@company.com')
          );
          RAISE;
        ELSE
          -- Wait before retry (Snowflake doesn't have SLEEP, so use CALL SYSTEM$WAIT)
          CALL SYSTEM$WAIT(60);  -- Wait 60 seconds
        END IF;
    END;
  END WHILE;
END;
```

#### Circuit Breaker Pattern

```sql
-- Stop pipeline if too many failures
DECLARE
  v_recent_failures INT;
BEGIN
  -- Check failure rate
  SELECT COUNT(*)
  INTO :v_recent_failures
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  WHERE name = 'task_daily_ods_extraction'
    AND scheduled_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    AND state = 'FAILED';

  -- Circuit breaker: Stop if >3 failures in 24h
  IF (v_recent_failures > 3) THEN
    CALL sp_send_email_alert(
      'CRITICAL: Circuit Breaker Triggered',
      '<p>Task has failed ' || :v_recent_failures || ' times in 24 hours. Manual intervention required.</p>',
      ARRAY_CONSTRUCT('ops-team@company.com')
    );

    -- Suspend task to prevent more failures
    ALTER TASK task_daily_ods_extraction SUSPEND;

    RAISE EXCEPTION 'Circuit breaker triggered';
  END IF;

  -- Continue with task logic...
END;
```

---

## Notification Setup

### Email Notifications

#### Setup Procedure

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
        'text/html'  -- Supports HTML
    );
    RETURN 'Email sent';
END;
$$;
```

#### Usage Examples

**Simple Alert:**
```sql
CALL sp_send_email_alert(
    'Pipeline Alert',
    'Task failed. Please investigate.',
    ARRAY_CONSTRUCT('ops-team@company.com')
);
```

**HTML Formatted Email:**
```sql
CALL sp_send_email_alert(
    'Daily Pipeline Summary',
    '<html><body>' ||
    '<h2>VES Pipeline Summary</h2>' ||
    '<table border="1">' ||
    '<tr><th>Metric</th><th>Value</th></tr>' ||
    '<tr><td>Records Processed</td><td>52,000</td></tr>' ||
    '<tr><td>DQ Score</td><td style="color:green">87.5</td></tr>' ||
    '</table>' ||
    '</body></html>',
    ARRAY_CONSTRUCT('data-team@company.com')
);
```

**Dynamic Content:**
```sql
DECLARE
  v_record_count INT;
  v_dq_score FLOAT;
  v_html VARCHAR;
BEGIN
  SELECT COUNT(*), AVG(dq_score)
  INTO :v_record_count, :v_dq_score
  FROM VESDW_PRD.staging.stg_veterans;

  v_html := '<h2>Results</h2>' ||
            '<p>Records: ' || :v_record_count || '</p>' ||
            '<p>DQ Score: ' || ROUND(:v_dq_score, 2) || '</p>';

  CALL sp_send_email_alert('Pipeline Report', :v_html, ARRAY_CONSTRUCT('team@company.com'));
END;
```

### Slack Notifications

#### Setup

1. **Create Slack Incoming Webhook:**
   - Go to https://api.slack.com/apps
   - Create new app or select existing
   - Enable "Incoming Webhooks"
   - Create webhook for desired channel
   - Copy webhook URL

2. **Create Snowflake Integration:**

```sql
CREATE NOTIFICATION INTEGRATION slack_webhook_int
  TYPE = QUEUE
  ENABLED = TRUE
  NOTIFICATION_PROVIDER = WEBHOOK
  DIRECTION = OUTBOUND
  WEBHOOK_URL = 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL';
```

3. **Create Slack Notification Procedure:**

```sql
CREATE OR REPLACE PROCEDURE sp_send_slack_alert(
    p_message VARCHAR,
    p_channel VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
        'slack_webhook_int',
        OBJECT_CONSTRUCT(
            'text', :p_message,
            'channel', :p_channel
        )
    );
    RETURN 'Slack message sent';
END;
$$;
```

4. **Usage:**

```sql
CALL sp_send_slack_alert(
    ':rotating_light: ALERT: Pipeline failed! Check logs immediately.',
    '#data-ops'
);
```

**Advanced Slack Formatting:**

```sql
CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
    'slack_webhook_int',
    OBJECT_CONSTRUCT(
        'text', 'Pipeline Update',
        'blocks', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'type', 'header',
                'text', OBJECT_CONSTRUCT('type', 'plain_text', 'text', 'VES Pipeline Alert')
            ),
            OBJECT_CONSTRUCT(
                'type', 'section',
                'fields', ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT('type', 'mrkdwn', 'text', '*Status:*\nFailed'),
                    OBJECT_CONSTRUCT('type', 'mrkdwn', 'text', '*Batch:*\nBATCH_20251117_001')
                )
            )
        )
    )
);
```

### Microsoft Teams Notifications

Similar to Slack—create Incoming Webhook connector in Teams, then use same procedure pattern.

---

## Monitoring and Alerting

### Built-in Monitoring Views

#### Task History

```sql
-- Recent task runs
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(second, scheduled_time, completed_time) AS duration_sec,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;
```

#### Task Dependency Graph

```sql
-- View all tasks and their dependencies
SELECT
    name,
    state,
    schedule,
    predecessors,
    warehouse
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'task_daily_ods_extraction',
    RECURSIVE => TRUE
));
```

### Custom Monitoring Views

#### Failed Tasks (Last 24h)

```sql
CREATE OR REPLACE VIEW vw_failed_tasks_24h AS
SELECT
    name AS task_name,
    state,
    scheduled_time,
    error_code,
    error_message,
    query_id  -- Use to debug with QUERY_HISTORY
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE state = 'FAILED'
  AND scheduled_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;
```

#### Long-Running Tasks

```sql
CREATE OR REPLACE VIEW vw_long_running_tasks AS
SELECT
    name AS task_name,
    state,
    scheduled_time,
    DATEDIFF(minute, scheduled_time, COALESCE(completed_time, CURRENT_TIMESTAMP())) AS duration_minutes,
    CASE
        WHEN DATEDIFF(minute, scheduled_time, COALESCE(completed_time, CURRENT_TIMESTAMP())) > 60 THEN 'CRITICAL'
        WHEN DATEDIFF(minute, scheduled_time, COALESCE(completed_time, CURRENT_TIMESTAMP())) > 30 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS alert_level
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY duration_minutes DESC;
```

#### Task Success Rate

```sql
CREATE OR REPLACE VIEW vw_task_success_rate AS
SELECT
    name AS task_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct,
    AVG(DATEDIFF(second, scheduled_time, completed_time)) AS avg_duration_seconds
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY name
ORDER BY success_rate_pct;
```

### Automated Alerting Task

```sql
CREATE OR REPLACE TASK task_hourly_monitoring
  WAREHOUSE = etl_task_wh
  SCHEDULE = 'USING CRON 0 * * * * America/New_York'  -- Every hour
AS
DECLARE
  v_failed_count INT;
BEGIN
  -- Check for failures
  SELECT COUNT(*)
  INTO :v_failed_count
  FROM vw_failed_tasks_24h
  WHERE scheduled_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP());

  -- Alert if failures detected
  IF (v_failed_count > 0) THEN
    CALL sp_send_email_alert(
      'ALERT: ' || :v_failed_count || ' Task(s) Failed',
      '<p>Check failed tasks: SELECT * FROM vw_failed_tasks_24h;</p>',
      ARRAY_CONSTRUCT('ops-team@company.com')
    );
  END IF;
END;

-- Activate monitoring
ALTER TASK task_hourly_monitoring RESUME;
```

---

## Streams for Incremental Processing

### What Are Streams?

**Streams** capture change data (inserts, updates, deletes) from a table, enabling incremental processing instead of full reloads.

### Use Case: Incremental Staging Layer

Instead of processing all ODS data daily, only process new/changed records:

#### Setup Stream

```sql
-- Create stream on ODS veterans table
CREATE OR REPLACE STREAM stream_ods_veterans_changes
  ON TABLE VESODS_PRDDATA_PRD.ods_veterans_source
  APPEND_ONLY = FALSE;  -- Track inserts, updates, deletes
```

#### Task Using Stream

```sql
CREATE OR REPLACE TASK task_incremental_staging
  WAREHOUSE = etl_task_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_ods_veterans_changes')  -- Only run if changes exist
AS
BEGIN
  -- Process only changed records
  MERGE INTO VESDW_PRD.staging.stg_veterans tgt
  USING stream_ods_veterans_changes src
  ON tgt.veteran_ssn = src.veteran_ssn
  WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' THEN
    DELETE
  WHEN MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    UPDATE SET
      first_name = src.first_name,
      last_name = src.last_name,
      updated_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    INSERT (veteran_ssn, first_name, last_name, created_timestamp)
    VALUES (src.veteran_ssn, src.first_name, src.last_name, CURRENT_TIMESTAMP());
END;

ALTER TASK task_incremental_staging RESUME;
```

**Benefits:**
- ✓ Only process changed data (faster, cheaper)
- ✓ Task only runs when changes exist (`WHEN SYSTEM$STREAM_HAS_DATA`)
- ✓ Exactly-once processing (stream offset advances on task success)

---

## Best Practices

### 1. Task Design

✅ **Do:**
- Keep tasks focused (one logical step per task)
- Use descriptive task names (`task_daily_staging_layer`, not `task_2`)
- Log start/end/status to metadata table
- Include error handling in every task
- Use warehouse auto-suspend to minimize costs

❌ **Don't:**
- Create circular dependencies (task A → task B → task A)
- Put complex business logic in task SQL (use stored procedures instead)
- Forget to resume tasks in reverse dependency order
- Run CPU-intensive operations on X-Small warehouses

### 2. Error Handling

✅ **Always Include:**
```sql
EXCEPTION
  WHEN OTHER THEN
    -- Log error
    INSERT INTO error_log (task_name, error, timestamp)
    VALUES ('my_task', SQLERRM, CURRENT_TIMESTAMP());

    -- Send notification
    CALL sp_send_email_alert('Task Failed', SQLERRM, ...);

    -- Re-raise to mark task as failed
    RAISE;
```

### 3. Notification Strategy

| Severity | Notification Method | Recipients | Example |
|----------|-------------------|------------|---------|
| **CRITICAL** | Immediate email + Slack | Ops team, on-call | Task failed, data not loaded |
| **WARNING** | Email (batched hourly) | Data team | Low DQ score, long-running task |
| **INFO** | Daily summary email | Data team, stakeholders | Pipeline completed successfully |

### 4. Warehouse Sizing

| Task Type | Warehouse Size | Example |
|-----------|----------------|---------|
| Validation queries | X-SMALL | Check row counts, DQ scores |
| Entity staging (1K-100K rows) | SMALL | Load veterans, evaluators |
| Full staging layer | MEDIUM | OMS/VEMS merge |
| Large facts (>1M rows) | LARGE | fact_exam_requests |
| Aggregations, complex joins | X-LARGE | Bottleneck analysis, rollups |

**Tip:** Start small, scale up if tasks timeout.

### 5. Scheduling

✅ **Best Times:**
- **1:00-3:00 AM:** Batch processing (low user activity)
- **Hourly (:00):** Incremental loads
- **Every 5 min:** Near-real-time monitoring

❌ **Avoid:**
- Running heavy tasks during business hours (9 AM - 5 PM)
- Overlapping schedules (ensure task completes before next run)

---

## Troubleshooting

### Problem 1: Task Not Running

**Symptoms:**
```sql
SHOW TASKS;
-- Shows: state = 'suspended'
```

**Solution:**
```sql
-- Resume task (and all dependencies)
ALTER TASK task_daily_dq_validation RESUME;
ALTER TASK task_daily_facts RESUME;
ALTER TASK task_daily_dimensions RESUME;
ALTER TASK task_daily_staging_layer RESUME;
ALTER TASK task_daily_ods_extraction RESUME;  -- Resume root LAST
```

---

### Problem 2: Task Fails Immediately

**Symptoms:**
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE state = 'FAILED';
-- Shows: error_code, error_message
```

**Common Causes:**

| Error | Cause | Solution |
|-------|-------|----------|
| `SQL compilation error` | Syntax error in task SQL | Fix SQL, recreate task |
| `Object does not exist` | Table/procedure not found | Verify object exists, check permissions |
| `Insufficient privileges` | Role lacks permissions | `GRANT USAGE ON WAREHOUSE ... TO ROLE` |
| `Warehouse does not exist` | Warehouse deleted/renamed | Recreate warehouse or update task |

**Debug Steps:**
```sql
-- Get query ID from task history
SELECT query_id FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = 'task_daily_staging_layer' ORDER BY scheduled_time DESC LIMIT 1;

-- View full error details
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_id = '<query_id_from_above>';
```

---

### Problem 3: Task Runs But Produces No Results

**Symptoms:** Task shows `SUCCEEDED` but no data in target table.

**Debug:**
```sql
-- Check task SQL
SHOW TASKS LIKE 'task_daily_staging_layer';
-- Review 'definition' column

-- Check if procedure was called
SELECT * FROM VESDW_PRD.metadata.etl_task_log
WHERE task_name = 'task_daily_staging_layer'
ORDER BY start_time DESC;

-- Check warehouse activity
SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
  WAREHOUSE_NAME => 'etl_task_wh',
  START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())
));
```

---

### Problem 4: High Costs

**Symptoms:** Snowflake bill is higher than expected.

**Investigate:**
```sql
-- Check warehouse usage by task
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 3 AS estimated_cost_usd  -- Assuming $3/credit
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD(month, -1, CURRENT_TIMESTAMP())
))
WHERE warehouse_name IN ('etl_task_wh', 'etl_heavy_task_wh')
GROUP BY warehouse_name;

-- Check if warehouses are auto-suspending
SHOW WAREHOUSES;
-- Verify: auto_suspend = 60, auto_resume = TRUE
```

**Optimization:**
- Lower warehouse size if possible
- Reduce `AUTO_SUSPEND` time (default 60 seconds is good)
- Use streams for incremental processing
- Consolidate small tasks into one task

---

## Cost Optimization

### Estimated Costs

**Example Daily Pipeline:**

| Task | Warehouse Size | Duration | Cost |
|------|----------------|----------|------|
| ODS Extraction | MEDIUM | 5 min | $0.33 |
| Staging Layer | MEDIUM | 10 min | $0.67 |
| Dimensions | MEDIUM | 5 min | $0.33 |
| Facts | LARGE | 15 min | $2.00 |
| DQ Validation | SMALL | 2 min | $0.13 |
| **Total** | | **37 min** | **$3.46/day** |

**Monthly:** ~$104

**Cost Optimization Tips:**
1. ✓ Use smallest warehouse that meets SLA
2. ✓ Set `AUTO_SUSPEND = 60` (suspend after 1 minute idle)
3. ✓ Use streams for incremental loads
4. ✓ Consolidate small tasks
5. ✓ Schedule during off-peak hours (no cost benefit, but better for users)

---

## Quick Reference

### Essential Commands

```sql
-- Create task
CREATE TASK my_task
  WAREHOUSE = etl_task_wh
  SCHEDULE = 'USING CRON 0 1 * * * America/New_York'
AS
  CALL my_stored_procedure();

-- Resume task (activate)
ALTER TASK my_task RESUME;

-- Suspend task (deactivate)
ALTER TASK my_task SUSPEND;

-- Execute task manually
EXECUTE TASK my_task;

-- View task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = 'my_task'
ORDER BY scheduled_time DESC;

-- View task definition
SHOW TASKS LIKE 'my_task';

-- Send email
CALL sp_send_email_alert(
  'Subject',
  'Message',
  ARRAY_CONSTRUCT('email@company.com')
);
```

---

## Next Steps

1. ✓ Review orchestration SQL: `snowflake/orchestration/01_snowflake_native_orchestration.sql`
2. ✓ Set up notification integrations
3. ✓ Create and test tasks in development environment
4. ✓ Deploy to production and activate tasks
5. ✓ Monitor daily with `vw_failed_tasks_24h` and `vw_task_success_rate`

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Author:** Data Team
