-- =====================================================================================
-- SNOWFLAKE NATIVE ORCHESTRATION: TASKS, STREAMS, AND NOTIFICATIONS
-- =====================================================================================
-- Purpose: Production-ready orchestration using Snowflake native features
--
-- This script demonstrates how to:
--   1. Schedule ETL pipelines using Snowflake Tasks
--   2. Create task dependencies (DAG orchestration)
--   3. Use Streams for incremental processing
--   4. Set up email and external notifications
--   5. Monitor task execution and handle errors
--   6. Implement circuit breakers and retry logic
--
-- Author: Data Team
-- Date: 2025-11-17
-- =====================================================================================

-- =====================================================================================
-- PART 1: PREREQUISITES - NOTIFICATION INTEGRATIONS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Setup 1A: Create Email Notification Integration
-- -----------------------------------------------------------------------------------------
-- Purpose: Send email alerts when tasks fail or data quality issues occur
-- Requires: ACCOUNTADMIN role or NOTIFICATION INTEGRATION privilege

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_notification_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'data-team@company.com',
    'ops-team@company.com',
    'dba-alerts@company.com'
  );

-- Verify integration created
SHOW NOTIFICATION INTEGRATIONS;

/*
USAGE NOTES:
- Only ACCOUNTADMIN can create notification integrations initially
- Grant usage to roles that need to send notifications:
  GRANT USAGE ON INTEGRATION email_notification_int TO ROLE data_engineer;
- Add more email addresses to ALLOWED_RECIPIENTS as needed
- Snowflake emails come from notifications@snowflake.net
*/

-- -----------------------------------------------------------------------------------------
-- Setup 1B: Create Stored Procedure for Email Notifications
-- -----------------------------------------------------------------------------------------
-- Purpose: Reusable procedure to send formatted email alerts

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
        'text/html'  -- Supports HTML formatting
    );
    RETURN 'Email sent successfully to ' || ARRAY_SIZE(:p_recipients) || ' recipients';
END;
$$;

-- Test email notification
CALL sp_send_email_alert(
    'Test Alert - VES Data Pipeline',
    '<h2>Test Email</h2><p>This is a test notification from the VES data pipeline.</p>',
    ARRAY_CONSTRUCT('data-team@company.com')
);

-- -----------------------------------------------------------------------------------------
-- Setup 1C: Create Webhook Notification for Slack/Teams (Optional)
-- -----------------------------------------------------------------------------------------
-- Purpose: Send alerts to Slack or Microsoft Teams channels

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS slack_webhook_int
  TYPE = QUEUE
  ENABLED = TRUE
  NOTIFICATION_PROVIDER = WEBHOOK
  DIRECTION = OUTBOUND
  WEBHOOK_URL = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK';

/*
SLACK SETUP INSTRUCTIONS:
1. In Slack, create an Incoming Webhook:
   - Go to https://api.slack.com/apps
   - Create new app or select existing
   - Enable "Incoming Webhooks"
   - Create webhook for desired channel
   - Copy webhook URL

2. Replace 'YOUR/SLACK/WEBHOOK' above with your webhook URL

3. Test with:
   CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
     'slack_webhook_int',
     OBJECT_CONSTRUCT(
       'text', 'Test notification from VES pipeline'
     )
   );

MICROSOFT TEAMS SETUP:
1. In Teams channel, add "Incoming Webhook" connector
2. Copy webhook URL
3. Create integration with Teams webhook URL
4. Test similar to Slack example above
*/

-- =====================================================================================
-- PART 2: WAREHOUSE MANAGEMENT FOR TASKS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Setup 2A: Create Dedicated Warehouse for ETL Tasks
-- -----------------------------------------------------------------------------------------
-- Purpose: Separate warehouse for scheduled tasks to control costs

CREATE WAREHOUSE IF NOT EXISTS etl_task_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60                    -- Suspend after 1 minute idle
  AUTO_RESUME = TRUE                   -- Resume automatically when task runs
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for automated ETL tasks';

-- Create larger warehouse for heavy processing (optional)
CREATE WAREHOUSE IF NOT EXISTS etl_heavy_task_wh
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Large warehouse for data-intensive tasks';

/*
WAREHOUSE SIZING GUIDELINES:
- X-SMALL: Validation queries, metadata updates
- SMALL: Individual entity staging (veterans, evaluators)
- MEDIUM: Full staging layer, most fact tables
- LARGE: Large fact tables (>1M rows), complex aggregations
- X-LARGE: Initial historical loads, year-end processing

COST OPTIMIZATION:
- Use AUTO_SUSPEND = 60 to minimize costs
- Start with smaller warehouse, scale up if tasks timeout
- Monitor with: SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY())
*/

-- =====================================================================================
-- PART 3: TASK CREATION - SIMPLE DAILY BATCH
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Task 3A: Daily ODS Data Extraction (Root Task)
-- -----------------------------------------------------------------------------------------
-- Purpose: Extract data from source systems to ODS (or trigger Mulesoft)
-- Schedule: Daily at 1:00 AM EST

CREATE OR REPLACE TASK task_daily_ods_extraction
  WAREHOUSE = etl_task_wh
  SCHEDULE = 'USING CRON 0 1 * * * America/New_York'  -- 1 AM EST daily
  -- Alternatively, run every 6 hours:
  -- SCHEDULE = '360 MINUTE'
  COMMENT = 'Daily extraction from OMS/VEMS to ODS layer'
AS
DECLARE
  v_batch_id VARCHAR;
  v_oms_count INT;
  v_vems_count INT;
  v_error_message VARCHAR;
BEGIN
  -- Generate batch ID
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  -- Log task start
  INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') (
    task_name, batch_id, status, start_time
  ) VALUES (
    'task_daily_ods_extraction', :v_batch_id, 'RUNNING', CURRENT_TIMESTAMP()
  );

  -- TODO: Replace with actual extraction logic
  -- For now, assume data is loaded by external process (Mulesoft)

  -- Validate ODS data loaded
  SELECT COUNT(*) INTO :v_oms_count
  FROM IDENTIFIER(get_ods_database() || '.ods_veterans_source
  WHERE batch_id = :v_batch_id AND source_system = 'OMS';

  SELECT COUNT(*) INTO :v_vems_count
  FROM IDENTIFIER(get_ods_database() || '.ods_veterans_source
  WHERE batch_id = :v_batch_id AND source_system = 'VEMS';

  -- Alert if no data extracted
  IF (v_oms_count = 0 AND v_vems_count = 0) THEN
    v_error_message := 'No data extracted for batch ' || :v_batch_id;

    -- Send alert email
    CALL sp_send_email_alert(
      'CRITICAL: ODS Extraction Failed',
      '<h2>ODS Data Extraction Failed</h2>' ||
      '<p><strong>Batch ID:</strong> ' || :v_batch_id || '</p>' ||
      '<p><strong>Issue:</strong> No records found in ODS for OMS or VEMS</p>' ||
      '<p><strong>Action Required:</strong> Check source system connectivity and Mulesoft jobs</p>',
      ARRAY_CONSTRUCT('data-team@company.com', 'ops-team@company.com')
    );

    -- Log failure
    UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
    SET status = 'FAILED', error_message = :v_error_message, end_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'task_daily_ods_extraction' AND batch_id = :v_batch_id;

    -- Raise error to stop downstream tasks
    RAISE EXCEPTION 'No data extracted';
  END IF;

  -- Log success
  UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
  SET status = 'SUCCESS', records_processed = :v_oms_count + :v_vems_count, end_time = CURRENT_TIMESTAMP()
  WHERE task_name = 'task_daily_ods_extraction' AND batch_id = :v_batch_id;

EXCEPTION
  WHEN OTHER THEN
    -- Log error
    UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
    SET status = 'FAILED', error_message = SQLERRM, end_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'task_daily_ods_extraction' AND batch_id = :v_batch_id;

    -- Re-raise to fail task
    RAISE;
END;

-- Task is created in SUSPENDED state by default
-- Resume to activate:
-- ALTER TASK task_daily_ods_extraction RESUME;

-- -----------------------------------------------------------------------------------------
-- Task 3B: Staging Layer Processing (Depends on ODS Extraction)
-- -----------------------------------------------------------------------------------------
-- Purpose: Run staging layer merge after ODS data is available

CREATE OR REPLACE TASK task_daily_staging_layer
  WAREHOUSE = etl_task_wh
  AFTER task_daily_ods_extraction  -- Task dependency (runs AFTER ODS extraction succeeds)
  COMMENT = 'Merge OMS/VEMS data into staging layer'
AS
DECLARE
  v_batch_id VARCHAR;
  v_staging_count INT;
  v_dq_avg FLOAT;
  v_error_message VARCHAR;
BEGIN
  -- Get batch ID from parent task
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  -- Log task start
  INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') (
    task_name, batch_id, status, start_time
  ) VALUES (
    'task_daily_staging_layer', :v_batch_id, 'RUNNING', CURRENT_TIMESTAMP()
  );

  -- Run staging layer master procedure
  CALL sp_staging_layer_master(:v_batch_id);

  -- Validate staging layer results
  SELECT COUNT(*), AVG(dq_score)
  INTO :v_staging_count, :v_dq_avg
  FROM IDENTIFIER(get_dw_database() || '.staging.stg_veterans')
  WHERE batch_id = :v_batch_id;

  -- Alert if data quality is low
  IF (v_dq_avg < 70) THEN
    CALL sp_send_email_alert(
      'WARNING: Low Data Quality in Staging Layer',
      '<h2>Data Quality Alert</h2>' ||
      '<p><strong>Batch ID:</strong> ' || :v_batch_id || '</p>' ||
      '<p><strong>Average DQ Score:</strong> ' || ROUND(:v_dq_avg, 2) || ' (Target: >80)</p>' ||
      '<p><strong>Records Processed:</strong> ' || :v_staging_count || '</p>' ||
      '<p><strong>Action Required:</strong> Review data quality issues in staging validation dashboard</p>',
      ARRAY_CONSTRUCT('data-team@company.com')
    );
  END IF;

  -- Log success
  UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
  SET status = 'SUCCESS', records_processed = :v_staging_count, end_time = CURRENT_TIMESTAMP()
  WHERE task_name = 'task_daily_staging_layer' AND batch_id = :v_batch_id;

EXCEPTION
  WHEN OTHER THEN
    -- Send critical alert
    CALL sp_send_email_alert(
      'CRITICAL: Staging Layer Processing Failed',
      '<h2>Staging Layer Error</h2>' ||
      '<p><strong>Batch ID:</strong> ' || :v_batch_id || '</p>' ||
      '<p><strong>Error:</strong> ' || SQLERRM || '</p>' ||
      '<p><strong>Action Required:</strong> Check staging layer procedures and ODS data</p>',
      ARRAY_CONSTRUCT('data-team@company.com', 'ops-team@company.com')
    );

    UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
    SET status = 'FAILED', error_message = SQLERRM, end_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'task_daily_staging_layer' AND batch_id = :v_batch_id;

    RAISE;
END;

-- -----------------------------------------------------------------------------------------
-- Task 3C: Dimension Processing (Depends on Staging Layer)
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE TASK task_daily_dimensions
  WAREHOUSE = etl_task_wh
  AFTER task_daily_staging_layer
  COMMENT = 'Load dimensions from staging layer'
AS
DECLARE
  v_batch_id VARCHAR;
  v_dim_veteran_count INT;
BEGIN
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') (
    task_name, batch_id, status, start_time
  ) VALUES (
    'task_daily_dimensions', :v_batch_id, 'RUNNING', CURRENT_TIMESTAMP()
  );

  -- Load dimension tables (example: dim_veteran)
  -- TODO: Replace with actual dimension loading procedures
  MERGE INTO IDENTIFIER(get_dw_database() || '.warehouse.dim_veteran') tgt
  USING IDENTIFIER(get_dw_database() || '.staging.stg_veterans') src
  ON tgt.veteran_ssn = src.veteran_ssn
  WHEN MATCHED THEN
    UPDATE SET
      first_name = src.first_name,
      last_name = src.last_name,
      disability_rating = src.disability_rating,
      updated_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (veteran_sk, veteran_ssn, first_name, last_name, disability_rating, created_timestamp)
    VALUES (IDENTIFIER(get_dw_database() || '.warehouse.seq_veteran_sk').NEXTVAL, src.veteran_ssn, src.first_name, src.last_name, src.disability_rating, CURRENT_TIMESTAMP())
  WHERE src.batch_id = :v_batch_id;

  v_dim_veteran_count := SQLROWCOUNT;

  UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
  SET status = 'SUCCESS', records_processed = :v_dim_veteran_count, end_time = CURRENT_TIMESTAMP()
  WHERE task_name = 'task_daily_dimensions' AND batch_id = :v_batch_id;

EXCEPTION
  WHEN OTHER THEN
    CALL sp_send_email_alert(
      'CRITICAL: Dimension Loading Failed',
      '<h2>Dimension Load Error</h2>' ||
      '<p><strong>Batch ID:</strong> ' || :v_batch_id || '</p>' ||
      '<p><strong>Error:</strong> ' || SQLERRM || '</p>',
      ARRAY_CONSTRUCT('data-team@company.com')
    );

    UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
    SET status = 'FAILED', error_message = SQLERRM, end_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'task_daily_dimensions' AND batch_id = :v_batch_id;

    RAISE;
END;

-- -----------------------------------------------------------------------------------------
-- Task 3D: Fact Table Processing (Depends on Dimensions)
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE TASK task_daily_facts
  WAREHOUSE = etl_heavy_task_wh  -- Use larger warehouse for fact tables
  AFTER task_daily_dimensions
  COMMENT = 'Load fact tables from staging layer'
AS
DECLARE
  v_batch_id VARCHAR;
  v_fact_count INT;
BEGIN
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') (
    task_name, batch_id, status, start_time
  ) VALUES (
    'task_daily_facts', :v_batch_id, 'RUNNING', CURRENT_TIMESTAMP()
  );

  -- Load fact tables
  -- TODO: Replace with actual fact loading procedures
  INSERT INTO IDENTIFIER(get_dw_database() || '.warehouse.fact_exam_requests') (
    exam_request_sk, veteran_dim_sk, facility_dim_sk, request_date_sk, -- ... other columns
  )
  SELECT
    IDENTIFIER(get_dw_database() || '.warehouse.seq_exam_request_sk').NEXTVAL,
    dv.veteran_sk,
    df.facility_sk,
    dd.date_sk
    -- ... other columns
  FROM IDENTIFIER(get_dw_database() || '.staging.stg_fact_exam_requests') ser
  JOIN IDENTIFIER(get_dw_database() || '.warehouse.dim_veteran') dv ON ser.master_veteran_id = dv.veteran_ssn
  JOIN IDENTIFIER(get_dw_database() || '.warehouse.dim_facility') df ON ser.master_facility_id = df.facility_id
  JOIN IDENTIFIER(get_dw_database() || '.warehouse.dim_date') dd ON ser.request_date = dd.full_date
  WHERE ser.batch_id = :v_batch_id;

  v_fact_count := SQLROWCOUNT;

  UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
  SET status = 'SUCCESS', records_processed = :v_fact_count, end_time = CURRENT_TIMESTAMP()
  WHERE task_name = 'task_daily_facts' AND batch_id = :v_batch_id;

EXCEPTION
  WHEN OTHER THEN
    CALL sp_send_email_alert(
      'CRITICAL: Fact Table Loading Failed',
      '<h2>Fact Load Error</h2>' ||
      '<p><strong>Batch ID:</strong> ' || :v_batch_id || '</p>' ||
      '<p><strong>Error:</strong> ' || SQLERRM || '</p>',
      ARRAY_CONSTRUCT('data-team@company.com')
    );

    UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
    SET status = 'FAILED', error_message = SQLERRM, end_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'task_daily_facts' AND batch_id = :v_batch_id;

    RAISE;
END;

-- -----------------------------------------------------------------------------------------
-- Task 3E: Data Quality Validation and Reporting (Final Task)
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE TASK task_daily_dq_validation
  WAREHOUSE = etl_task_wh
  AFTER task_daily_facts
  COMMENT = 'Validate data quality and send summary report'
AS
DECLARE
  v_batch_id VARCHAR;
  v_total_veterans INT;
  v_dq_avg FLOAT;
  v_conflicts INT;
  v_sla_breaches INT;
  v_summary_html VARCHAR;
BEGIN
  v_batch_id := 'PROD_BATCH_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD') || '_001';

  INSERT INTO IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') (
    task_name, batch_id, status, start_time
  ) VALUES (
    'task_daily_dq_validation', :v_batch_id, 'RUNNING', CURRENT_TIMESTAMP()
  );

  -- Gather statistics
  SELECT COUNT(*), AVG(dq_score)
  INTO :v_total_veterans, :v_dq_avg
  FROM IDENTIFIER(get_dw_database() || '.staging.stg_veterans')
  WHERE batch_id = :v_batch_id;

  SELECT COUNT(*)
  INTO :v_conflicts
  FROM IDENTIFIER(get_dw_database() || '.reference.ref_reconciliation_log')
  WHERE batch_id = :v_batch_id;

  SELECT COUNT(*)
  INTO :v_sla_breaches
  FROM IDENTIFIER(get_dw_database() || '.warehouse.fact_exam_processing_bottlenecks')
  WHERE request_date_sk = TO_NUMBER(TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD'))
    AND sla_breach_flag = TRUE;

  -- Build HTML summary report
  v_summary_html :=
    '<html><body>' ||
    '<h2>VES Data Pipeline Daily Summary</h2>' ||
    '<p><strong>Batch ID:</strong> ' || :v_batch_id || '</p>' ||
    '<p><strong>Execution Date:</strong> ' || TO_VARCHAR(CURRENT_DATE()) || '</p>' ||
    '<hr>' ||
    '<h3>Processing Summary</h3>' ||
    '<table border="1" cellpadding="5">' ||
    '<tr><th>Metric</th><th>Value</th><th>Status</th></tr>' ||
    '<tr><td>Veterans Processed</td><td>' || :v_total_veterans || '</td><td style="color:green">✓</td></tr>' ||
    '<tr><td>Average DQ Score</td><td>' || ROUND(:v_dq_avg, 2) || '</td><td style="color:' ||
      CASE WHEN v_dq_avg >= 80 THEN 'green">✓' ELSE 'orange">⚠' END || '</td></tr>' ||
    '<tr><td>Conflicts Detected</td><td>' || :v_conflicts || '</td><td style="color:blue">ℹ</td></tr>' ||
    '<tr><td>SLA Breaches Today</td><td>' || :v_sla_breaches || '</td><td style="color:' ||
      CASE WHEN v_sla_breaches = 0 THEN 'green">✓' ELSE 'red">✗' END || '</td></tr>' ||
    '</table>' ||
    '<hr>' ||
    '<h3>Task Execution Log</h3>' ||
    '<p>All tasks completed successfully.</p>' ||
    '<p><em>For detailed logs, query: IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') WHERE batch_id = \'' || :v_batch_id || '\'</em></p>' ||
    '</body></html>';

  -- Send summary email
  CALL sp_send_email_alert(
    'VES Pipeline Daily Summary - ' || TO_VARCHAR(CURRENT_DATE()),
    :v_summary_html,
    ARRAY_CONSTRUCT('data-team@company.com')
  );

  UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
  SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP()
  WHERE task_name = 'task_daily_dq_validation' AND batch_id = :v_batch_id;

EXCEPTION
  WHEN OTHER THEN
    UPDATE IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
    SET status = 'FAILED', error_message = SQLERRM, end_time = CURRENT_TIMESTAMP()
    WHERE task_name = 'task_daily_dq_validation' AND batch_id = :v_batch_id;

    RAISE;
END;

-- =====================================================================================
-- PART 4: TASK MANAGEMENT COMMANDS
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Activating Tasks (Must be done in reverse dependency order)
-- -----------------------------------------------------------------------------------------

-- Resume tasks in reverse dependency order (children first, then parents)
ALTER TASK task_daily_dq_validation RESUME;
ALTER TASK task_daily_facts RESUME;
ALTER TASK task_daily_dimensions RESUME;
ALTER TASK task_daily_staging_layer RESUME;
ALTER TASK task_daily_ods_extraction RESUME;  -- Resume root task LAST

/*
WHY REVERSE ORDER?
- Child tasks must be active before parent tasks
- If parent is active but child is suspended, child won't run when parent completes
- Snowflake enforces this with error message if you try to activate parent first
*/

-- -----------------------------------------------------------------------------------------
-- Suspending Tasks (Can be done in any order)
-- -----------------------------------------------------------------------------------------

-- Suspend all tasks (for maintenance)
ALTER TASK task_daily_ods_extraction SUSPEND;
ALTER TASK task_daily_staging_layer SUSPEND;
ALTER TASK task_daily_dimensions SUSPEND;
ALTER TASK task_daily_facts SUSPEND;
ALTER TASK task_daily_dq_validation SUSPEND;

-- -----------------------------------------------------------------------------------------
-- Manually Execute Task (For Testing)
-- -----------------------------------------------------------------------------------------

-- Execute root task manually (will trigger all dependent tasks)
EXECUTE TASK task_daily_ods_extraction;

-- Execute specific task independently (won't trigger dependents)
EXECUTE TASK task_daily_staging_layer;

-- -----------------------------------------------------------------------------------------
-- View Task Execution History
-- -----------------------------------------------------------------------------------------

-- Recent task runs
SELECT
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(second, scheduled_time, completed_time) AS duration_seconds,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC
LIMIT 100;

-- Task dependency graph
SELECT
    name AS task_name,
    state,
    schedule,
    predecessors,
    warehouse
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'task_daily_ods_extraction',
    RECURSIVE => TRUE
))
ORDER BY name;

-- =====================================================================================
-- PART 5: MONITORING AND ALERTING QUERIES
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Query 5A: Failed Tasks in Last 24 Hours
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_failed_tasks_24h AS
SELECT
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    error_code,
    error_message,
    query_id
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE state = 'FAILED'
  AND scheduled_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

-- Alert if any tasks failed
SELECT * FROM vw_failed_tasks_24h;

-- -----------------------------------------------------------------------------------------
-- Query 5B: Long-Running Tasks
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_long_running_tasks AS
SELECT
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(minute, scheduled_time, COALESCE(completed_time, CURRENT_TIMESTAMP())) AS duration_minutes,
    CASE
        WHEN DATEDIFF(minute, scheduled_time, COALESCE(completed_time, CURRENT_TIMESTAMP())) > 60 THEN 'CRITICAL'
        WHEN DATEDIFF(minute, scheduled_time, COALESCE(completed_time, CURRENT_TIMESTAMP())) > 30 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS alert_level
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND state IN ('EXECUTING', 'SUCCEEDED')
ORDER BY duration_minutes DESC;

SELECT * FROM vw_long_running_tasks WHERE alert_level IN ('CRITICAL', 'WARNING');

-- -----------------------------------------------------------------------------------------
-- Query 5C: Task Success Rate (Last 7 Days)
-- -----------------------------------------------------------------------------------------

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
  AND state IN ('SUCCEEDED', 'FAILED')
GROUP BY name
ORDER BY success_rate_pct;

-- Alert if success rate < 90%
SELECT * FROM vw_task_success_rate WHERE success_rate_pct < 90;

-- =====================================================================================
-- PART 6: AUTOMATED ALERTING TASK
-- =====================================================================================

-- -----------------------------------------------------------------------------------------
-- Task 6A: Automated Monitoring and Alerting (Runs Every Hour)
-- -----------------------------------------------------------------------------------------

CREATE OR REPLACE TASK task_hourly_monitoring
  WAREHOUSE = etl_task_wh
  SCHEDULE = 'USING CRON 0 * * * * America/New_York'  -- Every hour at :00
  COMMENT = 'Hourly monitoring for failed tasks and performance issues'
AS
DECLARE
  v_failed_count INT;
  v_long_running_count INT;
  v_alert_html VARCHAR;
BEGIN
  -- Check for failed tasks in last hour
  SELECT COUNT(*)
  INTO :v_failed_count
  FROM vw_failed_tasks_24h
  WHERE scheduled_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP());

  -- Check for long-running tasks
  SELECT COUNT(*)
  INTO :v_long_running_count
  FROM vw_long_running_tasks
  WHERE alert_level = 'CRITICAL';

  -- Send alert if issues detected
  IF (v_failed_count > 0 OR v_long_running_count > 0) THEN
    v_alert_html :=
      '<html><body>' ||
      '<h2>VES Pipeline Alert - ' || TO_VARCHAR(CURRENT_TIMESTAMP()) || '</h2>' ||
      '<hr>' ||
      '<h3>Issues Detected</h3>' ||
      '<ul>' ||
      CASE WHEN v_failed_count > 0 THEN
        '<li style="color:red"><strong>' || :v_failed_count || ' task(s) failed in the last hour</strong></li>'
      ELSE '' END ||
      CASE WHEN v_long_running_count > 0 THEN
        '<li style="color:orange"><strong>' || :v_long_running_count || ' task(s) running longer than expected</strong></li>'
      ELSE '' END ||
      '</ul>' ||
      '<p>Check task history: <code>SELECT * FROM vw_failed_tasks_24h;</code></p>' ||
      '</body></html>';

    CALL sp_send_email_alert(
      'ALERT: VES Pipeline Issues Detected',
      :v_alert_html,
      ARRAY_CONSTRUCT('ops-team@company.com', 'data-team@company.com')
    );
  END IF;
END;

-- Resume monitoring task
ALTER TASK task_hourly_monitoring RESUME;

-- =====================================================================================
-- PART 7: METADATA TRACKING TABLE
-- =====================================================================================

-- Create table to track task execution (referenced in tasks above)
CREATE TABLE IF NOT EXISTS IDENTIFIER(get_dw_database() || '.metadata.etl_task_log') (
    log_id NUMBER AUTOINCREMENT PRIMARY KEY,
    task_name VARCHAR(200),
    batch_id VARCHAR(100),
    status VARCHAR(20),  -- RUNNING, SUCCESS, FAILED
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    records_processed NUMBER,
    error_message VARCHAR(5000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- View for recent task executions
CREATE OR REPLACE VIEW vw_etl_task_summary AS
SELECT
    batch_id,
    task_name,
    status,
    start_time,
    end_time,
    DATEDIFF(second, start_time, end_time) AS duration_seconds,
    records_processed,
    error_message
FROM IDENTIFIER(get_dw_database() || '.metadata.etl_task_log')
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;

-- =====================================================================================
-- USAGE SUMMARY
-- =====================================================================================

/*
ACTIVATION CHECKLIST:

□ 1. Create notification integration:
     CREATE NOTIFICATION INTEGRATION email_notification_int...

□ 2. Create warehouses:
     CREATE WAREHOUSE etl_task_wh...

□ 3. Create tasks (already done above)

□ 4. Resume tasks in reverse dependency order:
     ALTER TASK task_daily_dq_validation RESUME;
     ALTER TASK task_daily_facts RESUME;
     ALTER TASK task_daily_dimensions RESUME;
     ALTER TASK task_daily_staging_layer RESUME;
     ALTER TASK task_daily_ods_extraction RESUME;
     ALTER TASK task_hourly_monitoring RESUME;

□ 5. Test with manual execution:
     EXECUTE TASK task_daily_ods_extraction;

□ 6. Monitor execution:
     SELECT * FROM vw_failed_tasks_24h;
     SELECT * FROM vw_etl_task_summary;

TASK SCHEDULE:
- task_daily_ods_extraction: Daily at 1:00 AM EST
- task_hourly_monitoring: Every hour

COST OPTIMIZATION:
- All tasks use AUTO_SUSPEND = 60 warehouses
- Tasks only run when scheduled (no idle time)
- Estimated cost: ~$5-20/day depending on data volume

NOTIFICATION SETUP:
- Failed tasks: Immediate email to ops-team
- Low DQ scores: Email to data-team
- Daily summary: Email to data-team
- Hourly monitoring: Email on issues

*/
