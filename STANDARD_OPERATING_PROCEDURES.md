# VES Data Warehouse - Standard Operating Procedures (SOPs)
## Operations Runbook for Data Engineering Team

**Purpose:** Define standard procedures for routine operations, maintenance, and issue resolution

**Audience:** Data Engineers, On-Call Engineers, Data Team Lead
**Classification:** INTERNAL - OPERATIONAL

**Version:** 1.0
**Last Updated:** 2025-11-17
**Author:** Data Team

---

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Weekly Maintenance](#weekly-maintenance)
3. [Monthly Tasks](#monthly-tasks)
4. [Incident Response](#incident-response)
5. [Deployment Procedures](#deployment-procedures)
6. [Data Quality Issue Resolution](#data-quality-issue-resolution)
7. [Performance Tuning](#performance-tuning)
8. [User Support](#user-support)
9. [On-Call Procedures](#on-call-procedures)
10. [Emergency Contacts](#emergency-contacts)

---

## Daily Operations

### Morning Health Check (9:00 AM EST - 15 minutes)

**Frequency:** Every business day
**Owner:** On-call engineer or designated team member
**Tools:** Snowflake UI, Monitoring Dashboard, Email

**Note:** All SQL queries in this document use configuration functions (`fn_get_dw_database()`, `fn_get_ods_database()`) to reference databases dynamically. This ensures the queries work across all environments (DEV, TEST, PROD).

#### Checklist

```sql
-- 1. Check pipeline health
SELECT
    pipeline_name,
    health_status,
    last_run_time,
    execution_status,
    data_quality_score
FROM IDENTIFIER(fn_get_dw_database() || '.metadata.vw_pipeline_health_dashboard')
WHERE health_status IN ('🔴 CRITICAL', '🟡 WARNING')
ORDER BY
    CASE health_status
        WHEN '🔴 CRITICAL' THEN 1
        ELSE 2
    END;
```

**Action Items:**
- [ ] Review output - any CRITICAL or WARNING pipelines?
- [ ] If CRITICAL: Create incident ticket, notify team in Slack
- [ ] If WARNING: Add to standup discussion, monitor

```sql
-- 2. Check overnight task execution
SELECT
    name AS task_name,
    state,
    scheduled_time,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -1, CURRENT_TIMESTAMP())
))
WHERE state != 'SUCCEEDED'
ORDER BY scheduled_time DESC;
```

**Action Items:**
- [ ] Any failed tasks? Investigate immediately
- [ ] Repeated failures? Escalate to team lead
- [ ] Document issues in shared log

```sql
-- 3. Check data quality scores
SELECT
    table_name,
    overall_quality_score,
    quality_grade,
    quality_status,
    total_failures,
    total_warnings
FROM IDENTIFIER(fn_get_dw_database() || '.metadata.vw_data_quality_summary')
WHERE quality_status IN ('🔴 ACTION REQUIRED', '🟡 REVIEW NEEDED')
ORDER BY overall_quality_score ASC;
```

**Action Items:**
- [ ] Any tables with failing quality checks?
- [ ] If ACTION REQUIRED: Investigate root cause
- [ ] Update #data-quality Slack channel

```sql
-- 4. Check warehouse credit usage (yesterday)
SELECT
    warehouse_name,
    SUM(credits_used) AS credits_yesterday,
    ROUND(SUM(credits_used) * 2.50, 2) AS cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -1, CURRENT_DATE())
  AND start_time < CURRENT_DATE()
GROUP BY warehouse_name
ORDER BY credits_yesterday DESC;
```

**Action Items:**
- [ ] Any unusual spikes in credit usage?
- [ ] Warehouse left running overnight? Investigate auto-suspend
- [ ] Monthly budget on track? Alert if >75% consumed mid-month

```sql
-- 5. Check for SLA breaches
SELECT
    pipeline_name,
    sla_type,
    compliance_status,
    actual_value,
    target_value
FROM IDENTIFIER(fn_get_dw_database() || '.metadata.vw_sla_compliance_dashboard')
WHERE compliance_status = 'BREACHED'
  AND sla_date >= CURRENT_DATE() - 1
ORDER BY pipeline_name;
```

**Action Items:**
- [ ] Document SLA breaches in incident log
- [ ] Notify stakeholders if user-facing impact
- [ ] Add to root cause analysis queue

#### Reporting

**Post results in Slack:**
```
🌅 Morning Health Check - [Date]
✅ Pipelines: X healthy, Y warnings, Z critical
✅ Tasks: X succeeded, Y failed
✅ Quality: Avg score X.X (X tables need attention)
✅ Credits: $XXX spent yesterday (YY% of budget)
✅ SLAs: X met, Y breached
```

---

### Daily Standup (9:30 AM EST - 15 minutes)

**Agenda:**
1. **Yesterday:** What did you complete?
2. **Today:** What are you working on?
3. **Blockers:** Anything preventing progress?
4. **Health Check Findings:** Any issues from morning check?

**Parking Lot:** Topics needing longer discussion → schedule separately

---

## Weekly Maintenance

### Monday: Review & Planning (10:00 AM EST - 1 hour)

#### Sprint Planning

- [ ] Review completed tickets from last week
- [ ] Prioritize new tickets for this week
- [ ] Assign tickets to team members
- [ ] Update sprint board in Jira

#### Performance Review

```sql
-- Top 10 slowest queries last week
SELECT
    query_id,
    user_name,
    warehouse_name,
    total_elapsed_time / 1000 / 60 AS duration_minutes,
    bytes_scanned / (1024*1024*1024) AS gb_scanned,
    LEFT(query_text, 100) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
  AND total_elapsed_time > 60000  -- >60 seconds
ORDER BY total_elapsed_time DESC
LIMIT 10;
```

**Action Items:**
- [ ] Identify optimization candidates
- [ ] Create optimization tickets
- [ ] Estimate savings potential

---

### Wednesday: Data Quality Deep Dive (2:00 PM EST - 30 minutes)

```sql
-- Execute full quality check suite
CALL sp_execute_all_dq_rules('WEEKLY_CHECK_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD'));

-- Review results
SELECT
    quality_dimension,
    total_rules,
    passed_rules,
    failed_rules,
    pass_rate_pct,
    quality_grade
FROM IDENTIFIER(fn_get_dw_database() || '.metadata.vw_dq_scorecard')
ORDER BY pass_rate_pct ASC;
```

**Action Items:**
- [ ] Review failed rules
- [ ] Investigate root causes
- [ ] Create remediation tickets
- [ ] Update DQ documentation if rules need adjustment

---

### Friday: Week-End Wrap-Up (3:00 PM EST - 30 minutes)

#### Deployment Summary

```sql
-- Review deployments this week
SELECT
    deployment_date,
    deployment_type,
    deployed_by,
    status,
    rollback_required
FROM IDENTIFIER(fn_get_dw_database() || '.metadata.deployment_log')
WHERE deployment_date >= DATE_TRUNC('week', CURRENT_DATE())
ORDER BY deployment_date DESC;
```

#### Credit Usage Report

```sql
-- Weekly credit summary
SELECT
    warehouse_name,
    SUM(credits_used) AS credits_this_week,
    ROUND(SUM(credits_used) * 2.50, 2) AS cost_usd,
    COUNT(DISTINCT DATE(start_time)) AS days_active
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('week', CURRENT_DATE())
GROUP BY warehouse_name
ORDER BY credits_this_week DESC;
```

**Deliverables:**
- [ ] Send weekly summary email to stakeholders
- [ ] Update sprint burndown chart
- [ ] Plan next week's priorities

---

## Monthly Tasks

### First Monday of Month: Capacity Planning (10:00 AM - 2 hours)

```sql
-- Monthly data growth analysis
SELECT
    table_schema,
    table_name,
    active_bytes / (1024*1024*1024) AS active_gb,
    time_travel_bytes / (1024*1024*1024) AS time_travel_gb,
    failsafe_bytes / (1024*1024*1024) AS failsafe_gb,
    (active_bytes + time_travel_bytes + failsafe_bytes) / (1024*1024*1024) AS total_gb
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_schema = 'WAREHOUSE'
ORDER BY active_bytes DESC;
```

**Action Items:**
- [ ] Project storage needs for next quarter
- [ ] Identify tables for archival
- [ ] Review and adjust retention policies
- [ ] Update capacity plan in Confluence

---

### Mid-Month: Security Review (Varies)

```sql
-- Review user access
SHOW GRANTS TO ROLE DATA_ANALYST;
SHOW GRANTS TO ROLE DATA_ENGINEER;

-- Review recent permission changes
SELECT
    grantee_name,
    privilege,
    granted_on,
    name,
    granted_by,
    created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE created_on >= DATEADD(month, -1, CURRENT_DATE())
ORDER BY created_on DESC;
```

**Action Items:**
- [ ] Verify principle of least privilege
- [ ] Remove access for departed team members
- [ ] Audit privileged accounts
- [ ] Update access control documentation

---

### End of Month: Financial Review & Reporting

```sql
-- Monthly cost breakdown
SELECT
    warehouse_name,
    SUM(credits_used) AS monthly_credits,
    ROUND(SUM(credits_used) * 2.50, 2) AS monthly_cost_usd,
    ROUND(AVG(credits_used), 2) AS avg_credits_per_hour
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '1 month'
  AND start_time < DATE_TRUNC('month', CURRENT_DATE())
GROUP BY warehouse_name
ORDER BY monthly_credits DESC;

-- Storage costs
SELECT
    ROUND(SUM(active_bytes) / (1024*1024*1024*1024), 2) AS active_tb,
    ROUND(SUM(time_travel_bytes) / (1024*1024*1024*1024), 2) AS time_travel_tb,
    ROUND(SUM(failsafe_bytes) / (1024*1024*1024*1024), 2) AS failsafe_tb,
    ROUND((SUM(active_bytes + time_travel_bytes + failsafe_bytes) / (1024*1024*1024*1024)) * 23, 2) AS estimated_storage_cost_usd
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS;
```

**Deliverables:**
- [ ] Monthly cost report to finance
- [ ] Budget variance analysis
- [ ] Cost optimization recommendations
- [ ] Update annual forecast

---

## Incident Response

### Incident Classification

| Severity | Description | Response Time | Escalation |
|----------|-------------|--------------|------------|
| **SEV-1** | Complete outage, data loss | <15 minutes | VP Engineering, CTO |
| **SEV-2** | Major degradation, partial data loss | <1 hour | Director of Data |
| **SEV-3** | Minor degradation, no data loss | <4 hours | Data Team Lead |
| **SEV-4** | Cosmetic issues, minimal impact | <24 hours | Assigned Engineer |

### SEV-1 Incident Procedure

**When:** Complete data warehouse outage, critical data loss, security breach

#### Immediate Response (0-15 minutes)

1. **Alert Team**
   ```
   @channel SEV-1 INCIDENT
   Issue: [Brief description]
   Impact: [User-facing impact]
   Incident Channel: #incident-YYYYMMDD-HHmm
   Incident Commander: [Your Name]
   ```

2. **Create Incident Channel**
   - Slack channel: `#incident-YYYYMMDD-HHmm`
   - Invite: Data team, DevOps, VP Engineering

3. **Assess Impact**
   ```sql
   -- Check if Snowflake is accessible
   SELECT CURRENT_TIMESTAMP();

   -- Check recent task failures
   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
   WHERE start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
     AND state = 'FAILED'
   ORDER BY start_time DESC;

   -- Check data freshness
   SELECT
       table_name,
       MAX(created_timestamp) AS last_update
   FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.fact_exam_requests')
   GROUP BY table_name;
   ```

4. **Notify Stakeholders**
   - Email template: `incidents/sev1_notification_template.html`
   - Include: Issue description, impact, ETA for resolution

#### Investigation & Resolution (15-60 minutes)

5. **Activate DR if Needed**
   - See: [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md)
   - Decision criteria: >1 hour estimated downtime

6. **Root Cause Analysis**
   - Check Snowflake status page: https://status.snowflake.com
   - Review recent deployments
   - Check task execution history
   - Review warehouse activity

7. **Implement Fix**
   - Document all actions in incident channel
   - Get approval from incident commander before changes
   - Test in dev first if possible

8. **Verify Resolution**
   ```sql
   -- Verify all pipelines healthy
   SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.metadata.vw_pipeline_health_dashboard')
   WHERE health_status != '🟢 HEALTHY';

   -- Verify data quality
   SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.metadata.vw_data_quality_summary')
   WHERE quality_status = '🔴 ACTION REQUIRED';
   ```

#### Post-Incident (1-24 hours)

9. **Communicate Resolution**
   ```
   @channel INCIDENT RESOLVED
   Resolution: [What was fixed]
   Root Cause: [Brief explanation]
   Preventive Measures: [What we'll do to prevent recurrence]
   Post-Mortem: Scheduled for [Date/Time]
   ```

10. **Post-Mortem**
    - Schedule within 48 hours
    - Template: `incidents/post_mortem_template.md`
    - Attendees: All incident responders + stakeholders
    - Deliverables:
      - Timeline of events
      - Root cause analysis
      - Action items to prevent recurrence
      - Update to runbooks/procedures

---

### SEV-2 Incident Procedure

**When:** Major performance degradation, significant data quality issues, partial system failure

**Response:**
- Follow SEV-1 procedure but with 1-hour response window
- No need to activate DR unless escalates to SEV-1
- Focus on containment and workarounds

---

### SEV-3 Incident Procedure

**When:** Minor issues, limited user impact, degraded but functional

**Response:**
1. Create Jira ticket with `incident` label
2. Investigate root cause
3. Implement fix
4. Notify affected users
5. Document resolution in ticket

---

## Deployment Procedures

### Code Deployment Workflow

#### 1. Development

```bash
# Create feature branch
git checkout main
git pull origin main
git checkout -b feature/VES-XXX-description

# Make changes
# Test in dev environment

# Commit
git add .
git commit -m "VES-XXX: Description

- Detailed change 1
- Detailed change 2
- Testing notes"

# Push
git push origin feature/VES-XXX-description
```

#### 2. Code Review

- [ ] Create pull request in GitHub
- [ ] Request review from at least 1 team member
- [ ] Address feedback
- [ ] Get approval
- [ ] Squash and merge to main

#### 3. Deploy to Dev

**Automatic:** CI/CD auto-deploys to dev on merge to main

**Manual verification:**
```bash
# Connect to dev
snowsql -c dev

# Run deployment script
!source deployments/deploy_to_dev.sql

# Verify
SELECT * FROM VESDW_DEV.metadata.deployment_log
ORDER BY deployment_timestamp DESC
LIMIT 1;
```

#### 4. Deploy to Staging

**Timing:** Every Tuesday 2 PM EST (if changes available)

```bash
# Create release branch
git checkout main
git pull origin main
git checkout -b release/YYYY-MM-DD

# Run deployment to staging
snowsql -c staging -f deployments/deploy_to_staging.sql

# Smoke test
snowsql -c staging -f tests/smoke_tests.sql

# Tag release
git tag -a v1.X.X -m "Release notes"
git push origin v1.X.X
```

#### 5. Deploy to Production

**Timing:** Every Thursday 2 PM EST (after staging soak period)

**Pre-Deployment Checklist:**
- [ ] All tests passing in staging
- [ ] No critical bugs reported
- [ ] Stakeholder approval received
- [ ] Deployment window scheduled
- [ ] Rollback plan prepared

**Deployment Steps:**

```bash
# 1. Notify users
# Email: deployments/prod_deployment_notification.html

# 2. Create backup
snowsql -c prod
CALL sp_create_daily_backup();

# 3. Execute deployment
!source deployments/deploy_to_prod.sql

# 4. Verify deployment
SELECT * FROM IDENTIFIER(fn_get_dw_database() || '.metadata.deployment_log')
ORDER BY deployment_timestamp DESC LIMIT 1;

# 5. Run smoke tests
!source tests/prod_smoke_tests.sql

# 6. Monitor for 1 hour
# Watch vw_pipeline_health_dashboard

# 7. Notify completion
# Email: deployments/prod_deployment_complete.html
```

**Rollback Procedure:**

If issues detected within 1 hour:

```sql
-- 1. Stop all tasks
ALTER TASK task_daily_ods_extraction SUSPEND;
ALTER TASK task_daily_staging_layer SUSPEND;
-- ... suspend all tasks

-- 2. Restore from backup
-- Use Time Travel or daily clone
CREATE TABLE fact_exam_requests_restore CLONE fact_exam_requests
    AT(TIMESTAMP => '<deployment_start_timestamp>');

-- 3. Swap tables
DROP TABLE fact_exam_requests;
ALTER TABLE fact_exam_requests_restore RENAME TO fact_exam_requests;

-- 4. Resume tasks
ALTER TASK task_daily_ods_extraction RESUME;
-- ... resume all tasks

-- 5. Log rollback
INSERT INTO deployment_log (deployment_type, status, notes)
VALUES ('ROLLBACK', 'COMPLETED', 'Rolled back due to: <reason>');
```

---

## Data Quality Issue Resolution

### Procedure: Investigate DQ Failure

**Trigger:** Quality rule fails in `vw_data_quality_summary`

#### Step 1: Identify Failure

```sql
-- Get failure details
SELECT
    check_id,
    table_name,
    check_type,
    check_name,
    check_status,
    expected_value,
    actual_value,
    variance_pct,
    severity
FROM IDENTIFIER(fn_get_dw_database() || '.metadata.data_quality_checks')
WHERE check_status = 'FAIL'
  AND check_timestamp >= CURRENT_DATE()
ORDER BY severity DESC, check_timestamp DESC;
```

#### Step 2: Analyze Root Cause

**For Completeness Issues:**
```sql
-- Find NULL values
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN <column> IS NULL THEN 1 END) AS null_count,
    ROUND(COUNT(CASE WHEN <column> IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS null_pct
FROM <table>
WHERE batch_id = '<latest_batch>';

-- Sample records with NULLs
SELECT *
FROM <table>
WHERE <column> IS NULL
  AND batch_id = '<latest_batch>'
LIMIT 100;
```

**For Accuracy Issues:**
```sql
-- Find invalid values
SELECT
    <column>,
    COUNT(*) AS occurrence_count
FROM <table>
WHERE <column> NOT IN (<valid_values>)
  AND batch_id = '<latest_batch>'
GROUP BY <column>
ORDER BY occurrence_count DESC;
```

**For Consistency Issues:**
```sql
-- Find mismatches between related tables
SELECT
    t1.id,
    t1.<column> AS table1_value,
    t2.<column> AS table2_value
FROM table1 t1
LEFT JOIN table2 t2 ON t1.id = t2.id
WHERE t1.<column> != t2.<column>
LIMIT 100;
```

#### Step 3: Determine Impact

```sql
-- Count affected records
SELECT
    COUNT(*) AS affected_records,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM <table>) AS affected_pct
FROM <table>
WHERE <failure_condition>;

-- Check if downstream systems impacted
SELECT DISTINCT
    pipeline_name,
    execution_status
FROM pipeline_health_metrics
WHERE table_name = '<affected_table>'
  AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 day';
```

#### Step 4: Remediate

**Option A: Fix at Source**
```sql
-- Update source data
UPDATE IDENTIFIER(fn_get_ods_database() || '.VEMS_CORE.<table>')
SET <column> = <corrected_value>
WHERE <condition>;

-- Re-run ETL
CALL sp_staging_layer_master('<batch_id>');
```

**Option B: Fix in Staging**
```sql
-- Apply transformation in staging
UPDATE IDENTIFIER(fn_get_dw_database() || '.staging.<table>')
SET <column> = CASE
    WHEN <condition1> THEN <value1>
    WHEN <condition2> THEN <value2>
    ELSE <column>
END
WHERE batch_id = '<batch_id>';
```

**Option C: Document Exception**
```sql
-- If data is correct despite failing rule
INSERT INTO data_quality_exceptions (
    rule_id,
    exception_reason,
    approved_by,
    valid_until
)
VALUES (
    <rule_id>,
    'Business confirmed this is valid: <explanation>',
    '<approver>',
    DATEADD(month, 3, CURRENT_DATE())
);
```

#### Step 5: Verify Fix

```sql
-- Re-run quality check
CALL sp_execute_dq_rule(<rule_id>, 'REMEDIATION_' || CURRENT_DATE());

-- Verify passed
SELECT execution_status, actual_value
FROM dq_rule_execution_history
WHERE rule_id = <rule_id>
  AND batch_id = 'REMEDIATION_' || CURRENT_DATE();
```

#### Step 6: Prevent Recurrence

- [ ] Update DQ rule if threshold incorrect
- [ ] Add validation to upstream system
- [ ] Update ETL procedure to handle edge case
- [ ] Document in knowledge base
- [ ] Add to regression test suite

---

## Performance Tuning

### Procedure: Optimize Slow Query

**Trigger:** Query taking >30 seconds, user complaint, high credit usage

#### Step 1: Identify Slow Query

```sql
-- Find slow queries
SELECT
    query_id,
    user_name,
    warehouse_name,
    total_elapsed_time / 1000 AS duration_seconds,
    bytes_scanned / (1024*1024*1024) AS gb_scanned,
    rows_produced,
    compilation_time,
    execution_time,
    LEFT(query_text, 200) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND total_elapsed_time > 30000  -- >30 seconds
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

#### Step 2: Analyze Query Profile

**In Snowflake UI:**
1. Go to History tab
2. Find query by `query_id`
3. Click "Query Profile"
4. Look for:
   - Partitions scanned (high = bad)
   - Bytes scanned (high = bad)
   - Spillage to disk (any = bad)
   - Exploding joins (cartesian products)

#### Step 3: Apply Optimizations

**Optimization 1: Add Clustering**
```sql
-- Check if table is clustered
SHOW TABLES LIKE '<table_name>';

-- If not clustered and large (>1M rows), add clustering
ALTER TABLE <table_name> CLUSTER BY (<filtered_columns>);

-- Monitor reclustering
SELECT *
FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    TABLE_NAME => '<table_name>'
))
ORDER BY start_time DESC;
```

**Optimization 2: Create Materialized View**
```sql
-- For frequently-run aggregations
CREATE MATERIALIZED VIEW mv_<name> AS
SELECT
    <dimensions>,
    <aggregates>
FROM <tables>
GROUP BY <dimensions>;

-- Test performance
SELECT * FROM mv_<name> WHERE <filters>;  -- Should be fast
```

**Optimization 3: Optimize SQL**
```sql
-- Before: SELECT *
SELECT * FROM fact_exam_requests WHERE request_date >= '2025-01-01';

-- After: Select only needed columns
SELECT
    exam_request_sk,
    veteran_dim_sk,
    request_date,
    exam_status
FROM fact_exam_requests
WHERE request_date >= '2025-01-01';
```

#### Step 4: Test & Measure

```sql
-- Clear cache to test true performance
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Run optimized query and measure
SELECT
    SYSTEM$QUERY_PERFORMANCE('<optimized_query>')
AS performance_metrics;

-- Re-enable cache
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```

#### Step 5: Document

- [ ] Add to optimization log
- [ ] Update query in application code
- [ ] Notify users of improvement
- [ ] Add to performance best practices doc

---

## User Support

### Handling Support Requests

**Channels:**
- Slack: #ves-data-support
- Email: data-support@company.com
- Jira: Submit ticket with `support` label

### Request Triage

| Priority | Response Time | Examples |
|----------|--------------|----------|
| **P1 - Urgent** | <2 hours | Report broken, executive presentation today |
| **P2 - High** | <4 hours | Data quality issue, incorrect results |
| **P3 - Medium** | <1 business day | Performance issue, new report request |
| **P4 - Low** | <3 business days | How-to questions, enhancement requests |

### Common Support Scenarios

#### Scenario 1: "My report is showing old data"

**Troubleshooting:**
```sql
-- Check data freshness
SELECT
    table_name,
    MAX(created_timestamp) AS last_update,
    DATEDIFF(hour, MAX(created_timestamp), CURRENT_TIMESTAMP()) AS hours_old
FROM <table>
GROUP BY table_name;

-- Check last pipeline run
SELECT
    pipeline_name,
    last_run_time,
    execution_status
FROM vw_pipeline_health_dashboard
WHERE pipeline_name LIKE '%<related_table>%';
```

**Resolution:**
- Data <1 hour old: Explain refresh schedule, data is current
- Pipeline failed: Re-run manually, investigate failure
- Expected data missing: Check source system availability

#### Scenario 2: "Numbers don't match between reports"

**Troubleshooting:**
```sql
-- Compare row counts
SELECT 'Report 1' AS source, COUNT(*) AS row_count FROM <table1>
UNION ALL
SELECT 'Report 2' AS source, COUNT(*) AS row_count FROM <table2>;

-- Compare filters
-- Ask user for exact filters used
-- Reproduce both queries
```

**Common Causes:**
- Different time periods
- Different filters
- SCD Type 2 (not filtering is_current = TRUE)
- Cached vs. fresh data

#### Scenario 3: "I need access to data"

**Process:**
1. Verify user identity
2. Confirm business need with manager
3. Grant appropriate role:

```sql
-- For analysts (read-only)
GRANT ROLE DATA_ANALYST TO USER <username>;

-- For engineers (read-write in dev)
GRANT ROLE DATA_ENGINEER_DEV TO USER <username>;
```

4. Document in access control log
5. Notify user of access granted

---

## On-Call Procedures

### On-Call Schedule

- **Rotation:** Weekly, Monday-Monday
- **Handoff:** Monday 9 AM EST
- **Tool:** PagerDuty

### On-Call Responsibilities

**During Business Hours (9 AM - 5 PM EST):**
- Monitor #ves-data-alerts Slack channel
- Respond to P1/P2 support requests
- Investigate automated alerts
- Perform daily health check

**After Hours:**
- Respond to PagerDuty alerts within 15 minutes
- Escalate to team lead if needed
- Document all actions in incident log

### Alert Types

| Alert | Severity | Response |
|-------|----------|----------|
| Pipeline failure | HIGH | Investigate and retry within 30 min |
| Data quality failure | MEDIUM | Log for next business day review |
| SLA breach | HIGH | Notify stakeholders, investigate |
| Warehouse budget exceeded | LOW | Log, review in next business day |
| Security alert | CRITICAL | Immediate response, escalate to security team |

### Handoff Process

**Monday morning:**
1. Review incident log from previous week
2. Review open support tickets
3. Brief incoming on-call on ongoing issues
4. Transfer PagerDuty rotation
5. Update on-call calendar

---

## Emergency Contacts

### Internal Contacts

| Role | Name | Phone | Email | Slack |
|------|------|-------|-------|-------|
| **Data Team Lead** | [Name] | [Phone] | data-lead@company.com | @data-lead |
| **On-Call (Current)** | [See PagerDuty] | [PagerDuty] | data-oncall@company.com | @oncall |
| **VP Engineering** | [Name] | [Phone] | vp-eng@company.com | @vp-eng |
| **Director of Data** | [Name] | [Phone] | data-director@company.com | @data-director |
| **Security Team** | [Team] | [Phone] | security@company.com | @security |
| **DevOps Lead** | [Name] | [Phone] | devops-lead@company.com | @devops-lead |

### External Contacts

| Vendor | Purpose | Contact | Support Portal |
|--------|---------|---------|----------------|
| **Snowflake** | Platform support | 1-844-SNOWFLK | https://community.snowflake.com |
| **[BI Tool]** | Dashboard issues | [Number] | [Portal] |
| **AWS** | Infrastructure | [Number] | https://console.aws.amazon.com |

### Escalation Path

```
SEV-1 Incident:
   On-Call Engineer (0-15 min)
         ↓
   Data Team Lead (15-30 min)
         ↓
   Director of Data (30-60 min)
         ↓
   VP Engineering (60-120 min)
         ↓
   CTO (2+ hours or critical business impact)
```

---

## Appendix: Quick Reference

### Common Commands

```sql
-- Resume all tasks
CALL sp_resume_all_tasks();

-- Suspend all tasks
CALL sp_suspend_all_tasks();

-- Force run a task
EXECUTE TASK task_daily_ods_extraction;

-- Check task status
SHOW TASKS;

-- View recent errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE state = 'FAILED'
ORDER BY scheduled_time DESC
LIMIT 20;

-- Clear result cache
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Check warehouse size
SHOW WAREHOUSES;

-- Resize warehouse
ALTER WAREHOUSE etl_wh SET WAREHOUSE_SIZE = LARGE;
```

### Useful Dashboards

- **Pipeline Health:** https://app.snowflake.com/dashboard/pipeline-health
- **Data Quality:** https://app.snowflake.com/dashboard/data-quality
- **Cost Monitoring:** https://app.snowflake.com/dashboard/cost-tracking
- **Executive KPIs:** https://app.snowflake.com/dashboard/executive

### Documentation Links

- [Disaster Recovery Plan](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md)
- [Performance Optimization Guide](PERFORMANCE_OPTIMIZATION_GUIDE.md)
- [Developer Onboarding](DEVELOPER_ONBOARDING_GUIDE.md)
- [Troubleshooting Playbook](TROUBLESHOOTING_PLAYBOOK.md)
- [Architecture Overview](DATA_PIPELINE_ARCHITECTURE.md)

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Review Frequency:** Quarterly
**Next Review:** 2026-02-17
**Document Owner:** Data Team Lead

**Feedback:** Submit improvements via PR or Slack #ves-data-team

---

**END OF STANDARD OPERATING PROCEDURES**
