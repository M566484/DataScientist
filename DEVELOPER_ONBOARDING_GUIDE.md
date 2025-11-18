# VES Data Warehouse - Developer Onboarding Guide
## Welcome to the Team! 🎉

**Purpose:** Get new data engineers and analysts productive in the VES data warehouse within their first week

**Estimated Time to Complete:** 1 week (with hands-on exercises)

**Version:** 1.0
**Last Updated:** 2025-11-17
**Author:** Data Team

---

## Table of Contents

1. [Welcome & Overview](#welcome--overview)
2. [Day 1: Environment Setup](#day-1-environment-setup)
3. [Day 2: Understanding the Architecture](#day-2-understanding-the-architecture)
4. [Day 3: Hands-On with Snowflake](#day-3-hands-on-with-snowflake)
5. [Day 4: Data Quality & Monitoring](#day-4-data-quality--monitoring)
6. [Day 5: Building Your First Feature](#day-5-building-your-first-feature)
7. [Week 2+: Advanced Topics](#week-2-advanced-topics)
8. [Resources & Support](#resources--support)
9. [Onboarding Checklist](#onboarding-checklist)

---

## Welcome & Overview

### What You'll Be Working On

The **VES (Veteran Evaluation Services) Data Warehouse** is a mission-critical system that tracks:
- 📋 Exam requests and processing
- 👨‍⚕️ Medical evaluations and quality metrics
- 🏥 Healthcare facility operations
- 📊 Performance analytics and bottleneck detection

**Our Impact:** We help ensure veterans receive timely, high-quality medical evaluations for disability claims.

### Team Structure

| Role | Responsibilities | Your Point of Contact |
|------|-----------------|----------------------|
| **Data Team Lead** | Architecture, strategy, escalations | [Name] |
| **Senior Data Engineer** | ETL pipelines, mentoring | [Name] |
| **Data Analyst** | Reports, dashboards, business logic | [Name] |
| **DevOps Engineer** | Infrastructure, deployments, monitoring | [Name] |

### Communication Channels

- **Slack:** #ves-data-team (daily standup, questions)
- **Jira:** VES-DW project (sprint planning, tasks)
- **Confluence:** Documentation wiki
- **GitHub:** Code repository (feature branches)
- **PagerDuty:** On-call rotation (after 30 days)

### Your First Week Goals

By end of week 1, you will:
- ✅ Have full access to all systems
- ✅ Understand the data architecture
- ✅ Run your first Snowflake queries
- ✅ Deploy code to dev environment
- ✅ Complete a small starter ticket
- ✅ Know how to get help when stuck

---

## Day 1: Environment Setup

### Morning: Access & Tools (3 hours)

#### Step 1: Request Access

**Action Items:**
1. Submit ServiceNow ticket for:
   - [ ] Snowflake account (role: `DATA_ENGINEER_DEV`)
   - [ ] GitHub repo access (VES-DataWarehouse)
   - [ ] Jira project access (VES-DW)
   - [ ] Confluence space access
   - [ ] VPN credentials (if remote)

2. Install required software:
```bash
# Download and install:
- Snowflake CLI (SnowSQL)
- Git
- VS Code or your preferred IDE
- DBeaver or DataGrip (SQL client)
- Slack Desktop
- Zoom
```

#### Step 2: Configure SnowSQL

Create `~/.snowsql/config`:
```ini
[connections.dev]
accountname = ABC12345.us-east-1
username = YOUR_USERNAME
password = YOUR_PASSWORD
dbname = VESDW_DEV
schemaname = warehouse
warehousename = DEV_WH
rolename = DATA_ENGINEER_DEV

[connections.prod]
accountname = ABC12345.us-east-1
username = YOUR_USERNAME
dbname = VESDW_PRD
warehousename = ANALYTICS_WH
rolename = DATA_ANALYST  # Read-only in prod initially
```

**Test Connection:**
```bash
snowsql -c dev
# Should connect successfully
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();
```

#### Step 3: Clone Repository

```bash
# Clone the repo
git clone https://github.com/your-org/VES-DataWarehouse.git
cd VES-DataWarehouse

# Create your feature branch
git checkout -b onboarding/YOUR_NAME

# Explore the structure
tree -L 2
```

**Repository Structure:**
```
VES-DataWarehouse/
├── snowflake/              # All Snowflake SQL code
│   ├── schema/             # Database setup
│   ├── dimensions/         # Dimension tables
│   ├── facts/              # Fact tables
│   ├── etl/                # ETL procedures
│   ├── monitoring/         # Monitoring & alerting
│   ├── quality/            # Data quality framework
│   ├── orchestration/      # Task scheduling
│   └── marts/              # Analytics/reporting layer
├── docs/                   # Documentation
├── tests/                  # Unit tests
├── scripts/                # Deployment scripts
└── README.md
```

### Afternoon: Read Core Documentation (4 hours)

**Required Reading (in order):**

1. **Start Here:** [README.md](README.md) - Project overview
2. **Architecture:** [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md)
3. **Dimensional Model:** [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md)
4. **Snowflake Basics:** [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) (first 30 pages)

**Exercise:** Document 3 questions you have after reading. Bring to tomorrow's 1:1.

### End of Day: Meet the Team (1 hour)

- **15-min intro with each team member**
- **30-min team standup observation**
- **Setup recurring 1:1 with mentor**

---

## Day 2: Understanding the Architecture

### Morning: Data Flow Deep Dive (3 hours)

#### The Big Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEMS                                │
│  ┌──────────────┐          ┌──────────────┐                     │
│  │  OMS (Legacy) │          │  VEMS (New)  │                     │
│  │  Veterans     │          │  Evaluations │                     │
│  └──────────────┘          └──────────────┘                     │
└────────┬─────────────────────────┬────────────────────────────┘
         │                         │
         ▼                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              ODS LAYER (VESODS_PRDDATA_PRD)                     │
│  Raw operational data, minimal transformation                    │
│  - VEMS_CORE schema (new system)                                │
│  - VEMS_PNM schema (provider network)                           │
│  - Refreshed hourly via API/SFTP                                │
└────────┬────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│              STAGING LAYER (VESDW_PRD.staging)                  │
│  Data integration and cleansing                                  │
│  - Merge OMS + VEMS data                                        │
│  - Entity crosswalks (match veterans across systems)            │
│  - Data quality scoring                                         │
│  - Conflict resolution                                          │
└────────┬────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│           WAREHOUSE LAYER (VESDW_PRD.warehouse)                 │
│  Dimensional model (Kimball methodology)                         │
│  - Dimensions: Veterans, Evaluators, Facilities, Dates          │
│  - Facts: Exam Requests, Evaluations, Appointments              │
│  - SCD Type 2 for slowly changing dimensions                    │
└────────┬────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│              MARTS LAYER (VESDW_PRD.marts)                      │
│  Business-friendly analytics                                     │
│  - Executive dashboards                                         │
│  - Operational reports                                          │
│  - Materialized views for performance                           │
└─────────────────────────────────────────────────────────────────┘
```

#### Hands-On Exercise: Trace a Record

**Goal:** Follow a single exam request through all layers

```sql
-- 1. Find a recent exam request in ODS
SELECT *
FROM VESODS_PRDDATA_PRD.VEMS_CORE.exam_requests
WHERE request_date >= CURRENT_DATE() - 7
LIMIT 1;
-- Note the exam_request_id: _____________

-- 2. Find it in staging (merged OMS+VEMS)
SELECT *
FROM VESDW_PRD.staging.stg_fact_exam_requests
WHERE source_exam_request_id = '<ID_FROM_STEP_1>';
-- Check: Is source_system = 'VEMS'? ___________

-- 3. Find it in the fact table (warehouse)
SELECT *
FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE source_exam_request_id = '<ID_FROM_STEP_1>';
-- Note the exam_request_sk (surrogate key): _____________

-- 4. Join to dimensions to get full context
SELECT
    d.full_date AS request_date,
    v.first_name || ' ' || v.last_name AS veteran_name,
    v.state AS veteran_state,
    e.first_name || ' ' || e.last_name AS evaluator_name,
    f.facility_name,
    er.exam_status,
    er.sla_met
FROM VESDW_PRD.warehouse.fact_exam_requests er
INNER JOIN VESDW_PRD.warehouse.dim_date d
    ON er.request_date_sk = d.date_sk
INNER JOIN VESDW_PRD.warehouse.dim_veteran v
    ON er.veteran_dim_sk = v.veteran_sk
LEFT JOIN VESDW_PRD.warehouse.dim_evaluator e
    ON er.assigned_evaluator_sk = e.evaluator_sk
LEFT JOIN VESDW_PRD.warehouse.dim_facility f
    ON er.facility_dim_sk = f.facility_sk
WHERE er.source_exam_request_id = '<ID_FROM_STEP_1>';
```

**Discussion Questions:**
1. Why do we have an ODS layer AND a staging layer?
2. What's the difference between `exam_request_id` and `exam_request_sk`?
3. Why are dimensions joined with LEFT JOIN but dates with INNER JOIN?

### Afternoon: Key Tables Reference (4 hours)

#### Dimension Tables (SCD Type 2)

| Table | Business Key | Purpose | Row Count (approx) |
|-------|-------------|---------|-------------------|
| `dim_date` | `full_date` | Calendar (2020-2030) | ~4,000 |
| `dim_veteran` | `veteran_ssn` | Veteran demographics | ~2M (5M with history) |
| `dim_evaluator` | `evaluator_npi` | Medical evaluators | ~5K (10K with history) |
| `dim_facility` | `facility_id` | VA facilities | ~1K |
| `dim_medical_condition` | `condition_code` | ICD codes | ~10K |
| `dim_evaluation_type` | `evaluation_type_code` | Exam types | ~200 |

**Exercise:** Query each dimension
```sql
-- Veterans: Check SCD Type 2
SELECT
    veteran_ssn,
    first_name,
    disability_rating,
    is_current,
    effective_start_date,
    effective_end_date
FROM VESDW_PRD.warehouse.dim_veteran
WHERE veteran_ssn = '123456789'  -- Example SSN
ORDER BY effective_start_date;

-- Evaluators: Find active in California
SELECT
    evaluator_npi,
    first_name || ' ' || last_name AS name,
    specialty,
    city,
    state
FROM VESDW_PRD.warehouse.dim_evaluator
WHERE state = 'CA'
  AND is_current = TRUE
ORDER BY specialty, last_name;
```

#### Fact Tables

| Table | Grain | Purpose | Rows/Month |
|-------|-------|---------|-----------|
| `fact_exam_requests` | One row per exam request | Exam lifecycle tracking | ~25K |
| `fact_evaluation` | One row per completed exam | Evaluation details & quality | ~23K |
| `fact_appointment_events` | One row per appointment event | Scheduling timeline | ~50K |
| `fact_exam_processing_bottlenecks` | One row per exam with bottleneck analysis | Performance analytics | ~25K |
| `fact_daily_snapshot` | One row per day | KPI tracking | ~30 |

**Exercise:** Run sample analytics
```sql
-- Exam volume by status (last 30 days)
SELECT
    exam_status,
    COUNT(*) AS exam_count,
    ROUND(AVG(DATEDIFF(day, request_date, completion_date)), 1) AS avg_cycle_days
FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE request_date >= CURRENT_DATE() - 30
GROUP BY exam_status
ORDER BY exam_count DESC;

-- Top 10 evaluators by volume (this month)
SELECT
    e.evaluator_npi,
    e.first_name || ' ' || e.last_name AS evaluator_name,
    COUNT(*) AS exams_completed,
    AVG(eval.exam_quality_score) AS avg_quality_score
FROM VESDW_PRD.warehouse.fact_evaluation eval
INNER JOIN VESDW_PRD.warehouse.dim_evaluator e
    ON eval.evaluator_dim_sk = e.evaluator_sk
WHERE eval.completion_date >= DATE_TRUNC('month', CURRENT_DATE())
  AND e.is_current = TRUE
GROUP BY e.evaluator_npi, e.first_name, e.last_name
ORDER BY exams_completed DESC
LIMIT 10;
```

---

## Day 3: Hands-On with Snowflake

### Morning: SQL Fundamentals in Snowflake (3 hours)

#### Exercise 1: Time Travel

```sql
-- Create a test table
CREATE OR REPLACE TABLE VESDW_DEV.warehouse.test_time_travel (
    id NUMBER,
    value VARCHAR,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert data
INSERT INTO test_time_travel VALUES (1, 'original', CURRENT_TIMESTAMP());

-- Wait 1 minute, then update
-- (Go get coffee ☕)

UPDATE test_time_travel SET value = 'updated' WHERE id = 1;

-- Query as of 2 minutes ago
SELECT * FROM test_time_travel AT(OFFSET => -120);  -- Original value

-- Query current state
SELECT * FROM test_time_travel;  -- Updated value

-- Recover from mistake
CREATE TABLE test_time_travel_backup CLONE test_time_travel AT(OFFSET => -120);
```

#### Exercise 2: Streams for CDC

```sql
-- Create stream on test table
CREATE STREAM stream_test_changes ON TABLE test_time_travel;

-- Make some changes
INSERT INTO test_time_travel VALUES (2, 'new row', CURRENT_TIMESTAMP());
UPDATE test_time_travel SET value = 'modified' WHERE id = 1;
DELETE FROM test_time_travel WHERE id = 2;

-- View changes captured by stream
SELECT
    id,
    value,
    METADATA$ACTION AS action,
    METADATA$ISUPDATE AS is_update,
    METADATA$ROW_ID AS row_id
FROM stream_test_changes;

-- Process stream (consume changes)
CREATE TABLE test_audit AS SELECT * FROM stream_test_changes;

-- Stream is now empty
SELECT * FROM stream_test_changes;  -- 0 rows
```

#### Exercise 3: Tasks & Scheduling

```sql
-- Create a simple task
CREATE TASK test_daily_summary
    WAREHOUSE = dev_wh
    SCHEDULE = '1 MINUTE'  -- For testing; use CRON in production
AS
    INSERT INTO test_audit
    SELECT id, value, CURRENT_TIMESTAMP() FROM test_time_travel;

-- Start the task
ALTER TASK test_daily_summary RESUME;

-- Wait 2 minutes, then check
SELECT * FROM test_audit ORDER BY updated_at DESC;

-- Stop the task
ALTER TASK test_daily_summary SUSPEND;

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TEST_DAILY_SUMMARY'
))
ORDER BY SCHEDULED_TIME DESC;
```

### Afternoon: Working with Real Data (4 hours)

#### Challenge 1: Build a Simple Report

**Business Question:** "Show me the top 5 states by exam volume this month, with average cycle time."

```sql
-- Your turn! Write the query here:




-- Solution (don't peek until you try!)
/*
SELECT
    v.state,
    COUNT(*) AS exam_count,
    ROUND(AVG(DATEDIFF(day, er.request_date, er.completion_date)), 1) AS avg_cycle_days,
    SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS sla_compliance_pct
FROM VESDW_PRD.warehouse.fact_exam_requests er
INNER JOIN VESDW_PRD.warehouse.dim_veteran v
    ON er.veteran_dim_sk = v.veteran_sk
WHERE er.request_date >= DATE_TRUNC('month', CURRENT_DATE())
  AND v.is_current = TRUE
GROUP BY v.state
ORDER BY exam_count DESC
LIMIT 5;
*/
```

#### Challenge 2: Identify Data Quality Issues

**Task:** Find veterans with incomplete profiles (missing critical fields)

```sql
-- Your query:




-- Solution:
/*
SELECT
    veteran_ssn,
    first_name,
    last_name,
    CASE WHEN email IS NULL THEN 'Missing' ELSE 'OK' END AS email_status,
    CASE WHEN phone IS NULL THEN 'Missing' ELSE 'OK' END AS phone_status,
    CASE WHEN state IS NULL THEN 'Missing' ELSE 'OK' END AS state_status,
    CASE WHEN disability_rating IS NULL THEN 'Missing' ELSE 'OK' END AS rating_status
FROM VESDW_PRD.warehouse.dim_veteran
WHERE is_current = TRUE
  AND (email IS NULL OR phone IS NULL OR state IS NULL OR disability_rating IS NULL)
ORDER BY last_name, first_name
LIMIT 100;
*/
```

#### Challenge 3: Create a Stored Procedure

**Task:** Write a procedure that takes a state abbreviation and returns exam statistics

```sql
CREATE OR REPLACE PROCEDURE sp_state_exam_summary(p_state VARCHAR)
RETURNS TABLE(
    metric_name VARCHAR,
    metric_value NUMBER
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT
            'Total Exams' AS metric_name,
            COUNT(*) AS metric_value
        FROM VESDW_PRD.warehouse.fact_exam_requests er
        INNER JOIN VESDW_PRD.warehouse.dim_veteran v
            ON er.veteran_dim_sk = v.veteran_sk
        WHERE v.state = :p_state
          AND v.is_current = TRUE
          AND er.request_date >= CURRENT_DATE() - 30

        UNION ALL

        SELECT
            'Avg Cycle Time (days)',
            ROUND(AVG(DATEDIFF(day, er.request_date, er.completion_date)), 1)
        FROM VESDW_PRD.warehouse.fact_exam_requests er
        INNER JOIN VESDW_PRD.warehouse.dim_veteran v
            ON er.veteran_dim_sk = v.veteran_sk
        WHERE v.state = :p_state
          AND v.is_current = TRUE
          AND er.request_date >= CURRENT_DATE() - 30

        UNION ALL

        SELECT
            'SLA Compliance %',
            ROUND(SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM VESDW_PRD.warehouse.fact_exam_requests er
        INNER JOIN VESDW_PRD.warehouse.dim_veteran v
            ON er.veteran_dim_sk = v.veteran_sk
        WHERE v.state = :p_state
          AND v.is_current = TRUE
          AND er.request_date >= CURRENT_DATE() - 30
    );
    RETURN TABLE(res);
END;
$$;

-- Test it
CALL sp_state_exam_summary('CA');
CALL sp_state_exam_summary('TX');
```

---

## Day 4: Data Quality & Monitoring

### Morning: Data Quality Framework (3 hours)

#### Understanding the DQ Framework

Read: `snowflake/quality/00_advanced_data_quality_framework.sql`

**Key Concepts:**
1. **6 Quality Dimensions:** Completeness, Accuracy, Consistency, Timeliness, Validity, Uniqueness
2. **Rule Catalog:** 40+ pre-built rules
3. **Automated Execution:** Scheduled daily via tasks
4. **Scorecards:** Visual dashboards showing quality trends

#### Exercise: Run Quality Checks

```sql
-- View all active quality rules
SELECT
    rule_code,
    rule_name,
    quality_dimension,
    severity,
    check_frequency
FROM VESDW_PRD.metadata.dq_rule_catalog
WHERE is_active = TRUE
ORDER BY severity DESC, quality_dimension;

-- Execute all quality rules
CALL sp_execute_all_dq_rules('ONBOARDING_TEST_' || CURRENT_USER());

-- View results
SELECT
    rule_code,
    rule_name,
    execution_status,
    expected_value,
    actual_value,
    execution_timestamp
FROM VESDW_PRD.metadata.dq_rule_execution_history reh
INNER JOIN VESDW_PRD.metadata.dq_rule_catalog rc
    ON reh.rule_id = rc.rule_id
WHERE reh.batch_id = 'ONBOARDING_TEST_' || CURRENT_USER()
ORDER BY execution_status, rule_code;

-- View quality scorecard
SELECT * FROM VESDW_PRD.metadata.vw_dq_scorecard;
```

#### Exercise: Create a Custom Quality Rule

**Task:** Add a rule to check that exam cycle time is < 30 days

```sql
INSERT INTO VESDW_PRD.metadata.dq_rule_catalog (
    rule_code,
    rule_name,
    rule_description,
    quality_dimension,
    severity,
    rule_category,
    target_schema,
    target_table,
    rule_sql,
    expected_result,
    failure_threshold,
    check_frequency,
    is_active
)
VALUES (
    'TIME_004',
    'Exam Cycle Time Within SLA',
    'Exam cycle time should be under 30 days for 95% of exams',
    'TIMELINESS',
    'HIGH',
    'BUSINESS_RULE',
    'warehouse',
    'fact_exam_requests',
    'SELECT COUNT(*) FROM VESDW_PRD.warehouse.fact_exam_requests WHERE DATEDIFF(day, request_date, completion_date) > 30 AND completion_date IS NOT NULL',
    '<5%',
    5.0,
    'DAILY',
    TRUE
);

-- Test your new rule
CALL sp_execute_dq_rule(
    (SELECT rule_id FROM VESDW_PRD.metadata.dq_rule_catalog WHERE rule_code = 'TIME_004'),
    'TEST_CUSTOM_RULE'
);
```

### Afternoon: Monitoring & Alerting (4 hours)

#### Understanding the Monitoring Dashboard

Read: `snowflake/monitoring/00_comprehensive_monitoring_dashboard.sql`

**Key Views:**
1. `vw_pipeline_health_dashboard` - Real-time pipeline status
2. `vw_data_quality_summary` - Quality scores by table
3. `vw_cost_optimization_opportunities` - Cost savings recommendations
4. `vw_sla_compliance_dashboard` - SLA tracking

#### Exercise: Explore Monitoring Views

```sql
-- Check pipeline health
SELECT
    pipeline_name,
    health_status,
    last_run_time,
    duration_minutes,
    success_rate_pct,
    data_quality_score
FROM VESDW_PRD.metadata.vw_pipeline_health_dashboard
ORDER BY
    CASE health_status
        WHEN '🔴 CRITICAL' THEN 1
        WHEN '🟡 WARNING' THEN 2
        ELSE 3
    END;

-- Review quality summary
SELECT * FROM VESDW_PRD.metadata.vw_data_quality_summary
WHERE quality_status != '🟢 PASSING'
ORDER BY overall_quality_score ASC;

-- Check for cost optimization opportunities
SELECT * FROM VESDW_PRD.metadata.vw_cost_optimization_opportunities
WHERE optimization_priority IN ('🔴 HIGH PRIORITY', '🟡 MEDIUM PRIORITY')
ORDER BY potential_annual_savings_usd DESC;
```

#### Exercise: Simulate and Respond to an Alert

**Scenario:** A pipeline fails during execution

```sql
-- Simulate pipeline execution with failure
CALL sp_record_pipeline_health(
    'test_veteran_load',
    'ODS_LOAD',
    'FAILED',  -- Status
    125.5,     -- Duration seconds
    0,         -- Records processed
    'DEV_WH',
    1.2,       -- Credits used
    0,         -- DQ score
    'TEST_BATCH_' || CURRENT_USER()
);

-- Check if alert was sent (review your email)
-- Review pipeline health dashboard
SELECT * FROM VESDW_PRD.metadata.vw_pipeline_health_dashboard
WHERE pipeline_name = 'test_veteran_load';
```

---

## Day 5: Building Your First Feature

### Morning: Pick a Starter Ticket (2 hours)

**Go to Jira:** Filter for label `good-first-issue`

**Suggested Starter Tickets:**
1. **Add a new quality rule** - Extend DQ framework with custom validation
2. **Create a new report** - Build analytics view for business stakeholder
3. **Optimize a slow query** - Add clustering or create materialized view
4. **Fix a data quality issue** - Investigate and resolve DQ failure

**Planning:**
- [ ] Read ticket description thoroughly
- [ ] Ask clarifying questions in ticket comments
- [ ] Break down into subtasks
- [ ] Estimate effort (use Fibonacci: 1, 2, 3, 5, 8)
- [ ] Get approval from mentor to proceed

### Afternoon: Implement Your Feature (6 hours)

#### Development Workflow

```bash
# 1. Create feature branch
git checkout main
git pull origin main
git checkout -b feature/VES-123-your-feature-name

# 2. Make your changes
# Edit files in VS Code

# 3. Test locally in dev environment
snowsql -c dev -f your_new_file.sql

# 4. Run any existing tests
cd tests
pytest test_your_feature.py

# 5. Commit with good message
git add .
git commit -m "VES-123: Add quality rule for exam cycle time

- Added new rule TIME_004 to dq_rule_catalog
- Tests cycle time < 30 days for 95% of exams
- Severity: HIGH, Frequency: DAILY
- Includes unit test for rule execution"

# 6. Push to remote
git push origin feature/VES-123-your-feature-name

# 7. Create pull request
# Go to GitHub and create PR
# Request review from mentor
```

#### Code Review Checklist

Before submitting PR, verify:
- [ ] Code follows naming conventions (see style guide)
- [ ] Added comments explaining complex logic
- [ ] No hardcoded values (use parameters/config)
- [ ] Tested in dev environment
- [ ] Updated relevant documentation
- [ ] No breaking changes to existing code
- [ ] Performance considerations addressed
- [ ] Error handling implemented

#### Getting Your PR Merged

1. **Address review feedback** - Make requested changes
2. **Re-test** - Verify changes work
3. **Get approval** - At least 1 approver required
4. **Merge** - Use "Squash and Merge"
5. **Deploy** - CI/CD auto-deploys to dev
6. **Verify** - Check deployment succeeded
7. **Close ticket** - Mark Jira ticket as Done
8. **Celebrate!** 🎉 You shipped code!

---

## Week 2+: Advanced Topics

### Week 2: Performance Optimization

**Focus:** Make queries faster and cheaper

**Reading:**
- [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md)

**Topics:**
- Clustering strategies
- Materialized views
- Query profiling and optimization
- Warehouse sizing

**Hands-On:**
- Analyze slow query from query history
- Create materialized view for common report
- Add clustering key to large table

### Week 3: Disaster Recovery & Operations

**Focus:** Keep systems running smoothly

**Reading:**
- [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md)
- [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md)

**Topics:**
- Backup and recovery procedures
- Incident response
- On-call rotation (if applicable)

**Hands-On:**
- Practice table recovery with Time Travel
- Simulate disaster recovery scenario
- Shadow on-call engineer

### Week 4: Advanced Analytics

**Focus:** Build complex business intelligence

**Reading:**
- Executive dashboard documentation

**Topics:**
- Complex dimensional modeling
- Slowly Changing Dimensions (SCD Type 2)
- Advanced SQL techniques
- BI tool integration (Tableau, Power BI)

**Hands-On:**
- Build dashboard in BI tool
- Implement new fact table
- Add SCD Type 2 dimension

---

## Resources & Support

### Documentation

| Resource | Purpose | Link |
|----------|---------|------|
| **Developer Guide** | Snowflake for SQL developers | [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) |
| **Architecture** | System design overview | [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) |
| **Dimensional Model** | Data model reference | [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) |
| **Performance** | Optimization strategies | [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) |
| **DR/BC Plan** | Disaster recovery | [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) |
| **SOPs** | Standard procedures | [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) |

### Getting Help

**When you're stuck:**

1. **Try these first (15 min):**
   - Search documentation
   - Check Confluence wiki
   - Review similar code in repo
   - Google the error message

2. **Ask in Slack (30 min):**
   - #ves-data-team channel
   - Include: what you tried, error message, relevant code
   - Tag @data-team if urgent

3. **Schedule time with mentor:**
   - For complex issues
   - For architecture discussions
   - For pair programming

4. **Escalate if critical:**
   - Production issue: Page on-call
   - Blocking your work: Notify team lead

### Learning Resources

**Snowflake:**
- [Snowflake University](https://learn.snowflake.com/) - Free courses
- [Snowflake Documentation](https://docs.snowflake.com/) - Official docs
- [Snowflake Community](https://community.snowflake.com/) - Forums

**SQL:**
- [Mode Analytics SQL Tutorial](https://mode.com/sql-tutorial/) - Interactive
- [SQL Zoo](https://sqlzoo.net/) - Practice problems

**Data Warehousing:**
- "The Data Warehouse Toolkit" by Ralph Kimball - Book
- "Designing Data-Intensive Applications" by Martin Kleppmann - Book

### Office Hours

**Team availability:**
- **Daily Standup:** 9:30 AM EST (15 min)
- **Office Hours:** Tue/Thu 2-3 PM EST (drop-in)
- **1:1 with Mentor:** Weekly (30 min)
- **Team Lunch:** Friday 12 PM (optional, social)

---

## Onboarding Checklist

### Week 1

**Day 1: Setup**
- [ ] All access granted (Snowflake, GitHub, Jira, Slack)
- [ ] SnowSQL configured and tested
- [ ] Repository cloned
- [ ] Read core documentation
- [ ] Met all team members

**Day 2: Architecture**
- [ ] Understand data flow (ODS → Staging → Warehouse → Marts)
- [ ] Traced a record through all layers
- [ ] Explored all dimension and fact tables
- [ ] Ran sample analytics queries

**Day 3: Snowflake Hands-On**
- [ ] Practiced Time Travel
- [ ] Created and consumed a Stream
- [ ] Created and ran a Task
- [ ] Completed 3 SQL challenges

**Day 4: Quality & Monitoring**
- [ ] Ran data quality checks
- [ ] Created custom quality rule
- [ ] Explored monitoring dashboards
- [ ] Simulated alert response

**Day 5: First Feature**
- [ ] Selected starter ticket
- [ ] Created feature branch
- [ ] Implemented feature
- [ ] Submitted pull request
- [ ] Addressed review feedback
- [ ] Merged to main

### Week 2-4

- [ ] Completed performance optimization exercise
- [ ] Shadowed on-call engineer
- [ ] Practiced disaster recovery procedure
- [ ] Built analytics dashboard
- [ ] Implemented SCD Type 2 dimension
- [ ] Shipped 3+ features to production

### 30-Day Review

**Mentor Assessment:**
- [ ] Comfortable with Snowflake SQL
- [ ] Understands data architecture
- [ ] Can deploy code independently
- [ ] Follows code review standards
- [ ] Communicates effectively
- [ ] Ready for on-call rotation (if applicable)

**Self-Assessment:**
- What went well? ___________________________
- What was challenging? ___________________________
- What would you like to learn next? ___________________________

---

## Welcome Aboard! 🚀

You're joining a team that's making a real difference for veterans. Your work will directly impact the quality and timeliness of disability evaluations.

**Questions?** Ask anytime in #ves-data-team

**Stuck?** Your mentor is here to help

**Ideas?** We encourage innovation and improvement

**Let's build something great together!**

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Maintained By:** Data Team Lead
**Feedback:** Submit improvements via PR or Slack
