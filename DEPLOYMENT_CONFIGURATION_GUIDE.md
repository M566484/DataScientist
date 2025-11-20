# VES Data Warehouse - Deployment Configuration Guide

**Version**: 1.0
**Last Updated**: 2025-11-20
**Purpose**: Step-by-step guide to configure and deploy the VES Data Warehouse

---

## Overview

This guide provides all the configuration values you need to customize before deploying the VES Data Warehouse to your Snowflake environment. The deployment is designed to be environment-agnostic and easily configurable.

---

## Table of Contents

1. [Pre-Deployment Checklist](#pre-deployment-checklist)
2. [Configuration Values](#configuration-values)
3. [Step-by-Step Deployment](#step-by-step-deployment)
4. [Post-Deployment Validation](#post-deployment-validation)
5. [Troubleshooting](#troubleshooting)

---

## Pre-Deployment Checklist

Before deploying, ensure you have:

- [ ] Snowflake account with ACCOUNTADMIN or SYSADMIN privileges
- [ ] Access to source systems (OMS, VEMS)
- [ ] Email addresses for alerts and notifications
- [ ] Slack/Teams webhook URL (optional)
- [ ] RBAC roles defined for your organization
- [ ] Database and warehouse names decided
- [ ] Estimated data volumes documented

---

## Configuration Values

### **1. Environment Configuration Table**

**Location**: Update in `/snowflake/schema/00_setup_database.sql` (lines 29-59)

**Configuration Schema**: `PLAYGROUND.CHAPPEM` (or your org's configuration schema)

#### **Required Values to Customize:**

```sql
-- Replace these values with your organization's specific values:

1. DW_DATABASE: 'VESDW_PRD'
   - Your data warehouse database name
   - Naming convention: <PROJECT>_<ENV>
   - Examples: 'VESDW_PRD', 'VESDW_DEV', 'VESDW_UAT'

2. ODS_DATABASE: 'VESODS_PRDDATA_PRD'
   - Your operational data store database name
   - Where raw data from OMS and VEMS lands
   - Examples: 'VESODS_PRD', 'VES_RAW_DATA'

3. ETL_WAREHOUSE: 'ETL_WH'
   - Snowflake warehouse for ETL processing
   - Recommended size: LARGE or X-LARGE
   - Auto-suspend: 60 seconds

4. ANALYTICS_WAREHOUSE: 'ANALYTICS_WH'
   - Snowflake warehouse for analytics queries
   - Recommended size: MEDIUM or LARGE
   - Auto-suspend: 300 seconds (5 minutes)
```

#### **How to Update:**

Edit `/snowflake/schema/00_setup_database.sql` lines 29-59:

```sql
MERGE INTO PLAYGROUND.CHAPPEM.environment_config AS target
USING (
    SELECT 'DW_DATABASE' AS config_key,
           'YOUR_DW_NAME_HERE' AS config_value,  -- ← CUSTOMIZE THIS
           'Data warehouse database name' AS config_description,
           'PRODUCTION' AS environment
    UNION ALL
    SELECT 'ODS_DATABASE' AS config_key,
           'YOUR_ODS_NAME_HERE' AS config_value,  -- ← CUSTOMIZE THIS
           'Operational data store database name' AS config_description,
           'PRODUCTION' AS environment
    UNION ALL
    SELECT 'ETL_WAREHOUSE' AS config_key,
           'YOUR_ETL_WH_HERE' AS config_value,  -- ← CUSTOMIZE THIS
           'Warehouse for ETL processing' AS config_description,
           'PRODUCTION' AS environment
    UNION ALL
    SELECT 'ANALYTICS_WAREHOUSE' AS config_key,
           'YOUR_ANALYTICS_WH_HERE' AS config_value,  -- ← CUSTOMIZE THIS
           'Warehouse for analytics queries' AS config_description,
           'PRODUCTION' AS environment
) AS source
-- ... rest of merge logic
```

---

### **2. Notification Configuration**

**Location**: Update in `/snowflake/orchestration/01_snowflake_native_orchestration.sql`

#### **Email Notification Integration (Lines 28-35)**

```sql
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_notification_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'data-team@YOUR_COMPANY.com',      -- ← CUSTOMIZE THIS
    'ops-team@YOUR_COMPANY.com',       -- ← CUSTOMIZE THIS
    'dba-alerts@YOUR_COMPANY.com'      -- ← CUSTOMIZE THIS
  );
```

**Action Required:**
1. Replace `YOUR_COMPANY.com` with your organization's email domain
2. Add/remove email addresses based on your team structure
3. Requires ACCOUNTADMIN role to create notification integrations

---

#### **Slack Webhook Integration (Optional, Lines 87-92)**

```sql
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS slack_webhook_int
  TYPE = QUEUE
  ENABLED = TRUE
  NOTIFICATION_PROVIDER = WEBHOOK
  DIRECTION = OUTBOUND
  WEBHOOK_URL = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK';  -- ← CUSTOMIZE THIS
```

**How to Get Your Slack Webhook URL:**
1. Go to https://api.slack.com/apps
2. Create new app or select existing
3. Enable "Incoming Webhooks"
4. Create webhook for desired channel (e.g., #data-alerts)
5. Copy the webhook URL and paste it in the configuration

**Microsoft Teams Alternative:**
```sql
WEBHOOK_URL = 'https://YOUR_TENANT.webhook.office.com/webhookb2/YOUR_WEBHOOK_ID'
```

---

### **3. Security & RBAC Configuration**

**Location**: Create these roles based on your organization's security model

#### **Recommended Role Structure:**

```sql
-- STEP 1: Create Roles (run as ACCOUNTADMIN)

CREATE ROLE IF NOT EXISTS ves_admin
    COMMENT = 'VES Data Warehouse administrators - full access';

CREATE ROLE IF NOT EXISTS ves_etl_engineer
    COMMENT = 'VES ETL engineers - can execute ETL procedures';

CREATE ROLE IF NOT EXISTS ves_data_analyst
    COMMENT = 'VES data analysts - read access to warehouse and marts';

CREATE ROLE IF NOT EXISTS ves_report_viewer
    COMMENT = 'VES report viewers - read-only access to marts only';

-- STEP 2: Grant Role Hierarchy
GRANT ROLE ves_report_viewer TO ROLE ves_data_analyst;
GRANT ROLE ves_data_analyst TO ROLE ves_etl_engineer;
GRANT ROLE ves_etl_engineer TO ROLE ves_admin;
GRANT ROLE ves_admin TO ROLE SYSADMIN;

-- STEP 3: Grant Database Privileges
GRANT USAGE ON DATABASE VESDW_PRD TO ROLE ves_report_viewer;  -- ← Use your DW name
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE ves_report_viewer;

-- Analysts: Read access to warehouse and marts
GRANT SELECT ON ALL TABLES IN SCHEMA VESDW_PRD.WAREHOUSE TO ROLE ves_data_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA VESDW_PRD.MARTS_CLINICAL TO ROLE ves_data_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA VESDW_PRD.MARTS_OPERATIONS TO ROLE ves_data_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA VESDW_PRD.MARTS_EXECUTIVE TO ROLE ves_data_analyst;

-- ETL Engineers: Execute procedures, write to staging/warehouse
GRANT ALL ON SCHEMA VESDW_PRD.STAGING TO ROLE ves_etl_engineer;
GRANT ALL ON SCHEMA VESDW_PRD.WAREHOUSE TO ROLE ves_etl_engineer;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ves_etl_engineer;

-- Admins: Full control
GRANT ALL ON DATABASE VESDW_PRD TO ROLE ves_admin;

-- STEP 4: Assign Roles to Users (CUSTOMIZE THESE)
GRANT ROLE ves_admin TO USER john_doe;
GRANT ROLE ves_etl_engineer TO USER jane_smith;
GRANT ROLE ves_data_analyst TO USER analyst1;
GRANT ROLE ves_report_viewer TO USER business_user1;
```

**Action Required:**
1. Customize role names to match your org's naming conventions
2. Replace user names with actual Snowflake user accounts
3. Adjust privileges based on your security requirements

---

### **4. Source System Configuration**

**Location**: Update in `/snowflake/reference/01_create_reference_tables.sql`

#### **System-of-Record Rules (Lines 96-123)**

```sql
INSERT INTO ref_system_of_record (
    entity_type,
    primary_source_system,
    fallback_source_system,
    reconciliation_rule
) VALUES
    -- CUSTOMIZE THESE BASED ON YOUR BUSINESS RULES
    ('VETERAN',        'OMS',  'VEMS', 'PREFER_PRIMARY'),   -- OMS is authoritative for veterans
    ('EVALUATOR',      'VEMS', 'OMS',  'PREFER_PRIMARY'),   -- VEMS is authoritative for evaluators
    ('FACILITY',       'OMS',  'VEMS', 'PREFER_PRIMARY'),   -- OMS is authoritative for facilities
    ('EXAM_REQUEST',   'OMS',  'VEMS', 'MOST_RECENT'),      -- Use most recent data
    ('APPOINTMENT',    'VEMS', NULL,   'SINGLE_SOURCE'),    -- VEMS only
    ('EVALUATION',     'OMS',  'VEMS', 'MERGE_FIELDS'),     -- Merge field-by-field
    ('QA_EVENT',       'OMS',  NULL,   'SINGLE_SOURCE'),    -- OMS only
    ('CLAIM',          'OMS',  NULL,   'SINGLE_SOURCE');    -- OMS only
```

**Reconciliation Rules:**
- `PREFER_PRIMARY`: Always use primary source; fallback only if NULL
- `MOST_RECENT`: Use record with latest timestamp
- `MERGE_FIELDS`: Merge field-by-field (primary first, then fallback)
- `SINGLE_SOURCE`: Only one source has this entity

**Action Required:**
1. Validate which system is authoritative for each entity type
2. Confirm with business owners
3. Document in your data governance framework

---

#### **Code Mapping Tables**

**Location**: `/snowflake/reference/01_create_reference_tables.sql` (lines 285-305)

**Example - Specialty Code Mapping:**

```sql
INSERT INTO ref_code_mapping_specialty (
    source_system, source_code, source_value,
    standard_code, standard_value, category
) VALUES
    -- OMS Codes
    ('OMS',  'PSYCH',      'Psychiatry',    'PSYCHIATRY',  'PSYCHIATRY',  'MENTAL_HEALTH'),
    ('OMS',  'ORTHO',      'Orthopedics',   'ORTHOPEDICS', 'ORTHOPEDICS', 'MUSCULOSKELETAL'),
    ('OMS',  'CARD',       'Cardiology',    'CARDIOLOGY',  'CARDIOLOGY',  'CARDIOVASCULAR'),

    -- VEMS Codes
    ('VEMS', 'PSYCHIATRY', 'Psychiatry',    'PSYCHIATRY',  'PSYCHIATRY',  'MENTAL_HEALTH'),
    ('VEMS', 'ORTHOPEDICS','Orthopedics',   'ORTHOPEDICS', 'ORTHOPEDICS', 'MUSCULOSKELETAL'),
    ('VEMS', 'CARDIOLOGY', 'Cardiology',    'CARDIOLOGY',  'CARDIOLOGY',  'CARDIOVASCULAR');
```

**Action Required:**
1. Extract actual code values from OMS and VEMS systems
2. Map to standardized values
3. Populate all code mapping tables:
   - `ref_code_mapping_specialty`
   - `ref_code_mapping_request_type`
   - `ref_code_mapping_status`
   - Add additional code mapping tables as needed

---

### **5. Warehouse Configuration**

**Location**: Create warehouses in Snowflake UI or SQL

```sql
-- ETL Warehouse (for data loading and transformations)
CREATE WAREHOUSE IF NOT EXISTS ETL_WH WITH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL processing';

-- Analytics Warehouse (for user queries)
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for analytics queries';

-- Resource Monitor (optional - prevents cost overruns)
CREATE RESOURCE MONITOR IF NOT EXISTS ves_cost_monitor WITH
    CREDIT_QUOTA = 1000  -- ← CUSTOMIZE: Monthly credit limit
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE ETL_WH SET RESOURCE_MONITOR = ves_cost_monitor;
ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = ves_cost_monitor;
```

**Action Required:**
1. Determine appropriate warehouse sizes based on data volume
2. Set credit quotas for resource monitors
3. Configure auto-suspend times based on usage patterns

---

## Step-by-Step Deployment

### **Phase 1: Pre-Deployment Setup (30 minutes)**

#### **Step 1.1: Customize Configuration Values**

1. Open `/snowflake/schema/00_setup_database.sql`
2. Update lines 29-59 with your database names
3. Verify function schema: `PLAYGROUND.CHAPPEM` or your org's schema

#### **Step 1.2: Configure Notifications**

1. Open `/snowflake/orchestration/01_snowflake_native_orchestration.sql`
2. Update email addresses (lines 28-35)
3. Add Slack/Teams webhook URL (lines 87-92) - optional

#### **Step 1.3: Create RBAC Roles**

1. Execute role creation SQL (see Section 3 above)
2. Assign roles to users
3. Document role assignments

#### **Step 1.4: Create Warehouses**

1. Execute warehouse creation SQL (see Section 5 above)
2. Configure resource monitors
3. Verify warehouses are created: `SHOW WAREHOUSES;`

---

### **Phase 2: Database & Schema Deployment (15 minutes)**

#### **Step 2.1: Run Database Setup**

```sql
-- Run in Snowflake UI or SnowSQL
-- File: /snowflake/schema/00_setup_database.sql

-- This script will:
-- 1. Create environment_config table
-- 2. Create utility functions (get_dw_database, etc.)
-- 3. Create database and all schemas
-- 4. Create file formats
-- 5. Grant initial permissions

-- Execute:
!source /path/to/snowflake/schema/00_setup_database.sql
```

**Validation:**
```sql
-- Verify database created
SHOW DATABASES LIKE '%VESDW%';

-- Verify schemas created
SHOW SCHEMAS IN DATABASE VESDW_PRD;  -- Use your DW name

-- Verify functions created
SHOW FUNCTIONS IN PLAYGROUND.CHAPPEM;

-- Test function
SELECT PLAYGROUND.CHAPPEM.get_dw_database();  -- Should return 'VESDW_PRD'
```

---

### **Phase 3: Deploy Data Model (30 minutes)**

#### **Step 3.1: Run Master Deployment Script**

```sql
-- File: /snowflake/schema/02_master_deployment.sql

-- This script will deploy in order:
-- 1. ODS tables (raw data landing)
-- 2. Reference tables (code mappings, crosswalks)
-- 3. Staging tables
-- 4. Dimension tables (9 dimensions)
-- 5. Fact tables (9 facts)
-- 6. ETL procedures
-- 7. Marts views

-- Execute:
!source /path/to/snowflake/schema/02_master_deployment.sql
```

**Expected Output:**
```
Deployment completed successfully

ODS Tables (Raw Data): 10 tables
Reference Tables (Multi-Source Mappings): 8 tables
Staging Tables: 10 tables
Dimension Tables: 9 tables
Fact Tables: 9 tables
Marts Views: 12 views
ETL Stored Procedures: 20 procedures
```

---

### **Phase 4: Populate Reference Data (1 hour)**

#### **Step 4.1: Populate Date Dimension**

```sql
-- File: /snowflake/schema/01_populate_date_dimension.sql

-- This will create 10 years of date records (3,650 rows)
!source /path/to/snowflake/schema/01_populate_date_dimension.sql

-- Verify:
SELECT COUNT(*) FROM dim_dates;  -- Should return 3650
SELECT * FROM dim_dates WHERE calendar_date = CURRENT_DATE();
```

#### **Step 4.2: Populate Code Mapping Tables**

```sql
-- File: /snowflake/reference/01_create_reference_tables.sql
-- Extract code values from OMS and VEMS
-- Populate ref_code_mapping_* tables

-- Example:
SELECT DISTINCT specialty_code FROM oms_raw_data.evaluators;  -- Get OMS codes
SELECT DISTINCT specialty_code FROM vems_raw_data.evaluators; -- Get VEMS codes

-- Insert mappings (see examples in file lines 285-333)
INSERT INTO ref_code_mapping_specialty VALUES (...);
```

#### **Step 4.3: Configure System-of-Record Rules**

```sql
-- Verify and update if needed
SELECT * FROM ref_system_of_record;

-- Add custom entity types if needed
INSERT INTO ref_system_of_record VALUES
    ('YOUR_ENTITY_TYPE', 'OMS', 'VEMS', 'PREFER_PRIMARY');
```

---

### **Phase 5: Initial Data Load (2-4 hours)**

#### **Step 5.1: Load ODS Layer**

```sql
-- Extract data from source systems to ODS
-- Generate batch ID
SET batch_id = 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

-- Load OMS data (example using COPY INTO)
COPY INTO ods_veterans_source
FROM @oms_stage/veterans/
FILE_FORMAT = (FORMAT_NAME = 'UTIL.csv_format')
PATTERN = '.*veterans.*\.csv'
ON_ERROR = ABORT_STATEMENT;

-- Load VEMS data
COPY INTO ods_veterans_source
FROM @vems_stage/veterans/
FILE_FORMAT = (FORMAT_NAME = 'UTIL.csv_format')
PATTERN = '.*veterans.*\.csv'
ON_ERROR = ABORT_STATEMENT;

-- Verify:
SELECT source_system, COUNT(*) FROM ods_veterans_source
WHERE batch_id = $batch_id
GROUP BY source_system;
```

#### **Step 5.2: Run ETL Procedures**

```sql
USE WAREHOUSE ETL_WH;

-- Build entity crosswalks (master data management)
CALL sp_build_crosswalk_veteran($batch_id);
CALL sp_build_crosswalk_evaluator($batch_id);
CALL sp_build_crosswalk_facility($batch_id);

-- Merge ODS to staging
CALL sp_merge_veteran_to_staging($batch_id);
CALL sp_merge_evaluator_to_staging($batch_id);
CALL sp_merge_facility_to_staging($batch_id);

-- Load dimensions (with SCD Type 2)
CALL sp_load_dim_veterans($batch_id);
CALL sp_load_dim_evaluators($batch_id);
CALL sp_load_dim_facilities($batch_id);

-- Load facts
CALL sp_load_fact_evaluations_completed($batch_id);
CALL sp_load_fact_appointments_scheduled($batch_id);
```

#### **Step 5.3: Validate Data Load**

```sql
-- Verify record counts
SELECT 'Dimension: Veterans' AS table_name, COUNT(*) AS row_count FROM dim_veterans
UNION ALL
SELECT 'Dimension: Evaluators', COUNT(*) FROM dim_evaluators
UNION ALL
SELECT 'Dimension: Facilities', COUNT(*) FROM dim_facilities
UNION ALL
SELECT 'Fact: Evaluations', COUNT(*) FROM fact_evaluations_completed
UNION ALL
SELECT 'Fact: Appointments', COUNT(*) FROM fact_appointments_scheduled;

-- Check data quality scores
SELECT
    AVG(dq_score) AS avg_dq_score,
    MIN(dq_score) AS min_dq_score,
    COUNT(*) AS total_records
FROM stg_veterans;
-- Expected: avg_dq_score > 95

-- Check for orphan records (should return 0)
SELECT COUNT(*) AS orphan_count
FROM fact_evaluations_completed f
LEFT JOIN dim_veterans v ON f.veteran_sk = v.veteran_sk
WHERE v.veteran_sk IS NULL;
```

---

### **Phase 6: Deploy Orchestration (1 hour)**

#### **Step 6.1: Create Notification Integrations**

```sql
-- Must run as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Execute notification setup
!source /path/to/snowflake/orchestration/01_snowflake_native_orchestration.sql
-- (Lines 1-150: Notification integrations and procedures)

-- Verify:
SHOW NOTIFICATION INTEGRATIONS;

-- Test email
CALL sp_send_email_alert(
    'Test Alert - VES Data Pipeline',
    'This is a test notification.',
    ARRAY_CONSTRUCT('your-email@company.com')
);
```

#### **Step 6.2: Create Streams for CDC**

```sql
-- Create streams on ODS tables for incremental processing
CREATE STREAM IF NOT EXISTS stream_ods_veterans_source
    ON TABLE ods_veterans_source
    COMMENT = 'CDC stream for veteran data changes';

CREATE STREAM IF NOT EXISTS stream_ods_evaluators_source
    ON TABLE ods_evaluators_source
    COMMENT = 'CDC stream for evaluator data changes';

-- Verify:
SHOW STREAMS;
```

#### **Step 6.3: Create Snowflake Tasks**

```sql
-- Master task (runs daily at 2 AM)
CREATE OR REPLACE TASK task_daily_etl_master
    WAREHOUSE = ETL_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
CALL sp_daily_etl_orchestrator();

-- Enable task
ALTER TASK task_daily_etl_master RESUME;

-- Verify:
SHOW TASKS;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY scheduled_time DESC
LIMIT 10;
```

---

### **Phase 7: Deploy Monitoring (30 minutes)**

#### **Step 7.1: Create Monitoring Dashboard**

```sql
-- Deploy monitoring views and procedures
!source /path/to/snowflake/monitoring/01_comprehensive_monitoring_dashboard.sql

-- Verify:
SHOW VIEWS IN SCHEMA WAREHOUSE LIKE 'vw_monitoring%';
```

#### **Step 7.2: Create Data Quality Framework**

```sql
-- Deploy DQ framework
!source /path/to/snowflake/quality/01_advanced_data_quality_framework.sql

-- Verify:
SELECT * FROM vw_dq_scorecard;
SELECT * FROM vw_dq_anomalies WHERE anomaly_detected = TRUE;
```

#### **Step 7.3: Setup Alerting**

```sql
-- Create alert task (runs every 15 minutes)
CREATE OR REPLACE TASK task_data_quality_alerts
    WAREHOUSE = ETL_WH
    SCHEDULE = '15 MINUTE'
AS
CALL sp_check_data_quality_and_alert();

ALTER TASK task_data_quality_alerts RESUME;
```

---

## Post-Deployment Validation

### **Validation Checklist**

Run these queries to validate deployment:

```sql
-- ============================================================
-- DEPLOYMENT VALIDATION SUITE
-- ============================================================

-- 1. Database and Schema Validation
SELECT 'Database exists: ' || COUNT(*) AS validation
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = 'VESDW_PRD';  -- Use your DW name

SELECT 'Schema count: ' || COUNT(*) AS validation
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = 'VESDW_PRD';
-- Expected: 10 schemas (REFERENCE, STAGING, WAREHOUSE, 6 MARTS, UTIL)

-- 2. Table Validation
SELECT
    TABLE_SCHEMA,
    COUNT(*) AS table_count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'VESDW_PRD'
  AND TABLE_TYPE = 'BASE TABLE'
GROUP BY TABLE_SCHEMA;
-- Expected counts:
-- WAREHOUSE: 18 tables (9 dims + 9 facts)
-- STAGING: ~10 tables
-- REFERENCE: ~8 tables

-- 3. Procedure Validation
SELECT COUNT(*) AS procedure_count
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_CATALOG = 'VESDW_PRD';
-- Expected: ~20 procedures

-- 4. View Validation
SELECT
    TABLE_SCHEMA,
    COUNT(*) AS view_count
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_CATALOG = 'VESDW_PRD'
GROUP BY TABLE_SCHEMA;
-- Expected: Multiple views in MARTS schemas

-- 5. Data Quality Check
SELECT
    'dim_veterans' AS table_name,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN is_current = TRUE THEN 1 END) AS current_records
FROM dim_veterans
UNION ALL
SELECT 'dim_evaluators', COUNT(*), COUNT(CASE WHEN is_current = TRUE THEN 1 END)
FROM dim_evaluators;

-- 6. Task Validation
SELECT
    NAME,
    STATE,
    SCHEDULE,
    WAREHOUSE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE DATABASE_NAME = 'VESDW_PRD'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- 7. Stream Validation
SELECT
    NAME,
    TABLE_NAME,
    STALE,
    MODE
FROM TABLE(INFORMATION_SCHEMA.STREAMS)
WHERE TABLE_CATALOG = 'VESDW_PRD';

-- 8. Notification Integration Validation
SHOW NOTIFICATION INTEGRATIONS;

-- 9. Role Validation
SELECT
    GRANTEE_NAME,
    PRIVILEGE,
    GRANTED_ON
FROM VESDW_PRD.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
WHERE GRANTEE_NAME IN ('VES_ADMIN', 'VES_ETL_ENGINEER', 'VES_DATA_ANALYST')
ORDER BY GRANTEE_NAME;

-- 10. Warehouse Validation
SHOW WAREHOUSES LIKE '%VES%';
```

### **Success Criteria**

Deployment is successful if:

- ✅ All databases and schemas created
- ✅ 18 tables in WAREHOUSE schema (9 dims + 9 facts)
- ✅ ~20 ETL procedures created
- ✅ Date dimension populated (3,650 rows)
- ✅ At least one successful ETL batch completed
- ✅ Data quality scores > 95%
- ✅ Notification integrations working (test email received)
- ✅ Snowflake tasks created and scheduled
- ✅ RBAC roles configured and assigned
- ✅ Monitoring dashboard accessible

---

## Troubleshooting

### **Common Issues**

#### **Issue 1: Function not found error**

**Error**: `SQL compilation error: Unknown function GET_DW_DATABASE`

**Solution**:
```sql
-- Verify function exists
SHOW FUNCTIONS IN PLAYGROUND.CHAPPEM;

-- If missing, recreate
CREATE OR REPLACE FUNCTION PLAYGROUND.CHAPPEM.get_dw_database()
RETURNS VARCHAR
LANGUAGE SQL
AS '
    SELECT config_value
    FROM PLAYGROUND.CHAPPEM.environment_config
    WHERE config_key = ''DW_DATABASE''
';

-- Update all references to use fully qualified name
SELECT PLAYGROUND.CHAPPEM.get_dw_database();
```

---

#### **Issue 2: Permission denied**

**Error**: `SQL access control error: Insufficient privileges`

**Solution**:
```sql
-- Grant necessary privileges
USE ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE VESDW_PRD TO ROLE ves_etl_engineer;
GRANT ALL ON SCHEMA VESDW_PRD.WAREHOUSE TO ROLE ves_etl_engineer;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ves_etl_engineer;
```

---

#### **Issue 3: Notification integration creation fails**

**Error**: `Insufficient privileges to create NOTIFICATION INTEGRATION`

**Solution**:
```sql
-- Must use ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

CREATE NOTIFICATION INTEGRATION email_notification_int ...;

-- Grant usage to other roles
GRANT USAGE ON INTEGRATION email_notification_int TO ROLE ves_admin;
```

---

#### **Issue 4: Data quality score too low**

**Error**: Average DQ score < 95%

**Solution**:
```sql
-- Identify DQ issues
SELECT
    dq_issues,
    COUNT(*) AS record_count,
    AVG(dq_score) AS avg_score
FROM stg_veterans
WHERE dq_issues IS NOT NULL
GROUP BY dq_issues
ORDER BY record_count DESC;

-- Review source data quality
-- Fix data at source or adjust DQ rules
```

---

#### **Issue 5: Task not running**

**Error**: Task shows as SUSPENDED or not executing

**Solution**:
```sql
-- Check task status
SHOW TASKS;

-- Resume task if suspended
ALTER TASK task_daily_etl_master RESUME;

-- Check task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_DAILY_ETL_MASTER'
ORDER BY SCHEDULED_TIME DESC;

-- Verify warehouse is running
ALTER WAREHOUSE ETL_WH RESUME IF SUSPENDED;
```

---

## Additional Resources

- **Documentation Index**: `/DOCUMENTATION_INDEX.md`
- **Standard Operating Procedures**: `/STANDARD_OPERATING_PROCEDURES.md`
- **Troubleshooting Playbook**: `/TROUBLESHOOTING_PLAYBOOK.md` (48 pages, 50+ scenarios)
- **Disaster Recovery Guide**: `/DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md`
- **Performance Optimization**: `/PERFORMANCE_OPTIMIZATION_GUIDE.md`
- **Developer Onboarding**: `/DEVELOPER_ONBOARDING_GUIDE.md` (5-day program)

---

## Support Contacts

**Data Engineering Team**: data-engineering@your-company.com
**DBA Team**: dba@your-company.com
**Business Owners**: ves-data-stewards@your-company.com

**Escalation Path**:
1. Check troubleshooting playbook
2. Review error logs: `SELECT * FROM etl_error_log ORDER BY error_timestamp DESC`
3. Contact data engineering team
4. Escalate to DBA team if infrastructure issue

---

**Document Version**: 1.0
**Last Updated**: 2025-11-20
**Next Review**: After first production deployment
