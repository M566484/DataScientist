# VES Data Warehouse - Disaster Recovery & Business Continuity Plan
## Comprehensive DR/BC Strategy for Mission-Critical Data

**Purpose:** Ensure rapid recovery and minimal data loss in disaster scenarios

**Version:** 2.0 (Enhanced)
**Last Updated:** 2025-11-17
**Author:** Data Team
**Classification:** INTERNAL - CRITICAL

---

## Table of Contents

1. [Overview & Objectives](#overview--objectives)
2. [Recovery Time & Point Objectives](#recovery-time--point-objectives)
3. [Backup Strategy](#backup-strategy)
4. [Disaster Recovery Procedures](#disaster-recovery-procedures)
5. [Failover & Failback Procedures](#failover--failback-procedures)
6. [Data Recovery Scenarios](#data-recovery-scenarios)
7. [Testing & Validation](#testing--validation)
8. [Incident Response](#incident-response)
9. [Business Continuity](#business-continuity)
10. [Contact Information](#contact-information)

---

## Overview & Objectives

### Disaster Recovery Goals

1. **Minimize Data Loss** - RPO (Recovery Point Objective) of <1 hour
2. **Rapid Recovery** - RTO (Recovery Time Objective) of <4 hours
3. **Maintain Data Integrity** - 100% accuracy in recovered data
4. **Enable Business Continuity** - Critical operations continue during outage
5. **Comply with Regulations** - Meet HIPAA and VA data retention requirements

### Scope

This plan covers:
- **VESDW_PRD** - Production data warehouse
- **VESODS_PRDDATA_PRD** - ODS databases (VEMS_CORE, VEMS_PNM)
- **ETL Pipelines** - Orchestration and data processing
- **Monitoring & Alerting** - Observability infrastructure
- **Metadata & Configurations** - System settings and definitions

---

## Recovery Time & Point Objectives

### RPO/RTO Targets by System Component

| Component | RPO (Data Loss) | RTO (Recovery Time) | Criticality | Backup Frequency |
|-----------|----------------|---------------------|-------------|------------------|
| **Production Warehouse (VESDW_PRD)** | <1 hour | <2 hours | CRITICAL | Continuous (Time Travel) + Daily |
| **ODS Layer (VESODS_PRDDATA_PRD)** | <1 hour | <2 hours | CRITICAL | Hourly snapshots |
| **Dimension Tables** | <4 hours | <4 hours | HIGH | Daily |
| **Fact Tables** | <1 hour | <2 hours | CRITICAL | Hourly |
| **Staging Tables** | <24 hours | <8 hours | MEDIUM | Daily |
| **ETL Code & Procedures** | <1 hour | <2 hours | CRITICAL | Git version control + Snowflake clones |
| **Monitoring & Metadata** | <4 hours | <8 hours | HIGH | Daily |
| **User-Facing Dashboards** | <4 hours | <8 hours | HIGH | Configuration backups |

---

## Backup Strategy

### Snowflake Native Features

#### 1. Time Travel (Automatic)

Snowflake maintains historical data for all tables:

```sql
-- Production tables: 7-day time travel
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests
    SET DATA_RETENTION_TIME_IN_DAYS = 7;

ALTER TABLE VESDW_PRD.warehouse.dim_veteran
    SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Critical reference tables: 90-day time travel (max)
ALTER TABLE VESDW_PRD.metadata.etl_task_log
    SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- Verify time travel settings
SELECT
    table_catalog,
    table_schema,
    table_name,
    retention_time
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('WAREHOUSE', 'STAGING', 'METADATA')
ORDER BY retention_time DESC;
```

**Recovery Example:**
```sql
-- Recover table from 2 hours ago
CREATE TABLE VESDW_PRD.warehouse.fact_exam_requests_recovered CLONE
    VESDW_PRD.warehouse.fact_exam_requests
    AT(OFFSET => -7200); -- 2 hours ago in seconds

-- Verify recovered data
SELECT COUNT(*) FROM VESDW_PRD.warehouse.fact_exam_requests_recovered;

-- Restore production table
DROP TABLE VESDW_PRD.warehouse.fact_exam_requests;
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests_recovered
    RENAME TO VESDW_PRD.warehouse.fact_exam_requests;
```

#### 2. Fail-Safe (Automatic)

Snowflake provides 7 days of fail-safe protection after time travel period expires.

**Important:** Fail-safe recovery requires Snowflake Support intervention.

#### 3. Zero-Copy Cloning (Daily Snapshots)

```sql
-- Daily full warehouse snapshot (automated via task)
CREATE OR REPLACE PROCEDURE sp_create_daily_backup()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_backup_name VARCHAR;
BEGIN
    v_backup_name := 'warehouse_backup_' || TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD');

    -- Clone entire warehouse schema
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS VESDW_PRD.' || :v_backup_name ||
        ' CLONE VESDW_PRD.warehouse';

    -- Log backup
    INSERT INTO VESDW_PRD.metadata.backup_log (
        backup_type,
        backup_name,
        backup_timestamp,
        backup_status
    )
    VALUES (
        'DAILY_CLONE',
        :v_backup_name,
        CURRENT_TIMESTAMP(),
        'SUCCESS'
    );

    RETURN 'Backup created: ' || :v_backup_name;
END;
$$;

-- Schedule daily backup at 2 AM
CREATE OR REPLACE TASK task_daily_warehouse_backup
    WAREHOUSE = etl_task_wh
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
    CALL sp_create_daily_backup();

ALTER TASK task_daily_warehouse_backup RESUME;
```

#### 4. Cross-Region Replication (Enterprise Edition)

```sql
-- Enable replication for disaster recovery
-- Note: Requires Snowflake Enterprise Edition or higher

-- Create replication group
CREATE REPLICATION GROUP rg_ves_production
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = VESDW_PRD, VESODS_PRDDATA_PRD
    ALLOWED_ACCOUNTS = ABC12345.US-WEST-2; -- DR region account

-- Enable replication schedule (every 4 hours)
ALTER REPLICATION GROUP rg_ves_production
    SET REPLICATION_SCHEDULE = '240 MINUTE';

-- Monitor replication status
SHOW REPLICATION DATABASES;
```

### Backup Verification

```sql
-- Create backup verification table
CREATE TABLE IF NOT EXISTS VESDW_PRD.metadata.backup_verification (
    verification_id NUMBER AUTOINCREMENT PRIMARY KEY,
    backup_name VARCHAR(200),
    verification_timestamp TIMESTAMP_NTZ,
    table_name VARCHAR(200),
    expected_row_count NUMBER(18,0),
    actual_row_count NUMBER(18,0),
    row_count_match BOOLEAN,
    checksum_expected VARCHAR(100),
    checksum_actual VARCHAR(100),
    checksum_match BOOLEAN,
    verification_status VARCHAR(20), -- PASS, FAIL
    error_message VARCHAR(1000)
);

-- Automated backup verification procedure
CREATE OR REPLACE PROCEDURE sp_verify_backup(
    p_backup_schema VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_table_cursor CURSOR FOR
        SELECT table_name FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'WAREHOUSE' AND table_type = 'BASE TABLE';
    v_table_name VARCHAR;
    v_prod_count NUMBER;
    v_backup_count NUMBER;
    v_match BOOLEAN;
BEGIN
    OPEN v_table_cursor;
    FOR record IN v_table_cursor DO
        v_table_name := record.table_name;

        -- Get production row count
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM VESDW_PRD.warehouse.' || :v_table_name
            INTO :v_prod_count;

        -- Get backup row count
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM VESDW_PRD.' || :p_backup_schema || '.' || :v_table_name
            INTO :v_backup_count;

        v_match := (:v_prod_count = :v_backup_count);

        -- Log verification result
        INSERT INTO VESDW_PRD.metadata.backup_verification (
            backup_name,
            verification_timestamp,
            table_name,
            expected_row_count,
            actual_row_count,
            row_count_match,
            verification_status
        )
        VALUES (
            :p_backup_schema,
            CURRENT_TIMESTAMP(),
            :v_table_name,
            :v_prod_count,
            :v_backup_count,
            :v_match,
            CASE WHEN :v_match THEN 'PASS' ELSE 'FAIL' END
        );
    END FOR;
    CLOSE v_table_cursor;

    RETURN 'Backup verification complete for ' || :p_backup_schema;
END;
$$;
```

---

## Disaster Recovery Procedures

### Scenario 1: Accidental Table Drop

**Impact:** Critical table deleted accidentally
**RPO:** <5 minutes
**RTO:** <15 minutes

**Recovery Steps:**

```sql
-- Step 1: Verify table is deleted
SHOW TABLES LIKE 'fact_exam_requests' IN SCHEMA VESDW_PRD.warehouse;

-- Step 2: Check time travel history
SELECT *
FROM VESDW_PRD.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_name = 'FACT_EXAM_REQUESTS';

-- Step 3: Undrop table (if within retention period)
UNDROP TABLE VESDW_PRD.warehouse.fact_exam_requests;

-- Step 4: Verify recovery
SELECT COUNT(*) FROM VESDW_PRD.warehouse.fact_exam_requests;
SELECT MAX(created_timestamp) FROM VESDW_PRD.warehouse.fact_exam_requests;

-- Step 5: Validate data integrity
CALL sp_run_data_quality_checks('fact_exam_requests', 'warehouse', 'RECOVERY_' || TO_VARCHAR(CURRENT_TIMESTAMP()));
```

**Recovery Time:** 5-10 minutes

### Scenario 2: Data Corruption

**Impact:** Incorrect data loaded into production tables
**RPO:** <1 hour
**RTO:** <30 minutes

**Recovery Steps:**

```sql
-- Step 1: Identify corruption timeframe
SELECT
    batch_id,
    created_timestamp,
    COUNT(*) AS record_count
FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE created_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '2 HOURS'
GROUP BY batch_id, created_timestamp
ORDER BY created_timestamp DESC;

-- Step 2: Calculate point-in-time before corruption
SET corruption_timestamp = '2025-11-17 14:30:00';

-- Step 3: Create recovery table from before corruption
CREATE TABLE VESDW_PRD.warehouse.fact_exam_requests_recovery CLONE
    VESDW_PRD.warehouse.fact_exam_requests
    AT(TIMESTAMP => $corruption_timestamp);

-- Step 4: Verify recovered data
SELECT COUNT(*) FROM VESDW_PRD.warehouse.fact_exam_requests_recovery;
SELECT MAX(created_timestamp) FROM VESDW_PRD.warehouse.fact_exam_requests_recovery;

-- Step 5: Backup corrupted table for investigation
CREATE TABLE VESDW_PRD.metadata.corrupted_fact_exam_requests_20251117 AS
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests;

-- Step 6: Restore clean data
DROP TABLE VESDW_PRD.warehouse.fact_exam_requests;
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests_recovery
    RENAME TO VESDW_PRD.warehouse.fact_exam_requests;

-- Step 7: Re-run ETL for missing data (if needed)
CALL sp_load_incremental_facts(
    'fact_exam_requests',
    $corruption_timestamp,
    CURRENT_TIMESTAMP()
);

-- Step 8: Validate full recovery
CALL sp_execute_all_dq_rules('POST_RECOVERY_' || TO_VARCHAR(CURRENT_TIMESTAMP()));
```

**Recovery Time:** 20-30 minutes

### Scenario 3: Complete Database Loss

**Impact:** Entire database unavailable (region outage, account issue)
**RPO:** <4 hours (last replication)
**RTO:** <4 hours

**Recovery Steps:**

```sql
-- Step 1: Activate DR environment (if replication enabled)
-- Execute in DR account (US-WEST-2 or alternate region)

-- Promote secondary database to primary
ALTER DATABASE VESDW_PRD_REPLICA REFRESH;
ALTER DATABASE VESDW_PRD_REPLICA ENABLE FAILOVER;

-- Step 2: Verify data freshness
SELECT
    table_name,
    MAX(created_timestamp) AS last_record_time,
    DATEDIFF(hour, MAX(created_timestamp), CURRENT_TIMESTAMP()) AS hours_behind
FROM VESDW_PRD_REPLICA.warehouse.fact_exam_requests
GROUP BY table_name
ORDER BY last_record_time DESC;

-- Step 3: Redirect applications to DR database
-- Update connection strings in all applications to point to DR account

-- Step 4: Resume ETL pipelines in DR environment
ALTER TASK task_daily_ods_extraction RESUME;
ALTER TASK task_daily_staging_layer RESUME;
-- ... resume all critical tasks

-- Step 5: Notify stakeholders
CALL sp_send_email_alert(
    'DR ACTIVATION: VES Data Warehouse Failover',
    '<h2>Disaster Recovery Activated</h2>' ||
    '<p>VES Data Warehouse has failed over to DR environment.</p>' ||
    '<p>Data freshness: ' || :hours_behind || ' hours behind</p>' ||
    '<p>All systems operational in DR region.</p>',
    ARRAY_CONSTRUCT('data-team@company.com', 'leadership@company.com')
);
```

**Recovery Time:** 2-4 hours (includes verification and cutover)

---

## Failover & Failback Procedures

### Failover Checklist

1. [ ] **Assess Impact** - Determine severity and scope of outage
2. [ ] **Notify Stakeholders** - Alert leadership and affected teams
3. [ ] **Activate DR Site** - Promote replica database if available
4. [ ] **Verify Data Integrity** - Run data quality checks
5. [ ] **Redirect Traffic** - Update application connection strings
6. [ ] **Resume Operations** - Restart ETL pipelines and tasks
7. [ ] **Monitor Performance** - Track DR site performance
8. [ ] **Document Incident** - Record all actions taken

### Failback Procedure (Return to Primary)

```sql
-- Step 1: Verify primary site is recovered and stable
SHOW DATABASES IN ACCOUNT ABC12345; -- Primary account

-- Step 2: Sync data from DR to primary
-- Use replication or data export/import

-- Step 3: Validate data consistency
-- Compare row counts, checksums between primary and DR

-- Step 4: Schedule maintenance window
-- Coordinate with stakeholders for cutover

-- Step 5: Redirect applications back to primary
-- Update connection strings to primary account

-- Step 6: Verify all systems operational
-- Run smoke tests on all critical processes

-- Step 7: Disable DR failover mode
ALTER DATABASE VESDW_PRD DISABLE FAILOVER TO ACCOUNTS ABC12345.US-WEST-2;

-- Step 8: Document lessons learned
-- Update DR procedures based on actual experience
```

---

## Data Recovery Scenarios

### Recovery Scenario Matrix

| Data Loss Type | Recovery Method | RPO | RTO | Procedure |
|----------------|----------------|-----|-----|-----------|
| **Table Dropped** | UNDROP | <1 min | <5 min | `UNDROP TABLE` |
| **Row Deleted (recent)** | Time Travel | <7 days | <10 min | `SELECT ... AT(OFFSET => -3600)` |
| **Row Deleted (old)** | Daily Backup Clone | <24 hours | <30 min | Restore from daily clone |
| **Data Corruption** | Time Travel + Re-ETL | <1 hour | <30 min | Clone before corruption + re-run ETL |
| **Schema Dropped** | UNDROP + Time Travel | <1 min | <15 min | `UNDROP SCHEMA` |
| **Database Dropped** | Replication/Backup | <4 hours | <4 hours | Promote replica or restore from backup |
| **Account Compromised** | Cross-Region Replica | <4 hours | <8 hours | Failover to DR account |

### Self-Service Recovery for Developers

```sql
-- Create safe recovery workspace
CREATE OR REPLACE PROCEDURE sp_create_recovery_workspace(
    p_table_name VARCHAR,
    p_hours_ago NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_workspace_name VARCHAR;
    v_offset_seconds NUMBER;
BEGIN
    v_workspace_name := :p_table_name || '_recovery_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');
    v_offset_seconds := :p_hours_ago * 3600 * -1;

    -- Create recovery table from time travel
    EXECUTE IMMEDIATE
        'CREATE TABLE VESDW_PRD.metadata.' || :v_workspace_name ||
        ' CLONE VESDW_PRD.warehouse.' || :p_table_name ||
        ' AT(OFFSET => ' || :v_offset_seconds || ')';

    -- Grant developer access
    EXECUTE IMMEDIATE
        'GRANT SELECT ON TABLE VESDW_PRD.metadata.' || :v_workspace_name ||
        ' TO ROLE data_analyst';

    RETURN 'Recovery workspace created: VESDW_PRD.metadata.' || :v_workspace_name ||
           '. Use this table to verify data before restoring to production.';
END;
$$;

-- Usage
CALL sp_create_recovery_workspace('fact_exam_requests', 3); -- 3 hours ago
```

---

## Testing & Validation

### Quarterly DR Testing

**Schedule:** Last Saturday of each quarter, 8 AM - 12 PM EST

**Test Plan:**

1. **Week Before:**
   - [ ] Review and update DR procedures
   - [ ] Verify backup integrity
   - [ ] Notify all stakeholders of test schedule

2. **Day Of:**
   - [ ] Simulate accidental table drop and recover
   - [ ] Simulate data corruption and restore from time travel
   - [ ] Test failover to DR region (if applicable)
   - [ ] Verify all ETL pipelines start successfully
   - [ ] Test data quality validation
   - [ ] Measure actual RTO/RPO achieved

3. **Week After:**
   - [ ] Document test results
   - [ ] Identify gaps and update procedures
   - [ ] Present findings to leadership
   - [ ] Update training materials

### Validation Queries

```sql
-- Backup completeness check
SELECT
    b.backup_name,
    b.backup_timestamp,
    COUNT(DISTINCT v.table_name) AS tables_verified,
    SUM(CASE WHEN v.verification_status = 'PASS' THEN 1 ELSE 0 END) AS tables_passed,
    SUM(CASE WHEN v.verification_status = 'FAIL' THEN 1 ELSE 0 END) AS tables_failed
FROM VESDW_PRD.metadata.backup_log b
LEFT JOIN VESDW_PRD.metadata.backup_verification v
    ON b.backup_name = v.backup_name
WHERE b.backup_timestamp >= CURRENT_DATE() - 7
GROUP BY b.backup_name, b.backup_timestamp
ORDER BY b.backup_timestamp DESC;

-- Replication lag monitoring
SELECT
    database_name,
    schema_name,
    replication_lag_seconds,
    CASE
        WHEN replication_lag_seconds < 3600 THEN '🟢 HEALTHY'
        WHEN replication_lag_seconds < 14400 THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END AS replication_health
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY
WHERE start_time >= CURRENT_DATE() - 1
ORDER BY replication_lag_seconds DESC;
```

---

## Incident Response

### Severity Levels

| Level | Impact | Response Time | Escalation |
|-------|--------|--------------|------------|
| **SEV-1 (Critical)** | Complete outage, data loss | <15 minutes | VP Engineering, CTO |
| **SEV-2 (High)** | Major degradation, partial data loss | <1 hour | Director of Data |
| **SEV-3 (Medium)** | Minor degradation, no data loss | <4 hours | Data Team Lead |
| **SEV-4 (Low)** | Minimal impact, cosmetic issues | <24 hours | Data Engineer |

### Incident Response Checklist

**SEV-1 Incident (Complete Outage):**

1. **Immediate (0-15 minutes):**
   - [ ] Page on-call engineer
   - [ ] Create incident channel (#incident-YYYYMMDD-HHmm)
   - [ ] Notify VP Engineering and CTO
   - [ ] Assess scope and impact

2. **Short-term (15-60 minutes):**
   - [ ] Activate DR site if needed
   - [ ] Communicate status to stakeholders every 30 minutes
   - [ ] Begin root cause analysis
   - [ ] Implement temporary workarounds

3. **Recovery (1-4 hours):**
   - [ ] Execute recovery procedures
   - [ ] Validate data integrity
   - [ ] Resume normal operations
   - [ ] Monitor for issues

4. **Post-Incident (24-72 hours):**
   - [ ] Complete post-mortem analysis
   - [ ] Document lessons learned
   - [ ] Update procedures
   - [ ] Schedule follow-up review

---

## Business Continuity

### Critical Operations During Outage

1. **Exam Scheduling** - Manual process using backup spreadsheets
2. **Payment Processing** - Queue for later processing
3. **Reporting** - Use last available daily snapshot
4. **Data Quality** - Defer non-critical validations

### Alternate Procedures

```markdown
## Manual Data Entry During Outage

1. Use Google Sheets template: [VES Manual Entry Template]
2. Record all transactions with timestamps
3. When system restored, bulk upload via:
   ```sql
   COPY INTO VESDW_PRD.staging.manual_entry_recovery
   FROM @manual_entry_stage
   FILE_FORMAT = (TYPE = CSV);
   ```
4. Reconcile and merge into production tables
```

---

## Contact Information

### Emergency Contacts

| Role | Name | Phone | Email |
|------|------|-------|-------|
| **Data Team Lead** | [Name] | [Phone] | data-lead@company.com |
| **On-Call Engineer** | [Rotation] | [PagerDuty] | data-oncall@company.com |
| **VP Engineering** | [Name] | [Phone] | vp-eng@company.com |
| **Snowflake Support** | 24/7 | 1-844-SNOWFLK | support@snowflake.com |
| **Security Team** | [Name] | [Phone] | security@company.com |

### Escalation Path

1. **Data Engineer** (0-30 min)
2. **Data Team Lead** (30-60 min)
3. **VP Engineering** (1-2 hours)
4. **CTO** (2+ hours or critical impact)

---

## Appendix: Recovery Scripts

### Quick Recovery Toolkit

```sql
-- 1. List all tables dropped in last 7 days
SHOW TABLES HISTORY IN SCHEMA VESDW_PRD.warehouse;

-- 2. Recover most recently dropped table
UNDROP TABLE VESDW_PRD.warehouse.<table_name>;

-- 3. Clone database to specific timestamp
CREATE DATABASE VESDW_PRD_RECOVERY CLONE VESDW_PRD
    AT(TIMESTAMP => '2025-11-17 08:00:00'::TIMESTAMP_NTZ);

-- 4. Compare row counts between environments
SELECT
    'PRODUCTION' AS environment,
    table_name,
    row_count
FROM VESDW_PRD.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
UNION ALL
SELECT
    'DR' AS environment,
    table_name,
    row_count
FROM VESDW_PRD_REPLICA.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'WAREHOUSE'
ORDER BY table_name, environment;
```

---

**Document Classification:** INTERNAL - CRITICAL
**Review Frequency:** Quarterly
**Next Review Date:** 2026-02-17
**Document Owner:** Data Team Lead

---

**END OF DISASTER RECOVERY & BUSINESS CONTINUITY PLAN**
