# Dynamic Tables Implementation Guide

## Overview

This guide explains how to implement and operate the **Dynamic Tables** approach for the VES Data Pipeline staging layer. Dynamic Tables provide a modern, declarative alternative to stored procedures with automatic refresh, built-in CDC, and dependency management.

## Table of Contents

1. [What Are Dynamic Tables?](#what-are-dynamic-tables)
2. [Architecture Overview](#architecture-overview)
3. [Implementation Steps](#implementation-steps)
4. [Migration Strategy](#migration-strategy)
5. [Monitoring & Operations](#monitoring--operations)
6. [Cost Management](#cost-management)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## What Are Dynamic Tables?

### Definition

Dynamic Tables are **self-refreshing materialized views** that automatically:
- Refresh when source data changes (Change Data Capture)
- Process data incrementally or fully based on SQL complexity
- Manage dependencies through a directed acyclic graph (DAG)
- Meet freshness SLAs defined by `TARGET_LAG`

### Key Benefits

| Feature | Benefit |
|---------|---------|
| **Declarative SQL** | No procedural code (stored procedures) to maintain |
| **Automatic Refresh** | No manual orchestration or scheduling needed |
| **Built-in CDC** | Automatic incremental processing without merge logic |
| **Dependency Management** | Snowflake manages the refresh order automatically |
| **Target Lag SLA** | Control data freshness (e.g., "30 minutes") |
| **Cost Efficiency** | Incremental mode reduces compute vs. full refreshes |

### When to Use Dynamic Tables

✅ **Use Dynamic Tables for:**
- Simple to moderate transformation complexity
- Fact tables with straightforward extraction logic
- Reference data (codes, lookups, facilities)
- Real-time or near-real-time data requirements (< 30 min lag)
- New pipelines starting from scratch

❌ **Keep Stored Procedures for:**
- Complex multi-step business logic with branching
- Heavy audit requirements (write to separate audit tables)
- Need for explicit error handling (TRY/CATCH)
- Batch-based processing with explicit `batch_id` tracking

---

## Architecture Overview

### Current Implementation (Stored Procedures)

```
ODS_RAW
  ↓ (manual CALL sp_staging_layer_master)
REFERENCE (Crosswalks)
  ↓ (sequential procedure calls)
STAGING (Merged Entities)
  ↓ (manual ETL)
WAREHOUSE (Star Schema)
```

### New Implementation (Dynamic Tables)

```
ODS_RAW
  ↓ (automatic refresh on data change)
REFERENCE (dt_crosswalk_facilities)
  ↓ (automatic DAG dependency)
STAGING (dt_stg_facilities, dt_stg_fact_*)
  ↓ (automatic refresh)
WAREHOUSE (Star Schema)
```

### Components Implemented

| Component | Type | Target Lag | Description |
|-----------|------|------------|-------------|
| `dt_crosswalk_facilities` | Reference | 15 min | Facility matching between OMS/VEMS |
| `dt_stg_facilities` | Staging | 30 min | Merged facility records |
| `dt_stg_fact_appointment_events` | Fact | 20 min | Appointment event facts |
| `dt_stg_fact_qa_events` | Fact | 30 min | QA event facts |
| `dt_vw_staging_dq_summary` | Dashboard | 10 min | Real-time data quality metrics |

---

## Implementation Steps

### Prerequisites

1. **Snowflake Edition**: Dynamic Tables require **Enterprise Edition or higher**
2. **Warehouse**: Create a dedicated warehouse for Dynamic Tables (recommended)
3. **Permissions**: `CREATE DYNAMIC TABLE` privilege on the schema

### Step 1: Set Up Environment Variables

```sql
-- Set database context
SET dw_database = (SELECT get_dw_database());
SET ods_database = (SELECT get_ods_database());
USE DATABASE IDENTIFIER($dw_database);
```

### Step 2: Create Dedicated Warehouse (Optional but Recommended)

```sql
-- Create warehouse for Dynamic Tables
CREATE WAREHOUSE IF NOT EXISTS dt_compute_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for Dynamic Tables refresh operations';
```

### Step 3: Deploy Dynamic Tables

Execute the SQL scripts in order:

```bash
# 1. Create dynamic crosswalks and staging tables
snowsql -f snowflake/staging/03_dynamic_tables_staging_layer.sql

# 2. Create monitoring views
snowsql -f snowflake/monitoring/dynamic_tables_monitoring.sql
```

Or execute in Snowflake UI:

```sql
-- From Snowflake UI
USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE <your_dw_database>;

-- Run snowflake/staging/03_dynamic_tables_staging_layer.sql
-- Run snowflake/monitoring/dynamic_tables_monitoring.sql
```

### Step 4: Verify Deployment

```sql
-- Check Dynamic Tables were created
SELECT
    name,
    schema_name,
    target_lag,
    refresh_mode,
    scheduling_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = CURRENT_DATABASE()
ORDER BY schema_name, name;

-- Expected output:
-- REFERENCE.dt_crosswalk_facilities
-- STAGING.dt_stg_facilities
-- STAGING.dt_stg_fact_appointment_events
-- STAGING.dt_stg_fact_qa_events
-- STAGING.dt_vw_staging_dq_summary
```

### Step 5: Initial Data Load

Dynamic Tables will automatically refresh when source data appears. To trigger initial load:

```sql
-- Option 1: Wait for automatic refresh (based on TARGET_LAG)
-- Dynamic Tables will refresh automatically when ODS data is available

-- Option 2: Force immediate refresh
ALTER DYNAMIC TABLE REFERENCE.dt_crosswalk_facilities REFRESH;
ALTER DYNAMIC TABLE STAGING.dt_stg_facilities REFRESH;
ALTER DYNAMIC TABLE STAGING.dt_stg_fact_appointment_events REFRESH;
ALTER DYNAMIC TABLE STAGING.dt_stg_fact_qa_events REFRESH;
ALTER DYNAMIC TABLE STAGING.dt_vw_staging_dq_summary REFRESH;
```

### Step 6: Validate Data

```sql
-- Check record counts
SELECT
    'dt_crosswalk_facilities' as table_name,
    COUNT(*) as record_count
FROM REFERENCE.dt_crosswalk_facilities
UNION ALL
SELECT
    'dt_stg_facilities',
    COUNT(*)
FROM STAGING.dt_stg_facilities
UNION ALL
SELECT
    'dt_stg_fact_appointment_events',
    COUNT(*)
FROM STAGING.dt_stg_fact_appointment_events
UNION ALL
SELECT
    'dt_stg_fact_qa_events',
    COUNT(*)
FROM STAGING.dt_stg_fact_qa_events;

-- Check data quality
SELECT * FROM STAGING.dt_vw_staging_dq_summary;
```

---

## Migration Strategy

### Phase 1: Parallel Run (Week 1-2)

Run both approaches in parallel to validate results:

```sql
-- Compare stored procedure vs dynamic table results
WITH sp_facilities AS (
    SELECT * FROM STAGING.stg_facilities
    WHERE batch_id = 'BATCH_20251120_001'
),
dt_facilities AS (
    SELECT * FROM STAGING.dt_stg_facilities
)
SELECT
    'Record Count Difference' as metric,
    (SELECT COUNT(*) FROM sp_facilities) as stored_proc_count,
    (SELECT COUNT(*) FROM dt_facilities) as dynamic_table_count,
    ABS((SELECT COUNT(*) FROM sp_facilities) - (SELECT COUNT(*) FROM dt_facilities)) as difference;
```

### Phase 2: Cutover (Week 3)

1. **Stop calling stored procedures** for facilities, appointment events, and QA events
2. **Point downstream consumers** to Dynamic Tables:
   ```sql
   -- Update downstream queries
   -- Old:
   SELECT * FROM STAGING.stg_facilities WHERE batch_id = ?;

   -- New:
   SELECT * FROM STAGING.dt_stg_facilities;
   ```
3. **Monitor closely** using monitoring queries
4. **Keep stored procedures** as backup for 2 weeks

### Phase 3: Cleanup (Week 4+)

1. Drop old stored procedures (after validation period):
   ```sql
   DROP PROCEDURE IF EXISTS sp_merge_facilities_to_staging(VARCHAR);
   DROP TABLE IF EXISTS STAGING.stg_facilities;
   ```
2. Rename dynamic tables to remove `dt_` prefix (optional):
   ```sql
   ALTER DYNAMIC TABLE dt_stg_facilities RENAME TO stg_facilities;
   ```

---

## Monitoring & Operations

### Health Dashboard

```sql
-- Real-time health check
SELECT * FROM monitoring.vw_dynamic_table_health;
```

**Key Metrics:**
- `health_status`: HEALTHY, UNHEALTHY, SUSPENDED
- `freshness_status`: FRESH, STALE
- `data_age_seconds`: How old is the data?
- `last_refresh_duration_seconds`: Performance metric

### Performance Monitoring

```sql
-- Check refresh history
SELECT * FROM monitoring.vw_dynamic_table_refresh_history
WHERE refresh_start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY refresh_start_time DESC;
```

### Alerting Setup

**Recommended Alerts:**

1. **Unhealthy Table Alert** (Critical)
   ```sql
   SELECT * FROM monitoring.vw_dynamic_table_health
   WHERE health_status = 'UNHEALTHY';
   ```
   - **Frequency**: Every 5 minutes
   - **Action**: Page on-call engineer

2. **Stale Data Alert** (Warning)
   ```sql
   SELECT * FROM monitoring.vw_dynamic_table_health
   WHERE freshness_status = 'STALE'
     AND data_age_seconds > 3600;  -- Over 1 hour old
   ```
   - **Frequency**: Every 15 minutes
   - **Action**: Slack notification

3. **Data Quality Alert** (Warning)
   ```sql
   SELECT * FROM STAGING.dt_vw_staging_dq_summary
   WHERE avg_dq_score < 70;
   ```
   - **Frequency**: Every hour
   - **Action**: Email data team

### Manual Operations

#### Pause a Dynamic Table

```sql
-- Suspend refresh (useful during maintenance)
ALTER DYNAMIC TABLE STAGING.dt_stg_facilities SUSPEND;
```

#### Resume a Dynamic Table

```sql
-- Resume refresh
ALTER DYNAMIC TABLE STAGING.dt_stg_facilities RESUME;
```

#### Force Refresh

```sql
-- Trigger immediate refresh (doesn't wait for TARGET_LAG)
ALTER DYNAMIC TABLE STAGING.dt_stg_facilities REFRESH;
```

#### Change Target Lag

```sql
-- Adjust freshness requirement
ALTER DYNAMIC TABLE STAGING.dt_stg_facilities
    SET TARGET_LAG = '15 minutes';
```

#### Change Warehouse

```sql
-- Move to larger warehouse if performance issues
ALTER DYNAMIC TABLE STAGING.dt_stg_facilities
    SET WAREHOUSE = larger_warehouse;
```

---

## Cost Management

### Understanding Costs

Dynamic Tables incur costs in two areas:

1. **Compute**: Warehouse time for refresh operations
2. **Storage**: Physical storage for the materialized data

### Cost Optimization Strategies

#### 1. Right-Size Target Lag

```sql
-- Less frequent refreshes = lower compute cost
-- Assess business needs vs. cost

-- High priority (real-time)
TARGET_LAG = '5 minutes'  -- Higher cost

-- Medium priority (near-real-time)
TARGET_LAG = '30 minutes'  -- Moderate cost

-- Low priority (hourly updates)
TARGET_LAG = '1 hour'  -- Lower cost
```

#### 2. Use Incremental Refresh

```sql
-- Ensure incremental mode is working
SELECT
    name,
    refresh_action,
    COUNT(*) as refresh_count
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    DATE_RANGE_START => DATEADD(day, -7, CURRENT_DATE())
))
WHERE database_name = CURRENT_DATABASE()
GROUP BY name, refresh_action;

-- Goal: High percentage of INCREMENTAL refreshes
```

**To enable incremental mode:**
- Avoid non-deterministic functions (`CURRENT_USER()`, `RANDOM()`)
- Avoid `UNION` (use `UNION ALL` instead)
- Use time-based filtering with static values

#### 3. Optimize Rolling Window

```sql
-- Shorter window = less data to process
-- Current: 7 days

WHERE ingestion_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())

-- If 3 days is sufficient:
WHERE ingestion_timestamp >= DATEADD(day, -3, CURRENT_TIMESTAMP())
```

#### 4. Monitor Compute Usage

```sql
-- Run monthly cost analysis
SELECT
    table_name,
    SUM(duration_seconds) / 3600.0 as total_compute_hours,
    -- Estimate cost (adjust for your warehouse size and credit cost)
    ROUND((SUM(duration_seconds) / 3600.0) * 2.0, 2) as estimated_monthly_cost_usd
FROM monitoring.vw_dynamic_table_refresh_history
WHERE refresh_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY table_name
ORDER BY total_compute_hours DESC;
```

### Cost vs. Stored Procedures

| Aspect | Stored Procedures | Dynamic Tables |
|--------|-------------------|----------------|
| Compute | Batch runs (e.g., 4x daily) | Continuous (based on TARGET_LAG) |
| Efficiency | Full refresh each time | Incremental CDC processing |
| Storage | Single copy | Single copy |
| **Typical Cost** | **Lower for infrequent updates** | **Lower for frequent updates** |

**Rule of Thumb:**
- **Batch updates (< 4x daily)**: Stored procedures may be cheaper
- **Near-real-time (every 15-30 min)**: Dynamic Tables likely cheaper
- **Real-time (< 5 min)**: Dynamic Tables significantly cheaper

---

## Troubleshooting

### Issue 1: Dynamic Table Not Refreshing

**Symptoms:**
- `freshness_status = 'STALE'`
- `data_age_seconds` increasing

**Diagnosis:**
```sql
SELECT
    name,
    scheduling_state,
    last_refresh_start_time,
    next_refresh_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE name = 'dt_stg_facilities';
```

**Solutions:**

1. **Check if suspended:**
   ```sql
   ALTER DYNAMIC TABLE STAGING.dt_stg_facilities RESUME;
   ```

2. **Check for errors in refresh history:**
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
   WHERE name = 'dt_stg_facilities'
   ORDER BY refresh_start_time DESC
   LIMIT 5;
   ```

3. **Force refresh:**
   ```sql
   ALTER DYNAMIC TABLE STAGING.dt_stg_facilities REFRESH;
   ```

### Issue 2: Slow Refresh Performance

**Symptoms:**
- `last_refresh_duration_seconds` > 300 seconds
- Performance degradation over time

**Diagnosis:**
```sql
-- Check refresh times trend
SELECT
    refresh_date,
    AVG(avg_duration_seconds) as avg_seconds,
    MAX(avg_duration_seconds) as max_seconds
FROM monitoring.vw_dynamic_table_performance_trends
WHERE table_name = 'dt_stg_facilities'
  AND refresh_date >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY refresh_date DESC;
```

**Solutions:**

1. **Verify incremental mode:**
   ```sql
   -- Check if using FULL refresh (slow) vs INCREMENTAL (fast)
   SELECT refresh_action, COUNT(*)
   FROM monitoring.vw_dynamic_table_refresh_history
   WHERE table_name = 'dt_stg_facilities'
   GROUP BY refresh_action;
   ```

2. **Optimize query:**
   - Add indexes on join keys in source tables
   - Reduce rolling window size (7 days → 3 days)
   - Add `QUALIFY` clause for deduplication

3. **Scale up warehouse:**
   ```sql
   ALTER DYNAMIC TABLE STAGING.dt_stg_facilities
       SET WAREHOUSE = medium_warehouse;
   ```

### Issue 3: Failed Refresh

**Symptoms:**
- `state = 'FAILED'` in refresh history
- `health_status = 'UNHEALTHY'`

**Diagnosis:**
```sql
SELECT
    name,
    state,
    refresh_start_time,
    refresh_end_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name = 'dt_stg_facilities'
  AND state = 'FAILED'
ORDER BY refresh_start_time DESC;
```

**Common Causes:**

1. **Source data issues** (NULL keys, data type mismatches)
2. **Dependency failure** (upstream table failed first)
3. **Warehouse issues** (suspended, insufficient resources)
4. **Permission issues** (lost access to source tables)

**Solutions:**

1. **Check source data quality:**
   ```sql
   -- Validate source data
   SELECT COUNT(*) FROM ODS_RAW.ods_facilities_source
   WHERE ingestion_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP());
   ```

2. **Check dependencies:**
   ```sql
   -- Ensure upstream crosswalk is healthy
   SELECT * FROM monitoring.vw_dynamic_table_health
   WHERE table_name = 'dt_crosswalk_facilities';
   ```

3. **Retry refresh:**
   ```sql
   ALTER DYNAMIC TABLE STAGING.dt_stg_facilities REFRESH;
   ```

### Issue 4: High Costs

**Symptoms:**
- Unexpected warehouse usage
- Frequent FULL refreshes instead of INCREMENTAL

**Diagnosis:**
```sql
-- Check compute usage
SELECT * FROM monitoring.vw_dynamic_table_performance_trends
WHERE table_name = 'dt_stg_facilities'
  AND refresh_date >= DATEADD(day, -7, CURRENT_DATE());
```

**Solutions:**

1. **Increase TARGET_LAG** (reduce refresh frequency):
   ```sql
   ALTER DYNAMIC TABLE STAGING.dt_stg_facilities
       SET TARGET_LAG = '1 hour';  -- Was 30 minutes
   ```

2. **Fix incremental mode blockers:**
   - Remove `CURRENT_TIMESTAMP()` from SELECT clause
   - Use `UNION ALL` instead of `UNION`
   - Remove UDFs if possible

3. **Optimize rolling window:**
   ```sql
   -- Reduce from 7 days to 3 days
   WHERE ingestion_timestamp >= DATEADD(day, -3, CURRENT_TIMESTAMP())
   ```

---

## Best Practices

### 1. Start Small

- ✅ Begin with low-risk tables (facilities, reference data)
- ✅ Validate results against stored procedures
- ✅ Gradually expand to more critical tables

### 2. Monitor Actively

- ✅ Set up alerts for unhealthy tables
- ✅ Review performance trends weekly
- ✅ Track costs monthly

### 3. Optimize for Incremental

- ✅ Avoid non-deterministic functions in SELECT clause
- ✅ Use `UNION ALL` instead of `UNION`
- ✅ Minimize UDF usage
- ✅ Use time-based filtering with static lookback periods

### 4. Right-Size Target Lag

- ✅ Align with business requirements (not engineering preference)
- ✅ Balance freshness vs. cost
- ✅ Different TARGET_LAG for different priority levels

### 5. Document Dependencies

- ✅ Maintain a dependency diagram
- ✅ Document upstream and downstream consumers
- ✅ Plan changes carefully to avoid breaking dependencies

### 6. Test Before Production

- ✅ Create dev/test versions first
- ✅ Validate data quality and performance
- ✅ Load test with production-like volumes

### 7. Maintain Disaster Recovery

- ✅ Keep stored procedures as backup for 2-4 weeks
- ✅ Document rollback procedures
- ✅ Test recovery scenarios

### 8. Version Control

- ✅ Store DDL in Git
- ✅ Use CI/CD for deployments
- ✅ Track changes with comments

---

## Additional Resources

### Snowflake Documentation

- [Dynamic Tables Overview](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Creating Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-create)
- [Dynamic Table Limitations](https://docs.snowflake.com/en/user-guide/dynamic-tables-limitations)
- [Monitoring Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-monitor)

### Internal Documentation

- `DATA_PIPELINE_ARCHITECTURE.md` - Overall pipeline design
- `STAGING_LAYER_IMPLEMENTATION_GUIDE.md` - Stored procedure approach
- `snowflake/staging/03_dynamic_tables_staging_layer.sql` - DDL scripts
- `snowflake/monitoring/dynamic_tables_monitoring.sql` - Monitoring queries

### Support

- **Slack**: #data-engineering
- **Email**: data-team@example.com
- **On-Call**: See PagerDuty schedule

---

## Appendix: Comparison Table

### Stored Procedures vs. Dynamic Tables

| Feature | Stored Procedures | Dynamic Tables |
|---------|-------------------|----------------|
| **Orchestration** | Manual (`CALL sp_*`) | Automatic |
| **Refresh Trigger** | Explicit batch_id | Source data change |
| **Dependencies** | Manual ordering | Automatic DAG |
| **Incremental Logic** | Custom DELETE+INSERT | Built-in CDC |
| **Batch Tracking** | `batch_id` column | Timestamp-based |
| **Complex Joins** | ✅ Supported | ✅ Supported |
| **Window Functions** | ✅ Supported | ✅ Supported |
| **UNION** | ✅ Supported | ⚠️ UNION ALL only for incremental |
| **UDFs** | ✅ Supported | ❌ Blocks incremental |
| **Non-deterministic Functions** | ✅ Supported | ⚠️ Blocks incremental |
| **Audit Logging** | ✅ Direct writes | ⚠️ Requires TASK+STREAM |
| **Error Handling** | ✅ TRY/CATCH | ❌ Limited |
| **Debugging** | ✅ Step-through | ⚠️ Limited visibility |
| **Testing** | ✅ Easy with specific batch | ⚠️ Harder to isolate |
| **Rollback** | ✅ Reprocess batch | ⚠️ Drop/recreate |
| **Code Maintenance** | ⚠️ More complex | ✅ Declarative SQL |
| **Real-time Support** | ❌ Batch only | ✅ < 5 min lag |
| **Compute Cost** | Batch-dependent | Refresh-frequency dependent |

---

## Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2025-11-20 | 1.0 | Initial implementation guide |

---

**End of Dynamic Tables Implementation Guide**
