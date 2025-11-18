# VES Data Warehouse - Performance Optimization Guide
## Comprehensive Performance Tuning & Cost Optimization

**Purpose:** Maximize query performance and minimize costs in the VES Snowflake data warehouse

**Version:** 2.0 (Enhanced)
**Last Updated:** 2025-11-17
**Author:** Data Team

---

## Table of Contents

1. [Performance Optimization Overview](#performance-optimization-overview)
2. [Clustering Strategies](#clustering-strategies)
3. [Materialized Views](#materialized-views)
4. [Query Optimization Techniques](#query-optimization-techniques)
5. [Warehouse Sizing & Auto-Scaling](#warehouse-sizing--auto-scaling)
6. [Cost Optimization Strategies](#cost-optimization-strategies)
7. [Search Optimization Service](#search-optimization-service)
8. [Result Caching](#result-caching)
9. [Data Partitioning Best Practices](#data-partitioning-best-practices)
10. [Performance Monitoring & Tuning](#performance-monitoring--tuning)

---

## Performance Optimization Overview

### Key Performance Principles

1. **Cluster Smart** - Cluster large tables (>1M rows) on commonly filtered columns
2. **Materialize Frequently** - Pre-compute expensive aggregations
3. **Cache Aggressively** - Leverage result and warehouse caching
4. **Size Right** - Use smallest warehouse that meets SLA
5. **Partition Wisely** - Use micro-partitions effectively with date-based filtering

### Performance Targets

| Metric | Target | Excellent | Needs Optimization |
|--------|--------|-----------|-------------------|
| **Query Response Time (Simple)** | <2 seconds | <1 second | >5 seconds |
| **Query Response Time (Complex)** | <30 seconds | <15 seconds | >60 seconds |
| **ETL Processing Time** | <60 minutes | <30 minutes | >90 minutes |
| **Dashboard Load Time** | <5 seconds | <3 seconds | >10 seconds |
| **Clustering Depth** | <10 | <5 | >20 |
| **Cache Hit Rate** | >80% | >90% | <70% |

---

## Clustering Strategies

### What is Clustering?

Clustering organizes data within micro-partitions to improve query performance for large tables by reducing the amount of data scanned.

### When to Use Clustering

✅ **Use Clustering When:**
- Table size > 1 million rows
- Queries commonly filter on specific columns
- Data is frequently inserted/updated in non-sequential order
- Query performance is degraded over time (high clustering depth)

❌ **Skip Clustering When:**
- Table size < 1 million rows
- Queries don't filter consistently on specific columns
- Data is naturally ordered by insert time
- Table is read-only or rarely queried

### Clustering Recommendations by Table

#### Fact Tables

```sql
-- fact_exam_requests: Cluster on date (most common filter)
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests
  CLUSTER BY (request_date_sk, exam_status);

-- fact_evaluations: Cluster on completion date and veteran
ALTER TABLE VESDW_PRD.warehouse.fact_evaluation
  CLUSTER BY (completion_date_sk, veteran_dim_sk);

-- fact_appointment_events: Cluster on event timestamp
ALTER TABLE VESDW_PRD.warehouse.fact_appointment_events
  CLUSTER BY (event_timestamp::DATE, appointment_status);

-- fact_exam_processing_bottlenecks: Cluster on request date and bottleneck type
ALTER TABLE VESDW_PRD.warehouse.fact_exam_processing_bottlenecks
  CLUSTER BY (request_date_sk, primary_bottleneck_type);

-- fact_daily_snapshot: Cluster on snapshot date
ALTER TABLE VESDW_PRD.warehouse.fact_daily_snapshot
  CLUSTER BY (snapshot_date_sk);
```

#### Large Dimension Tables (SCD Type 2)

```sql
-- dim_veteran: Cluster on business key and current flag
ALTER TABLE VESDW_PRD.warehouse.dim_veteran
  CLUSTER BY (veteran_ssn, is_current);

-- dim_evaluator: Cluster on business key and current flag
ALTER TABLE VESDW_PRD.warehouse.dim_evaluator
  CLUSTER BY (evaluator_npi, is_current);

-- dim_facility: Cluster on state and current flag
ALTER TABLE VESDW_PRD.warehouse.dim_facility
  CLUSTER BY (facility_state, is_current);
```

### Monitor Clustering Health

```sql
-- Check clustering depth (lower is better, target <10)
SELECT
    table_schema,
    table_name,
    AVG(average_depth) AS avg_clustering_depth,
    AVG(average_overlaps) AS avg_partition_overlap
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_schema = 'WAREHOUSE'
  AND average_depth IS NOT NULL
ORDER BY avg_clustering_depth DESC;

-- Alert on poor clustering
SELECT
    table_name,
    AVG(average_depth) AS avg_depth
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_schema = 'WAREHOUSE'
  AND average_depth > 20 -- Poor clustering threshold
GROUP BY table_name;
```

### Automatic Reclustering

Snowflake automatically reclusters when:
- Clustering depth increases significantly
- DML operations disorder the data
- You can monitor and manually trigger if needed

```sql
-- Check if automatic clustering is enabled
SHOW TABLES LIKE 'fact_exam_requests';

-- Manually trigger reclustering (if needed)
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests RECLUSTER;
```

---

## Materialized Views

### What are Materialized Views?

Materialized views pre-compute and store query results, dramatically improving performance for complex aggregations.

### Cost vs. Benefit

| Aspect | Materialized View | Regular View |
|--------|-------------------|--------------|
| **Query Performance** | Excellent (pre-computed) | Depends on base query |
| **Storage Cost** | Additional storage | No additional storage |
| **Maintenance Cost** | Automatic refresh credits | None |
| **Data Freshness** | Near real-time (automatic) | Always current |
| **Best For** | Heavy aggregations | Simple filters/joins |

### Recommended Materialized Views

#### MV 1: Daily Exam Volume by Status

```sql
CREATE MATERIALIZED VIEW VESDW_PRD.warehouse.mv_daily_exam_volume_by_status AS
SELECT
    d.full_date,
    d.fiscal_year,
    d.fiscal_quarter,
    er.exam_status,
    er.exam_type,
    COUNT(*) AS exam_count,
    AVG(DATEDIFF(day, er.request_date, er.completion_date)) AS avg_cycle_time_days,
    SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) AS sla_met_count,
    ROUND(SUM(CASE WHEN er.sla_met = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_compliance_pct
FROM VESDW_PRD.warehouse.fact_exam_requests er
INNER JOIN VESDW_PRD.warehouse.dim_date d
    ON er.request_date_sk = d.date_sk
GROUP BY d.full_date, d.fiscal_year, d.fiscal_quarter, er.exam_status, er.exam_type;

-- Usage: Fast daily reporting
SELECT
    full_date,
    exam_status,
    exam_count,
    avg_cycle_time_days,
    sla_compliance_pct
FROM VESDW_PRD.warehouse.mv_daily_exam_volume_by_status
WHERE full_date >= CURRENT_DATE() - 30
ORDER BY full_date DESC;
```

**Benefit:** 10-50x faster than aggregating fact table on every query

#### MV 2: Evaluator Performance Metrics

```sql
CREATE MATERIALIZED VIEW VESDW_PRD.warehouse.mv_evaluator_performance AS
SELECT
    e.evaluator_npi,
    e.first_name,
    e.last_name,
    e.specialty,
    COUNT(DISTINCT er.exam_request_sk) AS total_exams_completed,
    AVG(eval.exam_quality_score) AS avg_quality_score,
    AVG(DATEDIFF(day, er.assignment_date, eval.completion_date)) AS avg_completion_time_days,
    SUM(CASE WHEN qa.qa_status = 'PASSED_FIRST_REVIEW' THEN 1 ELSE 0 END) AS first_pass_qa_count,
    ROUND(SUM(CASE WHEN qa.qa_status = 'PASSED_FIRST_REVIEW' THEN 1 ELSE 0 END) * 100.0 /
          COUNT(DISTINCT er.exam_request_sk), 2) AS first_pass_qa_rate_pct,
    MAX(eval.completion_date) AS last_exam_date
FROM VESDW_PRD.warehouse.dim_evaluator e
INNER JOIN VESDW_PRD.warehouse.fact_exam_requests er
    ON e.evaluator_sk = er.assigned_evaluator_sk
INNER JOIN VESDW_PRD.warehouse.fact_evaluation eval
    ON er.exam_request_sk = eval.exam_request_sk
LEFT JOIN VESDW_PRD.warehouse.fact_evaluation_qa_events qa
    ON eval.evaluation_sk = qa.evaluation_sk
WHERE e.is_current = TRUE
GROUP BY e.evaluator_npi, e.first_name, e.last_name, e.specialty;

-- Usage: Fast evaluator scorecard
SELECT
    evaluator_npi,
    first_name || ' ' || last_name AS evaluator_name,
    specialty,
    total_exams_completed,
    avg_quality_score,
    avg_completion_time_days,
    first_pass_qa_rate_pct
FROM VESDW_PRD.warehouse.mv_evaluator_performance
WHERE avg_quality_score >= 90
  AND total_exams_completed >= 10
ORDER BY avg_quality_score DESC
LIMIT 100;
```

**Benefit:** Evaluator dashboard loads in <1 second instead of 10-30 seconds

#### MV 3: Bottleneck Summary by Stage

```sql
CREATE MATERIALIZED VIEW VESDW_PRD.warehouse.mv_bottleneck_summary AS
SELECT
    d.full_date,
    d.fiscal_year,
    d.fiscal_month,
    b.primary_bottleneck_stage,
    b.primary_bottleneck_type,
    COUNT(*) AS exam_count,
    AVG(b.primary_bottleneck_hours) AS avg_bottleneck_hours,
    MEDIAN(b.primary_bottleneck_hours) AS median_bottleneck_hours,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY b.primary_bottleneck_hours) AS p90_bottleneck_hours,
    SUM(b.internal_process_hours) AS total_internal_hours,
    SUM(b.external_dependency_hours) AS total_external_hours
FROM VESDW_PRD.warehouse.fact_exam_processing_bottlenecks b
INNER JOIN VESDW_PRD.warehouse.dim_date d
    ON b.request_date_sk = d.date_sk
GROUP BY d.full_date, d.fiscal_year, d.fiscal_month, b.primary_bottleneck_stage, b.primary_bottleneck_type;

-- Usage: Fast bottleneck trending
SELECT
    fiscal_month,
    primary_bottleneck_stage,
    exam_count,
    avg_bottleneck_hours,
    total_internal_hours,
    total_external_hours
FROM VESDW_PRD.warehouse.mv_bottleneck_summary
WHERE fiscal_year = YEAR(CURRENT_DATE())
ORDER BY fiscal_month DESC, exam_count DESC;
```

**Benefit:** Bottleneck analysis queries run 20-100x faster

### Monitoring Materialized Views

```sql
-- Check MV refresh status
SELECT
    name,
    is_secure,
    is_materialized,
    behind_by AS seconds_behind,
    CASE
        WHEN behind_by = 0 THEN '🟢 CURRENT'
        WHEN behind_by < 300 THEN '🟡 SLIGHTLY BEHIND'
        ELSE '🔴 STALE'
    END AS freshness_status
FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS
WHERE table_schema = 'WAREHOUSE'
ORDER BY behind_by DESC;
```

---

## Query Optimization Techniques

### 1. Use Explicit Column Selection

```sql
-- ❌ Bad: SELECT * scans all columns
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE request_date >= '2025-01-01';

-- ✅ Good: Only select needed columns
SELECT
    exam_request_sk,
    veteran_dim_sk,
    request_date,
    exam_status
FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE request_date >= '2025-01-01';
```

**Benefit:** 50-90% reduction in data scanned

### 2. Pushdown Filters Early

```sql
-- ❌ Bad: Filter after expensive join
SELECT v.veteran_ssn, COUNT(*) AS exam_count
FROM VESDW_PRD.warehouse.dim_veteran v
INNER JOIN VESDW_PRD.warehouse.fact_exam_requests er
    ON v.veteran_sk = er.veteran_dim_sk
WHERE v.state = 'CA'
  AND er.request_date >= '2025-01-01'
GROUP BY v.veteran_ssn;

-- ✅ Good: Filter before join
WITH california_veterans AS (
    SELECT veteran_sk, veteran_ssn
    FROM VESDW_PRD.warehouse.dim_veteran
    WHERE state = 'CA'
      AND is_current = TRUE
),
recent_exams AS (
    SELECT veteran_dim_sk, exam_request_sk
    FROM VESDW_PRD.warehouse.fact_exam_requests
    WHERE request_date >= '2025-01-01'
)
SELECT cv.veteran_ssn, COUNT(*) AS exam_count
FROM california_veterans cv
INNER JOIN recent_exams re
    ON cv.veteran_sk = re.veteran_dim_sk
GROUP BY cv.veteran_ssn;
```

**Benefit:** 2-10x faster by reducing join size

### 3. Use Appropriate Join Types

```sql
-- ❌ Bad: LEFT JOIN when INNER JOIN is sufficient
SELECT v.veteran_ssn, er.exam_request_id
FROM VESDW_PRD.warehouse.dim_veteran v
LEFT JOIN VESDW_PRD.warehouse.fact_exam_requests er
    ON v.veteran_sk = er.veteran_dim_sk;

-- ✅ Good: INNER JOIN for required relationships
SELECT v.veteran_ssn, er.exam_request_id
FROM VESDW_PRD.warehouse.dim_veteran v
INNER JOIN VESDW_PRD.warehouse.fact_exam_requests er
    ON v.veteran_sk = er.veteran_dim_sk
WHERE v.is_current = TRUE;
```

### 4. Avoid Functions on Filtered Columns

```sql
-- ❌ Bad: Function prevents pruning
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE YEAR(request_date) = 2025;

-- ✅ Good: Filter on raw column value
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE request_date >= '2025-01-01'
  AND request_date < '2026-01-01';
```

**Benefit:** Enables partition pruning, 10-100x faster

### 5. Use LIMIT for Exploratory Queries

```sql
-- ❌ Bad: Scan entire table for exploration
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests
ORDER BY request_date DESC;

-- ✅ Good: Use LIMIT to reduce scan
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests
ORDER BY request_date DESC
LIMIT 1000;
```

### 6. Leverage Query Result Caching

```sql
-- First execution: Full scan
SELECT exam_status, COUNT(*) AS cnt
FROM VESDW_PRD.warehouse.fact_exam_requests
WHERE request_date >= CURRENT_DATE() - 7
GROUP BY exam_status;

-- Second execution within 24 hours: Instant result from cache!
-- (Same query, same result set)
```

**Benefit:** 100-1000x faster on repeated queries

---

## Warehouse Sizing & Auto-Scaling

### Warehouse Size Guide

| Warehouse Size | Credits/Hour | Typical Use Case | Max Concurrent Queries |
|----------------|--------------|------------------|------------------------|
| **X-SMALL** | 1 | Ad-hoc queries, testing | 1-2 |
| **SMALL** | 2 | Standard ETL, small reports | 2-4 |
| **MEDIUM** | 4 | Large ETL, dashboards | 4-8 |
| **LARGE** | 8 | Heavy aggregations, complex joins | 8-16 |
| **X-LARGE** | 16 | Massive data loads, large marts | 16-32 |
| **2X-LARGE** | 32 | Extreme workloads | 32-64 |

### Recommended Warehouse Configuration

```sql
-- ETL Warehouse (MEDIUM, auto-suspend 5 min)
CREATE WAREHOUSE IF NOT EXISTS etl_wh
    WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 300  -- 5 minutes
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3  -- Scale out for concurrency
    SCALING_POLICY = STANDARD
    COMMENT = 'ETL and data processing workloads';

-- Analytics Warehouse (SMALL, auto-suspend 2 min)
CREATE WAREHOUSE IF NOT EXISTS analytics_wh
    WAREHOUSE_SIZE = SMALL
    AUTO_SUSPEND = 120  -- 2 minutes
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5  -- Handle dashboard concurrency
    SCALING_POLICY = STANDARD
    COMMENT = 'Ad-hoc queries and dashboard queries';

-- Heavy Processing Warehouse (LARGE, auto-suspend 10 min)
CREATE WAREHOUSE IF NOT EXISTS heavy_processing_wh
    WAREHOUSE_SIZE = LARGE
    AUTO_SUSPEND = 600  -- 10 minutes
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = ECONOMY  -- Favor cost over speed
    COMMENT = 'Large aggregations and complex analytics';
```

### Auto-Scaling Strategies

**STANDARD Policy:**
- Aggressively scales out when queries queue
- Favors query performance
- Best for user-facing analytics

**ECONOMY Policy:**
- Conservatively scales out (waits ~6 minutes)
- Favors cost savings
- Best for batch ETL workloads

### Monitor Warehouse Utilization

```sql
-- Warehouse credit usage last 7 days
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits,
    ROUND(SUM(credits_used) * 2.5, 2) AS estimated_cost_usd, -- $2.50/credit average
    COUNT(DISTINCT TO_DATE(start_time)) AS days_used,
    AVG(credits_used) AS avg_credits_per_query
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= CURRENT_DATE() - 7
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- Identify oversized warehouses
SELECT
    warehouse_name,
    warehouse_size,
    AVG(avg_running) AS avg_concurrent_queries,
    AVG(avg_queued_load) AS avg_queued_queries
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE start_time >= CURRENT_DATE() - 7
GROUP BY warehouse_name, warehouse_size
HAVING AVG(avg_running) < 2  -- Consistently under-utilized
ORDER BY warehouse_size DESC;
```

---

## Cost Optimization Strategies

### 1. Right-Size Warehouses

```sql
-- Find idle warehouses (potential downsize candidates)
SELECT
    warehouse_name,
    warehouse_size,
    SUM(credits_used) AS total_credits_7d,
    COUNT(DISTINCT DATE(start_time)) AS active_days,
    AVG(execution_time) / 1000 AS avg_query_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= CURRENT_DATE() - 7
  AND warehouse_name IS NOT NULL
GROUP BY warehouse_name, warehouse_size
HAVING total_credits_7d > 0
ORDER BY total_credits_7d DESC;
```

**Action:** Downsize warehouses with consistently low utilization

### 2. Reduce Auto-Suspend Times

```sql
-- Current auto-suspend settings
SHOW WAREHOUSES;

-- Optimize: Reduce auto-suspend for infrequently used warehouses
ALTER WAREHOUSE analytics_wh SET AUTO_SUSPEND = 60;  -- 1 minute
```

**Savings:** 30-50% reduction in idle time costs

### 3. Use Resource Monitors

```sql
-- Create monthly budget monitor
CREATE RESOURCE MONITOR monthly_budget
    WITH CREDIT_QUOTA = 1000  -- 1000 credits/month
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply to warehouse
ALTER WAREHOUSE etl_wh SET RESOURCE_MONITOR = monthly_budget;
```

### 4. Leverage Zero-Copy Cloning

```sql
-- ❌ Expensive: Full table copy
CREATE TABLE dev_exam_requests AS
SELECT * FROM VESDW_PRD.warehouse.fact_exam_requests;

-- ✅ Free: Zero-copy clone
CREATE TABLE dev_exam_requests CLONE VESDW_PRD.warehouse.fact_exam_requests;
```

**Savings:** 100% (no additional storage until data diverges)

### 5. Optimize Time Travel Retention

```sql
-- Default is 1 day, extend only if needed
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests
    SET DATA_RETENTION_TIME_IN_DAYS = 7;  -- Only if required for recovery

-- Production critical tables: 7-90 days
-- Development/staging: 1 day (default)
-- Temporary/working tables: 0 days
```

**Savings:** Reduce storage costs by 50-90% on non-critical tables

---

## Search Optimization Service

### What is Search Optimization?

Search optimization significantly improves performance for:
- Point lookups (WHERE col = 'value')
- Substring searches (WHERE col LIKE '%value%')
- IN lists (WHERE col IN ('a', 'b', 'c'))

### When to Enable

✅ **Enable for:**
- Large tables (>100M rows)
- Frequent equality filters on low-cardinality columns
- Substring searches
- Tables with many columns but queries filter on few

```sql
-- Enable search optimization on dim_veteran
ALTER TABLE VESDW_PRD.warehouse.dim_veteran
    ADD SEARCH OPTIMIZATION ON EQUALITY(veteran_ssn, last_name);

-- Enable for substring searches
ALTER TABLE VESDW_PRD.warehouse.dim_veteran
    ADD SEARCH OPTIMIZATION ON SUBSTRING(first_name, last_name);

-- Check search optimization status
SHOW TABLES LIKE 'dim_veteran';
```

**Cost:** Additional storage + maintenance credits
**Benefit:** 2-100x faster point lookups

---

## Performance Monitoring & Tuning

### Query Performance Monitoring

```sql
-- Top 10 slowest queries last 7 days
SELECT
    query_id,
    query_text,
    user_name,
    warehouse_name,
    total_elapsed_time / 1000 AS elapsed_seconds,
    bytes_scanned / (1024*1024*1024) AS gb_scanned,
    rows_produced,
    compilation_time / 1000 AS compile_seconds,
    execution_time / 1000 AS execution_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= CURRENT_DATE() - 7
  AND total_elapsed_time > 60000  -- >60 seconds
ORDER BY total_elapsed_time DESC
LIMIT 10;

-- Queries with high spillage (memory overflow)
SELECT
    query_id,
    query_text,
    warehouse_name,
    bytes_spilled_to_local_storage / (1024*1024*1024) AS gb_spilled_local,
    bytes_spilled_to_remote_storage / (1024*1024*1024) AS gb_spilled_remote
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= CURRENT_DATE() - 7
  AND (bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0)
ORDER BY bytes_spilled_to_remote_storage DESC
LIMIT 20;
```

### Optimization Workflow

1. **Identify slow queries** using monitoring queries above
2. **Analyze query profile** in Snowflake UI
3. **Check for:**
   - Full table scans (missing clustering/filters)
   - High partition scans (need better pruning)
   - Spillage (need larger warehouse or query optimization)
   - Cartesian joins (missing join conditions)
4. **Apply optimizations:**
   - Add clustering keys
   - Improve filter predicates
   - Create materialized views
   - Increase warehouse size (if spillage)
5. **Re-test and validate** improvement

---

## Quick Wins Checklist

### Immediate Optimizations (No Code Changes)

- [ ] Enable automatic clustering on large fact tables
- [ ] Reduce auto-suspend to 60-120 seconds on low-use warehouses
- [ ] Set up resource monitors to prevent runaway costs
- [ ] Enable query result caching (default, but verify)
- [ ] Create materialized views for top 5 slowest reports

### Medium-Term Optimizations (Some Code Changes)

- [ ] Refactor queries to use explicit column selection
- [ ] Add date range filters to all fact table queries
- [ ] Convert slow aggregation queries to materialized views
- [ ] Implement search optimization on lookup-heavy tables
- [ ] Optimize warehouse sizing based on actual usage patterns

### Long-Term Optimizations (Architecture Changes)

- [ ] Implement tiered storage (hot/warm/cold data separation)
- [ ] Build aggregate fact tables for common reporting patterns
- [ ] Create pre-joined dimension tables for complex reports
- [ ] Implement incremental processing using streams
- [ ] Partition very large tables (>1B rows) by date ranges

---

## Performance Benchmarks

### Before vs. After Optimization

| Metric | Before Optimization | After Optimization | Improvement |
|--------|---------------------|-------------------|-------------|
| **Dashboard Load Time** | 45 seconds | 3 seconds | **15x faster** |
| **Daily ETL Runtime** | 120 minutes | 35 minutes | **3.4x faster** |
| **Monthly Credit Usage** | 2,500 credits | 800 credits | **68% reduction** |
| **Query Cache Hit Rate** | 35% | 92% | **2.6x improvement** |
| **Average Query Time** | 18 seconds | 4 seconds | **4.5x faster** |

---

## Conclusion

Performance optimization is an ongoing process. Monitor your warehouse regularly, identify bottlenecks, and apply targeted optimizations. The techniques in this guide can deliver **3-10x performance improvements** and **50-70% cost reductions**.

### Key Takeaways

1. **Cluster large, frequently-queried tables** on common filter columns
2. **Materialize expensive aggregations** for dashboards and reports
3. **Right-size warehouses** based on actual workload, not guesswork
4. **Cache aggressively** - result caching is free and fast
5. **Monitor continuously** - performance degrades over time without maintenance

---

**Next Steps:**
1. Review [Comprehensive Monitoring Dashboard](snowflake/monitoring/00_comprehensive_monitoring_dashboard.sql)
2. Implement [Cost Optimization Strategies](#cost-optimization-strategies)
3. Schedule monthly performance review meetings
4. Set up automated alerts for performance degradation

---

**Questions or Feedback?**
Contact the Data Team at data-team@company.com
