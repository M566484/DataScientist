# Snowflake Development Guide for Data Warehouse Engineers
## From SQL Server/Traditional Databases to Snowflake

**Target Audience:** Experienced database developers transitioning to Snowflake
**Prerequisites:** Strong SQL skills, experience with SQL Server/Oracle
**Purpose:** Bridge the gap between traditional databases and Snowflake development

**Version:** 2.0
**Last Updated:** 2025-11-17
**Author:** Data Team

---

## Table of Contents

1. [Snowflake vs. Traditional Databases](#snowflake-vs-traditional-databases)
2. [Architecture Fundamentals](#architecture-fundamentals)
3. [Database Objects in Snowflake](#database-objects-in-snowflake)
4. [SQL Differences & Gotchas](#sql-differences--gotchas)
5. [Stored Procedures & Scripting](#stored-procedures--scripting)
6. [Performance Tuning](#performance-tuning)
7. [Data Loading & Unloading](#data-loading--unloading)
8. [Security & Access Control](#security--access-control)
9. [Cost Management](#cost-management)
10. [Development Workflow](#development-workflow)
11. [Common Patterns & Best Practices](#common-patterns--best-practices)
12. [Migration Tips](#migration-tips)

---

## Snowflake vs. Traditional Databases

### Key Paradigm Shifts

| Aspect | SQL Server | Snowflake | Impact |
|--------|---------------------|-----------|--------|
| **Compute & Storage** | Coupled | Separated | Scale independently, pay separately |
| **Indexes** | Critical for performance | Not supported | Use clustering keys instead |
| **Vacuuming** | Manual | Automatic | No maintenance overhead |
| **Partitioning** | Manual DDL | Automatic micro-partitions | No partition key definition needed |
| **Statistics** | Manual updates | Automatic | No ANALYZE TABLE needed |
| **Backups** | Manual/scheduled | Automatic (Time Travel) | Built-in, query-based recovery |
| **Scaling** | Resize cluster | Elastic warehouses | Instant scale up/down |
| **Concurrency** | Limited by resources | Multi-cluster warehouses | Near-infinite concurrency |

### What You DON'T Need to Worry About

❌ **Index maintenance** - No CREATE INDEX in Snowflake
❌ **Table statistics** - Automatically maintained
❌ **Vacuum/reorganize** - Automatic behind the scenes
❌ **Partition pruning tuning** - Automatic with micro-partitions
❌ **Storage capacity planning** - Elastic, auto-scaling storage
❌ **Lock contention** - Multi-version concurrency control (MVCC)

### What You DO Need to Learn

✅ **Warehouse sizing** - Right-size compute for workload
✅ **Clustering keys** - For large tables (>1M rows)
✅ **Result caching** - Understand when queries hit cache
✅ **Time Travel** - Use for recovery and auditing
✅ **Zero-copy cloning** - Instant table/schema/database copies
✅ **Virtual warehouses** - Separate compute for different workloads
✅ **Credit consumption** - Cost model is compute-based

---

## Architecture Fundamentals

### Snowflake's 3-Layer Architecture

```
┌─────────────────────────────────────────────────────┐
│                 CLOUD SERVICES                       │
│  (Query optimization, metadata, security, etc.)     │
└─────────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────────┐
│              COMPUTE LAYER (Virtual Warehouses)     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ ETL_WH   │  │ANALYTICS │  │ REPORT_WH│         │
│  │ (MEDIUM) │  │   (SMALL)│  │ (X-SMALL)│         │
│  └──────────┘  └──────────┘  └──────────┘         │
└─────────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────────┐
│            STORAGE LAYER (Micro-partitions)         │
│     All data stored in S3/Azure/GCP automatically   │
└─────────────────────────────────────────────────────┘
```

### Virtual Warehouses

Think of warehouses as temporary SQL Server instances that auto-start/stop:

```sql
-- Create warehouse (like provisioning a SQL Server instance)
CREATE WAREHOUSE etl_wh
    WAREHOUSE_SIZE = MEDIUM    -- X-SMALL to 6X-LARGE
    AUTO_SUSPEND = 300         -- Suspend after 5 min idle (saves $$)
    AUTO_RESUME = TRUE         -- Auto-start when query runs
    MIN_CLUSTER_COUNT = 1      -- Start with 1 cluster
    MAX_CLUSTER_COUNT = 3      -- Scale out to 3 for concurrency
    SCALING_POLICY = STANDARD; -- STANDARD or ECONOMY

-- Use warehouse (like "USE DATABASE" in SQL Server)
USE WAREHOUSE etl_wh;

-- Resize on the fly (no downtime!)
ALTER WAREHOUSE etl_wh SET WAREHOUSE_SIZE = LARGE;

-- Check warehouse status
SHOW WAREHOUSES;
```

**Key Differences from SQL Server:**
- Warehouses **share** the same data (unlike separate SQL instances)
- You can have **multiple warehouses** accessing same database simultaneously
- **Auto-suspend/resume** means you only pay when running queries
- **Resizing** is instant (unlike resizing traditional database clusters)

---

## Database Objects in Snowflake

### Database Hierarchy

```
ACCOUNT
├── DATABASE (like SQL Server database)
│   ├── SCHEMA (like SQL Server schema)
│   │   ├── TABLES
│   │   ├── VIEWS
│   │   ├── STORED PROCEDURES
│   │   ├── FUNCTIONS (UDF)
│   │   ├── SEQUENCES
│   │   ├── STREAMS (CDC - unique to Snowflake!)
│   │   └── TASKS (scheduled jobs - unique to Snowflake!)
│   └── ...
├── WAREHOUSES (compute resources)
├── ROLES (access control)
└── USERS
```

### Creating Databases & Schemas

```sql
-- SQL Server style (works in Snowflake)
CREATE DATABASE VESDW_PRD;
USE DATABASE VESDW_PRD;

CREATE SCHEMA warehouse;
USE SCHEMA warehouse;

-- Snowflake also supports fully-qualified names (recommended)
CREATE TABLE VESDW_PRD.warehouse.fact_exam_requests (...);

-- Check objects
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE VESDW_PRD;
SHOW TABLES IN SCHEMA VESDW_PRD.warehouse;
```

### Tables

```sql
-- Standard table creation (very similar to SQL Server)
CREATE TABLE VESDW_PRD.warehouse.fact_exam_requests (
    exam_request_sk NUMBER(38,0) NOT NULL,  -- BIGINT equivalent
    veteran_dim_sk NUMBER(38,0),
    request_date DATE,
    exam_status VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ,  -- NTZ = No Time Zone (like DATETIME2)

    -- Primary key (enforced for metadata, NOT physically enforced!)
    CONSTRAINT pk_exam_requests PRIMARY KEY (exam_request_sk)
);

-- ⚠️ KEY DIFFERENCE: Snowflake constraints are for metadata only!
-- They are NOT enforced like SQL Server. Use for BI tools, not data integrity.
```

**Snowflake Data Types vs. SQL Server:**

| SQL Server | Snowflake | Notes |
|------------|-----------|-------|
| `INT` | `NUMBER(38,0)` or `INTEGER` | Snowflake uses arbitrary precision |
| `BIGINT` | `NUMBER(38,0)` | Same as INT in Snowflake |
| `DECIMAL(10,2)` | `NUMBER(10,2)` | Exact same |
| `FLOAT` | `FLOAT` | Same |
| `VARCHAR(MAX)` | `VARCHAR` | Snowflake VARCHAR has no length limit |
| `NVARCHAR(100)` | `VARCHAR(100)` | Snowflake VARCHAR is always Unicode |
| `DATETIME` | `TIMESTAMP_NTZ` | NTZ = No time zone |
| `DATETIMEOFFSET` | `TIMESTAMP_TZ` | TZ = With time zone |
| `VARBINARY(MAX)` | `BINARY` | Same concept |
| `IDENTITY(1,1)` | `AUTOINCREMENT` | Similar, but see Sequences below |

### Sequences (Auto-increment)

```sql
-- SQL Server IDENTITY equivalent
CREATE SEQUENCE seq_exam_request_sk START = 1 INCREMENT = 1;

-- Use in table
CREATE TABLE fact_exam_requests (
    exam_request_sk NUMBER DEFAULT seq_exam_request_sk.NEXTVAL,
    -- OR use AUTOINCREMENT
    -- exam_request_sk NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    ...
);

-- Get next value in INSERT
INSERT INTO fact_exam_requests (exam_request_sk, ...)
VALUES (seq_exam_request_sk.NEXTVAL, ...);
```

### Views

```sql
-- Standard views (same as SQL Server)
CREATE OR REPLACE VIEW vw_veteran_summary AS
SELECT
    veteran_ssn,
    first_name,
    last_name,
    disability_rating
FROM dim_veteran
WHERE is_current = TRUE;

-- Materialized views (like indexed views in SQL Server, but better!)
CREATE MATERIALIZED VIEW mv_exam_summary AS
SELECT
    exam_status,
    COUNT(*) AS exam_count,
    AVG(cycle_time_days) AS avg_cycle_time
FROM fact_exam_requests
GROUP BY exam_status;

-- Key difference: Snowflake auto-refreshes materialized views!
-- No need for manual REFRESH like traditional databases
```

---

## SQL Differences & Gotchas

### Date/Time Functions

```sql
-- SQL Server: GETDATE(), DATEADD, DATEDIFF
-- Snowflake: CURRENT_TIMESTAMP(), DATEADD, DATEDIFF

-- Get current date/time
SELECT CURRENT_TIMESTAMP();  -- Same as GETDATE()
SELECT CURRENT_DATE();       -- Date only

-- Date arithmetic
-- SQL Server:
SELECT DATEADD(DAY, 7, GETDATE());

-- Snowflake (same):
SELECT DATEADD(DAY, 7, CURRENT_DATE());

-- BUT Snowflake also supports intervals:
SELECT CURRENT_DATE() + INTERVAL '7 DAYS';
SELECT CURRENT_TIMESTAMP() - INTERVAL '2 HOURS';

-- Date difference
-- SQL Server: DATEDIFF(DAY, start_date, end_date)
-- Snowflake: DATEDIFF(DAY, start_date, end_date)  -- SAME!

-- Extract parts
SELECT YEAR(request_date), MONTH(request_date), DAY(request_date);  -- Same as SQL Server
```

### String Functions

```sql
-- Most SQL Server string functions work identically
SELECT UPPER('hello'), LOWER('WORLD'), SUBSTRING('Snowflake', 1, 4);

-- Concatenation
SELECT 'Hello' || ' ' || 'World';  -- Snowflake uses || (not + like SQL Server)
SELECT CONCAT('Hello', ' ', 'World');  -- Also works

-- Pattern matching
SELECT * FROM veterans WHERE last_name LIKE 'Sm%';  -- Same as SQL Server
SELECT * FROM veterans WHERE email REGEXP '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$';  -- Regex support!
```

### NULL Handling

```sql
-- ISNULL equivalent: NVL or COALESCE
-- SQL Server:
SELECT ISNULL(disability_rating, 0) FROM veterans;

-- Snowflake (use COALESCE or NVL):
SELECT COALESCE(disability_rating, 0) FROM veterans;  -- Standard SQL
SELECT NVL(disability_rating, 0) FROM veterans;  -- Oracle-style (also supported)

-- IFNULL (MySQL-style) also works in Snowflake
SELECT IFNULL(disability_rating, 0) FROM veterans;
```

### CASE Expressions

```sql
-- Exactly the same as SQL Server!
SELECT
    CASE
        WHEN disability_rating >= 70 THEN 'High'
        WHEN disability_rating >= 30 THEN 'Medium'
        ELSE 'Low'
    END AS rating_category
FROM veterans;
```

### Window Functions

```sql
-- Same as SQL Server 2012+
SELECT
    veteran_ssn,
    exam_date,
    ROW_NUMBER() OVER (PARTITION BY veteran_ssn ORDER BY exam_date) AS exam_number,
    LAG(exam_date) OVER (PARTITION BY veteran_ssn ORDER BY exam_date) AS previous_exam_date,
    LEAD(exam_date) OVER (PARTITION BY veteran_ssn ORDER BY exam_date) AS next_exam_date
FROM fact_exam_requests;
```

### Pivot/Unpivot

```sql
-- Snowflake supports PIVOT (same as SQL Server)
SELECT *
FROM (
    SELECT exam_type, exam_status
    FROM fact_exam_requests
)
PIVOT (
    COUNT(*)
    FOR exam_status IN ('Requested', 'Assigned', 'Completed', 'Cancelled')
) AS pivot_table;
```

### CTEs (Common Table Expressions)

```sql
-- Exactly the same as SQL Server!
WITH veteran_summary AS (
    SELECT
        veteran_ssn,
        COUNT(*) AS exam_count
    FROM fact_exam_requests
    GROUP BY veteran_ssn
)
SELECT * FROM veteran_summary WHERE exam_count > 5;

-- Recursive CTEs also work
WITH RECURSIVE date_series AS (
    SELECT CURRENT_DATE() - 30 AS date
    UNION ALL
    SELECT DATEADD(DAY, 1, date) FROM date_series WHERE date < CURRENT_DATE()
)
SELECT * FROM date_series;
```

---

## Stored Procedures & Scripting

### Snowflake Scripting (SQL-based)

Snowflake introduced Snowflake Scripting - think of it as T-SQL for Snowflake.

```sql
-- Create stored procedure (similar to T-SQL)
CREATE OR REPLACE PROCEDURE sp_load_veteran_data(
    p_batch_id VARCHAR,
    p_load_date DATE
)
RETURNS VARCHAR
LANGUAGE SQL  -- Can also be JavaScript or Python
AS
$$
DECLARE
    v_row_count INTEGER;
    v_error_message VARCHAR;
BEGIN
    -- Insert data
    INSERT INTO stg_veterans (veteran_ssn, first_name, last_name, batch_id, load_date)
    SELECT ssn, fname, lname, :p_batch_id, :p_load_date
    FROM ods_veterans_source
    WHERE load_date = :p_load_date;

    -- Get row count (use SET not SELECT for assignment)
    SET v_row_count = (SELECT ROW_COUNT());

    -- Conditional logic
    IF (v_row_count = 0) THEN
        RETURN 'WARNING: No rows loaded';
    ELSIF (v_row_count < 1000) THEN
        RETURN 'WARNING: Low row count - ' || v_row_count;
    ELSE
        RETURN 'SUCCESS: Loaded ' || v_row_count || ' rows';
    END IF;

EXCEPTION
    WHEN OTHER THEN
        -- Error handling
        v_error_message := SQLERRM;
        INSERT INTO error_log (procedure_name, error_message, error_timestamp)
        VALUES ('sp_load_veteran_data', :v_error_message, CURRENT_TIMESTAMP());
        RAISE;
END;
$$;

-- Execute procedure
CALL sp_load_veteran_data('BATCH_001', CURRENT_DATE());
```

**Key Differences from T-SQL:**

| T-SQL | Snowflake SQL Scripting | Notes |
|-------|------------------------|-------|
| `DECLARE @var INT` | `DECLARE v_var INTEGER;` | Use `v_` prefix by convention |
| `SET @var = value` | `SET v_var = value;` | Same concept |
| `SELECT @var = column` | `SELECT column INTO :v_var` | Different syntax |
| `@@ROWCOUNT` | `ROW_COUNT()` | Function instead of global var |
| `PRINT 'msg'` | `RETURN 'msg';` | No PRINT, use RETURN |
| `RAISERROR` | `RAISE EXCEPTION 'msg';` | Different syntax |
| `BEGIN TRY...CATCH` | `BEGIN...EXCEPTION WHEN` | Different syntax |

### Cursors

```sql
-- Snowflake supports cursors (similar to SQL Server)
CREATE OR REPLACE PROCEDURE sp_process_batches()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    batch_cursor CURSOR FOR
        SELECT batch_id, load_date FROM batch_control WHERE status = 'PENDING';
    v_batch_id VARCHAR;
    v_load_date DATE;
BEGIN
    OPEN batch_cursor;

    FOR record IN batch_cursor DO
        v_batch_id := record.batch_id;
        v_load_date := record.load_date;

        -- Process each batch
        CALL sp_load_veteran_data(:v_batch_id, :v_load_date);
    END FOR;

    CLOSE batch_cursor;
    RETURN 'All batches processed';
END;
$$;
```

### JavaScript Stored Procedures

```sql
-- Alternative: Use JavaScript for complex logic
CREATE OR REPLACE PROCEDURE sp_complex_transformation(p_table VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // JavaScript code
    var sql_command = `INSERT INTO ` + P_TABLE + `_processed SELECT * FROM ` + P_TABLE;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();

    return "Processed " + result.getRowCount() + " rows";
$$;
```

---

## Performance Tuning

### Clustering Keys (Instead of Indexes)

```sql
-- In SQL Server, you'd create indexes:
-- CREATE INDEX idx_request_date ON fact_exam_requests(request_date);

-- In Snowflake, use clustering keys for large tables (>1M rows):
ALTER TABLE fact_exam_requests CLUSTER BY (request_date, exam_status);

-- Check clustering health (aim for depth <10)
SELECT
    table_name,
    average_depth,
    average_overlaps
FROM TABLE(INFORMATION_SCHEMA.CLUSTERING_INFORMATION('fact_exam_requests'));

-- Snowflake automatically maintains clustering (like SQL Server auto-updates stats)
```

### Query Optimization

```sql
-- Use EXPLAIN to see query plan (like SQL Server execution plan)
EXPLAIN SELECT * FROM fact_exam_requests WHERE request_date >= '2025-01-01';

-- Check query profile in Snowflake UI (superior to SQL Server execution plan)
-- Shows: partitions scanned, bytes scanned, cache hits, spillage

-- Optimization tips:
-- 1. Always filter on clustered columns first
-- 2. Use materialized views for expensive aggregations
-- 3. Avoid SELECT * - specify columns
-- 4. Use CTEs for readability (Snowflake optimizes them well)
```

### Result Caching

```sql
-- First run: Full scan
SELECT COUNT(*) FROM fact_exam_requests;  -- 30 seconds

-- Second run within 24 hours: Instant (from cache!)
SELECT COUNT(*) FROM fact_exam_requests;  -- <1 second

-- Result cache is FREE and automatic
-- Cache invalidates when underlying data changes
```

---

## Data Loading & Unloading

### Loading Data (COPY INTO)

```sql
-- Create file format (like SQL Server format file)
CREATE FILE FORMAT csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Create stage (like SQL Server external data source)
CREATE STAGE veterans_stage
    URL = 's3://my-bucket/veterans/'
    CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...')
    FILE_FORMAT = csv_format;

-- Load data from S3/Azure/GCS
COPY INTO stg_veterans
FROM @veterans_stage/veterans_20251117.csv
FILE_FORMAT = csv_format
ON_ERROR = CONTINUE  -- Skip bad rows (log to error table)
VALIDATION_MODE = RETURN_ERRORS;  -- Validate without loading

-- Bulk load from multiple files (auto-parallel)
COPY INTO stg_veterans
FROM @veterans_stage/
PATTERN = '.*veterans_.*\\.csv'
FILE_FORMAT = csv_format;

-- Check load history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STG_VETERANS',
    START_TIME => DATEADD(HOURS, -24, CURRENT_TIMESTAMP())
));
```

### Unloading Data (COPY INTO)

```sql
-- Export to S3 (like SQL Server BCP out)
COPY INTO @my_stage/export/veterans_
FROM (SELECT * FROM stg_veterans WHERE state = 'CA')
FILE_FORMAT = csv_format
MAX_FILE_SIZE = 104857600  -- 100 MB per file
OVERWRITE = TRUE;

-- Unload with Parquet format (better compression)
COPY INTO @my_stage/export/veterans_
FROM stg_veterans
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE;
```

---

## Security & Access Control

### Role-Based Access Control (RBAC)

```sql
-- Create roles (like SQL Server roles)
CREATE ROLE data_engineer;
CREATE ROLE data_analyst;
CREATE ROLE data_scientist;

-- Grant privileges (similar to SQL Server)
GRANT USAGE ON DATABASE VESDW_PRD TO ROLE data_analyst;
GRANT USAGE ON SCHEMA VESDW_PRD.warehouse TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA VESDW_PRD.warehouse TO ROLE data_analyst;

-- Grant warehouse access (unique to Snowflake)
GRANT USAGE ON WAREHOUSE analytics_wh TO ROLE data_analyst;

-- Grant to user
GRANT ROLE data_analyst TO USER john_doe;

-- Check grants
SHOW GRANTS TO ROLE data_analyst;
SHOW GRANTS ON TABLE fact_exam_requests;
```

### Row-Level Security

```sql
-- Create secure view with row filtering (like SQL Server RLS)
CREATE SECURE VIEW vw_veterans_filtered AS
SELECT *
FROM dim_veteran
WHERE state = CURRENT_ROLE()  -- Users only see their state's data
      OR CURRENT_ROLE() = 'ADMIN';  -- Admin sees all

-- Secure views hide definition from users (like SQL Server encrypted views)
```

---

## Cost Management

### Understanding Credits

```
1 Credit ≈ $2-$4 (varies by region/account)

Warehouse Size | Credits/Hour | Typical Monthly Cost (8 hrs/day)
---------------|--------------|--------------------------------
X-SMALL        | 1            | ~$480 - $960
SMALL          | 2            | ~$960 - $1,920
MEDIUM         | 4            | ~$1,920 - $3,840
LARGE          | 8            | ~$3,840 - $7,680
```

### Cost Optimization

```sql
-- 1. Auto-suspend warehouses when idle
ALTER WAREHOUSE etl_wh SET AUTO_SUSPEND = 60;  -- 1 minute idle

-- 2. Use smallest warehouse that meets SLA
-- Start with SMALL, only size up if queries timeout

-- 3. Use dedicated warehouses for different workloads
-- ETL_WH (MEDIUM), ANALYTICS_WH (SMALL), REPORTING_WH (X-SMALL)

-- 4. Monitor credit usage
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- 5. Use resource monitors (budget alerts)
CREATE RESOURCE MONITOR monthly_limit
    WITH CREDIT_QUOTA = 1000
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE etl_wh SET RESOURCE_MONITOR = monthly_limit;
```

---

## Development Workflow

### Version Control (Git)

```bash
# Store all SQL scripts in Git
project/
├── snowflake/
│   ├── schema/00_setup_database.sql
│   ├── dimensions/01_dim_veteran.sql
│   ├── facts/01_fact_exam_requests.sql
│   ├── etl/01_load_veterans.sql
│   └── tests/test_data_quality.sql
└── README.md

# Deploy using CI/CD
snowsql -f snowflake/schema/00_setup_database.sql
snowsql -f snowflake/dimensions/01_dim_veteran.sql
```

### Testing

```sql
-- Create test framework
CREATE SCHEMA testing;

-- Unit test procedure
CREATE OR REPLACE PROCEDURE test_load_veterans()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_row_count INTEGER;
BEGIN
    -- Setup test data
    CREATE OR REPLACE TABLE testing.ods_veterans_test AS
    SELECT * FROM ods_veterans_source LIMIT 10;

    -- Execute procedure under test
    CALL sp_load_veteran_data('TEST_BATCH', CURRENT_DATE());

    -- Validate results
    SELECT COUNT(*) INTO :v_row_count FROM testing.stg_veterans WHERE batch_id = 'TEST_BATCH';

    IF (v_row_count != 10) THEN
        RAISE EXCEPTION 'Test failed: Expected 10 rows, got ' || v_row_count;
    END IF;

    -- Cleanup
    DROP TABLE testing.ods_veterans_test;
    DELETE FROM testing.stg_veterans WHERE batch_id = 'TEST_BATCH';

    RETURN 'Test passed';
END;
$$;

-- Run test
CALL test_load_veterans();
```

---

## Common Patterns & Best Practices

### Pattern 1: Incremental Loading with Streams

```sql
-- Create stream (CDC - unique to Snowflake!)
CREATE STREAM stream_veterans_changes ON TABLE ods_veterans_source;

-- Process only changes (not full reload)
MERGE INTO stg_veterans tgt
USING stream_veterans_changes src
ON tgt.veteran_ssn = src.veteran_ssn
WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET tgt.first_name = src.first_name, ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);

-- Stream automatically advances on successful DML
```

### Pattern 2: Task Orchestration

```sql
-- Create scheduled task (like SQL Server Agent job)
CREATE TASK task_daily_veteran_load
    WAREHOUSE = etl_wh
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'  -- 2 AM EST daily
AS
    CALL sp_load_veteran_data('DAILY_' || TO_VARCHAR(CURRENT_DATE()), CURRENT_DATE());

-- Create dependent task (runs AFTER parent completes)
CREATE TASK task_daily_dimension_build
    WAREHOUSE = etl_wh
    AFTER task_daily_veteran_load  -- Dependency
AS
    CALL sp_build_dim_veteran();

-- Start tasks (in reverse dependency order!)
ALTER TASK task_daily_dimension_build RESUME;
ALTER TASK task_daily_veteran_load RESUME;

-- Monitor tasks
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE != 'SUCCEEDED'
ORDER BY SCHEDULED_TIME DESC;
```

### Pattern 3: Zero-Copy Cloning

```sql
-- Clone table instantly (no data copy!)
CREATE TABLE fact_exam_requests_backup CLONE fact_exam_requests;

-- Clone entire database (for dev environment)
CREATE DATABASE vesdw_dev CLONE VESDW_PRD;

-- Clone at point in time (Time Travel)
CREATE TABLE fact_exam_requests_yesterday CLONE fact_exam_requests
    AT(OFFSET => -86400);  -- 24 hours ago

-- Zero cost until data diverges!
```

---

## Migration Tips

### From SQL Server

1. **Indexes → Clustering Keys**
   - Identify most-filtered columns
   - Add clustering on large tables only

2. **IDENTITY → AUTOINCREMENT or Sequences**
   - Both work, sequences more flexible

3. **T-SQL Procedures → Snowflake SQL Scripting**
   - Most logic translates 1:1
   - Use JavaScript for complex transformations

4. **SQL Agent Jobs → Tasks**
   - Convert job steps to stored procedures
   - Chain tasks with AFTER clause

5. **Linked Servers → External Tables or Snowpipe**
   - No linked servers, use staging approach

---

## Quick Reference Card

### Essential Commands

```sql
-- List objects
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE VESDW_PRD;
SHOW TABLES IN SCHEMA warehouse;
SHOW WAREHOUSES;
SHOW TASKS;

-- Describe objects
DESCRIBE TABLE fact_exam_requests;
DESCRIBE VIEW vw_veteran_summary;
DESCRIBE PROCEDURE sp_load_veterans();

-- Query history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE START_TIME >= CURRENT_TIMESTAMP() - INTERVAL '1 DAY'
ORDER BY START_TIME DESC;

-- Storage usage
SELECT
    table_catalog,
    table_schema,
    table_name,
    active_bytes / (1024*1024*1024) AS active_gb,
    time_travel_bytes / (1024*1024*1024) AS time_travel_gb
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_schema = 'WAREHOUSE'
ORDER BY active_gb DESC;
```

---

## Conclusion

### Key Takeaways

1. **Forget About Indexes** - Use clustering keys on large tables only
2. **Embrace Warehouses** - Separate compute for different workloads
3. **Leverage Caching** - Query result cache is free and automatic
4. **Use Time Travel** - Built-in data recovery, no backups needed
5. **Think Declarative** - Let Snowflake optimize, don't micro-manage
6. **Monitor Credits** - Watch warehouse usage, not storage
7. **Automate with Tasks** - Native scheduling, no external orchestrator needed
8. **Stream Changes** - CDC built-in, no custom triggers needed

### Next Steps

1. **Hands-On Practice** - Create a test warehouse and experiment
2. **Read Official Docs** - https://docs.snowflake.com
3. **Join Community** - Snowflake Community forums
4. **Take Training** - Snowflake University (free courses)

---

**Happy Snowflake Development!**

For questions specific to this VES project, see:
- [Performance Optimization Guide](PERFORMANCE_OPTIMIZATION_GUIDE.md)
- [Disaster Recovery Plan](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md)
- [Data Quality Framework](snowflake/quality/00_advanced_data_quality_framework.sql)

---

**Document Version:** 2.0
**Last Updated:** 2025-11-17
**Maintainer:** Data Team
