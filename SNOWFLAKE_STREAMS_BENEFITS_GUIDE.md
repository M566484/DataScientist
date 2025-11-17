---
# Snowflake Streams: Benefits and Implementation Guide
## Change Data Capture (CDC) for Incremental Processing

**Purpose:** Understand the benefits of Snowflake Streams and how to use them for efficient incremental data processing in your VES data pipeline.

---

## Table of Contents

1. [Database Architecture Overview](#database-architecture-overview)
2. [What Are Snowflake Streams?](#what-are-snowflake-streams)
3. [Key Benefits](#key-benefits)
4. [How Streams Work](#how-streams-work)
5. [Use Cases for VES Pipeline](#use-cases-for-ves-pipeline)
6. [Implementation Examples](#implementation-examples)
7. [Streams vs Full Reloads](#streams-vs-full-reloads)
8. [Cost Savings Analysis](#cost-savings-analysis)
9. [Best Practices](#best-practices)
10. [Limitations and Considerations](#limitations-and-considerations)
11. [Real-World Examples](#real-world-examples)

---

## Database Architecture Overview

### VES Data Architecture

The VES data pipeline uses a multi-database architecture:

| Database | Schema | Purpose |
|----------|--------|---------|
| **VESODS_PRDDATA_PRD** | VEMS_CORE | VEMS core operational data (veterans, evaluators, exams) |
| **VESODS_PRDDATA_PRD** | VEMS_PNM | VEMS Provider Network Management data |
| **VESDW_PRD** | ods | Legacy OMS operational data store |
| **VESDW_PRD** | staging | Merged OMS + VEMS staging layer |
| **VESDW_PRD** | warehouse | Production dimensional model |
| **VESDW_PRD** | metadata | ETL control and audit tables |

**Important:** When creating streams, ensure you reference the correct database and schema:
```sql
-- VEMS core data
CREATE STREAM stream_vems_veterans
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.veterans;

-- Legacy OMS data
CREATE STREAM stream_oms_veterans
  ON TABLE VESDW_PRD.ods.ods_veterans_source;
```

---

## What Are Snowflake Streams?

### Definition

A **Snowflake Stream** is a database object that records Data Manipulation Language (DML) changes made to a table, including:
- **Inserts** - New rows added
- **Updates** - Existing rows modified
- **Deletes** - Rows removed

Think of a stream as a "change log" that automatically tracks what changed in a table since the last time you processed it.

### Visual Concept

```
┌─────────────────────────────────────────────────────────┐
│  SOURCE TABLE: ods_veterans_source                      │
│  (New data arrives daily from OMS/VEMS)                 │
└────────────────────┬────────────────────────────────────┘
                     │
                     │ Stream monitors changes
                     ▼
┌─────────────────────────────────────────────────────────┐
│  STREAM: stream_ods_veterans_changes                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Changed Rows Only (with metadata)               │   │
│  │ ├─ METADATA$ACTION: INSERT, UPDATE, DELETE      │   │
│  │ ├─ METADATA$ISUPDATE: TRUE/FALSE                │   │
│  │ └─ METADATA$ROW_ID: Unique row identifier       │   │
│  └─────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │
                     │ Process only changes
                     ▼
┌─────────────────────────────────────────────────────────┐
│  TARGET TABLE: stg_veterans                             │
│  (Only changed records processed - FAST & CHEAP!)       │
└─────────────────────────────────────────────────────────┘
```

### Key Characteristics

| Feature | Description |
|---------|-------------|
| **Automatic** | Snowflake automatically populates the stream with changes |
| **Transactional** | Stream offset advances only after successful consumption |
| **Metadata-Rich** | Includes action type, update flags, and row IDs |
| **Efficient** | Uses Snowflake's metadata to track changes (minimal overhead) |
| **Zero-Copy** | Doesn't duplicate data, uses pointers |

---

## Key Benefits

### 1. **Massive Cost Savings** 💰

**Problem with Full Reloads:**
```
Daily full reload:
- Table: 10M rows
- Processing time: 45 minutes
- Warehouse size: LARGE
- Cost per day: $15
- Cost per month: $450
```

**With Streams (Incremental):**
```
Daily incremental load:
- Changed rows: ~50K (0.5% of table)
- Processing time: 2 minutes
- Warehouse size: SMALL
- Cost per day: $0.50
- Cost per month: $15

SAVINGS: $435/month (97% reduction!) 🎉
```

### 2. **Dramatically Faster Processing** ⚡

| Approach | Rows Processed | Processing Time | Speed Improvement |
|----------|---------------|-----------------|-------------------|
| **Full Reload** | 10,000,000 | 45 minutes | Baseline |
| **Stream (Incremental)** | 50,000 | 2 minutes | **22.5x faster** |

**Real Impact:**
- Faster SLA compliance
- More frequent refresh windows
- Near-real-time data availability
- Reduced warehouse contention

### 3. **Exactly-Once Processing Guarantee** ✅

**Problem:** What if your ETL job fails halfway through?

**Without Streams:**
```sql
-- Process all data
INSERT INTO target SELECT * FROM source;
-- If this fails, you lose track of what was processed
-- Risk: Duplicates or missed records on retry
```

**With Streams:**
```sql
-- Stream offset only advances on successful COMMIT
BEGIN TRANSACTION;
  INSERT INTO target SELECT * FROM my_stream;
COMMIT;  -- ← Stream offset advances HERE

-- If transaction fails, stream retains all changes
-- Retry processes exact same changes - no duplicates!
```

### 4. **Automatic Change Tracking** 🔍

No need to:
- Add "last_modified" timestamp columns
- Write complex date-based queries
- Maintain watermark tables
- Track processed IDs manually

Snowflake handles all of this automatically!

### 5. **Supports All DML Operations** 📝

Tracks not just inserts, but also:
- **Updates** - Know which records changed
- **Deletes** - Remove records from target
- **Merges** - Complex upsert operations

**Example:**
```sql
-- Stream shows you:
-- - Row A: INSERTED
-- - Row B: UPDATED
-- - Row C: DELETED

-- You can handle each action differently
MERGE INTO target USING my_stream
  WHEN MATCHED AND METADATA$ACTION = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE
  WHEN NOT MATCHED THEN INSERT
```

### 6. **Zero Maintenance** 🛠️

**No manual work required:**
- ✅ Automatically captures changes
- ✅ No triggers to create/maintain
- ✅ No audit tables to manage
- ✅ No complex date logic
- ✅ No partition tracking

Just create the stream once and use it!

### 7. **Perfect for Micro-Batch Processing** 🔄

Process data as frequently as needed:
- Every 5 minutes ✓
- Every hour ✓
- Every day ✓

**Stream only has data when changes exist**, so you're not wasting compute on empty runs.

```sql
-- Task only runs when stream has data
CREATE TASK process_changes
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('my_stream')  -- ← Smart!
AS
  INSERT INTO target SELECT * FROM my_stream;
```

---

## How Streams Work

### Stream Lifecycle

```
1. CREATE STREAM
   ├─ Snowflake creates metadata pointer to current table state
   └─ Stream offset = current table version

2. TABLE CHANGES OCCUR
   ├─ Rows inserted, updated, deleted
   └─ Snowflake tracks changes in stream

3. QUERY STREAM
   ├─ SELECT * FROM my_stream
   └─ Returns only changed rows since stream creation

4. CONSUME STREAM
   ├─ DML operation in same transaction as stream read
   ├─ INSERT/MERGE using stream as source
   └─ On COMMIT, stream offset advances

5. STREAM NOW EMPTY
   └─ Until next set of changes occurs
```

### Stream Metadata Columns

Every stream includes special metadata columns:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `METADATA$ACTION` | VARCHAR | Type of change | 'INSERT', 'DELETE' |
| `METADATA$ISUPDATE` | BOOLEAN | TRUE if row is part of UPDATE | TRUE |
| `METADATA$ROW_ID` | VARCHAR | Unique row identifier | 'abc123...' |

**Understanding Updates:**

When a row is UPDATED, stream shows TWO records:
1. Old version with `METADATA$ACTION = 'DELETE'` and `METADATA$ISUPDATE = TRUE`
2. New version with `METADATA$ACTION = 'INSERT'` and `METADATA$ISUPDATE = TRUE`

This allows you to see before/after state!

### Stream Types

| Type | Description | Use Case |
|------|-------------|----------|
| **Standard** | Tracks INSERT, UPDATE, DELETE | Most common, full CDC |
| **Append-Only** | Tracks INSERT only | Immutable data (logs, events) |
| **Insert-Only** | Tracks INSERT only (legacy name) | Same as append-only |

**Create Standard Stream:**
```sql
CREATE STREAM my_stream ON TABLE my_table;
```

**Create Append-Only Stream:**
```sql
CREATE STREAM my_stream ON TABLE my_table
  APPEND_ONLY = TRUE;
```

---

## Use Cases for VES Pipeline

### Use Case 1: Incremental ODS to Staging

**Scenario:** Daily ODS data extraction from OMS/VEMS

**Problem:** Processing all 10M+ veteran records daily is slow and expensive.

**Solution:** Stream-based incremental load

```sql
-- Create stream on ODS table
CREATE STREAM stream_ods_veterans_changes
  ON TABLE VESODS_PRDDATA_PRD.ods_veterans_source;

-- Process only changes
CREATE TASK task_incremental_staging
  WAREHOUSE = etl_task_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_ods_veterans_changes')
AS
BEGIN
  MERGE INTO VESDW_PRD.staging.stg_veterans tgt
  USING stream_ods_veterans_changes src
  ON tgt.veteran_ssn = src.veteran_ssn
  WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' THEN
    DELETE
  WHEN MATCHED AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
      first_name = src.first_name,
      disability_rating = src.disability_rating,
      updated_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (veteran_ssn, first_name, disability_rating, created_timestamp)
    VALUES (src.veteran_ssn, src.first_name, src.disability_rating, CURRENT_TIMESTAMP());
END;
```

**Impact:**
- **Before:** 45 min, $15/day
- **After:** 2 min, $0.50/day
- **Savings:** 95% cost reduction, 22x faster

---

### Use Case 2: Real-Time Fact Table Updates

**Scenario:** Keep fact tables current as exam requests arrive

**Problem:** Full reload of fact tables is slow (millions of rows).

**Solution:** Stream on staging layer to incrementally update facts

```sql
-- Stream on staging exam requests
CREATE STREAM stream_stg_exam_requests
  ON TABLE VESDW_PRD.staging.stg_fact_exam_requests;

-- Task processes new/changed requests every 15 minutes
CREATE TASK task_update_fact_exam_requests
  WAREHOUSE = etl_task_wh
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_stg_exam_requests')
AS
BEGIN
  -- Insert new exam requests
  INSERT INTO VESDW_PRD.warehouse.fact_exam_requests (
    exam_request_sk,
    veteran_dim_sk,
    request_date_sk,
    request_type,
    sla_days
  )
  SELECT
    VESDW_PRD.warehouse.seq_exam_request_sk.NEXTVAL,
    dv.veteran_sk,
    dd.date_sk,
    s.request_type,
    s.sla_days
  FROM stream_stg_exam_requests s
  JOIN VESDW_PRD.warehouse.dim_veteran dv
    ON s.master_veteran_id = dv.veteran_ssn
  JOIN VESDW_PRD.warehouse.dim_date dd
    ON s.request_date = dd.full_date
  WHERE s.METADATA$ACTION = 'INSERT';
END;
```

**Impact:**
- Near-real-time fact table updates
- No need to wait for daily batch
- Users see latest data within 15 minutes

---

### Use Case 3: Audit Trail with Historical Changes

**Scenario:** Track all changes to veteran disability ratings for audit/compliance

**Problem:** Need to see history of rating changes over time.

**Solution:** Stream captures all updates for audit table

```sql
-- Stream captures all changes
CREATE STREAM stream_veteran_changes
  ON TABLE VESDW_PRD.staging.stg_veterans;

-- Task logs all changes to audit table
CREATE TASK task_audit_veteran_changes
  WAREHOUSE = etl_task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_veteran_changes')
AS
BEGIN
  INSERT INTO VESDW_PRD.audit.veteran_change_history (
    veteran_ssn,
    change_type,
    old_disability_rating,
    new_disability_rating,
    changed_timestamp
  )
  SELECT
    veteran_ssn,
    METADATA$ACTION AS change_type,
    CASE WHEN METADATA$ISUPDATE = TRUE AND METADATA$ACTION = 'DELETE'
      THEN disability_rating ELSE NULL END AS old_rating,
    CASE WHEN METADATA$ISUPDATE = TRUE AND METADATA$ACTION = 'INSERT'
      THEN disability_rating ELSE disability_rating END AS new_rating,
    CURRENT_TIMESTAMP()
  FROM stream_veteran_changes
  WHERE disability_rating IS NOT NULL;
END;
```

**Impact:**
- Complete audit trail of rating changes
- Compliance-ready
- No manual tracking needed

---

### Use Case 4: Data Quality Alerts

**Scenario:** Alert when critical data issues occur

**Problem:** Need to notify team immediately when bad data arrives.

**Solution:** Stream-based quality monitoring

```sql
-- Stream monitors incoming data
CREATE STREAM stream_dq_monitor
  ON TABLE VESODS_PRDDATA_PRD.ods_veterans_source;

-- Task checks quality every 5 minutes
CREATE TASK task_dq_alerts
  WAREHOUSE = etl_task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_dq_monitor')
AS
DECLARE
  v_invalid_count INT;
BEGIN
  -- Count quality issues in new data
  SELECT COUNT(*)
  INTO :v_invalid_count
  FROM stream_dq_monitor
  WHERE disability_rating NOT BETWEEN 0 AND 100
     OR first_name IS NULL
     OR veteran_ssn IS NULL;

  -- Send alert if issues found
  IF (v_invalid_count > 0) THEN
    CALL sp_send_email_alert(
      'Data Quality Alert',
      '<h2>Invalid Records Detected</h2>' ||
      '<p>Found ' || :v_invalid_count || ' records with quality issues.</p>',
      ARRAY_CONSTRUCT('data-quality-team@company.com')
    );
  END IF;
END;
```

**Impact:**
- Immediate detection of data quality issues
- Proactive alerting
- Prevents bad data from propagating

---

### Use Case 5: Handling Late Arriving Facts

**Scenario:** Exam data arrives late or out-of-order, requiring updates to already-loaded fact tables

**Problem:** When fact records arrive after dimension processing, or when fact corrections arrive days later, you need to:
- Identify which fact records are new vs late-arriving updates
- Update existing fact records without creating duplicates
- Maintain referential integrity with dimensions
- Track correction history for audit purposes

**Solution:** Stream-based late arriving fact detection and processing

```sql
-- Create stream on staging fact table
CREATE STREAM stream_stg_exam_requests_late_arriving
  ON TABLE VESDW_PRD.staging.stg_fact_exam_requests;

-- Task processes late arriving facts
CREATE TASK task_handle_late_arriving_facts
  WAREHOUSE = etl_task_wh
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_stg_exam_requests_late_arriving')
AS
DECLARE
  v_new_count INT;
  v_update_count INT;
  v_late_arriving_count INT;
BEGIN
  -- Identify late arriving facts (facts with dates in the past)
  WITH late_arriving_facts AS (
    SELECT
      s.*,
      f.exam_request_sk,
      CASE
        WHEN f.exam_request_sk IS NULL THEN 'NEW'
        WHEN s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = FALSE THEN 'NEW'
        WHEN s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = TRUE THEN 'UPDATE'
        WHEN f.exam_request_sk IS NOT NULL AND s.request_date < CURRENT_DATE() - 7 THEN 'LATE_ARRIVING'
        ELSE 'CURRENT'
      END AS arrival_type
    FROM stream_stg_exam_requests_late_arriving s
    LEFT JOIN VESDW_PRD.warehouse.fact_exam_requests f
      ON s.source_exam_request_id = f.source_exam_request_id
      AND s.source_system = f.source_system
  )

  -- Process NEW facts (standard insert)
  INSERT INTO VESDW_PRD.warehouse.fact_exam_requests (
    exam_request_sk,
    veteran_dim_sk,
    evaluator_dim_sk,
    request_date_sk,
    source_exam_request_id,
    source_system,
    request_type,
    disability_rating,
    sla_days,
    created_timestamp
  )
  SELECT
    VESDW_PRD.warehouse.seq_exam_request_sk.NEXTVAL,
    dv.veteran_sk,
    de.evaluator_sk,
    dd.date_sk,
    laf.source_exam_request_id,
    laf.source_system,
    laf.request_type,
    laf.disability_rating,
    laf.sla_days,
    CURRENT_TIMESTAMP()
  FROM late_arriving_facts laf
  JOIN VESDW_PRD.warehouse.dim_veteran dv
    ON laf.master_veteran_id = dv.veteran_ssn
    AND dv.is_current = TRUE
  JOIN VESDW_PRD.warehouse.dim_evaluator de
    ON laf.master_evaluator_id = de.evaluator_npi
    AND de.is_current = TRUE
  JOIN VESDW_PRD.warehouse.dim_date dd
    ON laf.request_date = dd.full_date
  WHERE laf.arrival_type = 'NEW';

  SELECT ROW_COUNT() INTO :v_new_count;

  -- Process LATE ARRIVING facts (update existing records)
  MERGE INTO VESDW_PRD.warehouse.fact_exam_requests tgt
  USING (
    SELECT
      f.exam_request_sk,
      dv.veteran_sk AS new_veteran_sk,
      de.evaluator_sk AS new_evaluator_sk,
      dd.date_sk AS new_request_date_sk,
      laf.request_type,
      laf.disability_rating,
      laf.sla_days,
      laf.source_exam_request_id,
      laf.source_system
    FROM late_arriving_facts laf
    JOIN VESDW_PRD.warehouse.fact_exam_requests f
      ON laf.source_exam_request_id = f.source_exam_request_id
      AND laf.source_system = f.source_system
    JOIN VESDW_PRD.warehouse.dim_veteran dv
      ON laf.master_veteran_id = dv.veteran_ssn
      AND dv.is_current = TRUE
    JOIN VESDW_PRD.warehouse.dim_evaluator de
      ON laf.master_evaluator_id = de.evaluator_npi
      AND de.is_current = TRUE
    JOIN VESDW_PRD.warehouse.dim_date dd
      ON laf.request_date = dd.full_date
    WHERE laf.arrival_type = 'LATE_ARRIVING'
  ) src
  ON tgt.exam_request_sk = src.exam_request_sk
  WHEN MATCHED THEN
    UPDATE SET
      veteran_dim_sk = src.new_veteran_sk,
      evaluator_dim_sk = src.new_evaluator_sk,
      request_date_sk = src.new_request_date_sk,
      request_type = src.request_type,
      disability_rating = src.disability_rating,
      sla_days = src.sla_days,
      updated_timestamp = CURRENT_TIMESTAMP(),
      is_late_arriving = TRUE;

  SELECT ROW_COUNT() INTO :v_late_arriving_count;

  -- Log late arriving facts for audit
  INSERT INTO VESDW_PRD.metadata.late_arriving_fact_log (
    fact_table_name,
    fact_sk,
    source_record_id,
    source_system,
    arrival_date,
    event_date,
    days_late,
    change_type,
    created_timestamp
  )
  SELECT
    'fact_exam_requests' AS fact_table_name,
    f.exam_request_sk,
    laf.source_exam_request_id,
    laf.source_system,
    CURRENT_DATE() AS arrival_date,
    laf.request_date AS event_date,
    DATEDIFF(day, laf.request_date, CURRENT_DATE()) AS days_late,
    laf.arrival_type AS change_type,
    CURRENT_TIMESTAMP()
  FROM late_arriving_facts laf
  LEFT JOIN VESDW_PRD.warehouse.fact_exam_requests f
    ON laf.source_exam_request_id = f.source_exam_request_id
    AND laf.source_system = f.source_system
  WHERE laf.arrival_type IN ('LATE_ARRIVING', 'UPDATE');

  -- Send alert if many late arriving facts detected
  IF (:v_late_arriving_count > 100) THEN
    CALL sp_send_email_alert(
      'Late Arriving Facts Alert',
      '<h2>High Volume of Late Arriving Facts</h2>' ||
      '<p>Detected ' || :v_late_arriving_count || ' late arriving exam requests.</p>' ||
      '<p>New records: ' || :v_new_count || '</p>' ||
      '<p>This may indicate an upstream data quality issue.</p>',
      ARRAY_CONSTRUCT('data-quality-team@company.com')
    );
  END IF;

  -- Log processing summary
  INSERT INTO VESDW_PRD.metadata.etl_task_log (
    task_name,
    batch_id,
    status,
    start_time,
    end_time,
    records_processed,
    error_message,
    created_timestamp
  )
  VALUES (
    'task_handle_late_arriving_facts',
    'LAF_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'),
    'SUCCESS',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    :v_new_count + :v_late_arriving_count,
    'New: ' || :v_new_count || ', Late: ' || :v_late_arriving_count,
    CURRENT_TIMESTAMP()
  );

EXCEPTION
  WHEN OTHER THEN
    INSERT INTO VESDW_PRD.metadata.etl_task_log (
      task_name,
      status,
      error_message,
      created_timestamp
    )
    VALUES (
      'task_handle_late_arriving_facts',
      'FAILED',
      SQLERRM,
      CURRENT_TIMESTAMP()
    );
    RAISE;
END;
```

#### Supporting Table for Tracking Late Arriving Facts

```sql
-- Create audit table to track late arriving facts
CREATE TABLE IF NOT EXISTS VESDW_PRD.metadata.late_arriving_fact_log (
    log_id NUMBER AUTOINCREMENT PRIMARY KEY,
    fact_table_name VARCHAR(200),
    fact_sk NUMBER,
    source_record_id VARCHAR(100),
    source_system VARCHAR(50),
    arrival_date DATE,
    event_date DATE,
    days_late NUMBER,
    change_type VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add flag to fact table to mark late arriving records
ALTER TABLE VESDW_PRD.warehouse.fact_exam_requests
  ADD COLUMN IF NOT EXISTS is_late_arriving BOOLEAN DEFAULT FALSE;
```

#### Monitoring Late Arriving Facts

```sql
-- Query to monitor late arriving fact patterns
CREATE OR REPLACE VIEW VESDW_PRD.metadata.vw_late_arriving_fact_summary AS
SELECT
    fact_table_name,
    arrival_date,
    COUNT(*) AS late_arriving_count,
    AVG(days_late) AS avg_days_late,
    MAX(days_late) AS max_days_late,
    MIN(days_late) AS min_days_late,
    source_system
FROM VESDW_PRD.metadata.late_arriving_fact_log
WHERE arrival_date >= CURRENT_DATE() - 30
GROUP BY fact_table_name, arrival_date, source_system
ORDER BY arrival_date DESC, late_arriving_count DESC;

-- Alert query for excessive late arrivals
SELECT
    fact_table_name,
    source_system,
    COUNT(*) AS late_arrivals_today,
    AVG(days_late) AS avg_days_late
FROM VESDW_PRD.metadata.late_arriving_fact_log
WHERE arrival_date = CURRENT_DATE()
  AND days_late > 7  -- More than 7 days late
GROUP BY fact_table_name, source_system
HAVING COUNT(*) > 50;  -- More than 50 late arrivals
```

**Impact:**
- Automatic detection and handling of late arriving facts
- No duplicate fact records
- Complete audit trail of late arrivals
- Proactive alerting when data arrives consistently late
- Ability to analyze patterns in data latency

**Key Benefits:**
1. **Data Quality:** Ensures fact tables remain accurate even when data arrives late
2. **Audit Trail:** Complete history of late arriving facts for compliance
3. **Automated Detection:** Stream automatically identifies new vs late-arriving records
4. **Alerting:** Proactive notification when late arrivals exceed thresholds
5. **Pattern Analysis:** Identify systemic upstream data quality issues

**When to Use This Pattern:**
- Source systems have unreliable delivery schedules
- Fact records can be corrected days/weeks after initial load
- Regulatory requirements mandate accurate historical data
- Multiple source systems with varying SLAs
- Need to distinguish between new facts and corrections

---

## Streams vs Full Reloads

### Detailed Comparison

| Aspect | Full Reload | Stream (Incremental) | Winner |
|--------|-------------|---------------------|--------|
| **Processing Time** | 45 minutes | 2 minutes | 🏆 Stream (22x faster) |
| **Cost per Run** | $15 | $0.50 | 🏆 Stream (97% cheaper) |
| **Network I/O** | All rows scanned | Only changed rows | 🏆 Stream |
| **Warehouse Size Needed** | LARGE | SMALL | 🏆 Stream |
| **Complexity** | Simple SELECT * | Slightly more complex | Full Reload |
| **Exactly-Once Guarantee** | Manual tracking | Automatic | 🏆 Stream |
| **Historical Changes** | Lost (overwrite) | Captured | 🏆 Stream |
| **Change Detection** | Manual (timestamps) | Automatic | 🏆 Stream |
| **Suitable for Real-Time** | No (too slow) | Yes (sub-minute) | 🏆 Stream |

### When to Use Each Approach

#### Use Full Reload When:
- ✅ Small tables (<100K rows)
- ✅ Data completely changes each run
- ✅ Simplicity is paramount
- ✅ One-time historical load
- ✅ No incremental logic needed

#### Use Streams When:
- ✅ Large tables (>1M rows)
- ✅ Only small % changes daily (<10%)
- ✅ Need near-real-time updates
- ✅ Want to track change history
- ✅ Cost optimization is important
- ✅ Exactly-once processing required

### Hybrid Approach

Best practice: **Use both!**

```sql
-- Weekly full reload (data quality check, catch drift)
CREATE TASK task_weekly_full_reload
  WAREHOUSE = etl_heavy_task_wh
  SCHEDULE = 'USING CRON 0 2 * * 0 America/New_York'  -- Sunday 2 AM
AS
  TRUNCATE TABLE VESDW_PRD.staging.stg_veterans;
  INSERT INTO VESDW_PRD.staging.stg_veterans
  SELECT * FROM VESODS_PRDDATA_PRD.ods_veterans_source;

-- Daily incremental (fast, cheap)
CREATE TASK task_daily_incremental
  WAREHOUSE = etl_task_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_ods_veterans_changes')
AS
  MERGE INTO VESDW_PRD.staging.stg_veterans
  USING stream_ods_veterans_changes ...;
```

**Benefits:**
- Daily incremental = fast & cheap
- Weekly full reload = data integrity validation
- Best of both worlds!

---

## Cost Savings Analysis

### Real-World Example: VES Veteran Data

**Assumptions:**
- Total veterans: 10,000,000 rows
- Daily changes: 50,000 rows (0.5% change rate)
- Processing frequency: Daily
- Warehouse cost: SMALL = $2/hour, LARGE = $8/hour

### Full Reload Approach

```
Processing:
- Rows scanned: 10,000,000
- Warehouse: LARGE (needed for volume)
- Duration: 45 minutes = 0.75 hours
- Cost per run: 0.75 hours × $8/hour = $6

Monthly:
- Runs per month: 30
- Total cost: 30 × $6 = $180/month
```

### Stream-Based Approach

```
Processing:
- Rows scanned: 50,000 (only changes)
- Warehouse: SMALL (sufficient for volume)
- Duration: 2 minutes = 0.033 hours
- Cost per run: 0.033 hours × $2/hour = $0.07

Monthly:
- Runs per month: 30
- Total cost: 30 × $0.07 = $2.10/month
```

### Savings

| Metric | Full Reload | Stream | Savings |
|--------|-------------|--------|---------|
| **Cost/Month** | $180 | $2.10 | **$177.90 (99%)** |
| **Time/Run** | 45 min | 2 min | **43 min (95%)** |
| **Rows/Run** | 10M | 50K | **99.5% reduction** |

### Annual Savings

```
Full Reload: $180/month × 12 = $2,160/year
Stream:      $2.10/month × 12 = $25/year

Annual Savings: $2,135 per table! 💰
```

**For entire VES pipeline (10+ tables):**
- Annual savings: **$21,350+**
- Time saved: **8,600+ minutes/year** (143 hours)

---

## Implementation Examples

### Example 1: Simple Append-Only Stream

**Use Case:** Event log table that only grows (no updates/deletes)

```sql
-- Create append-only stream
CREATE STREAM stream_exam_events
  ON TABLE VESODS_PRDDATA_PRD.ods_exam_events
  APPEND_ONLY = TRUE;

-- Process new events every 5 minutes
CREATE TASK task_process_exam_events
  WAREHOUSE = etl_task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_exam_events')
AS
BEGIN
  -- Simply insert new events
  INSERT INTO VESDW_PRD.warehouse.fact_exam_events
  SELECT
    seq_exam_event_sk.NEXTVAL,
    event_timestamp,
    event_type,
    exam_request_id
  FROM stream_exam_events;
END;
```

**Why Append-Only?**
- Faster (doesn't track updates/deletes)
- Simpler metadata
- Perfect for immutable data

---

### Example 2: Full CDC with MERGE

**Use Case:** Dimension table with inserts, updates, and deletes

```sql
-- Create standard stream (tracks all changes)
CREATE STREAM stream_evaluator_changes
  ON TABLE VESODS_PRDDATA_PRD.ods_evaluators_source;

-- Process changes with MERGE
CREATE TASK task_sync_evaluators
  WAREHOUSE = etl_task_wh
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_evaluator_changes')
AS
BEGIN
  MERGE INTO VESDW_PRD.warehouse.dim_evaluator tgt
  USING (
    SELECT
      evaluator_npi,
      first_name,
      last_name,
      specialty,
      METADATA$ACTION,
      METADATA$ISUPDATE
    FROM stream_evaluator_changes
  ) src
  ON tgt.evaluator_npi = src.evaluator_npi

  -- Handle deletes
  WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' AND src.METADATA$ISUPDATE = FALSE THEN
    DELETE

  -- Handle updates
  WHEN MATCHED AND src.METADATA$ACTION = 'INSERT' AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
      first_name = src.first_name,
      last_name = src.last_name,
      specialty = src.specialty,
      updated_timestamp = CURRENT_TIMESTAMP()

  -- Handle inserts
  WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    INSERT (
      evaluator_sk,
      evaluator_npi,
      first_name,
      last_name,
      specialty,
      created_timestamp
    )
    VALUES (
      seq_evaluator_sk.NEXTVAL,
      src.evaluator_npi,
      src.first_name,
      src.last_name,
      src.specialty,
      CURRENT_TIMESTAMP()
    );
END;
```

---

### Example 3: Change Data Archival

**Use Case:** Archive all changes for compliance

```sql
-- Stream captures all changes
CREATE STREAM stream_veteran_audit
  ON TABLE VESDW_PRD.staging.stg_veterans;

-- Archive every change to history table
CREATE TASK task_archive_changes
  WAREHOUSE = etl_task_wh
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_veteran_audit')
AS
BEGIN
  INSERT INTO VESDW_PRD.audit.veteran_change_archive (
    veteran_ssn,
    change_action,
    change_timestamp,
    first_name,
    last_name,
    disability_rating,
    is_update,
    row_id
  )
  SELECT
    veteran_ssn,
    METADATA$ACTION,
    CURRENT_TIMESTAMP(),
    first_name,
    last_name,
    disability_rating,
    METADATA$ISUPDATE,
    METADATA$ROW_ID
  FROM stream_veteran_audit;
END;
```

**Result:** Complete audit trail of every change, forever!

---

### Example 4: Conditional Processing

**Use Case:** Different logic for different change types

```sql
CREATE TASK task_smart_processing
  WAREHOUSE = etl_task_wh
  SCHEDULE = '10 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('my_stream')
AS
DECLARE
  v_insert_count INT;
  v_update_count INT;
  v_delete_count INT;
BEGIN
  -- Count change types
  SELECT
    SUM(CASE WHEN METADATA$ACTION = 'INSERT' THEN 1 ELSE 0 END),
    SUM(CASE WHEN METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE THEN 1 ELSE 0 END),
    SUM(CASE WHEN METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE THEN 1 ELSE 0 END)
  INTO :v_insert_count, :v_update_count, :v_delete_count
  FROM my_stream;

  -- Different logic based on change type
  IF (v_insert_count > 1000) THEN
    -- Bulk insert path
    INSERT INTO target SELECT * FROM my_stream WHERE METADATA$ACTION = 'INSERT';
  ELSIF (v_delete_count > 100) THEN
    -- Alert if many deletes
    CALL sp_send_email_alert('WARNING', 'Many deletes detected', ...);
  END IF;
END;
```

---

## Best Practices

### 1. **Stream Naming Conventions**

Follow these standardized naming conventions for stream objects:

#### Standard Stream Naming Pattern

```
stream_<source_system>_<entity>_<purpose>
```

**Components:**
- `stream_` - Required prefix
- `<source_system>` - Source system identifier (vems, oms, stg, dim, fact)
- `<entity>` - Entity or table name (veterans, evaluators, exams)
- `<purpose>` - Optional purpose suffix (changes, audit, dq, archive)

#### Naming Examples

```sql
-- ✅ Good: Clear, descriptive stream names

-- VEMS core data streams
CREATE STREAM stream_vems_veterans_changes
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.veterans;

CREATE STREAM stream_vems_evaluators_changes
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.evaluators;

-- OMS legacy data streams
CREATE STREAM stream_oms_veterans_changes
  ON TABLE VESDW_PRD.ods.ods_veterans_source;

-- Staging layer streams
CREATE STREAM stream_stg_veterans_changes
  ON TABLE VESDW_PRD.staging.stg_veterans;

CREATE STREAM stream_stg_veterans_audit
  ON TABLE VESDW_PRD.staging.stg_veterans
  COMMENT = 'Audit trail stream for compliance';

-- Fact table streams
CREATE STREAM stream_fact_exam_requests_changes
  ON TABLE VESDW_PRD.warehouse.fact_exam_requests;

-- Data quality monitoring stream
CREATE STREAM stream_vems_veterans_dq
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.veterans
  COMMENT = 'Data quality monitoring stream';
```

#### Purpose Suffixes

| Suffix | Purpose | Example |
|--------|---------|---------|
| `_changes` | General CDC (default) | `stream_vems_veterans_changes` |
| `_audit` | Audit trail / compliance | `stream_stg_veterans_audit` |
| `_dq` | Data quality monitoring | `stream_vems_evaluators_dq` |
| `_archive` | Historical archival | `stream_fact_exams_archive` |
| `_sync` | Synchronization between systems | `stream_oms_veterans_sync` |

#### Anti-Patterns

```sql
-- ❌ Bad: Generic, unclear names
CREATE STREAM my_stream ON TABLE veterans;
CREATE STREAM s1 ON TABLE veterans;
CREATE STREAM temp_stream ON TABLE veterans;

-- ❌ Bad: Inconsistent naming
CREATE STREAM veteran_stream ON TABLE veterans;
CREATE STREAM EvaluatorStream ON TABLE evaluators;
CREATE STREAM STREAM_exam_requests ON TABLE exam_requests;

-- ❌ Bad: Missing context
CREATE STREAM changes ON TABLE veterans;
CREATE STREAM audit ON TABLE veterans;
```

#### Stream Placement Best Practice

**Always create streams in the same schema as their source table:**

```sql
-- ✅ Correct: Stream in same schema as table
CREATE STREAM VESDW_PRD.staging.stream_stg_veterans_changes
  ON TABLE VESDW_PRD.staging.stg_veterans;

CREATE STREAM VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veterans_changes
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.veterans;

-- ❌ Incorrect: Stream in different database
CREATE STREAM VESDW_PRD.staging.stream_vems_veterans
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.veterans;
```

**Why?** Keeps streams organized with their source tables and simplifies permissions management.

---

### 2. **Stream Ownership and Lifecycle**

✅ **Do:**
```sql
-- Use consistent naming pattern
CREATE STREAM VESDW_PRD.staging.stream_stg_veterans_changes
  ON TABLE VESDW_PRD.staging.stg_veterans;

-- Add descriptive comments
CREATE STREAM stream_vems_veterans_changes
  ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.veterans
  COMMENT = 'CDC stream for incremental veteran data processing';
```

❌ **Don't:**
```sql
-- Generic stream names
CREATE STREAM my_stream ON TABLE ...;

-- Streams in different database from source
CREATE STREAM other_db.my_stream ON TABLE my_db.my_table;
```

---

### 3. **Monitor Stream Lag**

Check how much data is waiting in streams:

```sql
-- Check stream size
SELECT
    table_schema,
    table_name AS stream_name,
    stale,
    bytes,
    ROUND(bytes / 1024 / 1024 / 1024, 2) AS size_gb
FROM INFORMATION_SCHEMA.TABLES
WHERE table_type = 'STREAM'
  AND table_schema = 'STAGING'
ORDER BY bytes DESC;
```

**Alert if stream is growing too large:**
```sql
-- If stream > 10 GB, you're falling behind!
IF (stream_size_gb > 10) THEN
    CALL sp_send_email_alert('Stream Lag Alert', ...);
END IF;
```

---

### 4. **Use WHEN SYSTEM$STREAM_HAS_DATA**

**Always use this in task schedules:**

```sql
-- ✅ Good: Only runs when changes exist
CREATE TASK my_task
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('my_stream')
AS ...;

-- ❌ Bad: Runs every 5 min even if no changes
CREATE TASK my_task
  SCHEDULE = '5 MINUTE'
AS
  SELECT * FROM my_stream;  -- May be empty!
```

**Benefit:** Saves $$$ by not running when there's nothing to do.

---

### 5. **Handle Stream Offset Carefully**

**Stream offset advances on DML in same transaction:**

```sql
-- ✅ Correct: Stream consumed in transaction
BEGIN TRANSACTION;
  INSERT INTO target SELECT * FROM my_stream;
COMMIT;  -- Stream offset advances here

-- ❌ Wrong: Stream not consumed
BEGIN TRANSACTION;
  SELECT COUNT(*) FROM my_stream;  -- Just querying
COMMIT;  -- Stream offset does NOT advance!
```

**Rule:** Stream offset only advances when you **INSERT, UPDATE, DELETE, or MERGE** using the stream.

---

### 6. **Recreate Streams for Full Reload**

When doing a full reload, recreate the stream:

```sql
-- Full reload procedure
BEGIN TRANSACTION;
  -- Drop stream
  DROP STREAM IF EXISTS stream_my_table;

  -- Full reload
  TRUNCATE TABLE target;
  INSERT INTO target SELECT * FROM source;

  -- Recreate stream (fresh start)
  CREATE STREAM stream_my_table ON TABLE source;
COMMIT;
```

**Why?** Prevents stream from thinking truncated data was "deleted."

---

### 7. **Test Stream Logic with Manual Queries**

Before automating, test manually:

```sql
-- See what's in the stream
SELECT *, METADATA$ACTION, METADATA$ISUPDATE
FROM my_stream
LIMIT 100;

-- Test your MERGE without committing
BEGIN TRANSACTION;
  MERGE INTO target USING my_stream ...;
  -- Check results
  SELECT COUNT(*) FROM target;
ROLLBACK;  -- Don't commit, stream offset unchanged
```

---

### 8. **Use Append-Only for Immutable Data**

**If data never updates/deletes, use append-only:**

```sql
-- Append-only is faster and simpler
CREATE STREAM stream_log_events
  ON TABLE log_events
  APPEND_ONLY = TRUE;
```

**Use cases:**
- Log tables
- Event streams
- Immutable fact tables
- Sensor/IoT data

---

### 9. **Document Stream Dependencies**

Keep a registry of streams:

```sql
CREATE TABLE VESDW_PRD.metadata.stream_registry (
    stream_name VARCHAR,
    source_table VARCHAR,
    target_table VARCHAR,
    task_name VARCHAR,
    stream_type VARCHAR,
    created_date DATE,
    purpose VARCHAR
);

-- Example entry
INSERT INTO stream_registry VALUES (
    'stream_ods_veterans_changes',
    'VESODS_PRDDATA_PRD.ods_veterans_source',
    'VESDW_PRD.staging.stg_veterans',
    'task_incremental_staging',
    'STANDARD',
    CURRENT_DATE(),
    'Incremental veteran data from ODS to staging'
);
```

---

## Limitations and Considerations

### 1. **14-Day Data Retention**

**Limitation:** Streams can only access changes from the last 14 days (extended data retention period).

**Impact:**
- If you don't consume a stream for >14 days, you may miss changes
- Stream becomes "stale"

**Solution:**
```sql
-- Check if stream is stale
SELECT stale
FROM INFORMATION_SCHEMA.TABLES
WHERE table_name = 'STREAM_ODS_VETERANS_CHANGES'
  AND table_type = 'STREAM';

-- If stale = TRUE, do a full reload
```

**Best Practice:** Process streams at least once every 7 days.

---

### 2. **No Streams on External Tables**

**Limitation:** Cannot create streams on external tables (e.g., data in S3).

**Workaround:**
1. Load external data into native Snowflake table
2. Create stream on native table

---

### 3. **No Streams on Views**

**Limitation:** Streams can only be created on base tables, not views.

**Workaround:**
- Create stream on underlying base table
- Apply view logic in consumption query

---

### 4. **Transactional Consumption Required**

**Limitation:** Stream offset only advances with DML operations, not SELECT.

**Not Consumed:**
```sql
-- Just reading doesn't consume stream
SELECT * FROM my_stream;
```

**Consumed:**
```sql
-- These consume the stream:
INSERT INTO target SELECT * FROM my_stream;
MERGE INTO target USING my_stream ...;
DELETE FROM target WHERE id IN (SELECT id FROM my_stream);
```

---

### 5. **Table Truncate Implications**

**Behavior:** If source table is truncated, stream shows all rows as DELETED.

**Example:**
```sql
-- Source has 1M rows
-- Stream is empty (no recent changes)

TRUNCATE TABLE source;

-- Stream now shows 1M rows with METADATA$ACTION = 'DELETE'
```

**Solution:** Recreate stream after truncate (see Best Practice #5).

---

### 6. **Performance with Very Large Streams**

**Issue:** If stream accumulates millions of rows, consumption can be slow.

**Solution:**
- Process streams more frequently
- Use larger warehouse for batch consumption
- Consider partitioning strategy

---

## Real-World Examples

### Example 1: Daily Veteran Data Sync

**Before (Full Reload):**
```
Daily Process:
- Extract 10M veterans from OMS
- Full reload to staging: 45 min, LARGE warehouse
- Cost: $6/day = $180/month
```

**After (Stream-Based):**
```sql
CREATE STREAM stream_oms_veterans
  ON TABLE VESODS_PRDDATA_PRD.ods_veterans_source;

CREATE TASK task_sync_veterans
  WAREHOUSE = etl_task_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_oms_veterans')
AS
  MERGE INTO VESDW_PRD.staging.stg_veterans
  USING stream_oms_veterans ...;
```

**Results:**
- Daily changes: ~50K rows (0.5%)
- Processing time: 2 min, SMALL warehouse
- Cost: $0.07/day = $2.10/month
- **Savings: $177.90/month (99%)**

---

### Example 2: Near-Real-Time Exam Request Dashboard

**Requirement:** Executives want to see exam request counts updated every 15 minutes.

**Before:** Not possible (daily batch only)

**After:**
```sql
CREATE STREAM stream_exam_requests
  ON TABLE VESODS_PRDDATA_PRD.ods_exam_requests_source;

CREATE TASK task_update_dashboard
  WAREHOUSE = etl_task_wh
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_exam_requests')
AS
  MERGE INTO VESDW_PRD.warehouse.fact_exam_requests ...;
```

**Results:**
- Dashboard updates every 15 minutes
- Users see near-real-time data
- Minimal cost (only processes changes)

---

### Example 3: Compliance Audit Trail

**Requirement:** Track every change to veteran disability ratings for 7 years.

**Solution:**
```sql
CREATE STREAM stream_rating_changes
  ON TABLE VESDW_PRD.staging.stg_veterans;

CREATE TASK task_audit_ratings
  WAREHOUSE = etl_task_wh
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('stream_rating_changes')
AS
BEGIN
  INSERT INTO VESDW_PRD.audit.rating_change_history (
    veteran_ssn,
    old_rating,
    new_rating,
    change_timestamp,
    change_type
  )
  SELECT
    veteran_ssn,
    LAG(disability_rating) OVER (PARTITION BY veteran_ssn ORDER BY METADATA$ROW_ID) AS old_rating,
    disability_rating AS new_rating,
    CURRENT_TIMESTAMP(),
    METADATA$ACTION
  FROM stream_rating_changes
  WHERE disability_rating IS NOT NULL;
END;
```

**Results:**
- Complete audit trail of all rating changes
- Sub-minute latency
- Compliance-ready

---

## Quick Reference

### Common Stream Commands

```sql
-- Create stream
CREATE STREAM stream_name ON TABLE table_name;

-- Create append-only stream
CREATE STREAM stream_name ON TABLE table_name APPEND_ONLY = TRUE;

-- View stream contents
SELECT *, METADATA$ACTION, METADATA$ISUPDATE FROM stream_name;

-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('stream_name');

-- View stream metadata
SHOW STREAMS LIKE 'stream_name';

-- Check stream size
SELECT bytes FROM INFORMATION_SCHEMA.TABLES
WHERE table_name = 'STREAM_NAME' AND table_type = 'STREAM';

-- Drop stream
DROP STREAM stream_name;

-- Recreate stream (reset offset)
CREATE OR REPLACE STREAM stream_name ON TABLE table_name;
```

### Stream Metadata Columns

| Column | Type | Description |
|--------|------|-------------|
| `METADATA$ACTION` | VARCHAR | 'INSERT' or 'DELETE' |
| `METADATA$ISUPDATE` | BOOLEAN | TRUE if part of UPDATE |
| `METADATA$ROW_ID` | VARCHAR | Unique row identifier |

### Decision Tree: Should I Use a Stream?

```
Is table size > 1M rows? ────── NO ──→ Use full reload
         │
        YES
         │
         ▼
Do < 10% of rows change daily? ── NO ──→ Use full reload
         │
        YES
         │
         ▼
Need near-real-time updates? ─── YES ──→ USE STREAM ✓
         │
        NO
         │
         ▼
Want to save 90%+ on costs? ─── YES ──→ USE STREAM ✓
```

---

## Conclusion

### Key Takeaways

1. **Streams Save Money** - 90-99% cost reduction for incremental processing
2. **Streams Save Time** - 10-50x faster than full reloads
3. **Streams Are Simple** - Automatic change tracking, no manual logic
4. **Streams Are Reliable** - Exactly-once processing guarantee
5. **Streams Enable Real-Time** - Sub-minute data latency possible

### When to Use Streams

✅ **Perfect for:**
- Large tables (>1M rows)
- Low change rate (<10% daily)
- Near-real-time requirements
- Cost optimization
- Audit trails
- Incremental ETL

❌ **Not ideal for:**
- Small tables (<100K rows)
- High change rate (>50% daily)
- External tables
- One-time loads

### Next Steps

1. ✅ Identify candidate tables for streams (large, low change rate)
2. ✅ Create streams on high-value tables
3. ✅ Test stream consumption logic
4. ✅ Deploy stream-based tasks
5. ✅ Monitor stream performance and cost savings
6. ✅ Expand to more tables based on success

### Additional Resources

- **Snowflake Docs:** https://docs.snowflake.com/en/user-guide/streams
- **Streams Best Practices:** https://docs.snowflake.com/en/user-guide/streams-intro
- **Task + Stream Patterns:** See `SNOWFLAKE_ORCHESTRATION_GUIDE.md`

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Author:** Data Team

**Questions?** Review the implementation examples or consult the orchestration guide for task integration patterns.
