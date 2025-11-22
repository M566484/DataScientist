# Accumulating Snapshot Fact Tables: Data Flow Guide

## Table of Contents
1. [Overview](#overview)
2. [What Are Accumulating Snapshot Fact Tables?](#what-are-accumulating-snapshot-fact-tables)
3. [Accumulating Snapshots in VES Data Warehouse](#accumulating-snapshots-in-ves-data-warehouse)
4. [Pipeline Architecture](#pipeline-architecture)
5. [Detailed Data Flow](#detailed-data-flow)
6. [The MERGE Pattern: Key to Accumulating Snapshots](#the-merge-pattern-key-to-accumulating-snapshots)
7. [Real-World Example: Exam Request Journey](#real-world-example-exam-request-journey)
8. [Performance Considerations](#performance-considerations)
9. [Query Patterns](#query-patterns)
10. [File Reference Guide](#file-reference-guide)

---

## Overview

This document explains how data flows through the VES Data Warehouse pipeline for **accumulating snapshot fact tables**—a specialized dimensional modeling pattern designed to track multi-stage business processes from start to finish.

**Key Concept**: Unlike traditional fact tables that insert new rows for every event, accumulating snapshot fact tables maintain **one row per process instance** and **update that row** as the process moves through defined stages/milestones.

---

## What Are Accumulating Snapshot Fact Tables?

### Definition

An accumulating snapshot fact table tracks a business process with a **clear beginning and end**, capturing multiple milestone dates as the process progresses.

### Characteristics

| Feature | Description |
|---------|-------------|
| **Grain** | One row per business process instance (e.g., one exam request, one claim) |
| **Updates** | Rows are updated frequently as process moves through stages |
| **Growth Rate** | Moderate—one row per process instance ever created |
| **Date Keys** | Multiple date foreign keys—one for each milestone in the process |
| **Null Handling** | Future milestone dates remain NULL until reached |
| **Metrics** | Pre-calculated lag facts (days between milestones) |

### When to Use

✅ **Perfect For:**
- Processes with defined stages/milestones (e.g., order fulfillment, claim processing, exam requests)
- Tracking cycle times and stage durations
- SLA monitoring and bottleneck identification
- "How long did each stage take?" questions

❌ **Not Appropriate For:**
- One-time events without stages (use transaction facts instead)
- Continuous measurements (use periodic snapshots instead)
- Processes without clear start/end points

---

## Accumulating Snapshots in VES Data Warehouse

### Our Three Accumulating Snapshot Fact Tables

| Fact Table | Location | Purpose | Milestones |
|------------|----------|---------|------------|
| **fact_exam_requests** | `snowflake/facts/07_fact_exam_requests.sql` | Tracks exam requests from submission to completion | 10 stages: Received → Validated → Eligibility Confirmed → Assigned → Accepted → Scheduled → Completed → Closed/Cancelled |
| **fact_claim_status_changes** | `snowflake/facts/02_fact_claim_status.sql` | Tracks disability claims through processing stages | 9 stages: Filed → Received → Initial Review → Evidence Request → Evidence Received → Exam Scheduled → Exam Completed → Decision → Notification |
| **fact_appointments_scheduled** | `snowflake/facts/03_fact_appointment.sql` | Tracks appointment scheduling and attendance | 5 stages: Requested → Scheduled → Confirmed → Completed → No-Show |

### Example Structure: fact_exam_requests

```sql
CREATE TABLE fact_exam_requests (
    -- Surrogate Key
    exam_request_sk INTEGER PRIMARY KEY,

    -- Dimension Foreign Keys
    veteran_sk INTEGER NOT NULL,
    facility_sk INTEGER,
    exam_request_type_sk INTEGER NOT NULL,
    assigned_evaluator_sk INTEGER,

    -- ============================================
    -- MULTIPLE DATE FOREIGN KEYS (ONE PER MILESTONE)
    -- ============================================
    request_received_date_sk INTEGER NOT NULL,       -- Stage 1 (always populated)
    request_validated_date_sk INTEGER,               -- Stage 2 (NULL until validated)
    eligibility_confirmed_date_sk INTEGER,           -- Stage 3 (NULL until confirmed)
    examiner_assigned_date_sk INTEGER,               -- Stage 4 (NULL until assigned)
    examiner_accepted_date_sk INTEGER,               -- Stage 5 (NULL until accepted)
    appointment_scheduled_date_sk INTEGER,           -- Stage 6 (NULL until scheduled)
    exam_completed_date_sk INTEGER,                  -- Stage 7 (NULL until completed)
    request_closed_date_sk INTEGER,                  -- Stage 8 (NULL until closed)
    request_cancelled_date_sk INTEGER,               -- Stage 9 (NULL if not cancelled)

    -- ============================================
    -- PRE-CALCULATED LAG FACTS (DAYS BETWEEN MILESTONES)
    -- ============================================
    days_to_assignment INTEGER,                      -- Received → Assigned
    days_to_scheduling INTEGER,                      -- Assigned → Scheduled
    days_to_completion INTEGER,                      -- Scheduled → Completed
    total_cycle_time_days INTEGER,                   -- Received → Closed

    -- ============================================
    -- SLA TRACKING
    -- ============================================
    sla_days_allowed INTEGER,
    sla_due_date DATE,
    sla_met_flag BOOLEAN,
    sla_variance_days INTEGER,

    -- ============================================
    -- CURRENT STATUS (UPDATED AS PROCESS MOVES)
    -- ============================================
    assignment_status VARCHAR(50),                   -- UNASSIGNED, IN_QUEUE, ASSIGNED, etc.
    request_priority VARCHAR(20),                    -- ROUTINE, PRIORITY, URGENT, EXPEDITE

    -- Audit Columns
    created_timestamp TIMESTAMP_NTZ,
    updated_timestamp TIMESTAMP_NTZ
)
CLUSTER BY (request_received_date_sk, assignment_status);
```

**Key Observation**: Notice how milestone dates are **nullable**—they remain NULL until that stage is reached. This allows us to query for "in-progress" vs. "completed" requests.

---

## Pipeline Architecture

### Four-Layer Architecture

The VES Data Warehouse follows a **medallion-style architecture** with four distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCE SYSTEMS                              │
│  - VES OMS (Operations Management System)                       │
│  - VEMS (Veterans Exam Management System)                       │
│  - VA Systems (Veterans Affairs)                                │
└────────────────────────┬────────────────────────────────────────┘
                         │ Extract (ELT)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│             LAYER 1: ODS_RAW (Raw Data Lake)                    │
│  Tables: ods_exam_requests_source, ods_claims_source            │
│  Purpose: Verbatim copy of source data (no transformations)     │
│  Load: Incremental / CDC (Change Data Capture)                  │
└────────────────────────┬────────────────────────────────────────┘
                         │ Transform & Validate
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│           LAYER 2: STAGING (Transformation Layer)               │
│  Tables: stg_fact_exam_requests, stg_fact_claim_status          │
│  Purpose: Cleansed, validated, business-rule-applied data       │
│  Operations:                                                     │
│    - Standardize field names and values                         │
│    - Calculate derived metrics (SLA, lag times)                 │
│    - Resolve dimension keys                                     │
│    - Data quality validation                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │ MERGE (Accumulating Logic)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│      LAYER 3: WAREHOUSE (Dimensional Model - Kimball)           │
│  Tables: fact_exam_requests, fact_claim_status_changes          │
│  Purpose: Production-ready dimensional model                    │
│  Load Pattern: MERGE with COALESCE (key to accumulating!)       │
│  Operations:                                                     │
│    - INSERT new process instances                               │
│    - UPDATE existing rows with new milestone dates              │
│    - Preserve already-set milestones                            │
└────────────────────────┬────────────────────────────────────────┘
                         │ Aggregate & Curate
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│          LAYER 4: MARTS (Analytics & Reporting)                 │
│  Views: vw_exam_request_performance, vw_claim_analytics         │
│  Purpose: Business-friendly aggregations and KPIs               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Detailed Data Flow

### Step-by-Step: How an Exam Request Flows Through the Pipeline

Let's trace a single exam request from source to warehouse to understand the accumulating snapshot pattern.

---

### **STEP 1: Extract from Source → ODS_RAW**

**Location**: `snowflake/ods/01_create_ods_tables.sql`

**Source System**: VES OMS (Operations Management System)

A new exam request arrives from the VA:

```json
{
  "exam_request_id": "ER-2024-12345",
  "va_request_number": "VA-20240115-0001",
  "veteran_va_id": "V123456789",
  "request_received_date": "2024-01-15",
  "request_status": "NEW",
  "assignment_status": "UNASSIGNED",
  "requested_conditions": "Back pain, PTSD",
  "request_priority": "ROUTINE",
  "sla_days_allowed": 30,
  "facility_code": "FAC-001"
}
```

**Action**: Raw data is loaded into `ods_exam_requests_source` **exactly as received** (no transformations).

**Table**: `ods_exam_requests_source`

```sql
SELECT * FROM ods_exam_requests_source WHERE exam_request_id = 'ER-2024-12345';

-- Result (Day 1):
exam_request_id     | va_request_number    | veteran_va_id | request_received_date | assignment_status | ...
--------------------|----------------------|---------------|----------------------|-------------------|----
ER-2024-12345      | VA-20240115-0001     | V123456789    | 2024-01-15           | UNASSIGNED        | ...
```

**File Reference**: `snowflake/ods/01_create_ods_tables.sql:45-120`

---

### **STEP 2: Transform → STAGING**

**Location**: `snowflake/etl/02_etl_procedures_facts.sql`

**Procedure**: `sp_transform_ods_to_staging_exam_requests(p_batch_id)`

**Action**: TRUNCATE staging table and INSERT transformed data

**Transformations Applied**:

1. **Standardize Identifiers**
   ```sql
   COALESCE(veteran_va_id, 'SSN-' || veteran_ssn) AS veteran_id
   ```

2. **Calculate Derived Metrics**
   ```sql
   DATEDIFF(day, request_received_date, CURRENT_DATE()) AS days_pending,
   CASE WHEN appointment_scheduled_date IS NOT NULL THEN TRUE ELSE FALSE END AS scheduled_flag,
   CASE
       WHEN ARRAY_SIZE(SPLIT(requested_conditions, ',')) > 3 THEN TRUE
       ELSE FALSE
   END AS complex_case_flag
   ```

3. **Apply Business Rules**
   ```sql
   CASE
       WHEN request_priority = 'EXPEDITE' THEN sla_days_allowed * 0.5
       WHEN request_priority = 'URGENT' THEN sla_days_allowed * 0.75
       ELSE sla_days_allowed
   END AS effective_sla_days
   ```

4. **Resolve Dimension Keys**
   ```sql
   LEFT JOIN dim_veterans vet
       ON src.veteran_id = vet.veteran_id
       AND vet.is_current = TRUE
   LEFT JOIN dim_dates d_req
       ON src.request_received_date = d_req.full_date
   LEFT JOIN dim_facilities fac
       ON src.facility_code = fac.facility_code
       AND fac.is_current = TRUE
   ```

**Result in Staging**:

```sql
SELECT * FROM stg_fact_exam_requests WHERE exam_request_id = 'ER-2024-12345';

-- Result (cleansed and enriched):
exam_request_id | veteran_sk | facility_sk | request_received_date_sk | days_to_assignment | scheduled_flag | ...
----------------|------------|-------------|-------------------------|-------------------|----------------|----
ER-2024-12345  | 456        | 12          | 20240115                | NULL              | FALSE          | ...
```

**File Reference**: `snowflake/etl/02_etl_procedures_facts.sql:150-280`

---

### **STEP 3: MERGE → WAREHOUSE (The Magic Happens Here!)**

**Location**: `snowflake/etl/02_etl_procedures_facts.sql`

**Procedure**: `sp_load_fact_exam_requests(p_batch_id)`

**Action**: MERGE INTO fact table using accumulating snapshot logic

**Key Pattern**: `MERGE` statement with `COALESCE` to preserve existing milestone dates

```sql
MERGE INTO fact_exam_requests AS tgt
USING stg_fact_exam_requests AS src
ON tgt.exam_request_id = src.exam_request_id  -- Business key match

-- ============================================
-- SCENARIO 1: NEW REQUEST (NOT MATCHED)
-- ============================================
WHEN NOT MATCHED THEN
    INSERT (
        exam_request_sk,
        veteran_sk,
        facility_sk,
        exam_request_type_sk,
        request_received_date_sk,
        request_validated_date_sk,      -- NULL initially
        eligibility_confirmed_date_sk,  -- NULL initially
        examiner_assigned_date_sk,      -- NULL initially
        appointment_scheduled_date_sk,  -- NULL initially
        exam_completed_date_sk,         -- NULL initially
        request_closed_date_sk,         -- NULL initially
        assignment_status,
        request_priority,
        sla_days_allowed,
        created_timestamp
    )
    VALUES (
        src.exam_request_sk,
        src.veteran_sk,
        src.facility_sk,
        src.exam_request_type_sk,
        src.request_received_date_sk,   -- Always populated
        src.request_validated_date_sk,  -- NULL on Day 1
        src.eligibility_confirmed_date_sk,  -- NULL on Day 1
        src.examiner_assigned_date_sk,  -- NULL on Day 1
        src.appointment_scheduled_date_sk,  -- NULL on Day 1
        src.exam_completed_date_sk,     -- NULL on Day 1
        src.request_closed_date_sk,     -- NULL on Day 1
        src.assignment_status,          -- 'UNASSIGNED'
        src.request_priority,           -- 'ROUTINE'
        src.sla_days_allowed,           -- 30
        CURRENT_TIMESTAMP()
    )

-- ============================================
-- SCENARIO 2: EXISTING REQUEST (MATCHED) - THE ACCUMULATING MAGIC!
-- ============================================
WHEN MATCHED THEN
    UPDATE SET
        -- ===== ACCUMULATING SNAPSHOT PATTERN =====
        -- Use COALESCE to preserve existing dates while accepting new ones
        -- Pattern: COALESCE(new_value, old_value)
        --   - If new_value is NOT NULL, use it
        --   - If new_value is NULL, keep old_value
        --   - This prevents overwriting milestone dates once set

        request_validated_date_sk = COALESCE(
            src.request_validated_date_sk,
            tgt.request_validated_date_sk
        ),

        eligibility_confirmed_date_sk = COALESCE(
            src.eligibility_confirmed_date_sk,
            tgt.eligibility_confirmed_date_sk
        ),

        examiner_assigned_date_sk = COALESCE(
            src.examiner_assigned_date_sk,
            tgt.examiner_assigned_date_sk
        ),

        examiner_accepted_date_sk = COALESCE(
            src.examiner_accepted_date_sk,
            tgt.examiner_accepted_date_sk
        ),

        appointment_scheduled_date_sk = COALESCE(
            src.appointment_scheduled_date_sk,
            tgt.appointment_scheduled_date_sk
        ),

        exam_completed_date_sk = COALESCE(
            src.exam_completed_date_sk,
            tgt.exam_completed_date_sk
        ),

        request_closed_date_sk = COALESCE(
            src.request_closed_date_sk,
            tgt.request_closed_date_sk
        ),

        -- ===== ALWAYS UPDATE THESE (NOT ACCUMULATED) =====
        -- Current status and metrics are recalculated each time

        assigned_evaluator_sk = src.assigned_evaluator_sk,
        assignment_status = src.assignment_status,
        days_to_assignment = src.days_to_assignment,
        days_to_scheduling = src.days_to_scheduling,
        days_to_completion = src.days_to_completion,
        total_cycle_time_days = src.total_cycle_time_days,
        sla_met_flag = src.sla_met_flag,
        sla_variance_days = src.sla_variance_days,
        updated_timestamp = CURRENT_TIMESTAMP();
```

**File Reference**: `snowflake/etl/02_etl_procedures_facts.sql:450-580`

---

### **Visual Timeline: How a Single Row Evolves**

Let's watch row `ER-2024-12345` evolve through its lifecycle:

**Day 1 (January 15): Request Received**
```sql
SELECT * FROM fact_exam_requests WHERE exam_request_id = 'ER-2024-12345';

exam_request_sk | request_received_date_sk | examiner_assigned_date_sk | appointment_scheduled_date_sk | exam_completed_date_sk | assignment_status | days_to_assignment | total_cycle_time_days
----------------|-------------------------|--------------------------|------------------------------|----------------------|-------------------|--------------------|---------------------
12345          | 20240115                | NULL                      | NULL                          | NULL                 | UNASSIGNED        | NULL               | NULL
```

**Day 3 (January 18): Examiner Assigned**

Source system updates:
```sql
UPDATE ods_exam_requests_source
SET
    assignment_status = 'ASSIGNED',
    assigned_evaluator_npi = 'NPI123456',
    assignment_date = '2024-01-18'
WHERE exam_request_id = 'ER-2024-12345';
```

After ETL runs:
```sql
SELECT * FROM fact_exam_requests WHERE exam_request_id = 'ER-2024-12345';

exam_request_sk | request_received_date_sk | examiner_assigned_date_sk | appointment_scheduled_date_sk | exam_completed_date_sk | assignment_status | days_to_assignment | total_cycle_time_days
----------------|-------------------------|--------------------------|------------------------------|----------------------|-------------------|--------------------|---------------------
12345          | 20240115                | 20240118 ← NEW!          | NULL                          | NULL                 | ASSIGNED          | 3 ← CALCULATED     | NULL
```

**Day 5 (January 20): Appointment Scheduled**

Source system updates:
```sql
UPDATE ods_exam_requests_source
SET
    appointment_scheduled_date = '2024-01-20',
    scheduled_exam_date = '2024-01-25'
WHERE exam_request_id = 'ER-2024-12345';
```

After ETL runs:
```sql
SELECT * FROM fact_exam_requests WHERE exam_request_id = 'ER-2024-12345';

exam_request_sk | request_received_date_sk | examiner_assigned_date_sk | appointment_scheduled_date_sk | exam_completed_date_sk | assignment_status | days_to_assignment | days_to_scheduling | total_cycle_time_days
----------------|-------------------------|--------------------------|------------------------------|----------------------|-------------------|--------------------|--------------------|---------------------
12345          | 20240115                | 20240118 ← PRESERVED     | 20240120 ← NEW!               | NULL                 | SCHEDULED         | 3 ← PRESERVED      | 2 ← CALCULATED     | NULL
```

**Notice**: The `examiner_assigned_date_sk` value (20240118) was **preserved** thanks to COALESCE!

**Day 10 (January 25): Exam Completed**

Source system updates:
```sql
UPDATE ods_exam_requests_source
SET
    exam_status = 'COMPLETED',
    exam_completed_date = '2024-01-25'
WHERE exam_request_id = 'ER-2024-12345';
```

After ETL runs:
```sql
SELECT * FROM fact_exam_requests WHERE exam_request_id = 'ER-2024-12345';

exam_request_sk | request_received_date_sk | examiner_assigned_date_sk | appointment_scheduled_date_sk | exam_completed_date_sk | assignment_status | days_to_assignment | days_to_scheduling | days_to_completion | total_cycle_time_days | sla_met_flag
----------------|-------------------------|--------------------------|------------------------------|----------------------|-------------------|--------------------|--------------------|--------------------|-----------------------|-------------
12345          | 20240115                | 20240118 ← PRESERVED     | 20240120 ← PRESERVED          | 20240125 ← NEW!      | COMPLETED         | 3 ← PRESERVED      | 2 ← PRESERVED      | 5 ← CALCULATED     | 10 ← CALCULATED       | TRUE (10 < 30)
```

**Day 11 (January 26): Request Closed**

Source system updates:
```sql
UPDATE ods_exam_requests_source
SET
    request_status = 'CLOSED',
    request_closed_date = '2024-01-26'
WHERE exam_request_id = 'ER-2024-12345';
```

After ETL runs (FINAL STATE):
```sql
SELECT * FROM fact_exam_requests WHERE exam_request_id = 'ER-2024-12345';

exam_request_sk | request_received_date_sk | examiner_assigned_date_sk | appointment_scheduled_date_sk | exam_completed_date_sk | request_closed_date_sk | assignment_status | days_to_assignment | days_to_scheduling | days_to_completion | total_cycle_time_days | sla_met_flag | sla_variance_days
----------------|-------------------------|--------------------------|------------------------------|----------------------|------------------------|-------------------|--------------------|--------------------|--------------------|-----------------------|--------------|-------------------
12345          | 20240115                | 20240118 ← PRESERVED     | 20240120 ← PRESERVED          | 20240125 ← PRESERVED | 20240126 ← NEW!        | CLOSED            | 3 ← PRESERVED      | 2 ← PRESERVED      | 5 ← PRESERVED      | 11 ← FINAL            | TRUE         | -19 (11-30)
```

**Final Result**: We now have **one complete row** capturing the entire lifecycle with all milestone dates preserved and all lag metrics calculated!

---

## The MERGE Pattern: Key to Accumulating Snapshots

### Why COALESCE Is Critical

The `COALESCE(new_value, old_value)` pattern is the **secret sauce** of accumulating snapshot fact tables.

**Problem Without COALESCE**:
```sql
-- BAD: This would overwrite milestone dates with NULL on subsequent updates
UPDATE SET examiner_assigned_date_sk = src.examiner_assigned_date_sk
```

If the source system only sends the "current" state, this would set previously-captured milestone dates back to NULL!

**Solution With COALESCE**:
```sql
-- GOOD: This preserves existing dates and only updates when new values arrive
UPDATE SET examiner_assigned_date_sk = COALESCE(
    src.examiner_assigned_date_sk,  -- Use new value if present
    tgt.examiner_assigned_date_sk   -- Otherwise keep old value
)
```

### COALESCE Logic Flow

```
NEW VALUE from source?
    │
    ├─ YES (NOT NULL) ──→ Use new value (milestone just reached!)
    │
    └─ NO (NULL) ──→ Keep old value (milestone already captured)
```

### Real Code Example

From `snowflake/etl/02_etl_procedures_facts.sql:520-540`:

```sql
MERGE INTO fact_exam_requests AS tgt
USING stg_fact_exam_requests AS src
ON tgt.exam_request_id = src.exam_request_id

WHEN MATCHED THEN UPDATE SET

    -- Milestone dates: ACCUMULATE (preserve once set)
    request_validated_date_sk = COALESCE(src.request_validated_date_sk, tgt.request_validated_date_sk),
    eligibility_confirmed_date_sk = COALESCE(src.eligibility_confirmed_date_sk, tgt.eligibility_confirmed_date_sk),
    examiner_assigned_date_sk = COALESCE(src.examiner_assigned_date_sk, tgt.examiner_assigned_date_sk),
    examiner_accepted_date_sk = COALESCE(src.examiner_accepted_date_sk, tgt.examiner_accepted_date_sk),
    appointment_scheduled_date_sk = COALESCE(src.appointment_scheduled_date_sk, tgt.appointment_scheduled_date_sk),
    exam_completed_date_sk = COALESCE(src.exam_completed_date_sk, tgt.exam_completed_date_sk),
    request_closed_date_sk = COALESCE(src.request_closed_date_sk, tgt.request_closed_date_sk),

    -- Current state: REPLACE (always use latest)
    assignment_status = src.assignment_status,
    request_priority = src.request_priority,
    days_to_assignment = src.days_to_assignment,
    total_cycle_time_days = src.total_cycle_time_days,
    sla_met_flag = src.sla_met_flag,

    -- Audit
    updated_timestamp = CURRENT_TIMESTAMP();
```

---

## Real-World Example: Exam Request Journey

### Complete End-to-End Flow

Let's trace exam request `ER-2024-99999` through the entire pipeline with realistic data.

#### **Initial State (ODS)**

```sql
-- ods_exam_requests_source (Day 1: January 15, 2024)
exam_request_id: ER-2024-99999
va_request_number: VA-20240115-9999
veteran_va_id: V987654321
request_received_date: 2024-01-15
request_status: NEW
assignment_status: UNASSIGNED
requested_conditions: PTSD, Hearing Loss, Back Pain
request_priority: URGENT
sla_days_allowed: 30
facility_code: FAC-SEATTLE
```

#### **After Staging Transformation**

```sql
-- stg_fact_exam_requests (Day 1)
exam_request_id: ER-2024-99999
veteran_sk: 789 (resolved from dim_veterans)
facility_sk: 15 (resolved from dim_facilities)
exam_request_type_sk: 2 (C&P Exam)
request_received_date_sk: 20240115
request_validated_date_sk: NULL
examiner_assigned_date_sk: NULL
appointment_scheduled_date_sk: NULL
exam_completed_date_sk: NULL
assignment_status: UNASSIGNED
request_priority: URGENT
sla_days_allowed: 30
effective_sla_days: 22.5 (30 * 0.75 for URGENT priority)
complex_case_flag: TRUE (3 conditions)
days_pending: 0
scheduled_flag: FALSE
```

#### **After First MERGE (Day 1)**

```sql
-- fact_exam_requests (Day 1: INSERT NEW ROW)
exam_request_sk: 99999
veteran_sk: 789
facility_sk: 15
exam_request_type_sk: 2
request_received_date_sk: 20240115 ✓
request_validated_date_sk: NULL
eligibility_confirmed_date_sk: NULL
examiner_assigned_date_sk: NULL
examiner_accepted_date_sk: NULL
appointment_scheduled_date_sk: NULL
exam_completed_date_sk: NULL
request_closed_date_sk: NULL
assignment_status: UNASSIGNED
request_priority: URGENT
sla_days_allowed: 30
sla_due_date: 2024-02-14
days_to_assignment: NULL
days_to_scheduling: NULL
days_to_completion: NULL
total_cycle_time_days: NULL
sla_met_flag: NULL
created_timestamp: 2024-01-15 08:30:00
updated_timestamp: 2024-01-15 08:30:00
```

#### **Day 2 Update: Validation Complete**

Source system updates:
```sql
-- ods_exam_requests_source (Day 2: January 16)
request_status: VALIDATED
request_validated_date: 2024-01-16
```

After MERGE:
```sql
-- fact_exam_requests (Day 2: UPDATE EXISTING ROW)
exam_request_sk: 99999
request_received_date_sk: 20240115 (PRESERVED)
request_validated_date_sk: 20240116 ← NEW!
eligibility_confirmed_date_sk: NULL
examiner_assigned_date_sk: NULL
-- ... other milestones still NULL ...
assignment_status: VALIDATED
updated_timestamp: 2024-01-16 09:00:00 ← UPDATED
```

#### **Day 4 Update: Examiner Assigned**

Source system updates:
```sql
-- ods_exam_requests_source (Day 4: January 18)
assignment_status: ASSIGNED
assigned_evaluator_npi: NPI123456789
assignment_date: 2024-01-18
examiner_accepted_date: 2024-01-18
```

After MERGE:
```sql
-- fact_exam_requests (Day 4: UPDATE EXISTING ROW)
exam_request_sk: 99999
request_received_date_sk: 20240115 (PRESERVED)
request_validated_date_sk: 20240116 (PRESERVED)
eligibility_confirmed_date_sk: 20240117 (came in with assignment update)
examiner_assigned_date_sk: 20240118 ← NEW!
examiner_accepted_date_sk: 20240118 ← NEW!
appointment_scheduled_date_sk: NULL
-- ... other milestones still NULL ...
assigned_evaluator_sk: 456 (resolved)
assignment_status: ASSIGNED
days_to_assignment: 3 ← CALCULATED (Jan 18 - Jan 15)
updated_timestamp: 2024-01-18 10:15:00
```

#### **Day 7 Update: Appointment Scheduled**

Source system updates:
```sql
-- ods_exam_requests_source (Day 7: January 21)
appointment_scheduled_date: 2024-01-21
scheduled_exam_date: 2024-01-28
appointment_status: SCHEDULED
```

After MERGE:
```sql
-- fact_exam_requests (Day 7: UPDATE EXISTING ROW)
exam_request_sk: 99999
request_received_date_sk: 20240115 (PRESERVED)
request_validated_date_sk: 20240116 (PRESERVED)
eligibility_confirmed_date_sk: 20240117 (PRESERVED)
examiner_assigned_date_sk: 20240118 (PRESERVED)
examiner_accepted_date_sk: 20240118 (PRESERVED)
appointment_scheduled_date_sk: 20240121 ← NEW!
exam_completed_date_sk: NULL
request_closed_date_sk: NULL
assignment_status: SCHEDULED
days_to_assignment: 3 (PRESERVED)
days_to_scheduling: 3 ← CALCULATED (Jan 21 - Jan 18)
updated_timestamp: 2024-01-21 14:30:00
```

#### **Day 14 Update: Exam Completed**

Source system updates:
```sql
-- ods_exam_requests_source (Day 14: January 28)
exam_status: COMPLETED
exam_completed_date: 2024-01-28
```

After MERGE:
```sql
-- fact_exam_requests (Day 14: UPDATE EXISTING ROW)
exam_request_sk: 99999
request_received_date_sk: 20240115 (PRESERVED)
request_validated_date_sk: 20240116 (PRESERVED)
eligibility_confirmed_date_sk: 20240117 (PRESERVED)
examiner_assigned_date_sk: 20240118 (PRESERVED)
examiner_accepted_date_sk: 20240118 (PRESERVED)
appointment_scheduled_date_sk: 20240121 (PRESERVED)
exam_completed_date_sk: 20240128 ← NEW!
request_closed_date_sk: NULL
assignment_status: COMPLETED
days_to_assignment: 3 (PRESERVED)
days_to_scheduling: 3 (PRESERVED)
days_to_completion: 7 ← CALCULATED (Jan 28 - Jan 21)
total_cycle_time_days: 13 ← CALCULATED (Jan 28 - Jan 15)
sla_met_flag: TRUE ← CALCULATED (13 < 22.5)
sla_variance_days: -9.5 ← CALCULATED (13 - 22.5)
updated_timestamp: 2024-01-28 16:45:00
```

#### **Day 15 Update: Request Closed (FINAL STATE)**

Source system updates:
```sql
-- ods_exam_requests_source (Day 15: January 29)
request_status: CLOSED
request_closed_date: 2024-01-29
```

After MERGE (FINAL):
```sql
-- fact_exam_requests (Day 15: FINAL UPDATE)
exam_request_sk: 99999
veteran_sk: 789
facility_sk: 15
exam_request_type_sk: 2
assigned_evaluator_sk: 456

-- ALL MILESTONE DATES NOW POPULATED (COMPLETE LIFECYCLE)
request_received_date_sk: 20240115 ✓ (PRESERVED)
request_validated_date_sk: 20240116 ✓ (PRESERVED)
eligibility_confirmed_date_sk: 20240117 ✓ (PRESERVED)
examiner_assigned_date_sk: 20240118 ✓ (PRESERVED)
examiner_accepted_date_sk: 20240118 ✓ (PRESERVED)
appointment_scheduled_date_sk: 20240121 ✓ (PRESERVED)
exam_completed_date_sk: 20240128 ✓ (PRESERVED)
request_closed_date_sk: 20240129 ✓ NEW! (FINAL MILESTONE)
request_cancelled_date_sk: NULL (not applicable)

-- ALL LAG METRICS CALCULATED
days_to_assignment: 3
days_to_scheduling: 3
days_to_completion: 7
total_cycle_time_days: 14 ← FINAL (Jan 29 - Jan 15)

-- SLA TRACKING (FINAL)
sla_days_allowed: 30
sla_due_date: 2024-02-14
sla_met_flag: TRUE
sla_variance_days: -8.5 (14 - 22.5 effective SLA)

-- FINAL STATUS
assignment_status: CLOSED
request_priority: URGENT

-- AUDIT
created_timestamp: 2024-01-15 08:30:00 (never changes)
updated_timestamp: 2024-01-29 09:00:00 (last update)
```

### Summary: One Row, 15 Days of Updates

```
┌──────────────────────────────────────────────────────────────────┐
│  ER-2024-99999: Complete Lifecycle in ONE ROW                    │
├──────────────────────────────────────────────────────────────────┤
│  Day 1:  INSERT new row (received)                               │
│  Day 2:  UPDATE with validated date                              │
│  Day 4:  UPDATE with assignment dates                            │
│  Day 7:  UPDATE with scheduling date                             │
│  Day 14: UPDATE with completion date                             │
│  Day 15: UPDATE with closed date (FINAL)                         │
├──────────────────────────────────────────────────────────────────┤
│  Result: 1 row × 6 updates = Complete process history            │
└──────────────────────────────────────────────────────────────────┘
```

**Key Takeaway**: The MERGE with COALESCE pattern allows us to **accumulate** milestone dates over time while **updating** current status and metrics—all in a single row!

---

## Performance Considerations

### Clustering Strategy

All accumulating snapshot fact tables use **clustering keys** optimized for common query patterns:

```sql
-- fact_exam_requests
CLUSTER BY (request_received_date_sk, assignment_status);

-- Rationale:
-- 1. request_received_date_sk: Most queries filter by date range
-- 2. assignment_status: Common WHERE/GROUP BY column

-- fact_claim_status_changes
CLUSTER BY (claim_sk, rating_decision_date_sk);

-- Rationale:
-- 1. claim_sk: Lookup by specific claim
-- 2. rating_decision_date_sk: Date-range analysis on decisions
```

### MERGE Performance

The MERGE operation is efficient because:

1. **Match on Business Key**: Uses indexed natural key (`exam_request_id`)
2. **Single Row Operations**: Updates one row per process instance (not scanning full table)
3. **Clustering Optimization**: Snowflake micro-partitions reduce scan range
4. **Incremental Loads**: Only processes changed/new records from staging

### Update Frequency

| Fact Table | Typical Update Frequency | Rationale |
|------------|-------------------------|-----------|
| fact_exam_requests | Every 4 hours (6× daily) | Exam lifecycle events occur throughout day |
| fact_claim_status_changes | Daily | Claim processing is slower-moving |
| fact_appointments_scheduled | Every 2 hours (12× daily) | Appointment scheduling is time-sensitive |

**File Reference**: `snowflake/orchestration/02_metadata_driven_orchestration.sql:120-145`

---

## Query Patterns

### Common Analytical Queries

#### 1. Average Cycle Time by Stage

```sql
SELECT
    AVG(days_to_assignment) AS avg_days_to_assignment,
    AVG(days_to_scheduling) AS avg_days_to_scheduling,
    AVG(days_to_completion) AS avg_days_to_completion,
    AVG(total_cycle_time_days) AS avg_total_cycle_time
FROM fact_exam_requests
WHERE request_received_date_sk >= 20240101  -- YTD
  AND request_closed_date_sk IS NOT NULL;   -- Completed only
```

#### 2. SLA Compliance Rate

```sql
SELECT
    request_priority,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) AS met_sla,
    ROUND(100.0 * SUM(CASE WHEN sla_met_flag = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_compliance_pct
FROM fact_exam_requests
WHERE request_closed_date_sk >= 20240101
GROUP BY request_priority
ORDER BY request_priority;

-- Result:
-- EXPEDITE   | 145  | 142     | 97.93%
-- URGENT     | 823  | 789     | 95.87%
-- PRIORITY   | 1,456| 1,388   | 95.33%
-- ROUTINE    | 3,892| 3,654   | 93.88%
```

#### 3. Identify Bottlenecks

```sql
SELECT
    CASE
        WHEN days_to_assignment > 5 THEN 'Assignment Bottleneck'
        WHEN days_to_scheduling - days_to_assignment > 10 THEN 'Scheduling Bottleneck'
        WHEN days_to_completion - days_to_scheduling > 14 THEN 'Completion Bottleneck'
        ELSE 'No Bottleneck'
    END AS bottleneck_type,
    COUNT(*) AS request_count,
    AVG(total_cycle_time_days) AS avg_cycle_time
FROM fact_exam_requests
WHERE request_closed_date_sk >= 20240101
GROUP BY bottleneck_type
ORDER BY request_count DESC;
```

#### 4. In-Progress Requests (Milestone Analysis)

```sql
-- Find requests stuck at specific stages
SELECT
    assignment_status,
    COUNT(*) AS requests_at_stage,
    AVG(DATEDIFF(day, dd.full_date, CURRENT_DATE())) AS avg_days_in_stage
FROM fact_exam_requests fer
JOIN dim_dates dd ON fer.request_received_date_sk = dd.date_sk
WHERE request_closed_date_sk IS NULL  -- Still in progress
GROUP BY assignment_status
ORDER BY avg_days_in_stage DESC;

-- Result:
-- UNASSIGNED     | 45   | 12.3 days  ← Attention needed!
-- IN_QUEUE       | 123  | 6.7 days
-- ASSIGNED       | 234  | 4.2 days
-- SCHEDULED      | 456  | 2.1 days
```

#### 5. Join with Dimensions for Rich Analysis

```sql
SELECT
    dv.veteran_name,
    df.facility_name,
    de.evaluator_name,
    d_req.full_date AS request_date,
    d_comp.full_date AS completion_date,
    fer.total_cycle_time_days,
    fer.sla_met_flag
FROM fact_exam_requests fer
JOIN dim_veterans dv ON fer.veteran_sk = dv.veteran_sk
JOIN dim_facilities df ON fer.facility_sk = df.facility_sk
JOIN dim_evaluators de ON fer.assigned_evaluator_sk = de.evaluator_sk
JOIN dim_dates d_req ON fer.request_received_date_sk = d_req.date_sk
LEFT JOIN dim_dates d_comp ON fer.exam_completed_date_sk = d_comp.date_sk
WHERE fer.request_closed_date_sk >= 20240101
  AND fer.sla_met_flag = FALSE
ORDER BY fer.total_cycle_time_days DESC
LIMIT 100;
```

---

## File Reference Guide

### Core Implementation Files

| File Path | Description | Lines of Interest |
|-----------|-------------|-------------------|
| `snowflake/facts/07_fact_exam_requests.sql` | Exam request fact table DDL | Complete file (defines structure) |
| `snowflake/facts/02_fact_claim_status.sql` | Claim status fact table DDL | Complete file |
| `snowflake/facts/03_fact_appointment.sql` | Appointment fact table DDL | Complete file |
| `snowflake/ods/01_create_ods_tables.sql` | ODS raw tables (source landing) | Lines 45-120, 200-280 |
| `snowflake/staging/01_create_staging_tables.sql` | Staging transformation tables | Lines 150-350 |
| `snowflake/etl/02_etl_procedures_facts.sql` | ETL procedures (MERGE logic) | Lines 150-280 (transform), 450-580 (merge) |
| `snowflake/orchestration/02_metadata_driven_orchestration.sql` | ETL orchestration & scheduling | Lines 120-145 (config), 200-350 (procedures) |

### Documentation Files

| File Path | Description |
|-----------|-------------|
| `FACT_TABLE_TYPES_GUIDE.md` | Comprehensive guide on fact table types (65 KB) |
| `DATA_PIPELINE_ARCHITECTURE.md` | Four-layer pipeline architecture (42 KB) |
| `DIMENSIONAL_MODEL_DOCUMENTATION.md` | Dimensional model design & specs (50 KB) |
| `ACCUMULATING_SNAPSHOT_DATA_FLOW.md` | **THIS DOCUMENT** |

---

## Summary: The Accumulating Snapshot Journey

### Key Principles

1. **One Row Per Process Instance**
   - Each exam request, claim, or appointment gets ONE row in the fact table
   - That row is updated as the process moves through stages

2. **Multiple Date Foreign Keys**
   - One date key per milestone in the process
   - Nullable until milestone is reached

3. **MERGE with COALESCE**
   - `COALESCE(new_value, old_value)` preserves existing milestone dates
   - Prevents overwriting historical dates with NULLs

4. **Pre-Calculated Lag Facts**
   - Days between milestones calculated during ETL
   - Enables fast analytical queries without complex date math

5. **Current Status Tracking**
   - `assignment_status`, `request_priority` always reflect current state
   - Updated on every MERGE

### The Four-Layer Flow

```
SOURCE SYSTEMS (VES OMS, VEMS, VA)
    ↓ Extract (raw data)
ODS_RAW (ods_exam_requests_source)
    ↓ Transform (cleanse, validate, calculate)
STAGING (stg_fact_exam_requests)
    ↓ MERGE with COALESCE (accumulate milestones)
WAREHOUSE (fact_exam_requests)
    ↓ Aggregate (KPIs, dashboards)
MARTS (vw_exam_request_performance)
```

### Why This Pattern Works

✅ **Efficient Storage**: One row per process (not one row per event)
✅ **Complete History**: All milestone dates preserved
✅ **Fast Queries**: Pre-calculated lag facts, clustered storage
✅ **Easy Analysis**: "How long did each stage take?" queries are simple
✅ **SLA Tracking**: Built-in compliance monitoring
✅ **Idempotent**: Re-running ETL produces same result (thanks to COALESCE)

---

## Next Steps

To dive deeper into specific aspects:

- **Learn about other fact table types**: See `FACT_TABLE_TYPES_GUIDE.md`
- **Understand full pipeline architecture**: See `DATA_PIPELINE_ARCHITECTURE.md`
- **Explore dimensional model design**: See `DIMENSIONAL_MODEL_DOCUMENTATION.md`
- **Review actual SQL code**: Start with `snowflake/facts/07_fact_exam_requests.sql`

---

**Document Version**: 1.0
**Last Updated**: 2024-01-29
**Author**: VES Data Warehouse Team
**Contact**: data-engineering@ves.com
