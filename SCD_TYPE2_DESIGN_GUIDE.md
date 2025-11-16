# Slowly Changing Dimension Type 2 (SCD Type 2) Design Guide
## VES Dimensional Model - Historical Change Tracking

**Date**: 2025-11-16
**Standards**: VES Snowflake Naming Conventions v1.0, Kimball Methodology
**Purpose**: Document SCD Type 2 implementation and usage patterns

---

## Overview

The VES dimensional model uses **SCD Type 2** for dimensions that require full historical tracking of attribute changes. This preserves complete history while maintaining referential integrity with fact tables.

### Dimensions Using SCD Type 2

| Dimension | Business Key | Why SCD Type 2? |
|-----------|--------------|-----------------|
| `dim_veterans` | `veteran_id` | Track changes in disability rating, address, service records |
| `dim_evaluators` | `evaluator_id` | Track changes in credentials, certifications, performance |
| `dim_facilities` | `facility_id` | Track changes in facility type, VISN, accreditation |
| `dim_claims` | `claim_id` | Track changes in claim status, decision dates |

### Dimensions Using SCD Type 1

| Dimension | Why Type 1? |
|-----------|-------------|
| `dim_dates` | Static reference dimension |
| `dim_evaluation_types` | Relatively static, changes rare |
| `dim_medical_conditions` | ICD-10 codes, DBQ forms stable |
| `dim_appointments` | Transaction-level attributes, no history needed |

---

## SCD Type 2 Pattern Components

### Required Columns

Every SCD Type 2 dimension includes these three columns:

```sql
-- Date span tracking
effective_start_date TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
effective_end_date TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),

-- Current record flag
is_current BOOLEAN NOT NULL DEFAULT TRUE
```

### Column Definitions

#### 1. **effective_start_date**
- **Type**: TIMESTAMP_NTZ (timestamp with no timezone)
- **Purpose**: Marks when this version of the record became effective
- **Default**: CURRENT_TIMESTAMP() - Set to now when record is inserted
- **Usage**: Join to fact tables with "as of" date logic

#### 2. **effective_end_date**
- **Type**: TIMESTAMP_NTZ
- **Purpose**: Marks when this version was superseded by a new version
- **Default**: '9999-12-31 23:59:59' - "End of time" for current records
- **Usage**: Determines if record was active on a given date

#### 3. **is_current**
- **Type**: BOOLEAN
- **Purpose**: Quick filter for current active record (performance optimization)
- **Default**: TRUE for new records
- **Usage**: WHERE is_current = TRUE (much faster than date comparison)

---

## How SCD Type 2 Works: Real-World Example

### Scenario: Veteran Disability Rating Changes

**Business Context**: A veteran's disability rating increases over time as conditions worsen or new conditions are service-connected.

**Timeline**:
- **2023-01-15**: Veteran John Smith initially rated at 30% disability
- **2023-08-20**: Rating increases to 50% after new evaluation
- **2024-03-10**: Rating increases to 70% after appeal approved

### Data in dim_veterans Table (3 rows for same veteran)

```sql
┌──────────────┬────────────┬──────────────────┬──────────────────────┬─────────────────────────────┬────────────────────────────┬────────────┐
│ veteran_sk   │ veteran_id │ full_name        │ current_disability_  │ effective_start_date        │ effective_end_date         │ is_current │
│              │ (bus. key) │                  │ rating               │                             │                            │            │
├──────────────┼────────────┼──────────────────┼──────────────────────┼─────────────────────────────┼────────────────────────────┼────────────┤
│ 1001         │ VET-12345  │ John Smith       │ 30                   │ 2023-01-15 00:00:00         │ 2023-08-20 00:00:00        │ FALSE      │
│ 1002         │ VET-12345  │ John Smith       │ 50                   │ 2023-08-20 00:00:00         │ 2024-03-10 00:00:00        │ FALSE      │
│ 1003         │ VET-12345  │ John Smith       │ 70                   │ 2024-03-10 00:00:00         │ 9999-12-31 23:59:59        │ TRUE       │
└──────────────┴────────────┴──────────────────┴──────────────────────┴─────────────────────────────┴────────────────────────────┴────────────┘

Key Points:
✓ Same veteran_id (VET-12345) appears 3 times with different veteran_sk values
✓ Each row has non-overlapping date ranges
✓ Only the latest version has is_current = TRUE
✓ effective_end_date = 9999-12-31 indicates current record
✓ Historical records have is_current = FALSE and real end dates
```

### Querying SCD Type 2 Data

#### Query 1: Get Current Record Only

```sql
-- What is John Smith's CURRENT disability rating?
SELECT
    veteran_sk,
    veteran_id,
    full_name,
    current_disability_rating,
    effective_start_date
FROM dim_veterans
WHERE veteran_id = 'VET-12345'
  AND is_current = TRUE;

/*
Result:
┌──────────────┬────────────┬──────────────┬──────────────────────┬─────────────────────────────┐
│ veteran_sk   │ veteran_id │ full_name    │ current_disability_  │ effective_start_date        │
│              │            │              │ rating               │                             │
├──────────────┼────────────┼──────────────┼──────────────────────┼─────────────────────────────┤
│ 1003         │ VET-12345  │ John Smith   │ 70                   │ 2024-03-10 00:00:00         │
└──────────────┴────────────┴──────────────┴──────────────────────┴─────────────────────────────┘

Answer: 70% (current as of today)
*/
```

#### Query 2: Get Historical Record "As Of" Specific Date

```sql
-- What was John Smith's disability rating as of September 1, 2023?
SELECT
    veteran_sk,
    veteran_id,
    full_name,
    current_disability_rating,
    effective_start_date,
    effective_end_date
FROM dim_veterans
WHERE veteran_id = 'VET-12345'
  AND effective_start_date <= '2023-09-01 00:00:00'
  AND effective_end_date > '2023-09-01 00:00:00';

/*
Result:
┌──────────────┬────────────┬──────────────┬──────────────────────┬─────────────────────────────┬────────────────────────────┐
│ veteran_sk   │ veteran_id │ full_name    │ current_disability_  │ effective_start_date        │ effective_end_date         │
│              │            │              │ rating               │                             │                            │
├──────────────┼────────────┼──────────────┼──────────────────────┼─────────────────────────────┼────────────────────────────┤
│ 1002         │ VET-12345  │ John Smith   │ 50                   │ 2023-08-20 00:00:00         │ 2024-03-10 00:00:00        │
└──────────────┴────────────┴──────────────┴──────────────────────┴─────────────────────────────┴────────────────────────────┘

Answer: 50% (he had just been increased from 30% on Aug 20)
*/
```

#### Query 3: Get Complete History

```sql
-- Show me John Smith's complete disability rating history
SELECT
    veteran_sk,
    veteran_id,
    full_name,
    current_disability_rating,
    effective_start_date,
    effective_end_date,
    is_current,
    DATEDIFF('day', effective_start_date, effective_end_date) AS days_at_this_rating
FROM dim_veterans
WHERE veteran_id = 'VET-12345'
ORDER BY effective_start_date;

/*
Result:
┌──────────────┬────────────┬──────────────┬──────────────────────┬─────────────────────────────┬────────────────────────────┬────────────┬────────────────────┐
│ veteran_sk   │ veteran_id │ full_name    │ current_disability_  │ effective_start_date        │ effective_end_date         │ is_current │ days_at_this_rating│
│              │            │              │ rating               │                             │                            │            │                    │
├──────────────┼────────────┼──────────────┼──────────────────────┼─────────────────────────────┼────────────────────────────┼────────────┼────────────────────┤
│ 1001         │ VET-12345  │ John Smith   │ 30                   │ 2023-01-15 00:00:00         │ 2023-08-20 00:00:00        │ FALSE      │ 217                │
│ 1002         │ VET-12345  │ John Smith   │ 50                   │ 2023-08-20 00:00:00         │ 2024-03-10 00:00:00        │ FALSE      │ 203                │
│ 1003         │ VET-12345  │ John Smith   │ 70                   │ 2024-03-10 00:00:00         │ 9999-12-31 23:59:59        │ TRUE       │ (ongoing)          │
└──────────────┴────────────┴──────────────┴──────────────────────┴─────────────────────────────┴────────────────────────────┴────────────┴────────────────────┘

Answer: 30% for 217 days, then 50% for 203 days, currently 70%
*/
```

---

## Joining Fact Tables to SCD Type 2 Dimensions

### Challenge: Time-Correct Joins

When a fact event occurs, we need to join to the dimension record that was **active at that time**, not necessarily the current record.

### Solution: Point-in-Time Join

```sql
-- Join evaluation fact to veteran dimension "as of" evaluation date
SELECT
    e.evaluation_id,
    e.evaluation_date_sk,
    d.full_date AS evaluation_date,
    v.veteran_sk,
    v.full_name,
    v.current_disability_rating AS rating_at_time_of_evaluation,
    e.recommended_rating_percentage
FROM fct_evaluations_completed e
INNER JOIN dim_dates d
    ON e.evaluation_date_sk = d.date_sk
INNER JOIN dim_veterans v
    ON e.veteran_sk = v.veteran_sk
    -- Point-in-time join: Get veteran record active on evaluation date
    AND d.full_date >= v.effective_start_date
    AND d.full_date < v.effective_end_date
WHERE e.evaluation_id = 'EVAL-12345';

/*
This ensures we get the veteran's disability rating as it was
on the date of the evaluation, not the current rating.
*/
```

### Simplified Join (When Facts Store correct veteran_sk)

If the fact table already stores the correct `veteran_sk` that was current at event time:

```sql
-- Simple join - fact already has correct veteran_sk from event time
SELECT
    e.evaluation_id,
    e.evaluation_date_sk,
    v.veteran_sk,
    v.full_name,
    v.current_disability_rating AS rating_at_time_of_evaluation
FROM fct_evaluations_completed e
INNER JOIN dim_veterans v
    ON e.veteran_sk = v.veteran_sk
WHERE e.evaluation_id = 'EVAL-12345';

/*
This works because when the evaluation occurred, the ETL process
stored the veteran_sk value that was current at that time.
*/
```

---

## ETL Patterns for SCD Type 2

### Pattern 1: Insert New Record (Change Detected)

```sql
-- Step 1: Close out current record
UPDATE dim_veterans
SET
    effective_end_date = CURRENT_TIMESTAMP(),
    is_current = FALSE,
    updated_timestamp = CURRENT_TIMESTAMP()
WHERE veteran_id = 'VET-12345'
  AND is_current = TRUE;

-- Step 2: Insert new version
INSERT INTO dim_veterans (
    veteran_id,
    full_name,
    current_disability_rating,
    effective_start_date,
    effective_end_date,
    is_current,
    created_timestamp
)
VALUES (
    'VET-12345',
    'John Smith',
    70,  -- New rating
    CURRENT_TIMESTAMP(),
    TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    TRUE,
    CURRENT_TIMESTAMP()
);

-- Result: New veteran_sk generated (e.g., 1003)
```

### Pattern 2: No Change Detected (Update Current Record)

```sql
-- Only update non-tracked attributes (Type 1 attributes within SCD Type 2 record)
-- For example: Update contact info (phone, email) without creating new version
UPDATE dim_veterans
SET
    phone = '555-0199',  -- Updated phone
    email = 'john.smith@newmail.com',  -- Updated email
    updated_timestamp = CURRENT_TIMESTAMP()
WHERE veteran_id = 'VET-12345'
  AND is_current = TRUE;

-- Note: effective_start_date, effective_end_date, is_current unchanged
-- Only the current record is updated, no new version created
```

### Pattern 3: Initial Load (First Insert)

```sql
-- First time loading veteran into warehouse
INSERT INTO dim_veterans (
    veteran_id,
    full_name,
    current_disability_rating,
    effective_start_date,
    effective_end_date,
    is_current,
    created_timestamp
)
VALUES (
    'VET-12345',
    'John Smith',
    30,
    '2023-01-15 00:00:00',  -- Historical start date
    TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    TRUE,
    CURRENT_TIMESTAMP()
);
```

---

## Determining What Triggers a New SCD Type 2 Version

### Tracked Attributes (Create New Version)

Changes to these attributes should create a new SCD Type 2 version:

**dim_veterans**:
- ✓ `current_disability_rating` - Critical for historical analysis
- ✓ `disability_rating_category` - Derived from rating
- ✓ `service_branch` - If veteran transfers branches (rare but possible)
- ✓ `priority_group` - Changes based on disability rating
- ✓ `combat_veteran_flag` - If status changes

**dim_evaluators**:
- ✓ `credentials` - MD, DO, PhD additions
- ✓ `specialty` - If provider changes specialty
- ✓ `va_certified_flag` - Certification status
- ✓ `active_flag` - Employment status changes

**dim_facilities**:
- ✓ `facility_type` - Clinic to Hospital upgrade
- ✓ `visn_number` - Organizational reassignment
- ✓ `accreditation_status` - Critical quality indicator

### Non-Tracked Attributes (Update in Place - Type 1)

Changes to these attributes do NOT create new version:

**dim_veterans**:
- ✗ `address_line1`, `address_line2` - Address changes don't need history
- ✗ `city`, `state`, `zip_code` - Contact info updates
- ✗ `phone`, `email` - Contact info updates
- ✗ `age` - Calculated field, changes naturally

**dim_evaluators**:
- ✗ `phone`, `email` - Contact information
- ✗ `average_evaluation_time_minutes` - Performance metric
- ✗ `total_evaluations_completed` - Counter that increments

---

## Identifying Changes: Slowly Changing Dimension (SCD) ETL Logic

### Change Detection Query

```sql
-- Detect if veteran record has changed in tracked attributes
WITH source_data AS (
    -- Latest data from source system
    SELECT
        'VET-12345' AS veteran_id,
        'John Smith' AS full_name,
        70 AS current_disability_rating,  -- Changed from 50 to 70
        'Army' AS service_branch
),
current_data AS (
    -- Current record in dimension
    SELECT
        veteran_id,
        full_name,
        current_disability_rating,
        service_branch
    FROM dim_veterans
    WHERE veteran_id = 'VET-12345'
      AND is_current = TRUE
)
SELECT
    CASE
        WHEN s.current_disability_rating <> c.current_disability_rating THEN 'CHANGE_DETECTED'
        WHEN s.service_branch <> c.service_branch THEN 'CHANGE_DETECTED'
        ELSE 'NO_CHANGE'
    END AS scd_action
FROM source_data s
LEFT JOIN current_data c ON s.veteran_id = c.veteran_id;

/*
Result: 'CHANGE_DETECTED' because disability rating changed
Action: Execute Pattern 1 (Insert new version, close old version)
*/
```

---

## Best Practices

### 1. Use Surrogate Keys in Fact Tables ✓

```sql
-- CORRECT: Fact table uses veteran_sk (surrogate key)
CREATE TABLE fct_evaluations_completed (
    evaluation_fact_sk INTEGER,
    veteran_sk INTEGER,  -- ✓ Surrogate key from dimension
    evaluation_date_sk INTEGER,
    ...
);

-- WRONG: Fact table uses veteran_id (business key)
CREATE TABLE fct_evaluations_completed_wrong (
    evaluation_fact_sk INTEGER,
    veteran_id VARCHAR(50),  -- ✗ Business key - breaks with SCD Type 2
    ...
);
```

**Why?** The fact table's `veteran_sk` points to the specific version that was active at event time.

### 2. Never Delete Dimension Records ✓

```sql
-- CORRECT: Set is_current = FALSE, preserve history
UPDATE dim_veterans
SET is_current = FALSE
WHERE veteran_id = 'VET-12345' AND is_current = TRUE;

-- WRONG: DELETE - loses history
DELETE FROM dim_veterans WHERE veteran_id = 'VET-12345';  -- ✗ NEVER DO THIS
```

### 3. Use Effective Dates for Point-in-Time Queries ✓

```sql
-- CORRECT: Get dimension value as of specific date
SELECT v.*
FROM dim_veterans v
WHERE v.veteran_id = 'VET-12345'
  AND '2023-09-01' >= v.effective_start_date
  AND '2023-09-01' < v.effective_end_date;

-- SUBOPTIMAL: Only use is_current flag (loses historical accuracy)
SELECT v.*
FROM dim_veterans v
WHERE v.veteran_id = 'VET-12345'
  AND v.is_current = TRUE;  -- This gets TODAY's value, not historical
```

### 4. Set effective_end_date = 9999-12-31 for Current Records ✓

```sql
-- CORRECT: Use far future date
effective_end_date = TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')

-- WRONG: Use NULL
effective_end_date = NULL  -- ✗ Makes date range queries complex
```

**Why?** Using 9999-12-31 allows simple `BETWEEN` clause queries without NULL handling.

### 5. Index is_current for Performance ✓

```sql
-- Add search optimization for is_current filtering
ALTER TABLE dim_veterans
  ADD SEARCH OPTIMIZATION ON EQUALITY(is_current, veteran_id);
```

This speeds up queries like `WHERE is_current = TRUE`.

---

## Common Query Patterns

### Pattern 1: Get Current Records Only

```sql
SELECT *
FROM dim_veterans
WHERE is_current = TRUE;

-- Returns only the latest version of each veteran
```

### Pattern 2: Count Distinct Entities

```sql
-- How many unique veterans in the warehouse?
SELECT COUNT(DISTINCT veteran_id)
FROM dim_veterans;

-- OR (more efficient with is_current)
SELECT COUNT(*)
FROM dim_veterans
WHERE is_current = TRUE;
```

### Pattern 3: Audit Changes Over Time

```sql
-- Show all disability rating changes for a veteran
SELECT
    veteran_id,
    veteran_sk,
    current_disability_rating,
    effective_start_date,
    effective_end_date,
    DATEDIFF('day', effective_start_date, effective_end_date) AS days_at_rating,
    LEAD(current_disability_rating) OVER (
        PARTITION BY veteran_id
        ORDER BY effective_start_date
    ) AS next_rating,
    LEAD(effective_start_date) OVER (
        PARTITION BY veteran_id
        ORDER BY effective_start_date
    ) AS next_change_date
FROM dim_veterans
WHERE veteran_id = 'VET-12345'
ORDER BY effective_start_date;
```

### Pattern 4: Find Records That Changed

```sql
-- Which veterans had disability rating increases in 2024?
WITH current_ratings AS (
    SELECT veteran_id, current_disability_rating
    FROM dim_veterans
    WHERE is_current = TRUE
),
previous_ratings AS (
    SELECT
        veteran_id,
        current_disability_rating,
        ROW_NUMBER() OVER (
            PARTITION BY veteran_id
            ORDER BY effective_end_date DESC
        ) AS rn
    FROM dim_veterans
    WHERE is_current = FALSE
      AND YEAR(effective_end_date) = 2024
)
SELECT
    c.veteran_id,
    p.current_disability_rating AS previous_rating,
    c.current_disability_rating AS current_rating,
    (c.current_disability_rating - p.current_disability_rating) AS rating_increase
FROM current_ratings c
INNER JOIN previous_ratings p
    ON c.veteran_id = p.veteran_id
    AND p.rn = 1
WHERE c.current_disability_rating > p.current_disability_rating;
```

---

## Performance Optimization

### 1. Clustering

```sql
-- Cluster dimension by business key and is_current for faster lookups
ALTER TABLE dim_veterans CLUSTER BY (veteran_id, is_current);
ALTER TABLE dim_evaluators CLUSTER BY (evaluator_id, is_current);
ALTER TABLE dim_facilities CLUSTER BY (facility_id, is_current);
ALTER TABLE dim_claims CLUSTER BY (claim_id, is_current);
```

### 2. Search Optimization

```sql
-- Enable search optimization for commonly filtered columns
ALTER TABLE dim_veterans
  ADD SEARCH OPTIMIZATION ON EQUALITY(veteran_id, is_current);
```

### 3. Materialized Views for Current Records

```sql
-- Create materialized view of current records for better performance
CREATE MATERIALIZED VIEW mv_dim_veterans_current AS
SELECT *
FROM dim_veterans
WHERE is_current = TRUE;

-- Query the view instead of full table
SELECT * FROM mv_dim_veterans_current WHERE veteran_id = 'VET-12345';
```

---

## Testing SCD Type 2 Implementation

### Test Case 1: Verify New Version Created

```sql
-- Before change
SELECT COUNT(*) FROM dim_veterans WHERE veteran_id = 'VET-TEST';
-- Expected: 1 record

-- Execute change (disability rating 30 → 50)
-- ... run SCD Type 2 ETL process ...

-- After change
SELECT COUNT(*) FROM dim_veterans WHERE veteran_id = 'VET-TEST';
-- Expected: 2 records

-- Verify only one is current
SELECT COUNT(*) FROM dim_veterans
WHERE veteran_id = 'VET-TEST' AND is_current = TRUE;
-- Expected: 1 record
```

### Test Case 2: Verify Date Ranges Don't Overlap

```sql
-- Check for overlapping date ranges (should return 0)
SELECT
    v1.veteran_id,
    v1.veteran_sk AS sk1,
    v2.veteran_sk AS sk2,
    v1.effective_start_date AS start1,
    v1.effective_end_date AS end1,
    v2.effective_start_date AS start2,
    v2.effective_end_date AS end2
FROM dim_veterans v1
INNER JOIN dim_veterans v2
    ON v1.veteran_id = v2.veteran_id
    AND v1.veteran_sk < v2.veteran_sk  -- Different versions
    AND v1.effective_start_date < v2.effective_end_date
    AND v1.effective_end_date > v2.effective_start_date;  -- Overlap condition

-- Expected: 0 rows (no overlaps)
```

### Test Case 3: Verify Gaps Don't Exist

```sql
-- Check for gaps in date coverage
WITH ordered_versions AS (
    SELECT
        veteran_id,
        veteran_sk,
        effective_start_date,
        effective_end_date,
        LEAD(effective_start_date) OVER (
            PARTITION BY veteran_id
            ORDER BY effective_start_date
        ) AS next_start_date
    FROM dim_veterans
)
SELECT *
FROM ordered_versions
WHERE next_start_date IS NOT NULL
  AND effective_end_date <> next_start_date;

-- Expected: 0 rows (no gaps)
```

---

## Summary

The VES dimensional model implements **full SCD Type 2** with:

✅ **effective_start_date** - When record became active
✅ **effective_end_date** - When record was superseded (or 9999-12-31 for current)
✅ **is_current** - Performance optimization flag

This design enables:
- ✓ Complete historical tracking of attribute changes
- ✓ Point-in-time analysis ("What was the value on date X?")
- ✓ Trend analysis over time
- ✓ Audit trails for compliance
- ✓ Correct joins between facts and dimensions

All SCD Type 2 dimensions are ready to track changes effectively!

---

**Document Version**: 1.0
**Author**: Data Engineering Team
**Related Files**:
- `snowflake/dimensions/02_dim_veteran.sql` (SCD Type 2 example)
- `snowflake/dimensions/03_dim_evaluator.sql` (SCD Type 2)
- `snowflake/dimensions/04_dim_facility.sql` (SCD Type 2)
- `snowflake/dimensions/07_dim_claim.sql` (SCD Type 2)
