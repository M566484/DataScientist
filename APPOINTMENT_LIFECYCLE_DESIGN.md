# Appointment Lifecycle Design
## Comprehensive Event Tracking for VES Appointments

**Date**: 2025-11-16
**Standards**: VES Snowflake Naming Conventions v1.0
**Purpose**: Document how the dimensional model handles appointment lifecycle tracking

---

## Overview

The VES dimensional model uses a **dual-table approach** to comprehensively track appointment lifecycles:

1. **`fct_appointments_scheduled`** - Accumulating snapshot (current state)
2. **`fct_appointment_events`** - Transaction fact (complete history)

This design addresses the complex lifecycle where appointments can be:
- Scheduled → Confirmed → Completed ✅
- Scheduled → Cancelled → Rescheduled → Completed ✅
- Scheduled → No-Show → Rescheduled → Cancelled ✅
- Multiple reschedules before final completion ✅

---

## Two-Table Architecture

### Table 1: `fct_appointments_scheduled` (Accumulating Snapshot)

**Purpose**: Track the **current state** and overall metrics for each appointment
**Grain**: One row per appointment (updated as appointment progresses)
**Pattern**: Accumulating Snapshot Fact

**Key Characteristics**:
- ✓ Single source of truth for "what's the current status?"
- ✓ All milestone dates in one row (requested, scheduled, completed, cancelled)
- ✓ Aggregated metrics (total reschedule count, total wait time)
- ✓ Latest values for cancellation reason, satisfaction score, etc.
- ✗ Does NOT preserve history of state changes

**Use Cases**:
- Current appointment status dashboards
- Wait time reporting
- Completion rate analysis
- Satisfaction scoring
- "How many appointments are scheduled for next week?"
- "What's the average wait time for appointments?"

---

### Table 2: `fct_appointment_events` (Transaction Fact) ⭐ NEW

**Purpose**: Capture **complete history** of all appointment lifecycle events
**Grain**: One row per event (scheduled, confirmed, rescheduled, cancelled, completed, no-show, etc.)
**Pattern**: Transaction Fact

**Key Characteristics**:
- ✓ Complete immutable history of every event
- ✓ Tracks sequence of events (event_sequence_number)
- ✓ Links rescheduled appointments (previous_appointment_id, new_appointment_id)
- ✓ Captures who/what/when/why for each event
- ✓ Enables trend analysis and pattern detection
- ✗ Requires joins/aggregation for current state

**Use Cases**:
- Lifecycle analysis: "What percentage of appointments are rescheduled before completion?"
- Cancellation pattern analysis: "When do most cancellations occur?"
- User behavior: "How many veterans cancel within 24 hours?"
- System performance: "How long does it take to process a reschedule event?"
- Audit trail: "Show me every state change for appointment #12345"

---

## Lifecycle Example Scenarios

### Scenario 1: Successful Appointment (Simple Path)

```
Timeline:
Day 1:  Veteran requests appointment
Day 3:  System schedules appointment for Day 15
Day 10: Veteran confirms attendance
Day 15: Veteran arrives and completes appointment

Data in fct_appointment_events (4 rows):
┌─────────────────┬──────────────┬──────────┬────────────┐
│ appointment_id  │ event_type   │ event_date│ sequence_# │
├─────────────────┼──────────────┼──────────┼────────────┤
│ APPT-12345      │ SCHEDULED    │ Day 3    │ 1          │
│ APPT-12345      │ CONFIRMED    │ Day 10   │ 2          │
│ APPT-12345      │ COMPLETED    │ Day 15   │ 3          │
└─────────────────┴──────────────┴──────────┴────────────┘

Data in fct_appointments_scheduled (1 row, updated 3 times):
┌─────────────────┬──────────────┬──────────────┬───────────────┬──────────┐
│ appointment_id  │ scheduled_   │ confirmed_   │ completed_    │ cancelled_│
│                 │ date_sk      │ flag         │ date_sk       │ flag      │
├─────────────────┼──────────────┼──────────────┼───────────────┼───────────┤
│ APPT-12345      │ 20250103     │ TRUE         │ 20250115      │ FALSE     │
└─────────────────┴──────────────┴──────────────┴───────────────┴───────────┘
```

---

### Scenario 2: Cancelled and Rescheduled (Complex Path)

```
Timeline:
Day 1:  Veteran requests appointment
Day 3:  System schedules APPT-12345 for Day 15
Day 8:  Veteran cancels due to conflict
Day 8:  System creates new appointment APPT-12346 for Day 22
Day 20: Veteran arrives late but completes APPT-12346

Data in fct_appointment_events (5 rows):
┌─────────────────┬──────────────┬──────────┬────────┬──────────────────┬──────────────────┐
│ appointment_id  │ event_type   │ event_   │ seq_#  │ previous_appt_id │ new_appt_id      │
│                 │              │ date     │        │                  │                  │
├─────────────────┼──────────────┼──────────┼────────┼──────────────────┼──────────────────┤
│ APPT-12345      │ SCHEDULED    │ Day 3    │ 1      │ NULL             │ NULL             │
│ APPT-12345      │ CANCELLED    │ Day 8    │ 2      │ NULL             │ APPT-12346       │
│ APPT-12346      │ RESCHEDULED  │ Day 8    │ 1      │ APPT-12345       │ NULL             │
│ APPT-12346      │ LATE_ARRIVAL │ Day 20   │ 2      │ NULL             │ NULL             │
│ APPT-12346      │ COMPLETED    │ Day 20   │ 3      │ NULL             │ NULL             │
└─────────────────┴──────────────┴──────────┴────────┴──────────────────┴──────────────────┘

Data in fct_appointments_scheduled (2 rows):

Row 1 (Original - Cancelled):
┌─────────────────┬──────────────┬──────────────┬──────────┬─────────────┐
│ appointment_id  │ scheduled_   │ cancelled_   │ cancelled│ new_appt_id │
│                 │ date_sk      │ date_sk      │ flag     │             │
├─────────────────┼──────────────┼──────────────┼──────────┼─────────────┤
│ APPT-12345      │ 20250103     │ 20250108     │ TRUE     │ APPT-12346  │
└─────────────────┴──────────────┴──────────────┴──────────┴─────────────┘

Row 2 (Rescheduled - Completed):
┌─────────────────┬──────────────┬──────────────┬──────────┬──────────────┐
│ appointment_id  │ scheduled_   │ completed_   │ late_    │ reschedule_  │
│                 │ date_sk      │ date_sk      │ arrival  │ count        │
├─────────────────┼──────────────┼──────────────┼──────────┼──────────────┤
│ APPT-12346      │ 20250108     │ 20250120     │ TRUE     │ 1            │
└─────────────────┴──────────────┴──────────────┴──────────┴──────────────┘
```

---

### Scenario 3: Multiple Reschedules (Real-World Complex)

```
Timeline:
Day 1:  APPT-100 scheduled for Day 10
Day 5:  APPT-100 cancelled (veteran sick) → APPT-101 created for Day 20
Day 15: APPT-101 cancelled (provider unavailable) → APPT-102 created for Day 25
Day 25: Veteran no-show
Day 26: APPT-103 created for Day 35
Day 35: Completed successfully

Data in fct_appointment_events (9 rows):
┌─────────────────┬──────────────┬──────────┬────────┬──────────────────┬──────────────────┐
│ appointment_id  │ event_type   │ event_   │ seq_#  │ previous_appt_id │ new_appt_id      │
│                 │              │ date     │        │                  │                  │
├─────────────────┼──────────────┼──────────┼────────┼──────────────────┼──────────────────┤
│ APPT-100        │ SCHEDULED    │ Day 1    │ 1      │ NULL             │ NULL             │
│ APPT-100        │ CANCELLED    │ Day 5    │ 2      │ NULL             │ APPT-101         │
│ APPT-101        │ RESCHEDULED  │ Day 5    │ 1      │ APPT-100         │ NULL             │
│ APPT-101        │ CANCELLED    │ Day 15   │ 2      │ NULL             │ APPT-102         │
│ APPT-102        │ RESCHEDULED  │ Day 15   │ 1      │ APPT-101         │ NULL             │
│ APPT-102        │ NO_SHOW      │ Day 25   │ 2      │ NULL             │ NULL             │
│ APPT-103        │ RESCHEDULED  │ Day 26   │ 1      │ APPT-102         │ NULL             │
│ APPT-103        │ CONFIRMED    │ Day 30   │ 2      │ NULL             │ NULL             │
│ APPT-103        │ COMPLETED    │ Day 35   │ 3      │ NULL             │ NULL             │
└─────────────────┴──────────────┴──────────┴────────┴──────────────────┴──────────────────┘

Benefits of Event Table:
✓ Can trace complete lineage: APPT-100 → 101 → 102 → 103
✓ Can analyze: "3 reschedules before completion"
✓ Can identify patterns: "2 cancellations, 1 no-show before success"
✓ Can calculate: Total time from initial request to completion = 34 days
```

---

## Sample Analytical Queries

### Query 1: Appointment Lifecycle Funnel Analysis

```sql
-- Show the funnel from scheduled → completed with all intermediate states
WITH appointment_funnel AS (
  SELECT
    event_type,
    COUNT(DISTINCT appointment_id) AS appointment_count,
    COUNT(*) AS event_count
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointment_events
  WHERE event_date_sk >= 20250101
  GROUP BY event_type
)
SELECT
  event_type,
  appointment_count,
  event_count,
  ROUND(100.0 * appointment_count / SUM(appointment_count) OVER (), 2) AS pct_of_total
FROM appointment_funnel
ORDER BY appointment_count DESC;

/*
Expected Output:
┌──────────────┬──────────────────┬─────────────┬──────────────┐
│ event_type   │ appointment_count│ event_count │ pct_of_total │
├──────────────┼──────────────────┼─────────────┼──────────────┤
│ SCHEDULED    │ 10,000           │ 10,000      │ 40.0%        │
│ COMPLETED    │ 7,500            │ 7,500       │ 30.0%        │
│ CONFIRMED    │ 6,000            │ 6,000       │ 24.0%        │
│ CANCELLED    │ 2,000            │ 2,200       │ 8.0%         │
│ RESCHEDULED  │ 1,500            │ 1,800       │ 6.0%         │
│ NO_SHOW      │ 500              │ 500         │ 2.0%         │
└──────────────┴──────────────────┴─────────────┴──────────────┘
*/
```

---

### Query 2: Calculate Average Reschedules Before Completion

```sql
-- How many times is an appointment rescheduled before final completion?
WITH reschedule_chain AS (
  SELECT
    appointment_id,
    previous_appointment_id,
    event_type,
    event_sequence_number
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointment_events
  WHERE event_type IN ('RESCHEDULED', 'COMPLETED')
),
completed_appointments AS (
  SELECT DISTINCT
    appointment_id AS final_appointment_id
  FROM reschedule_chain
  WHERE event_type = 'COMPLETED'
),
reschedule_count AS (
  SELECT
    ca.final_appointment_id,
    COUNT(DISTINCT rc.appointment_id) - 1 AS reschedule_count  -- -1 to exclude final appointment
  FROM completed_appointments ca
  LEFT JOIN reschedule_chain rc
    ON rc.appointment_id = ca.final_appointment_id
    OR rc.previous_appointment_id = ca.final_appointment_id
    -- Recursive logic to trace back through reschedule chain
  GROUP BY ca.final_appointment_id
)
SELECT
  reschedule_count,
  COUNT(*) AS appointment_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM reschedule_count
GROUP BY reschedule_count
ORDER BY reschedule_count;

/*
Expected Output:
┌──────────────────┬──────────────────┬────────────┐
│ reschedule_count │ appointment_count│ percentage │
├──────────────────┼──────────────────┼────────────┤
│ 0                │ 6,000            │ 80.0%      │  -- No reschedules
│ 1                │ 1,200            │ 16.0%      │  -- Rescheduled once
│ 2                │ 250              │ 3.3%       │  -- Rescheduled twice
│ 3                │ 45               │ 0.6%       │  -- Rescheduled 3 times
│ 4+               │ 5                │ 0.1%       │  -- Rescheduled 4+ times
└──────────────────┴──────────────────┴────────────┘
*/
```

---

### Query 3: Cancellation Timing Analysis

```sql
-- When do cancellations occur relative to scheduled appointment date?
SELECT
  CASE
    WHEN e.advance_notice_hours >= 168 THEN '7+ days advance'
    WHEN e.advance_notice_hours >= 72 THEN '3-7 days advance'
    WHEN e.advance_notice_hours >= 24 THEN '1-3 days advance'
    WHEN e.advance_notice_hours >= 2 THEN 'Same day (2+ hours)'
    ELSE 'Last minute (<2 hours)'
  END AS cancellation_timing,
  e.cancelled_by,
  COUNT(*) AS cancellation_count,
  AVG(e.advance_notice_hours) AS avg_notice_hours
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointment_events e
WHERE e.event_type = 'CANCELLED'
  AND e.event_date_sk >= 20250101
GROUP BY 1, 2
ORDER BY 2, 1;

/*
Expected Output:
┌─────────────────────┬──────────────┬────────────────────┬──────────────────┐
│ cancellation_timing │ cancelled_by │ cancellation_count │ avg_notice_hours │
├─────────────────────┼──────────────┼────────────────────┼──────────────────┤
│ 7+ days advance     │ Veteran      │ 800                │ 240.5            │
│ 3-7 days advance    │ Veteran      │ 600                │ 96.2             │
│ 1-3 days advance    │ Veteran      │ 400                │ 36.8             │
│ Same day (2+ hours) │ Veteran      │ 150                │ 5.3              │
│ Last minute         │ Veteran      │ 50                 │ 0.8              │
│ 7+ days advance     │ Provider     │ 120                │ 336.0            │
│ 3-7 days advance    │ Provider     │ 80                 │ 108.5            │
│ Same day            │ System       │ 45                 │ 4.2              │
└─────────────────────┴──────────────┴────────────────────┴──────────────────┘
*/
```

---

### Query 4: Track Complete Lifecycle for Specific Appointment

```sql
-- Show complete history for a specific appointment including reschedules
WITH RECURSIVE appointment_chain AS (
  -- Base case: Start with a specific appointment
  SELECT
    e.appointment_id,
    e.event_id,
    e.event_type,
    e.event_timestamp,
    e.event_sequence_number,
    e.previous_appointment_id,
    e.new_appointment_id,
    e.cancelled_by,
    e.cancellation_reason_description,
    1 AS chain_level,
    CAST(e.appointment_id AS VARCHAR(1000)) AS appointment_lineage
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointment_events e
  WHERE e.appointment_id = 'APPT-12345'
    AND e.event_sequence_number = 1  -- Start with first event

  UNION ALL

  -- Recursive case: Follow the chain
  SELECT
    e.appointment_id,
    e.event_id,
    e.event_type,
    e.event_timestamp,
    e.event_sequence_number,
    e.previous_appointment_id,
    e.new_appointment_id,
    e.cancelled_by,
    e.cancellation_reason_description,
    ac.chain_level + 1,
    ac.appointment_lineage || ' → ' || e.appointment_id
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointment_events e
  INNER JOIN appointment_chain ac
    ON e.appointment_id = ac.new_appointment_id  -- Follow reschedules forward
    OR e.previous_appointment_id = ac.appointment_id  -- Follow reschedules backward
  WHERE e.event_sequence_number > 1  -- Get subsequent events
)
SELECT
  appointment_id,
  event_type,
  event_timestamp,
  event_sequence_number,
  cancelled_by,
  cancellation_reason_description,
  new_appointment_id,
  chain_level,
  appointment_lineage
FROM appointment_chain
ORDER BY event_timestamp;

/*
Expected Output:
┌────────────┬─────────────┬─────────────────────┬──────────┬─────────────┬──────────────────┬────────────┬────────┬────────────────────────────┐
│appointment │ event_type  │ event_timestamp     │ sequence │ cancelled_by│ reason           │ new_appt   │ chain  │ lineage                    │
│ _id        │             │                     │          │             │                  │            │ level  │                            │
├────────────┼─────────────┼─────────────────────┼──────────┼─────────────┼──────────────────┼────────────┼────────┼────────────────────────────┤
│APPT-12345  │ SCHEDULED   │2025-01-03 09:00:00  │ 1        │ NULL        │ NULL             │ NULL       │ 1      │APPT-12345                  │
│APPT-12345  │ CANCELLED   │2025-01-08 14:30:00  │ 2        │ Veteran     │ Scheduling       │APPT-12346  │ 1      │APPT-12345                  │
│            │             │                     │          │             │   conflict       │            │        │                            │
│APPT-12346  │ RESCHEDULED │2025-01-08 14:31:00  │ 1        │ NULL        │ NULL             │ NULL       │ 2      │APPT-12345 → APPT-12346     │
│APPT-12346  │ CONFIRMED   │2025-01-18 10:00:00  │ 2        │ NULL        │ NULL             │ NULL       │ 2      │APPT-12345 → APPT-12346     │
│APPT-12346  │ COMPLETED   │2025-01-22 11:45:00  │ 3        │ NULL        │ NULL             │ NULL       │ 2      │APPT-12345 → APPT-12346     │
└────────────┴─────────────┴─────────────────────┴──────────┴─────────────┴──────────────────┴────────────┴────────┴────────────────────────────┘
*/
```

---

### Query 5: Join Both Tables for Complete Picture

```sql
-- Combine current state with event history
SELECT
  -- Current state from accumulating snapshot
  appt.appointment_id,
  appt.veteran_sk,
  appt.scheduled_date_sk,
  appt.completed_date_sk,
  appt.reschedule_count,
  appt.completed_flag,
  appt.cancelled_flag,

  -- Event history from transaction fact
  evt.event_type,
  evt.event_timestamp,
  evt.event_sequence_number,
  evt.cancelled_by,
  evt.cancellation_reason_description

FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointments_scheduled appt
LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointment_events evt
  ON appt.appointment_id = evt.appointment_id

WHERE appt.appointment_id = 'APPT-12345'
ORDER BY evt.event_timestamp;
```

---

## Data Loading Patterns

### Pattern 1: New Appointment Scheduled

```sql
-- Step 1: Insert event record
INSERT INTO fct_appointment_events (
  appointment_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number,
  veteran_sk, facility_sk, scheduled_appointment_date, ...
)
VALUES (
  'APPT-12345', 'SCHEDULED', 20250103,
  '2025-01-03 09:00:00', 1,
  123, 456, '2025-01-15', ...
);

-- Step 2: Insert accumulating snapshot record
INSERT INTO fct_appointments_scheduled (
  appointment_id, veteran_sk, facility_sk,
  scheduled_date_sk, scheduled_flag, ...
)
VALUES (
  'APPT-12345', 123, 456,
  20250103, TRUE, ...
);
```

---

### Pattern 2: Appointment Cancelled and Rescheduled

```sql
-- Step 1: Insert cancellation event
INSERT INTO fct_appointment_events (
  appointment_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number,
  cancelled_by, cancellation_reason_code,
  new_appointment_id, ...
)
VALUES (
  'APPT-12345', 'CANCELLED', 20250108,
  '2025-01-08 14:30:00', 2,
  'Veteran', 'CONFLICT',
  'APPT-12346', ...
);

-- Step 2: Update original appointment snapshot
UPDATE fct_appointments_scheduled
SET
  cancelled_flag = TRUE,
  cancelled_date_sk = 20250108,
  cancelled_by = 'Veteran',
  cancellation_reason_code = 'CONFLICT',
  new_appointment_id = 'APPT-12346',
  updated_timestamp = CURRENT_TIMESTAMP()
WHERE appointment_id = 'APPT-12345';

-- Step 3: Insert reschedule event for new appointment
INSERT INTO fct_appointment_events (
  appointment_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number,
  previous_appointment_id,
  reschedule_reason_code, ...
)
VALUES (
  'APPT-12346', 'RESCHEDULED', 20250108,
  '2025-01-08 14:31:00', 1,
  'APPT-12345',
  'CONFLICT', ...
);

-- Step 4: Insert new appointment snapshot
INSERT INTO fct_appointments_scheduled (
  appointment_id, veteran_sk, facility_sk,
  scheduled_date_sk, reschedule_count,
  rescheduled_flag, ...
)
VALUES (
  'APPT-12346', 123, 456,
  20250108, 1,
  TRUE, ...
);
```

---

### Pattern 3: Appointment Completed

```sql
-- Step 1: Insert completion event
INSERT INTO fct_appointment_events (
  appointment_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number,
  actual_start_time, actual_end_time,
  actual_duration_minutes, ...
)
VALUES (
  'APPT-12346', 'COMPLETED', 20250122,
  '2025-01-22 11:45:00', 3,
  '10:00:00', '11:45:00',
  105, ...
);

-- Step 2: Update appointment snapshot with completion details
UPDATE fct_appointments_scheduled
SET
  completed_flag = TRUE,
  completed_date_sk = 20250122,
  actual_start_time = '10:00:00',
  actual_end_time = '11:45:00',
  actual_duration_minutes = 105,
  attended_flag = TRUE,
  updated_timestamp = CURRENT_TIMESTAMP()
WHERE appointment_id = 'APPT-12346';
```

---

## Benefits of This Design

### ✅ Comprehensive Tracking
- **Complete audit trail** of every appointment state change
- **Lineage tracking** through reschedule chains
- **Event sequencing** for understanding temporal order

### ✅ Flexible Analysis
- **Current state queries**: Use `fct_appointments_scheduled`
- **Historical pattern analysis**: Use `fct_appointment_events`
- **Combined views**: Join both tables

### ✅ Business Intelligence
- Identify **reschedule patterns** (who, when, why)
- Measure **cancellation impact** on operations
- Track **patient behavior** across lifecycle
- Calculate **true cycle times** from initial request to completion

### ✅ Operational Insights
- **Capacity planning**: Predict reschedule rates
- **Resource optimization**: Staff based on completion patterns
- **Process improvement**: Reduce cancellation rates
- **Compliance**: Track adherence to wait time goals

### ✅ Data Quality
- **Immutable history** in events table
- **Single source of truth** for current state
- **Referential integrity** via foreign keys
- **Audit capability** for regulatory compliance

---

## Performance Considerations

### Indexing Strategy

```sql
-- Event table clustering (already defined)
CLUSTER BY (event_date_sk, appointment_id);

-- Recommended search optimization
ALTER TABLE fct_appointment_events
  ADD SEARCH OPTIMIZATION ON EQUALITY(appointment_id, event_type);

-- Snapshot table clustering (already defined)
CLUSTER BY (appointment_date_sk, facility_sk);
```

### Partition Pruning

```sql
-- Always filter on event_date_sk for partition pruning
SELECT *
FROM fct_appointment_events
WHERE event_date_sk >= 20250101  -- Enables partition pruning
  AND appointment_id = 'APPT-12345';
```

---

## Migration Path

### Step 1: Deploy New Table
```sql
-- Run: snowflake/facts/05_fact_appointment_events.sql
```

### Step 2: Backfill Historical Events (If Needed)
```sql
-- Extract historical events from snapshot table or source systems
-- Transform into event format
-- Load into fct_appointment_events
```

### Step 3: Update ETL Processes
- Modify appointment scheduling process to write to both tables
- Modify cancellation process to write to both tables
- Modify completion process to write to both tables

### Step 4: Update Reporting
- Migrate reports to use appropriate table
- Create combined views if needed
- Deprecate old queries that can't answer lifecycle questions

---

## Conclusion

The **dual-table approach** provides:

1. **Simplicity** for "what's happening now" queries → Use `fct_appointments_scheduled`
2. **Depth** for "how did we get here" analysis → Use `fct_appointment_events`
3. **Flexibility** for complex lifecycle analysis → Join both tables

This design fully addresses the appointment lifecycle challenge:
- ✅ Scheduled → Cancelled → Rescheduled → Completed
- ✅ Multiple reschedules before completion
- ✅ Complete audit trail
- ✅ Pattern analysis and trending
- ✅ Regulatory compliance

---

**Next Steps**:
1. Review and approve `fct_appointment_events` schema
2. Deploy to development environment
3. Test with sample data
4. Update ETL processes
5. Migrate reporting queries

---

**Document Version**: 1.0
**Author**: Data Engineering Team
**Status**: Proposed Enhancement
**Related Files**:
- `snowflake/facts/03_fact_appointment.sql` (existing accumulating snapshot)
- `snowflake/facts/05_fact_appointment_events.sql` (new transaction fact)
