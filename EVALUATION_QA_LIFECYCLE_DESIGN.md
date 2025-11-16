# Evaluation QA Lifecycle Design
## Quality Assurance Review Process Tracking for VES Evaluations

**Date**: 2025-11-16
**Standards**: VES Snowflake Naming Conventions v1.0
**Purpose**: Document how the dimensional model handles evaluation QA review cycles

---

## Overview

The VES dimensional model uses a **dual-table approach** to comprehensively track evaluation quality assurance lifecycles:

1. **`fct_evaluations_completed`** - Accumulating snapshot (current state of evaluation)
2. **`fct_evaluation_qa_events`** - Transaction fact (complete QA history)

This design addresses the complex QA workflow where evaluations go through:
- Examiner submits findings → QA Review → Needs clarification
- Examiner submits clarification → QA Review again → Still needs clarification
- Examiner submits additional clarification → QA Review → Approved
- Report sent to VA

**Multiple QA cycles with iterative clarification loops are fully tracked.**

---

## The Business Process

### QA Workflow Stages

```
┌─────────────────────────────────────────────────────────────────┐
│                    EVALUATION QA LIFECYCLE                      │
└─────────────────────────────────────────────────────────────────┘

Stage 1: INITIAL SUBMISSION
├─ Examiner completes evaluation
├─ Examiner submits findings/DBQ to VES
└─ Event: INITIAL_SUBMISSION

Stage 2: QA REVIEW (Cycle 1)
├─ QA reviewer assigned
├─ QA review started
├─ QA performs quality checks
└─ Event: QA_REVIEW_COMPLETED
    ├─ Outcome: APPROVED → Go to Stage 5
    ├─ Outcome: NEEDS_CLARIFICATION → Go to Stage 3
    └─ Outcome: REJECTED → Return to examiner

Stage 3: CLARIFICATION REQUEST
├─ QA identifies deficiencies
├─ QA requests specific clarifications
├─ Examiner notified
└─ Event: CLARIFICATION_REQUESTED
    ├─ Deficiency type: INCOMPLETE_EXAM
    ├─ Deficiency type: MISSING_NEXUS
    ├─ Deficiency type: INSUFFICIENT_RATIONALE
    └─ Deficiency type: MISSING_DBQ_ITEMS

Stage 4: CLARIFICATION SUBMISSION
├─ Examiner provides clarification
├─ Clarification submitted back to VES
└─ Event: CLARIFICATION_SUBMITTED
    ├─ Method: ADDENDUM
    ├─ Method: REVISED_DBQ
    ├─ Method: PHONE_CONSULT
    └─ Method: ADDITIONAL_EXAM
    └─ Loop back to Stage 2 (next QA cycle)

Stage 5: FINAL APPROVAL
├─ All QA checks passed
├─ Report approved for VA submission
└─ Event: APPROVED

Stage 6: SENT TO VA
├─ Report transmitted to VA
├─ Confirmation received
└─ Event: SENT_TO_VA

LOOP: Stages 2-4 can repeat multiple times until approval
```

---

## Two-Table Architecture

### Table 1: `fct_evaluations_completed` (Accumulating Snapshot)

**Purpose**: Track the **current state** of evaluation including final QA status
**Grain**: One row per evaluation per medical condition
**Pattern**: Accumulating Snapshot Fact (updated as QA progresses)

**Current QA Fields** (limited):
```sql
-- Existing fields (lines 78-81 of current fact table)
review_required_flag BOOLEAN
qa_reviewed_flag BOOLEAN
qa_reviewer_id VARCHAR(50)
qa_review_date DATE
```

**Limitations**:
- ✗ Only tracks ONE QA review (can't handle multiple cycles)
- ✗ No clarification request/response tracking
- ✗ No deficiency details
- ✗ No quality scores
- ✗ Loses history of QA iterations

**Recommended Enhancement** (add these columns):
```sql
-- Enhanced QA summary fields
qa_cycles_total INTEGER,
qa_first_pass_approval_flag BOOLEAN,
qa_final_approval_date DATE,
qa_sent_to_va_date DATE,
qa_total_clarifications_requested INTEGER,
qa_days_in_review INTEGER,
qa_overall_quality_score INTEGER
```

---

### Table 2: `fct_evaluation_qa_events` (Transaction Fact) ⭐ NEW

**Purpose**: Capture **complete history** of all QA review cycles and events
**Grain**: One row per QA event (submission, review, clarification, approval, etc.)
**Pattern**: Transaction Fact (immutable event history)

**Key Characteristics**:
- ✓ Complete immutable history of every QA event
- ✓ Tracks multiple QA review cycles (`qa_cycle_number`)
- ✓ Captures event sequence (`event_sequence_number`)
- ✓ Records deficiency details for each review
- ✓ Tracks clarification requests and responses
- ✓ Links QA reviewers and quality scores
- ✓ Enables SLA tracking and compliance monitoring

**Use Cases**:
- QA process analysis: "What percentage of evaluations pass first QA review?"
- Deficiency trending: "What are the most common QA deficiencies?"
- Examiner performance: "Which examiners require the most clarifications?"
- SLA compliance: "How long does QA process take on average?"
- Quality improvement: "Are QA scores improving over time?"
- Audit trail: "Show me every QA event for evaluation #67890"

---

## Lifecycle Example Scenarios

### Scenario 1: First-Pass Approval (Ideal Case)

```
Timeline:
Day 1:  Examiner completes evaluation for PTSD
Day 2:  Examiner submits complete DBQ with nexus opinion
Day 3:  QA reviewer (Sarah) assigned, begins review
Day 3:  QA review completed - ALL CHECKS PASS
Day 3:  Evaluation APPROVED on first review
Day 4:  Report sent to VA

Data in fct_evaluation_qa_events (4 rows):
┌───────────────┬────────────────────────┬──────────┬──────┬──────────┬────────────────┐
│ evaluation_id │ event_type             │ event_   │ seq# │ qa_cycle │ review_outcome │
│               │                        │ date     │      │ _number  │                │
├───────────────┼────────────────────────┼──────────┼──────┼──────────┼────────────────┤
│ EVAL-67890    │ INITIAL_SUBMISSION     │ Day 2    │ 1    │ NULL     │ NULL           │
│ EVAL-67890    │ QA_REVIEW_STARTED      │ Day 3    │ 2    │ 1        │ NULL           │
│ EVAL-67890    │ QA_REVIEW_COMPLETED    │ Day 3    │ 3    │ 1        │ APPROVED       │
│ EVAL-67890    │ SENT_TO_VA             │ Day 4    │ 4    │ 1        │ NULL           │
└───────────────┴────────────────────────┴──────────┴──────┴──────────┴────────────────┘

QA Metrics Captured:
- first_pass_approval_flag = TRUE
- total_qa_cycles_at_event = 1
- days_in_qa_process = 1
- overall_quality_score = 95
- sla_met_flag = TRUE
```

---

### Scenario 2: One Clarification Cycle (Common Case)

```
Timeline:
Day 1:  Examiner completes evaluation for Back Condition
Day 2:  Examiner submits DBQ
Day 3:  QA Review 1 - NEEDS CLARIFICATION
        Issue: Nexus opinion unclear, DBQ item 15 incomplete
Day 3:  QA sends clarification request to examiner
Day 5:  Examiner submits clarification (addendum)
Day 6:  QA Review 2 - ALL CHECKS PASS
Day 6:  Evaluation APPROVED
Day 7:  Report sent to VA

Data in fct_evaluation_qa_events (7 rows):
┌───────────────┬────────────────────────┬──────────┬──────┬──────────┬────────────────┬──────────────┐
│ evaluation_id │ event_type             │ event_   │ seq# │ qa_cycle │ review_outcome │ deficiency_  │
│               │                        │ date     │      │ _number  │                │ category     │
├───────────────┼────────────────────────┼──────────┼──────┼──────────┼────────────────┼──────────────┤
│ EVAL-67891    │ INITIAL_SUBMISSION     │ Day 2    │ 1    │ NULL     │ NULL           │ NULL         │
│ EVAL-67891    │ QA_REVIEW_STARTED      │ Day 3    │ 2    │ 1        │ NULL           │ NULL         │
│ EVAL-67891    │ QA_REVIEW_COMPLETED    │ Day 3    │ 3    │ 1        │ NEEDS_CLARIF   │ MISSING_NEXUS│
│ EVAL-67891    │ CLARIFICATION_REQUESTED│ Day 3    │ 4    │ 1        │ NULL           │ MISSING_NEXUS│
│ EVAL-67891    │ CLARIFICATION_SUBMITTED│ Day 5    │ 5    │ 2        │ NULL           │ NULL         │
│ EVAL-67891    │ QA_REVIEW_COMPLETED    │ Day 6    │ 6    │ 2        │ APPROVED       │ NULL         │
│ EVAL-67891    │ SENT_TO_VA             │ Day 7    │ 7    │ 2        │ NULL           │ NULL         │
└───────────────┴────────────────────────┴──────────┴──────┴──────────┴────────────────┴──────────────┘

QA Metrics Captured:
- first_pass_approval_flag = FALSE
- total_qa_cycles_at_event = 2
- total_clarifications_requested = 1
- days_in_qa_process = 4
- deficiency_count = 2 (nexus + DBQ item)
- clarification_response_method = ADDENDUM
```

---

### Scenario 3: Multiple Clarification Cycles (Complex Case)

```
Timeline:
Day 1:  Examiner completes complex evaluation for TBI
Day 2:  Examiner submits DBQ
Day 3:  QA Review 1 - NEEDS CLARIFICATION
        Issues: Missing functional assessment, incomplete rationale, 3 DBQ items
Day 3:  QA requests clarification
Day 6:  Examiner submits clarification (revised DBQ)
Day 7:  QA Review 2 - STILL NEEDS CLARIFICATION
        Issues: Functional assessment still inadequate, 1 DBQ item still incomplete
Day 7:  QA requests additional clarification (escalated to QA manager)
Day 10: Examiner submits additional clarification (phone consult + addendum)
Day 11: QA Review 3 - ALL CHECKS PASS
Day 11: Evaluation APPROVED
Day 12: Report sent to VA

Data in fct_evaluation_qa_events (11 rows):
┌───────────────┬────────────────────────┬──────────┬──────┬──────────┬────────────────┬─────────────────┬────────────┐
│ evaluation_id │ event_type             │ event_   │ seq# │ qa_cycle │ review_outcome │ deficiency_     │ escalated_ │
│               │                        │ date     │      │ _number  │                │ count           │ flag       │
├───────────────┼────────────────────────┼──────────┼──────┼──────────┼────────────────┼─────────────────┼────────────┤
│ EVAL-67892    │ INITIAL_SUBMISSION     │ Day 2    │ 1    │ NULL     │ NULL           │ 0               │ FALSE      │
│ EVAL-67892    │ QA_REVIEW_STARTED      │ Day 3    │ 2    │ 1        │ NULL           │ 0               │ FALSE      │
│ EVAL-67892    │ QA_REVIEW_COMPLETED    │ Day 3    │ 3    │ 1        │ NEEDS_CLARIF   │ 5               │ FALSE      │
│ EVAL-67892    │ CLARIFICATION_REQUESTED│ Day 3    │ 4    │ 1        │ NULL           │ 5               │ FALSE      │
│ EVAL-67892    │ CLARIFICATION_SUBMITTED│ Day 6    │ 5    │ 2        │ NULL           │ 0               │ FALSE      │
│ EVAL-67892    │ QA_REVIEW_STARTED      │ Day 7    │ 6    │ 2        │ NULL           │ 0               │ FALSE      │
│ EVAL-67892    │ QA_REVIEW_COMPLETED    │ Day 7    │ 7    │ 2        │ NEEDS_CLARIF   │ 2               │ TRUE       │
│ EVAL-67892    │ CLARIFICATION_REQUESTED│ Day 7    │ 8    │ 2        │ NULL           │ 2               │ TRUE       │
│ EVAL-67892    │ CLARIFICATION_SUBMITTED│ Day 10   │ 9    │ 3        │ NULL           │ 0               │ FALSE      │
│ EVAL-67892    │ QA_REVIEW_COMPLETED    │ Day 11   │ 10   │ 3        │ APPROVED       │ 0               │ FALSE      │
│ EVAL-67892    │ SENT_TO_VA             │ Day 12   │ 11   │ 3        │ NULL           │ 0               │ FALSE      │
└───────────────┴────────────────────────┴──────────┴──────┴──────────┴────────────────┴─────────────────┴────────────┘

QA Metrics Captured:
- first_pass_approval_flag = FALSE
- total_qa_cycles_at_event = 3
- total_clarifications_requested = 2
- days_in_qa_process = 9
- deficiency_count = 5 initially, then 2, then 0
- escalated_flag = TRUE (cycle 2)
- clarification_response_method = REVISED_DBQ, then PHONE_CONSULT + ADDENDUM
- overall_quality_score = 75 (lower due to issues)
```

---

## Key Event Types Tracked

### 1. INITIAL_SUBMISSION
- Examiner submits completed evaluation/DBQ to VES
- Marks start of QA process
- Captures submission timestamp and version

### 2. QA_REVIEW_STARTED
- QA reviewer assigned and begins review
- Tracks which QA team/reviewer
- Starts QA timer for SLA tracking

### 3. QA_REVIEW_COMPLETED
- QA review finished with an outcome
- Outcome can be: APPROVED, NEEDS_CLARIFICATION, REJECTED, INSUFFICIENT
- Captures quality scores and deficiency details

### 4. CLARIFICATION_REQUESTED
- QA identifies deficiencies and requests clarification
- Specifies type of clarification needed
- Includes specific DBQ items flagged
- Sets due date and priority

### 5. CLARIFICATION_SUBMITTED
- Examiner provides clarification/corrections
- Tracks method: ADDENDUM, REVISED_DBQ, PHONE_CONSULT, ADDITIONAL_EXAM
- Starts next QA cycle

### 6. APPROVED
- Final approval for VA submission
- All QA checks passed
- May include approval notes or conditions

### 7. REJECTED (rare)
- Evaluation rejected, must be redone
- Tracks rejection reason

### 8. SENT_TO_VA
- Report transmitted to VA
- Includes VA confirmation number
- Marks end of VES QA process

---

## Sample Analytical Queries

### Query 1: First-Pass Approval Rate

```sql
-- What percentage of evaluations pass QA on first review?
WITH first_reviews AS (
  SELECT
    evaluation_id,
    event_type,
    review_outcome,
    qa_cycle_number,
    first_pass_approval_flag
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluation_qa_events
  WHERE event_type = 'QA_REVIEW_COMPLETED'
    AND qa_cycle_number = 1
    AND event_date_sk >= 20250101
),
approval_summary AS (
  SELECT
    CASE WHEN review_outcome = 'APPROVED' THEN 'First Pass Approval'
         WHEN review_outcome = 'NEEDS_CLARIFICATION' THEN 'Needs Clarification'
         WHEN review_outcome = 'REJECTED' THEN 'Rejected'
         ELSE 'Other'
    END AS outcome_category,
    COUNT(*) AS evaluation_count
  FROM first_reviews
  GROUP BY 1
)
SELECT
  outcome_category,
  evaluation_count,
  ROUND(100.0 * evaluation_count / SUM(evaluation_count) OVER (), 2) AS percentage
FROM approval_summary
ORDER BY evaluation_count DESC;

/*
Expected Output:
┌─────────────────────────┬───────────────────┬────────────┐
│ outcome_category        │ evaluation_count  │ percentage │
├─────────────────────────┼───────────────────┼────────────┤
│ First Pass Approval     │ 6,500             │ 65.0%      │
│ Needs Clarification     │ 3,200             │ 32.0%      │
│ Rejected                │ 300               │ 3.0%       │
└─────────────────────────┴───────────────────┴────────────┘
*/
```

---

### Query 2: Average QA Cycles by Evaluation Type

```sql
-- How many QA cycles does each evaluation type require on average?
WITH final_events AS (
  SELECT
    e.evaluation_id,
    e.evaluation_type_sk,
    et.evaluation_type_name,
    MAX(e.qa_cycle_number) AS total_qa_cycles,
    MAX(e.event_sequence_number) AS total_events
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluation_qa_events e
  INNER JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluation_types et
    ON e.evaluation_type_sk = et.evaluation_type_sk
  WHERE e.event_date_sk >= 20250101
    AND e.event_type = 'SENT_TO_VA'  -- Only completed evaluations
  GROUP BY 1, 2, 3
)
SELECT
  evaluation_type_name,
  COUNT(DISTINCT evaluation_id) AS evaluation_count,
  AVG(total_qa_cycles) AS avg_qa_cycles,
  MIN(total_qa_cycles) AS min_qa_cycles,
  MAX(total_qa_cycles) AS max_qa_cycles,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_qa_cycles) AS median_qa_cycles
FROM final_events
GROUP BY evaluation_type_name
ORDER BY avg_qa_cycles DESC;

/*
Expected Output:
┌──────────────────────────┬───────────────────┬────────────────┬────────────────┬────────────────┬───────────────────┐
│ evaluation_type_name     │ evaluation_count  │ avg_qa_cycles  │ min_qa_cycles  │ max_qa_cycles  │ median_qa_cycles  │
├──────────────────────────┼───────────────────┼────────────────┼────────────────┼────────────────┼───────────────────┤
│ Mental Health (PTSD)     │ 1,200             │ 2.3            │ 1              │ 5              │ 2.0               │
│ TBI                      │ 800               │ 2.1            │ 1              │ 6              │ 2.0               │
│ Musculoskeletal          │ 3,500             │ 1.5            │ 1              │ 4              │ 1.0               │
│ Hearing Loss             │ 2,000             │ 1.2            │ 1              │ 3              │ 1.0               │
│ Respiratory              │ 1,500             │ 1.4            │ 1              │ 3              │ 1.0               │
└──────────────────────────┴───────────────────┴────────────────┴────────────────┴────────────────┴───────────────────┘
*/
```

---

### Query 3: Most Common QA Deficiencies

```sql
-- What are the top deficiency categories?
SELECT
  deficiency_category,
  deficiency_severity,
  COUNT(*) AS occurrence_count,
  COUNT(DISTINCT evaluation_id) AS evaluations_affected,
  AVG(deficiency_count) AS avg_deficiencies_per_occurrence,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluation_qa_events
WHERE event_type = 'CLARIFICATION_REQUESTED'
  AND event_date_sk >= 20250101
  AND deficiency_found_flag = TRUE
GROUP BY deficiency_category, deficiency_severity
ORDER BY occurrence_count DESC
LIMIT 10;

/*
Expected Output:
┌───────────────────────────┬─────────────────────┬──────────────────┬─────────────────────┬───────────────────────────────┬────────────────────┐
│ deficiency_category       │ deficiency_severity │ occurrence_count │ evaluations_affected│ avg_deficiencies_per_occurrence│ percentage_of_total│
├───────────────────────────┼─────────────────────┼──────────────────┼─────────────────────┼───────────────────────────────┼────────────────────┤
│ MISSING_NEXUS             │ MAJOR               │ 1,250            │ 1,200               │ 1.8                           │ 28.5%              │
│ INSUFFICIENT_RATIONALE    │ MODERATE            │ 980              │ 950                 │ 2.1                           │ 22.3%              │
│ MISSING_DBQ_ITEMS         │ MODERATE            │ 850              │ 820                 │ 2.5                           │ 19.4%              │
│ INCOMPLETE_EXAM           │ MAJOR               │ 520              │ 510                 │ 1.3                           │ 11.9%              │
│ FUNCTIONAL_ASSESSMENT_GAP │ MODERATE            │ 450              │ 440                 │ 1.6                           │ 10.3%              │
│ DIAGNOSTIC_CRITERIA_GAP   │ MINOR               │ 320              │ 315                 │ 1.2                           │ 7.3%               │
└───────────────────────────┴─────────────────────┴──────────────────┴─────────────────────┴───────────────────────────────┴────────────────────┘
*/
```

---

### Query 4: Examiner QA Performance Metrics

```sql
-- Which examiners have the best QA performance?
WITH examiner_qa_metrics AS (
  SELECT
    e.evaluator_sk,
    ev.full_name AS examiner_name,
    ev.specialty,
    COUNT(DISTINCT e.evaluation_id) AS total_evaluations_submitted,
    SUM(CASE WHEN e.first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) AS first_pass_approvals,
    AVG(e.total_qa_cycles_at_event) AS avg_qa_cycles,
    AVG(e.overall_quality_score) AS avg_quality_score,
    SUM(e.total_clarifications_requested) AS total_clarifications,
    AVG(e.days_in_qa_process) AS avg_days_in_qa
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluation_qa_events e
  INNER JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators ev
    ON e.evaluator_sk = ev.evaluator_sk
  WHERE e.event_type = 'SENT_TO_VA'
    AND e.event_date_sk >= 20250101
    AND ev.is_current = TRUE
  GROUP BY e.evaluator_sk, ev.full_name, ev.specialty
  HAVING COUNT(DISTINCT e.evaluation_id) >= 20  -- Minimum 20 evaluations
)
SELECT
  examiner_name,
  specialty,
  total_evaluations_submitted,
  first_pass_approvals,
  ROUND(100.0 * first_pass_approvals / total_evaluations_submitted, 2) AS first_pass_rate_pct,
  ROUND(avg_qa_cycles, 1) AS avg_qa_cycles,
  ROUND(avg_quality_score, 1) AS avg_quality_score,
  total_clarifications,
  ROUND(avg_days_in_qa, 1) AS avg_days_in_qa,
  CASE
    WHEN (first_pass_approvals::FLOAT / total_evaluations_submitted) >= 0.80
         AND avg_quality_score >= 90 THEN 'EXCELLENT'
    WHEN (first_pass_approvals::FLOAT / total_evaluations_submitted) >= 0.65
         AND avg_quality_score >= 80 THEN 'GOOD'
    WHEN (first_pass_approvals::FLOAT / total_evaluations_submitted) >= 0.50 THEN 'NEEDS_IMPROVEMENT'
    ELSE 'REQUIRES_ATTENTION'
  END AS performance_tier
FROM examiner_qa_metrics
ORDER BY first_pass_rate_pct DESC, avg_quality_score DESC
LIMIT 20;

/*
Expected Output:
┌────────────────────┬──────────────────┬──────────────────────────┬─────────────────────┬─────────────────────┬────────────────┬───────────────────┬──────────────────────┬──────────────────┬───────────────────┐
│ examiner_name      │ specialty        │ total_evaluations_submitted│ first_pass_approvals│ first_pass_rate_pct │ avg_qa_cycles  │ avg_quality_score │ total_clarifications │ avg_days_in_qa   │ performance_tier  │
├────────────────────┼──────────────────┼──────────────────────────┼─────────────────────┼─────────────────────┼────────────────┼───────────────────┼──────────────────────┼──────────────────┼───────────────────┤
│ Dr. Sarah Johnson  │ Orthopedics      │ 145                      │ 125                 │ 86.2%               │ 1.2            │ 93.5              │ 20                   │ 2.1              │ EXCELLENT         │
│ Dr. Michael Chen   │ Cardiology       │ 98                       │ 81                  │ 82.7%               │ 1.3            │ 91.2              │ 17                   │ 2.3              │ EXCELLENT         │
│ Dr. Emily Rodriguez│ Psychiatry       │ 156                      │ 112                 │ 71.8%               │ 1.6            │ 87.3              │ 44                   │ 3.2              │ GOOD              │
│ Dr. James Wilson   │ Neurology        │ 87                       │ 56                  │ 64.4%               │ 1.8            │ 82.1              │ 31                   │ 3.8              │ NEEDS_IMPROVEMENT │
└────────────────────┴──────────────────┴──────────────────────────┴─────────────────────┴─────────────────────┴────────────────┴───────────────────┴──────────────────────┴──────────────────┴───────────────────┘
*/
```

---

### Query 5: QA Process Duration Analysis

```sql
-- How long does the QA process take from submission to VA delivery?
WITH qa_timeline AS (
  SELECT
    evaluation_id,
    MIN(CASE WHEN event_type = 'INITIAL_SUBMISSION' THEN event_timestamp END) AS submission_time,
    MAX(CASE WHEN event_type = 'SENT_TO_VA' THEN event_timestamp END) AS va_delivery_time,
    MAX(qa_cycle_number) AS total_qa_cycles,
    MAX(total_clarifications_requested) AS total_clarifications
  FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluation_qa_events
  WHERE event_date_sk >= 20250101
  GROUP BY evaluation_id
  HAVING submission_time IS NOT NULL AND va_delivery_time IS NOT NULL
)
SELECT
  total_qa_cycles AS qa_cycles,
  total_clarifications AS clarifications_requested,
  COUNT(*) AS evaluation_count,
  AVG(DATEDIFF('day', submission_time, va_delivery_time)) AS avg_days_submission_to_va,
  MIN(DATEDIFF('day', submission_time, va_delivery_time)) AS min_days,
  MAX(DATEDIFF('day', submission_time, va_delivery_time)) AS max_days,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('day', submission_time, va_delivery_time)) AS median_days,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DATEDIFF('day', submission_time, va_delivery_time)) AS p90_days
FROM qa_timeline
GROUP BY total_qa_cycles, total_clarifications
ORDER BY total_qa_cycles, total_clarifications;

/*
Expected Output:
┌───────────┬──────────────────────────┬───────────────────┬──────────────────────────┬──────────┬──────────┬─────────────┬──────────┐
│ qa_cycles │ clarifications_requested │ evaluation_count  │ avg_days_submission_to_va│ min_days │ max_days │ median_days │ p90_days │
├───────────┼──────────────────────────┼───────────────────┼──────────────────────────┼──────────┼──────────┼─────────────┼──────────┤
│ 1         │ 0                        │ 6,500             │ 2.3                      │ 1        │ 5        │ 2.0         │ 3.0      │
│ 2         │ 1                        │ 2,800             │ 5.6                      │ 3        │ 12       │ 5.0         │ 8.0      │
│ 3         │ 2                        │ 380               │ 9.2                      │ 5        │ 18       │ 9.0         │ 14.0     │
│ 4         │ 3                        │ 45                │ 13.8                     │ 8        │ 25       │ 13.0        │ 20.0     │
└───────────┴──────────────────────────┴───────────────────┴──────────────────────────┴──────────┴──────────┴─────────────┴──────────┘
*/
```

---

### Query 6: Complete QA History for Single Evaluation

```sql
-- Show complete QA lifecycle for a specific evaluation
SELECT
  qa_event_id,
  event_type,
  event_timestamp,
  event_sequence_number,
  qa_cycle_number,
  review_outcome,
  deficiency_category,
  deficiency_count,
  clarification_type,
  qa_reviewer_name,
  overall_quality_score,
  days_in_qa_process,
  event_notes
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluation_qa_events
WHERE evaluation_id = 'EVAL-67892'
ORDER BY event_sequence_number;

/*
Expected Output (11 rows for complex case):
┌──────────────┬────────────────────────┬─────────────────────┬──────────┬──────────┬────────────────┬────────────────────┬──────────────┬──────────────────────┬─────────────────┬─────────────────────┬──────────────────┬──────────────────────────────┐
│ qa_event_id  │ event_type             │ event_timestamp     │ sequence │ qa_cycle │ review_outcome │ deficiency_category│ deficiency_  │ clarification_type   │ qa_reviewer_    │ overall_quality_    │ days_in_qa_      │ event_notes              │
│              │                        │                     │          │          │                │                    │ count        │                      │ name            │ score               │ process          │                          │
├──────────────┼────────────────────────┼─────────────────────┼──────────┼──────────┼────────────────┼────────────────────┼──────────────┼──────────────────────┼─────────────────┼─────────────────────┼──────────────────┼──────────────────────────┤
│ QAE-1001     │ INITIAL_SUBMISSION     │ 2025-01-02 14:30:00 │ 1        │ NULL     │ NULL           │ NULL               │ 0            │ NULL                 │ NULL            │ NULL                │ 0                │ TBI evaluation submitted │
│ QAE-1002     │ QA_REVIEW_STARTED      │ 2025-01-03 09:00:00 │ 2        │ 1        │ NULL           │ NULL               │ 0            │ NULL                 │ Sarah Johnson   │ NULL                │ 1                │ Assigned to QA reviewer  │
│ QAE-1003     │ QA_REVIEW_COMPLETED    │ 2025-01-03 16:45:00 │ 3        │ 1        │ NEEDS_CLARIF   │ INCOMPLETE_EXAM    │ 5            │ NULL                 │ Sarah Johnson   │ 68                  │ 1                │ Multiple deficiencies    │
│ QAE-1004     │ CLARIFICATION_REQUESTED│ 2025-01-03 17:00:00 │ 4        │ 1        │ NULL           │ INCOMPLETE_EXAM    │ 5            │ FUNCTIONAL_ASSESSMENT│ Sarah Johnson   │ NULL                │ 1                │ Need functional details  │
│ QAE-1005     │ CLARIFICATION_SUBMITTED│ 2025-01-06 11:20:00 │ 5        │ 2        │ NULL           │ NULL               │ 0            │ NULL                 │ NULL            │ NULL                │ 4                │ Revised DBQ submitted    │
│ QAE-1006     │ QA_REVIEW_STARTED      │ 2025-01-07 08:30:00 │ 6        │ 2        │ NULL           │ NULL               │ 0            │ NULL                 │ Sarah Johnson   │ NULL                │ 5                │ Second review cycle      │
│ QAE-1007     │ QA_REVIEW_COMPLETED    │ 2025-01-07 14:15:00 │ 7        │ 2        │ NEEDS_CLARIF   │ FUNCTIONAL_ASSESS  │ 2            │ NULL                 │ Sarah Johnson   │ 73                  │ 5                │ Still needs improvement  │
│ QAE-1008     │ CLARIFICATION_REQUESTED│ 2025-01-07 14:30:00 │ 8        │ 2        │ NULL           │ FUNCTIONAL_ASSESS  │ 2            │ FUNCTIONAL_ASSESSMENT│ Sarah Johnson   │ NULL                │ 5                │ Escalated to QA manager  │
│ QAE-1009     │ CLARIFICATION_SUBMITTED│ 2025-01-10 10:00:00 │ 9        │ 3        │ NULL           │ NULL               │ 0            │ NULL                 │ NULL            │ NULL                │ 8                │ Phone consult + addendum │
│ QAE-1010     │ QA_REVIEW_COMPLETED    │ 2025-01-11 15:30:00 │ 10       │ 3        │ APPROVED       │ NULL               │ 0            │ NULL                 │ Sarah Johnson   │ 85                  │ 9                │ All checks passed        │
│ QAE-1011     │ SENT_TO_VA             │ 2025-01-12 09:00:00 │ 11       │ 3        │ NULL           │ NULL               │ 0            │ NULL                 │ NULL            │ NULL                │ 10               │ Transmitted to VA        │
└──────────────┴────────────────────────┴─────────────────────┴──────────┴──────────┴────────────────┴────────────────────┴──────────────┴──────────────────────┴─────────────────┴─────────────────────┴──────────────────┴──────────────────────────┘
*/
```

---

## Data Loading Patterns

### Pattern 1: Initial Submission

```sql
-- Step 1: Insert initial submission event
INSERT INTO fct_evaluation_qa_events (
  evaluation_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number, qa_cycle_number,
  veteran_sk, evaluator_sk, facility_sk,
  evaluation_type_sk, medical_condition_sk, claim_sk,
  document_version_number, ...
)
VALUES (
  'EVAL-67890', 'INITIAL_SUBMISSION', 20250102,
  '2025-01-02 14:30:00', 1, NULL,
  123, 456, 789,
  101, 202, 303,
  1, ...
);

-- Step 2: Update evaluation fact table (optional - if adding summary fields)
UPDATE fct_evaluations_completed
SET
  qa_in_progress_flag = TRUE,
  qa_submission_date = '2025-01-02',
  updated_timestamp = CURRENT_TIMESTAMP()
WHERE evaluation_id = 'EVAL-67890';
```

---

### Pattern 2: QA Review with Clarification Request

```sql
-- Step 1: Insert QA review completed event
INSERT INTO fct_evaluation_qa_events (
  evaluation_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number, qa_cycle_number,
  review_outcome, deficiency_found_flag, deficiency_count,
  deficiency_category, deficiency_severity,
  qa_reviewer_sk, qa_reviewer_name, qa_team,
  overall_quality_score, completeness_score,
  nexus_quality_score, ...
)
VALUES (
  'EVAL-67891', 'QA_REVIEW_COMPLETED', 20250103,
  '2025-01-03 16:45:00', 3, 1,
  'NEEDS_CLARIFICATION', TRUE, 2,
  'MISSING_NEXUS', 'MAJOR',
  789, 'Sarah Johnson', 'Medical QA',
  68, 75, 55, ...
);

-- Step 2: Insert clarification request event
INSERT INTO fct_evaluation_qa_events (
  evaluation_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number, qa_cycle_number,
  is_clarification_request, clarification_type,
  clarification_description, clarification_due_date,
  clarification_priority, specific_dbq_items_flagged,
  qa_reviewer_sk, examiner_notified_flag,
  examiner_notification_method, ...
)
VALUES (
  'EVAL-67891', 'CLARIFICATION_REQUESTED', 20250103,
  '2025-01-03 17:00:00', 4, 1,
  TRUE, 'NEXUS_EXPLANATION',
  'Please provide clearer nexus opinion linking condition to service. DBQ item 15 incomplete.',
  '2025-01-08',
  'ROUTINE', '15',
  789, TRUE,
  'EMAIL', ...
);

-- Step 3: Update evaluation fact table
UPDATE fct_evaluations_completed
SET
  qa_reviewed_flag = TRUE,
  qa_clarification_requested = TRUE,
  qa_cycles_total = 1,
  updated_timestamp = CURRENT_TIMESTAMP()
WHERE evaluation_id = 'EVAL-67891';
```

---

### Pattern 3: Clarification Submission

```sql
-- Insert clarification submission event
INSERT INTO fct_evaluation_qa_events (
  evaluation_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number, qa_cycle_number,
  is_clarification_response, clarification_response_text,
  clarification_response_complete, clarification_response_method,
  document_version_number, ...
)
VALUES (
  'EVAL-67891', 'CLARIFICATION_SUBMITTED', 20250105,
  '2025-01-05 11:20:00', 5, 2,
  TRUE, 'Added detailed nexus opinion explaining service connection. Completed DBQ item 15.',
  TRUE, 'ADDENDUM',
  2, ...
);

-- Loop: This triggers another QA review cycle
```

---

### Pattern 4: Final Approval and VA Submission

```sql
-- Step 1: Insert approval event
INSERT INTO fct_evaluation_qa_events (
  evaluation_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number, qa_cycle_number,
  is_final_approval, approved_by, approved_timestamp,
  review_outcome, overall_quality_score,
  first_pass_approval_flag, total_qa_cycles_at_event,
  days_in_qa_process, sla_met_flag, ...
)
VALUES (
  'EVAL-67891', 'APPROVED', 20250106,
  '2025-01-06 15:30:00', 6, 2,
  TRUE, 'Sarah Johnson', '2025-01-06 15:30:00',
  'APPROVED', 85,
  FALSE, 2,
  4, TRUE, ...
);

-- Step 2: Insert sent to VA event
INSERT INTO fct_evaluation_qa_events (
  evaluation_id, event_type, event_date_sk,
  event_timestamp, event_sequence_number, qa_cycle_number,
  sent_to_va_flag, sent_to_va_timestamp,
  va_submission_method, va_confirmation_number, ...
)
VALUES (
  'EVAL-67891', 'SENT_TO_VA', 20250107,
  '2025-01-07 09:00:00', 7, 2,
  TRUE, '2025-01-07 09:00:00',
  'ELECTRONIC', 'VA-CONF-123456', ...
);

-- Step 3: Update evaluation fact table with final QA summary
UPDATE fct_evaluations_completed
SET
  qa_reviewed_flag = TRUE,
  qa_final_approval_date = '2025-01-06',
  qa_sent_to_va_date = '2025-01-07',
  qa_cycles_total = 2,
  qa_total_clarifications_requested = 1,
  qa_days_in_review = 4,
  qa_overall_quality_score = 85,
  qa_first_pass_approval_flag = FALSE,
  updated_timestamp = CURRENT_TIMESTAMP()
WHERE evaluation_id = 'EVAL-67891';
```

---

## Benefits of This Design

### ✅ Complete QA Audit Trail
- **Every QA event captured**: Submission, review, clarification, approval
- **Immutable history**: Never lose track of what happened when
- **Regulatory compliance**: Full audit trail for VA oversight

### ✅ Quality Improvement Analytics
- **Identify deficiency patterns**: What issues occur most frequently?
- **Track quality trends**: Are quality scores improving over time?
- **Examiner coaching**: Target training based on common deficiencies
- **QA team efficiency**: Which reviewers are most efficient?

### ✅ Process Optimization
- **Cycle time analysis**: How long does QA actually take?
- **Bottleneck identification**: Where do evaluations get stuck?
- **SLA tracking**: Are we meeting QA turnaround goals?
- **First-pass rate**: Trending toward better quality submissions

### ✅ Examiner Performance Management
- **Quality scorecards**: Track examiner quality metrics
- **Peer comparison**: Benchmark against specialty peers
- **Improvement tracking**: Monitor individual progress over time
- **Recognition opportunities**: Identify high performers

### ✅ Flexible Reporting
- **Current state**: Use `fct_evaluations_completed` for snapshots
- **Historical analysis**: Use `fct_evaluation_qa_events` for trends
- **Combined views**: Join both for complete picture

---

## Performance Considerations

### Indexing Strategy

```sql
-- Event table clustering (already defined)
CLUSTER BY (event_date_sk, evaluation_id);

-- Recommended search optimization
ALTER TABLE fct_evaluation_qa_events
  ADD SEARCH OPTIMIZATION ON EQUALITY(evaluation_id, event_type, qa_cycle_number);
```

### Query Optimization Tips

```sql
-- Always filter on event_date_sk for partition pruning
SELECT *
FROM fct_evaluation_qa_events
WHERE event_date_sk >= 20250101  -- Enables partition pruning
  AND evaluation_id = 'EVAL-67890';

-- Use materialized views for common aggregations
CREATE MATERIALIZED VIEW mv_qa_performance_daily AS
SELECT
  event_date_sk,
  COUNT(DISTINCT evaluation_id) AS evaluations_reviewed,
  AVG(overall_quality_score) AS avg_quality_score,
  SUM(CASE WHEN first_pass_approval_flag = TRUE THEN 1 ELSE 0 END) AS first_pass_approvals
FROM fct_evaluation_qa_events
WHERE event_type = 'QA_REVIEW_COMPLETED'
GROUP BY event_date_sk;
```

---

## Recommended Enhancements to fct_evaluations_completed

Add these summary columns to the accumulating snapshot:

```sql
-- Add to fct_evaluations_completed table
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_cycles_total INTEGER;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_first_pass_approval_flag BOOLEAN;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_final_approval_date DATE;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_sent_to_va_date DATE;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_total_clarifications_requested INTEGER;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_days_in_review INTEGER;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_overall_quality_score INTEGER;
ALTER TABLE fct_evaluations_completed ADD COLUMN qa_deficiency_count_total INTEGER;

-- Add column comments
COMMENT ON COLUMN fct_evaluations_completed.qa_cycles_total IS 'Total number of QA review cycles before approval';
COMMENT ON COLUMN fct_evaluations_completed.qa_first_pass_approval_flag IS 'TRUE if approved on first QA review without clarifications';
COMMENT ON COLUMN fct_evaluations_completed.qa_final_approval_date IS 'Date of final QA approval';
COMMENT ON COLUMN fct_evaluations_completed.qa_sent_to_va_date IS 'Date report was sent to VA';
COMMENT ON COLUMN fct_evaluations_completed.qa_total_clarifications_requested IS 'Total number of clarification requests';
COMMENT ON COLUMN fct_evaluations_completed.qa_days_in_review IS 'Total days in QA review process';
COMMENT ON COLUMN fct_evaluations_completed.qa_overall_quality_score IS 'Final overall quality score from QA';
COMMENT ON COLUMN fct_evaluations_completed.qa_deficiency_count_total IS 'Total deficiencies found across all QA cycles';
```

---

## Conclusion

The **dual-table QA lifecycle design** provides:

1. **Complete visibility** into QA review process from submission to VA delivery
2. **Detailed tracking** of clarification cycles and deficiency patterns
3. **Performance metrics** for examiners and QA reviewers
4. **Process optimization** insights for reducing cycle times
5. **Quality improvement** data for targeted training and coaching
6. **Regulatory compliance** with full audit trails

This addresses your QA workflow:
- ✅ Examiner submits → QA reviews → Needs clarification
- ✅ Examiner clarifies → QA reviews again → Still needs clarification
- ✅ Examiner clarifies more → QA reviews → Approved
- ✅ Report sent to VA

**Every step is tracked with complete context and metrics.**

---

**Next Steps**:
1. Review and approve `fct_evaluation_qa_events` schema
2. Consider adding summary columns to `fct_evaluations_completed`
3. Deploy to development environment
4. Test with sample QA workflow data
5. Update ETL processes to capture QA events
6. Build QA performance dashboards

---

**Document Version**: 1.0
**Author**: Data Engineering Team
**Status**: Proposed Enhancement
**Related Files**:
- `snowflake/facts/01_fact_evaluation.sql` (existing evaluation fact)
- `snowflake/facts/06_fact_evaluation_qa_events.sql` (new QA events fact)
