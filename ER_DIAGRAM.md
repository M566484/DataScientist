# Veteran Evaluation Services - Entity Relationship Diagram

## Star Schema Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     VETERAN EVALUATION DATA WAREHOUSE                        │
│                          DIMENSIONAL MODEL (STAR SCHEMA)                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                           DIMENSION TABLES                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐     │
│  │   DIM_DATE      │      │  DIM_VETERAN    │      │ DIM_EVALUATOR   │     │
│  ├─────────────────┤      ├─────────────────┤      ├─────────────────┤     │
│  │ DATE_KEY (PK)   │      │ VETERAN_KEY(PK) │      │EVALUATOR_KEY(PK)│     │
│  │ FULL_DATE       │      │ VETERAN_ID      │      │ EVALUATOR_ID    │     │
│  │ YEAR_NUMBER     │      │ FIRST_NAME      │      │ FULL_NAME       │     │
│  │ QUARTER_NUMBER  │      │ LAST_NAME       │      │ SPECIALTY       │     │
│  │ MONTH_NUMBER    │      │ SERVICE_BRANCH  │      │ CREDENTIALS     │     │
│  │ FISCAL_YEAR     │      │ DISABILITY_PCT  │      │ ACTIVE_FLAG     │     │
│  │ IS_HOLIDAY      │      │ IS_CURRENT      │      │ IS_CURRENT      │     │
│  └─────────────────┘      └─────────────────┘      └─────────────────┘     │
│                                                                               │
│  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐     │
│  │  DIM_FACILITY   │      │ DIM_EVAL_TYPE   │      │ DIM_MEDICAL_    │     │
│  ├─────────────────┤      ├─────────────────┤      │   CONDITION     │     │
│  │FACILITY_KEY(PK) │      │EVAL_TYPE_KEY(PK)│      ├─────────────────┤     │
│  │ FACILITY_ID     │      │ EVAL_TYPE_ID    │      │ MED_COND_KEY(PK)│     │
│  │ FACILITY_NAME   │      │ EVAL_TYPE_NAME  │      │ CONDITION_NAME  │     │
│  │ STATE           │      │ CATEGORY        │      │ ICD10_CODE      │     │
│  │ VISN_NUMBER     │      │ DURATION_MIN    │      │ DIAGNOSTIC_CODE │     │
│  │ IS_CURRENT      │      │ ACTIVE_FLAG     │      │ PRESUMPTIVE_FLG │     │
│  └─────────────────┘      └─────────────────┘      └─────────────────┘     │
│                                                                               │
│  ┌─────────────────┐      ┌─────────────────┐                               │
│  │   DIM_CLAIM     │      │ DIM_APPOINTMENT │                               │
│  ├─────────────────┤      ├─────────────────┤                               │
│  │  CLAIM_KEY (PK) │      │APPT_KEY (PK)    │                               │
│  │  CLAIM_ID       │      │ APPOINTMENT_ID  │                               │
│  │  CLAIM_NUMBER   │      │ APPT_TYPE       │                               │
│  │  CLAIM_STATUS   │      │ APPT_STATUS     │                               │
│  │  CLAIM_TYPE     │      │ DURATION_MIN    │                               │
│  │  IS_CURRENT     │      │ RESCHEDULED_FLG │                               │
│  └─────────────────┘      └─────────────────┘                               │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

                                      │
                                      │
                                      ▼

┌──────────────────────────────────────────────────────────────────────────────┐
│                              FACT TABLES                                      │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                         FACT_EVALUATION                                       │
│                    (Transaction Fact Table)                                   │
│            Grain: One row per evaluation per condition                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  EVALUATION_FACT_KEY (PK)                                                    │
│                                                                               │
│  Foreign Keys (Dimensions):                                                  │
│  ├─ VETERAN_KEY ────────────────────────► DIM_VETERAN                        │
│  ├─ EVALUATOR_KEY ──────────────────────► DIM_EVALUATOR                      │
│  ├─ FACILITY_KEY ───────────────────────► DIM_FACILITY                       │
│  ├─ EVALUATION_TYPE_KEY ────────────────► DIM_EVALUATION_TYPE                │
│  ├─ MEDICAL_CONDITION_KEY ──────────────► DIM_MEDICAL_CONDITION              │
│  ├─ CLAIM_KEY ──────────────────────────► DIM_CLAIM                          │
│  ├─ APPOINTMENT_KEY ────────────────────► DIM_APPOINTMENT                    │
│  ├─ EVALUATION_DATE_KEY ────────────────► DIM_DATE                           │
│  ├─ SCHEDULED_DATE_KEY ─────────────────► DIM_DATE                           │
│  └─ CLAIM_DATE_KEY ─────────────────────► DIM_DATE                           │
│                                                                               │
│  Degenerate Dimensions:                                                      │
│  ├─ EVALUATION_ID                                                            │
│  ├─ DBQ_FORM_ID                                                              │
│  └─ EXAM_REQUEST_ID                                                          │
│                                                                               │
│  Facts (Measures):                                                           │
│  ├─ EVALUATION_DURATION_MINUTES                                              │
│  ├─ TOTAL_WAIT_DAYS                                                          │
│  ├─ EVALUATION_COST_AMOUNT                                                   │
│  ├─ RECOMMENDED_RATING_PERCENTAGE                                            │
│  ├─ REPORT_COMPLETENESS_SCORE                                                │
│  ├─ ATTENDED_FLAG                                                            │
│  ├─ NO_SHOW_FLAG                                                             │
│  ├─ SUFFICIENT_EXAM_FLAG                                                     │
│  └─ TELEHEALTH_FLAG                                                          │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                      FACT_CLAIM_STATUS                                        │
│                 (Accumulating Snapshot Fact Table)                            │
│                Grain: One row per claim status change                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  CLAIM_STATUS_FACT_KEY (PK)                                                  │
│                                                                               │
│  Foreign Keys:                                                               │
│  ├─ VETERAN_KEY ────────────────────────► DIM_VETERAN                        │
│  ├─ CLAIM_KEY ──────────────────────────► DIM_CLAIM                          │
│  ├─ FACILITY_KEY ───────────────────────► DIM_FACILITY                       │
│  │                                                                            │
│  │  Multiple Date Keys (Milestones):                                         │
│  ├─ CLAIM_FILED_DATE_KEY ───────────────► DIM_DATE                           │
│  ├─ CLAIM_RECEIVED_DATE_KEY ────────────► DIM_DATE                           │
│  ├─ EXAM_SCHEDULED_DATE_KEY ────────────► DIM_DATE                           │
│  ├─ EXAM_COMPLETED_DATE_KEY ────────────► DIM_DATE                           │
│  └─ RATING_DECISION_DATE_KEY ───────────► DIM_DATE                           │
│                                                                               │
│  Facts (Measures):                                                           │
│  ├─ DAYS_IN_PREVIOUS_STATUS                                                  │
│  ├─ TOTAL_DAYS_PENDING                                                       │
│  ├─ DAYS_TO_COMPLETE                                                         │
│  ├─ DAYS_CLAIM_TO_INITIAL_REVIEW                                             │
│  ├─ DAYS_EXAM_TO_DECISION                                                    │
│  ├─ RATING_PERCENTAGE_GRANTED                                                │
│  ├─ SERVICE_CONNECTED_GRANTED                                                │
│  ├─ DECISION_MADE                                                            │
│  └─ REMAND_FLAG                                                              │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                       FACT_APPOINTMENT                                        │
│                    (Transaction Fact Table)                                   │
│                 Grain: One row per appointment                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  APPOINTMENT_FACT_KEY (PK)                                                   │
│                                                                               │
│  Foreign Keys:                                                               │
│  ├─ VETERAN_KEY ────────────────────────► DIM_VETERAN                        │
│  ├─ EVALUATOR_KEY ──────────────────────► DIM_EVALUATOR                      │
│  ├─ FACILITY_KEY ───────────────────────► DIM_FACILITY                       │
│  ├─ EVALUATION_TYPE_KEY ────────────────► DIM_EVALUATION_TYPE                │
│  ├─ APPOINTMENT_KEY ────────────────────► DIM_APPOINTMENT                    │
│  ├─ CLAIM_KEY ──────────────────────────► DIM_CLAIM                          │
│  ├─ REQUESTED_DATE_KEY ─────────────────► DIM_DATE                           │
│  ├─ SCHEDULED_DATE_KEY ─────────────────► DIM_DATE                           │
│  └─ APPOINTMENT_DATE_KEY ───────────────► DIM_DATE                           │
│                                                                               │
│  Facts (Measures):                                                           │
│  ├─ TOTAL_WAIT_DAYS                                                          │
│  ├─ ACTUAL_DURATION_MINUTES                                                  │
│  ├─ DURATION_VARIANCE_MINUTES                                                │
│  ├─ ATTENDED_FLAG                                                            │
│  ├─ NO_SHOW_FLAG                                                             │
│  ├─ CANCELLED_FLAG                                                           │
│  ├─ RESCHEDULE_COUNT                                                         │
│  ├─ MEETS_VA_WAIT_TIME_GOAL                                                  │
│  ├─ SATISFACTION_SCORE                                                       │
│  ├─ TRAVEL_DISTANCE_MILES                                                    │
│  └─ TELEHEALTH_FLAG                                                          │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                     FACT_DAILY_SNAPSHOT                                       │
│                 (Periodic Snapshot Fact Table)                                │
│              Grain: One row per facility per date                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  DAILY_SNAPSHOT_KEY (PK)                                                     │
│                                                                               │
│  Foreign Keys:                                                               │
│  ├─ FACILITY_KEY ───────────────────────► DIM_FACILITY                       │
│  └─ SNAPSHOT_DATE_KEY ──────────────────► DIM_DATE                           │
│                                                                               │
│  Facts (Aggregated Measures):                                                │
│  ├─ EVALUATIONS_COMPLETED_COUNT                                              │
│  ├─ EVALUATION_COMPLETION_RATE                                               │
│  ├─ AVERAGE_WAIT_TIME_DAYS                                                   │
│  ├─ CLAIMS_PENDING_COUNT                                                     │
│  ├─ EVALUATION_BACKLOG_COUNT                                                 │
│  ├─ WAIT_TIME_COMPLIANCE_RATE                                                │
│  ├─ TOTAL_EVALUATION_COSTS                                                   │
│  ├─ AVERAGE_COST_PER_EVALUATION                                              │
│  ├─ AVERAGE_SATISFACTION_SCORE                                               │
│  └─ NET_PROMOTER_SCORE                                                       │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────┐
│                         KEY RELATIONSHIPS                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Conformed Dimensions (shared across fact tables):                           │
│  • DIM_VETERAN                                                                │
│  • DIM_DATE                                                                   │
│  • DIM_FACILITY                                                               │
│  • DIM_EVALUATOR                                                              │
│  • DIM_CLAIM                                                                  │
│                                                                               │
│  Role-Playing Dimensions:                                                    │
│  • DIM_DATE used in multiple roles:                                          │
│    - Evaluation Date, Scheduled Date, Claim Date                             │
│    - Requested Date, Appointment Date, Completed Date                        │
│    - Various milestone dates in claim processing                             │
│                                                                               │
│  Slowly Changing Dimensions (Type 2):                                        │
│  • DIM_VETERAN (track disability rating changes)                             │
│  • DIM_EVALUATOR (track credential/performance changes)                      │
│  • DIM_FACILITY (track facility changes)                                     │
│  • DIM_CLAIM (track claim status changes)                                    │
│                                                                               │
│  All Type 2 SCDs include:                                                    │
│  - EFFECTIVE_START_DATE                                                      │
│  - EFFECTIVE_END_DATE                                                        │
│  - IS_CURRENT flag                                                           │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────┐
│                      BUSINESS PROCESS MAPPING                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  1. Evaluation Process:                                                      │
│     FACT_APPOINTMENT → FACT_EVALUATION → FACT_CLAIM_STATUS                   │
│                                                                               │
│  2. Claim Processing Pipeline:                                               │
│     Claim Filed → Exam Scheduled → Exam Completed → Decision Made            │
│     (Tracked in FACT_CLAIM_STATUS)                                           │
│                                                                               │
│  3. Performance Monitoring:                                                  │
│     Daily aggregation from transaction facts → FACT_DAILY_SNAPSHOT           │
│                                                                               │
│  4. Reporting Areas:                                                         │
│     • Wait Time Analysis (FACT_APPOINTMENT)                                  │
│     • Evaluation Quality (FACT_EVALUATION)                                   │
│     • Claim Processing Efficiency (FACT_CLAIM_STATUS)                        │
│     • Operational KPIs (FACT_DAILY_SNAPSHOT)                                 │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Cardinality

```
DIM_VETERAN (100,000s of records)
    ↓ 1:Many
FACT_EVALUATION (Millions of records)
    ↓ Many:1
DIM_MEDICAL_CONDITION (100s of records)

DIM_FACILITY (100s of records)
    ↓ 1:Many
FACT_DAILY_SNAPSHOT (100s of records per day)

DIM_DATE (3,650 records for 10 years)
    ↓ 1:Many
All FACT tables (referenced in multiple date roles)
```

## Indexing Strategy

- Primary Keys: All surrogate keys (auto-increment)
- Foreign Keys: All dimension references in fact tables
- Common Filter Columns: Status flags, IS_CURRENT, dates, facility/state
- Clustering Keys: Consider clustering facts by date + facility for query performance
