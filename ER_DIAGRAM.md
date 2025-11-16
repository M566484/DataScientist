> **Updated to align with VES Snowflake Naming Conventions v1.0**

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
│  │   dim_dates     │      │  dim_veterans   │      │ dim_evaluators  │     │
│  ├─────────────────┤      ├─────────────────┤      ├─────────────────┤     │
│  │ date_sk (PK)    │      │ veteran_sk (PK) │      │ evaluator_sk(PK)│     │
│  │ full_date       │      │ veteran_id      │      │ evaluator_id    │     │
│  │ year_number     │      │ first_name      │      │ full_name       │     │
│  │ quarter_number  │      │ last_name       │      │ specialty       │     │
│  │ month_number    │      │ service_branch  │      │ credentials     │     │
│  │ fiscal_year     │      │ disability_pct  │      │ active_flag     │     │
│  │ is_holiday      │      │ is_current      │      │ is_current      │     │
│  └─────────────────┘      └─────────────────┘      └─────────────────┘     │
│                                                                               │
│  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐     │
│  │ dim_facilities  │      │ dim_evaluation_ │      │ dim_medical_    │     │
│  ├─────────────────┤      │      types      │      │   conditions    │     │
│  │ facility_sk (PK)│      ├─────────────────┤      ├─────────────────┤     │
│  │ facility_id     │      │eval_type_sk (PK)│      │ med_cond_sk (PK)│     │
│  │ facility_name   │      │ eval_type_id    │      │ condition_name  │     │
│  │ state           │      │ eval_type_name  │      │ icd10_code      │     │
│  │ visn_number     │      │ category        │      │ diagnostic_code │     │
│  │ is_current      │      │ duration_min    │      │ presumptive_flg │     │
│  └─────────────────┘      │ active_flag     │      └─────────────────┘     │
│                            └─────────────────┘                               │
│                                                                               │
│  ┌─────────────────┐      ┌─────────────────┐                               │
│  │   dim_claims    │      │ dim_appointments│                               │
│  ├─────────────────┤      ├─────────────────┤                               │
│  │  claim_sk (PK)  │      │ appt_sk (PK)    │                               │
│  │  claim_id       │      │ appointment_id  │                               │
│  │  claim_number   │      │ appt_type       │                               │
│  │  claim_status   │      │ appt_status     │                               │
│  │  claim_type     │      │ duration_min    │                               │
│  │  is_current     │      │ rescheduled_flg │                               │
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
│                     fct_evaluations_completed                                 │
│                    (Transaction Fact Table)                                   │
│            Grain: One row per evaluation per condition                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  evaluation_fact_sk (PK)                                                     │
│                                                                               │
│  Foreign Keys (Dimensions):                                                  │
│  ├─ veteran_sk ─────────────────────────► dim_veterans                       │
│  ├─ evaluator_sk ───────────────────────► dim_evaluators                     │
│  ├─ facility_sk ────────────────────────► dim_facilities                     │
│  ├─ evaluation_type_sk ─────────────────► dim_evaluation_types               │
│  ├─ medical_condition_sk ───────────────► dim_medical_conditions             │
│  ├─ claim_sk ───────────────────────────► dim_claims                         │
│  ├─ appointment_sk ─────────────────────► dim_appointments                   │
│  ├─ evaluation_date_sk ─────────────────► dim_dates                          │
│  ├─ scheduled_date_sk ──────────────────► dim_dates                          │
│  └─ claim_date_sk ──────────────────────► dim_dates                          │
│                                                                               │
│  Degenerate Dimensions:                                                      │
│  ├─ evaluation_id                                                            │
│  ├─ dbq_form_id                                                              │
│  └─ exam_request_id                                                          │
│                                                                               │
│  Facts (Measures):                                                           │
│  ├─ evaluation_duration_minutes                                              │
│  ├─ total_wait_days                                                          │
│  ├─ evaluation_cost_amount                                                   │
│  ├─ recommended_rating_percentage                                            │
│  ├─ report_completeness_score                                                │
│  ├─ attended_flag                                                            │
│  ├─ no_show_flag                                                             │
│  ├─ sufficient_exam_flag                                                     │
│  └─ telehealth_flag                                                          │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                      fct_claim_status_changes                                 │
│                 (Accumulating Snapshot Fact Table)                            │
│                Grain: One row per claim status change                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  claim_status_fact_sk (PK)                                                   │
│                                                                               │
│  Foreign Keys:                                                               │
│  ├─ veteran_sk ─────────────────────────► dim_veterans                       │
│  ├─ claim_sk ───────────────────────────► dim_claims                         │
│  ├─ facility_sk ────────────────────────► dim_facilities                     │
│  │                                                                            │
│  │  Multiple Date Keys (Milestones):                                         │
│  ├─ claim_filed_date_sk ────────────────► dim_dates                          │
│  ├─ claim_received_date_sk ─────────────► dim_dates                          │
│  ├─ exam_scheduled_date_sk ─────────────► dim_dates                          │
│  ├─ exam_completed_date_sk ─────────────► dim_dates                          │
│  └─ rating_decision_date_sk ────────────► dim_dates                          │
│                                                                               │
│  Facts (Measures):                                                           │
│  ├─ days_in_previous_status                                                  │
│  ├─ total_days_pending                                                       │
│  ├─ days_to_complete                                                         │
│  ├─ days_claim_to_initial_review                                             │
│  ├─ days_exam_to_decision                                                    │
│  ├─ rating_percentage_granted                                                │
│  ├─ service_connected_granted                                                │
│  ├─ decision_made                                                            │
│  └─ remand_flag                                                              │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                       fct_appointments_scheduled                              │
│                    (Transaction Fact Table)                                   │
│                 Grain: One row per appointment                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  appointment_fact_sk (PK)                                                    │
│                                                                               │
│  Foreign Keys:                                                               │
│  ├─ veteran_sk ─────────────────────────► dim_veterans                       │
│  ├─ evaluator_sk ───────────────────────► dim_evaluators                     │
│  ├─ facility_sk ────────────────────────► dim_facilities                     │
│  ├─ evaluation_type_sk ─────────────────► dim_evaluation_types               │
│  ├─ appointment_sk ─────────────────────► dim_appointments                   │
│  ├─ claim_sk ───────────────────────────► dim_claims                         │
│  ├─ requested_date_sk ──────────────────► dim_dates                          │
│  ├─ scheduled_date_sk ──────────────────► dim_dates                          │
│  └─ appointment_date_sk ────────────────► dim_dates                          │
│                                                                               │
│  Facts (Measures):                                                           │
│  ├─ total_wait_days                                                          │
│  ├─ actual_duration_minutes                                                  │
│  ├─ duration_variance_minutes                                                │
│  ├─ attended_flag                                                            │
│  ├─ no_show_flag                                                             │
│  ├─ cancelled_flag                                                           │
│  ├─ reschedule_count                                                         │
│  ├─ meets_va_wait_time_goal                                                  │
│  ├─ satisfaction_score                                                       │
│  ├─ travel_distance_miles                                                    │
│  └─ telehealth_flag                                                          │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                     fct_daily_facility_snapshot                               │
│                 (Periodic Snapshot Fact Table)                                │
│              Grain: One row per facility per date                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  daily_snapshot_sk (PK)                                                      │
│                                                                               │
│  Foreign Keys:                                                               │
│  ├─ facility_sk ────────────────────────► dim_facilities                     │
│  └─ snapshot_date_sk ───────────────────► dim_dates                          │
│                                                                               │
│  Facts (Aggregated Measures):                                                │
│  ├─ evaluations_completed_count                                              │
│  ├─ evaluation_completion_rate                                               │
│  ├─ average_wait_time_days                                                   │
│  ├─ claims_pending_count                                                     │
│  ├─ evaluation_backlog_count                                                 │
│  ├─ wait_time_compliance_rate                                                │
│  ├─ total_evaluation_costs                                                   │
│  ├─ average_cost_per_evaluation                                              │
│  ├─ average_satisfaction_score                                               │
│  └─ net_promoter_score                                                       │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────┐
│                         KEY RELATIONSHIPS                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Conformed Dimensions (shared across fact tables):                           │
│  • dim_veterans                                                               │
│  • dim_dates                                                                  │
│  • dim_facilities                                                             │
│  • dim_evaluators                                                             │
│  • dim_claims                                                                 │
│                                                                               │
│  Role-Playing Dimensions:                                                    │
│  • dim_dates used in multiple roles:                                         │
│    - Evaluation Date, Scheduled Date, Claim Date                             │
│    - Requested Date, Appointment Date, Completed Date                        │
│    - Various milestone dates in claim processing                             │
│                                                                               │
│  Slowly Changing Dimensions (Type 2):                                        │
│  • dim_veterans (track disability rating changes)                            │
│  • dim_evaluators (track credential/performance changes)                     │
│  • dim_facilities (track facility changes)                                   │
│  • dim_claims (track claim status changes)                                   │
│                                                                               │
│  All Type 2 SCDs include:                                                    │
│  - effective_start_date                                                      │
│  - effective_end_date                                                        │
│  - is_current flag                                                           │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────┐
│                      BUSINESS PROCESS MAPPING                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  1. Evaluation Process:                                                      │
│     fct_appointments_scheduled → fct_evaluations_completed →                 │
│     fct_claim_status_changes                                                 │
│                                                                               │
│  2. Claim Processing Pipeline:                                               │
│     Claim Filed → Exam Scheduled → Exam Completed → Decision Made            │
│     (Tracked in fct_claim_status_changes)                                    │
│                                                                               │
│  3. Performance Monitoring:                                                  │
│     Daily aggregation from transaction facts → fct_daily_facility_snapshot   │
│                                                                               │
│  4. Reporting Areas:                                                         │
│     • Wait Time Analysis (fct_appointments_scheduled)                        │
│     • Evaluation Quality (fct_evaluations_completed)                         │
│     • Claim Processing Efficiency (fct_claim_status_changes)                 │
│     • Operational KPIs (fct_daily_facility_snapshot)                         │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Cardinality

```
dim_veterans (100,000s of records)
    ↓ 1:Many
fct_evaluations_completed (Millions of records)
    ↓ Many:1
dim_medical_conditions (100s of records)

dim_facilities (100s of records)
    ↓ 1:Many
fct_daily_facility_snapshot (100s of records per day)

dim_dates (3,650 records for 10 years)
    ↓ 1:Many
All FACT tables (referenced in multiple date roles)
```

## Performance Optimization Strategy

### Snowflake-Specific Approach

Snowflake does not use traditional indexes. Instead, the model uses:

**1. Primary Keys** (Metadata Only)
- All surrogate keys defined as PRIMARY KEY
- Used for documentation and BI tool optimization
- NOT enforced by Snowflake

**2. Foreign Keys** (Metadata Only)
- All dimension references in fact tables
- Helps query optimizer understand relationships
- NOT enforced by Snowflake

**3. Clustering Keys** (Implemented in DDL)

All fact tables include clustering keys for optimal query performance:

- `fct_evaluations_completed`: `CLUSTER BY (evaluation_date_sk, facility_sk)`
- `fct_claim_status_changes`: `CLUSTER BY (claim_sk, rating_decision_date_sk)`
- `fct_appointments_scheduled`: `CLUSTER BY (appointment_date_sk, facility_sk)`
- `fct_daily_facility_snapshot`: `CLUSTER BY (snapshot_date_sk, facility_sk)`

Clustering organizes micro-partitions for efficient data pruning during queries.

**4. Automatic Optimizations**
- Micro-partitioning (automatic)
- Columnar storage (automatic)
- Result caching (automatic)
- Query optimization based on metadata (automatic)
