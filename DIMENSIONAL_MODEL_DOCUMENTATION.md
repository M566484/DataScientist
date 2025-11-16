> **Updated to align with VES Snowflake Naming Conventions v1.0**

# Veteran Evaluation Services - Dimensional Model Documentation

## Overview

This dimensional model is designed for reporting and analytics on veteran evaluation services data, following the Kimball methodology. The model is physically implemented in Snowflake and supports comprehensive reporting on medical evaluations, disability claims, and appointment scheduling.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Database Structure](#database-structure)
3. [Dimension Tables](#dimension-tables)
4. [Fact Tables](#fact-tables)
5. [Data Model Diagram](#data-model-diagram)
6. [Common Queries](#common-queries)
7. [ETL Considerations](#etl-considerations)
8. [Deployment Instructions](#deployment-instructions)

---

## Architecture Overview

### Design Principles

- **Kimball Dimensional Modeling**: Star schema design for optimal query performance
- **Snowflake Optimized**: Leverages Snowflake-specific features and data types
- **Slowly Changing Dimensions**: Type 2 SCD for tracking historical changes
- **Multiple Fact Table Grains**: Transaction, accumulating snapshot, and periodic snapshot fact tables
- **Conformed Dimensions**: Shared dimensions across fact tables for consistency

### Key Features

- Comprehensive veteran demographic and service history tracking
- Medical evaluation tracking with quality metrics
- Claim processing workflow with milestone tracking
- Appointment scheduling and wait time analysis
- Daily performance snapshots for KPI monitoring
- Telehealth and community care support
- VA-specific metrics (fiscal year, priority groups, etc.)

---

## Database Structure

### Schemas

```
VETERAN_EVALUATION_DW (Database)
├── WAREHOUSE (Schema) - Dimension and fact tables
├── STG (Schema) - Staging area for ETL
└── UTIL (Schema) - Utility objects (procedures, functions, sequences)
```

### File Formats

- **CSV_FORMAT**: For CSV data loads
- **JSON_FORMAT**: For JSON data loads

---

## Dimension Tables

### 1. dim_dates
**Type**: Conformed Dimension (Type 1)
**Purpose**: Standard date dimension for time-based analysis
**Grain**: One row per day
**Business Key**: full_date

**Key Attributes**:
- Standard calendar attributes (year, quarter, month, week, day)
- VA fiscal year support (starts October 1)
- Business day indicators
- Federal holiday flags

**File**: `snowflake/dimensions/01_dim_date.sql`

**Usage**: Used across all fact tables for time-based filtering and aggregation

---

### 2. dim_veterans
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: Veteran demographic and service information
**Grain**: One row per veteran per version
**Business Key**: veteran_id
**Surrogate Key**: veteran_sk

**Key Attributes**:
- Personal information (name, DOB, contact)
- Military service details (branch, era, rank)
- Disability rating and status
- VA enrollment and priority group
- SCD tracking (effective_start_date, effective_end_date, is_current)

**File**: `snowflake/dimensions/02_dim_veteran.sql`

**Why Type 2**: Track changes in disability ratings, contact information, and benefit status over time

---

### 3. dim_evaluators
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: Medical professionals and evaluators
**Grain**: One row per evaluator per version
**Business Key**: evaluator_id
**Surrogate Key**: evaluator_sk

**Key Attributes**:
- Professional credentials and specialties
- License and certification information
- Employment details
- Performance metrics
- Active/inactive status

**File**: `snowflake/dimensions/03_dim_evaluator.sql`

**Why Type 2**: Track changes in credentials, employment, and performance over time

---

### 4. dim_facilities
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: VA medical centers and evaluation facilities
**Grain**: One row per facility per version
**Business Key**: facility_id
**Surrogate Key**: facility_sk

**Key Attributes**:
- Facility type and characteristics
- Location and VISN (Veterans Integrated Service Network)
- Services offered and capacity
- Accreditation status

**File**: `snowflake/dimensions/04_dim_facility.sql`

**Why Type 2**: Track changes in facility capabilities, accreditation, and status

---

### 5. dim_evaluation_types
**Type**: Type 1 Dimension
**Purpose**: Types of medical evaluations
**Grain**: One row per evaluation type
**Business Key**: evaluation_type_id
**Surrogate Key**: evaluation_type_sk

**Key Attributes**:
- Evaluation category (C&P Exam, IME, etc.)
- Duration and complexity
- Regulatory requirements
- Scheduling requirements

**File**: `snowflake/dimensions/05_dim_evaluation_type.sql`

---

### 6. dim_medical_conditions
**Type**: Type 1 Dimension
**Purpose**: Medical conditions being evaluated
**Grain**: One row per medical condition
**Business Key**: medical_condition_id
**Surrogate Key**: medical_condition_sk

**Key Attributes**:
- Condition classification
- ICD-10 and VA diagnostic codes
- DBQ form references
- Rating information
- Service connection indicators

**File**: `snowflake/dimensions/06_dim_medical_condition.sql`

---

### 7. dim_claims
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: VA disability claims information
**Grain**: One row per claim per version
**Business Key**: claim_id
**Surrogate Key**: claim_sk

**Key Attributes**:
- Claim type and status
- Processing information
- Decision details
- Priority indicators
- Evidence and exam flags

**File**: `snowflake/dimensions/07_dim_claim.sql`

**Why Type 2**: Track claim status changes and processing history

---

### 8. dim_appointments
**Type**: Type 1 Dimension
**Purpose**: Appointment scheduling details
**Grain**: One row per appointment
**Business Key**: appointment_id
**Surrogate Key**: appointment_sk

**Key Attributes**:
- Appointment type and status
- Scheduling details
- Reminder and confirmation information
- Wait time categorization

**File**: `snowflake/dimensions/08_dim_appointment.sql`

---

## Fact Tables

### 1. fct_evaluations_completed
**Type**: Transaction Fact Table
**Purpose**: Core fact table for medical evaluations
**Grain**: One row per evaluation per medical condition
**File**: `snowflake/facts/01_fact_evaluation.sql`

**Dimensions**:
- dim_veterans
- dim_evaluators
- dim_facilities
- dim_evaluation_types
- dim_medical_conditions
- dim_claims
- dim_appointments
- dim_dates (multiple role-playing: evaluation, scheduled, claim)

**Key Metrics**:
- **Duration Metrics**: evaluation_duration_minutes, variance_minutes
- **Attendance Metrics**: attended_flag, no_show_flag, cancelled_flag
- **Wait Time Metrics**: days_from_request_to_schedule, total_wait_days
- **Quality Metrics**: report_completeness_score, sufficient_exam_flag
- **Clinical Metrics**: recommended_rating_percentage, nexus_opinion
- **Financial Metrics**: evaluation_cost_amount, travel_reimbursement_amount

**Use Cases**:
- Evaluation completion analysis
- Evaluator performance tracking
- Quality assurance monitoring
- Wait time analysis
- Cost analysis

---

### 2. fct_claim_status_changes
**Type**: Accumulating Snapshot Fact Table
**Purpose**: Track claim status changes over time
**Grain**: One row per claim status change
**File**: `snowflake/facts/02_fact_claim_status.sql`

**Dimensions**:
- dim_veterans
- dim_claims
- dim_facilities
- dim_dates (multiple milestones: filed, received, review, exam, decision, etc.)

**Key Metrics**:
- **Processing Time Metrics**: days_to_complete, days_claim_to_initial_review
- **Milestone Metrics**: exam_completed, decision_made, notification_sent
- **Decision Metrics**: rating_percentage_granted, service_connected_granted
- **Quality Metrics**: remand_flag, sufficient_evidence_flag

**Use Cases**:
- Claim processing pipeline analysis
- Bottleneck identification
- Processing time trends
- Decision outcome analysis

---

### 3. fct_appointments_scheduled
**Type**: Transaction Fact Table
**Purpose**: Track appointment scheduling and attendance
**Grain**: One row per appointment
**File**: `snowflake/facts/03_fact_appointment.sql`

**Dimensions**:
- dim_veterans
- dim_evaluators
- dim_facilities
- dim_evaluation_types
- dim_appointments
- dim_claims
- dim_dates (multiple: requested, scheduled, appointment, completed, cancelled)

**Key Metrics**:
- **Wait Time Metrics**: days_from_request_to_schedule, total_wait_days
- **Duration Metrics**: actual_duration_minutes, duration_variance_minutes
- **Attendance Metrics**: attended_flag, no_show_flag, late_arrival_flag
- **Cancellation Metrics**: cancellation_hours_notice, reschedule_count
- **Quality Metrics**: satisfaction_score, technical_issues_flag
- **Access Metrics**: travel_distance_miles, telehealth_flag

**Use Cases**:
- Wait time compliance (VA goal: 20-28 days)
- No-show rate analysis
- Telehealth adoption tracking
- Appointment utilization analysis
- Patient satisfaction monitoring

---

### 4. fct_daily_facility_snapshot
**Type**: Periodic Snapshot Fact Table
**Purpose**: Daily snapshot of key performance indicators
**Grain**: One row per facility per date
**File**: `snowflake/facts/04_fact_daily_snapshot.sql`

**Dimensions**:
- dim_facilities
- dim_dates (snapshot date)

**Key Metrics**:
- **Volume Metrics**: evaluations_completed_count, claims_received_count
- **Efficiency Metrics**: evaluation_completion_rate, appointment_utilization_rate
- **Quality Metrics**: sufficient_exam_rate, average_report_completeness_score
- **Backlog Metrics**: evaluation_backlog_count, claims_pending_count
- **Wait Time Metrics**: average_wait_time_days, wait_time_compliance_rate
- **Financial Metrics**: total_evaluation_costs, average_cost_per_evaluation
- **Satisfaction Metrics**: average_satisfaction_score, net_promoter_score

**Use Cases**:
- Daily operational dashboards
- Trend analysis over time
- Facility performance comparison
- Capacity planning
- Executive KPI reporting

---

## Data Model Diagram

### Star Schema Structure

```
                    dim_dates
                       |
                       |
    dim_veterans ------ |------ dim_evaluators
           |           |              |
           |           |              |
           |     fct_evaluations_     |
           |        completed         |
           |           |              |
    dim_claims -------- |------ dim_facilities
                       |
                       |
              dim_evaluation_types
                       |
              dim_medical_conditions
```

### Fact Table Relationships

```
fct_evaluations_completed
    ├── dim_veterans (many-to-one)
    ├── dim_evaluators (many-to-one)
    ├── dim_facilities (many-to-one)
    ├── dim_evaluation_types (many-to-one)
    ├── dim_medical_conditions (many-to-one)
    ├── dim_claims (many-to-one)
    ├── dim_appointments (many-to-one)
    └── dim_dates (many-to-one, multiple roles)

fct_claim_status_changes
    ├── dim_veterans (many-to-one)
    ├── dim_claims (many-to-one)
    ├── dim_facilities (many-to-one)
    └── dim_dates (many-to-one, multiple milestones)

fct_appointments_scheduled
    ├── dim_veterans (many-to-one)
    ├── dim_evaluators (many-to-one)
    ├── dim_facilities (many-to-one)
    ├── dim_evaluation_types (many-to-one)
    ├── dim_appointments (many-to-one)
    ├── dim_claims (many-to-one)
    └── dim_dates (many-to-one, multiple roles)

fct_daily_facility_snapshot
    ├── dim_facilities (many-to-one)
    └── dim_dates (many-to-one)
```

---

## Common Queries

### 1. Evaluation Completion Rate by Facility

```sql
SELECT
    f.facility_name,
    f.state,
    d.year_month,
    COUNT(fe.evaluation_fact_sk) AS total_evaluations,
    SUM(CASE WHEN fe.evaluation_completed_flag = TRUE THEN 1 ELSE 0 END) AS completed_evaluations,
    ROUND(completed_evaluations / NULLIF(total_evaluations, 0) * 100, 2) AS completion_rate_pct
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluations_completed fe
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities f ON fe.facility_sk = f.facility_sk
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d ON fe.evaluation_date_sk = d.date_sk
WHERE d.year_number = 2024
  AND f.is_current = TRUE
GROUP BY f.facility_name, f.state, d.year_month
ORDER BY d.year_month, completion_rate_pct DESC;
```

### 2. Average Wait Times by Service Branch

```sql
SELECT
    v.service_branch,
    ROUND(AVG(fa.total_wait_days), 1) AS avg_wait_days,
    ROUND(AVG(fa.days_from_request_to_schedule), 1) AS avg_scheduling_days,
    COUNT(fa.appointment_fact_sk) AS total_appointments,
    SUM(CASE WHEN fa.meets_va_wait_time_goal = TRUE THEN 1 ELSE 0 END) AS within_goal,
    ROUND(within_goal / NULLIF(total_appointments, 0) * 100, 2) AS compliance_rate_pct
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointments_scheduled fa
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans v ON fa.veteran_sk = v.veteran_sk
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d ON fa.appointment_date_sk = d.date_sk
WHERE d.fiscal_year = 2024
  AND v.is_current = TRUE
  AND fa.attended_flag = TRUE
GROUP BY v.service_branch
ORDER BY avg_wait_days DESC;
```

### 3. Claim Processing Performance

```sql
SELECT
    fc.current_status,
    COUNT(DISTINCT fc.claim_sk) AS claim_count,
    ROUND(AVG(fc.total_days_pending), 1) AS avg_days_pending,
    ROUND(AVG(fc.days_claim_to_initial_review), 1) AS avg_days_to_review,
    ROUND(AVG(fc.days_exam_to_decision), 1) AS avg_days_exam_to_decision,
    SUM(fc.service_connected_granted) AS total_granted,
    SUM(fc.service_connected_denied) AS total_denied,
    ROUND(total_granted / NULLIF(total_granted + total_denied, 0) * 100, 2) AS grant_rate_pct
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_claim_status_changes fc
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d ON fc.rating_decision_date_sk = d.date_sk
WHERE d.fiscal_year = 2024
GROUP BY fc.current_status
ORDER BY claim_count DESC;
```

### 4. Evaluator Performance Metrics

```sql
SELECT
    e.full_name,
    e.specialty,
    f.facility_name,
    COUNT(fe.evaluation_fact_sk) AS total_evaluations,
    ROUND(AVG(fe.evaluation_duration_minutes), 1) AS avg_duration_minutes,
    ROUND(AVG(fe.report_completeness_score), 2) AS avg_completeness_score,
    SUM(CASE WHEN fe.sufficient_exam_flag = TRUE THEN 1 ELSE 0 END) AS sufficient_exams,
    ROUND(sufficient_exams / NULLIF(total_evaluations, 0) * 100, 2) AS sufficient_exam_rate_pct,
    ROUND(AVG(fe.report_timeliness_days), 1) AS avg_report_turnaround_days
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_evaluations_completed fe
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators e ON fe.evaluator_sk = e.evaluator_sk
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities f ON fe.facility_sk = f.facility_sk
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d ON fe.evaluation_date_sk = d.date_sk
WHERE d.fiscal_year = 2024
  AND e.is_current = TRUE
  AND e.active_flag = TRUE
GROUP BY e.full_name, e.specialty, f.facility_name
HAVING total_evaluations >= 10
ORDER BY avg_completeness_score DESC, total_evaluations DESC;
```

### 5. Telehealth Adoption and Performance

```sql
SELECT
    d.year_month,
    COUNT(fa.appointment_fact_sk) AS total_appointments,
    SUM(CASE WHEN fa.telehealth_flag = TRUE THEN 1 ELSE 0 END) AS telehealth_appointments,
    ROUND(telehealth_appointments / NULLIF(total_appointments, 0) * 100, 2) AS telehealth_rate_pct,
    AVG(CASE WHEN fa.telehealth_flag = TRUE THEN fa.satisfaction_score END) AS telehealth_satisfaction,
    AVG(CASE WHEN fa.telehealth_flag = FALSE THEN fa.satisfaction_score END) AS in_person_satisfaction,
    SUM(CASE WHEN fa.telehealth_flag = TRUE AND fa.technical_issues_flag = TRUE THEN 1 ELSE 0 END) AS technical_issues
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_appointments_scheduled fa
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d ON fa.appointment_date_sk = d.date_sk
WHERE d.year_number = 2024
  AND fa.attended_flag = TRUE
GROUP BY d.year_month
ORDER BY d.year_month;
```

### 6. Daily Performance Dashboard

```sql
SELECT
    d.full_date,
    f.facility_name,
    fds.evaluations_completed_count,
    fds.evaluation_completion_rate,
    fds.average_wait_time_days,
    fds.wait_time_compliance_rate,
    fds.claims_pending_count,
    fds.evaluation_backlog_count,
    fds.average_satisfaction_score,
    fds.total_evaluation_costs
FROM VETERAN_EVALUATION_DW.WAREHOUSE.fct_daily_facility_snapshot fds
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities f ON fds.facility_sk = f.facility_sk
JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d ON fds.snapshot_date_sk = d.date_sk
WHERE d.full_date >= CURRENT_DATE - 30
  AND f.is_current = TRUE
ORDER BY d.full_date DESC, f.facility_name;
```

---

## ETL Considerations

### Loading Sequence

1. **Dimension Tables** (load first)
   - dim_dates (populate using procedure)
   - dim_veterans (Type 2 SCD)
   - dim_evaluators (Type 2 SCD)
   - dim_facilities (Type 2 SCD)
   - dim_evaluation_types (Type 1)
   - dim_medical_conditions (Type 1)
   - dim_claims (Type 2 SCD)
   - dim_appointments (Type 1)

2. **Fact Tables** (load after dimensions)
   - fct_evaluations_completed
   - fct_claim_status_changes
   - fct_appointments_scheduled
   - fct_daily_facility_snapshot (calculated from other facts)

### Type 2 SCD Logic

For dimensions using Type 2 SCD (veterans, evaluators, facilities, claims):

```sql
-- Check for changes
-- If changed:
--   1. Update existing row: SET is_current = FALSE, effective_end_date = CURRENT_TIMESTAMP
--   2. Insert new row: SET is_current = TRUE, effective_start_date = CURRENT_TIMESTAMP
-- If no change:
--   3. No action (or update updated_timestamp)
```

### Data Quality Checks

Implement these checks in your ETL process:

1. **Referential Integrity**: Verify foreign keys exist in dimension tables
2. **Date Consistency**: Ensure date keys exist in dim_dates
3. **SCD Validation**: Verify only one is_current = TRUE per business key
4. **Metric Validation**: Check for NULL or negative values in key metrics
5. **Duplicate Prevention**: Check for duplicate degenerate dimensions (evaluation_id, claim_id, etc.)

### Performance Optimization

#### Snowflake-Specific Optimizations

Snowflake does not use traditional indexes. Instead, it uses several optimization techniques:

1. **Clustering Keys** (Already Implemented)

   All fact tables are pre-configured with clustering keys optimized for common query patterns:

   - `fct_evaluations_completed`: Clustered by `(evaluation_date_sk, facility_sk)`
   - `fct_claim_status_changes`: Clustered by `(claim_sk, rating_decision_date_sk)`
   - `fct_appointments_scheduled`: Clustered by `(appointment_date_sk, facility_sk)`
   - `fct_daily_facility_snapshot`: Clustered by `(snapshot_date_sk, facility_sk)`

   Clustering improves query performance by co-locating similar data in micro-partitions.

2. **Automatic Micro-Partitioning**

   Snowflake automatically partitions tables into micro-partitions (50-500 MB compressed).
   The clustering keys help organize these partitions for efficient pruning during queries.

3. **Primary and Foreign Keys** (Metadata Only)

   The primary and foreign key constraints in the DDL are for metadata purposes only.
   Snowflake does NOT enforce these constraints, but they help:
   - Query optimizers understand relationships
   - BI tools generate better queries
   - Documentation of data relationships

4. **Materialized Views** (Optional)

   Create materialized views for frequently accessed aggregations:
   ```sql
   CREATE MATERIALIZED VIEW mv_monthly_eval_summary AS
   SELECT
       facility_sk,
       DATE_TRUNC('month', d.full_date) AS month,
       COUNT(*) AS eval_count,
       AVG(evaluation_duration_minutes) AS avg_duration
   FROM fct_evaluations_completed fe
   JOIN dim_dates d ON fe.evaluation_date_sk = d.date_sk
   GROUP BY facility_sk, DATE_TRUNC('month', d.full_date);
   ```

5. **Search Optimization Service** (Optional)

   For dimension tables with high-cardinality string columns:
   ```sql
   ALTER TABLE dim_veterans ADD SEARCH OPTIMIZATION ON EQUALITY(veteran_id);
   ALTER TABLE dim_evaluators ADD SEARCH OPTIMIZATION ON EQUALITY(evaluator_id);
   ```

6. **Incremental Loads**

   Use Snowflake STREAM and TASK for continuous data integration:
   ```sql
   -- Create stream to track changes
   CREATE STREAM veteran_changes ON TABLE staging.veteran_source;

   -- Create task to process changes
   CREATE TASK load_veteran_dimension
       WAREHOUSE = ETL_WH
       SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('veteran_changes')
   AS
       -- Insert/Update logic here
   ```

7. **Result Caching**

   Snowflake automatically caches query results for 24 hours. Identical queries return
   cached results instantly at no cost.

8. **Column Pruning**

   Snowflake stores data in columnar format. Select only needed columns to minimize I/O:
   ```sql
   -- Good: Only select needed columns
   SELECT veteran_sk, evaluation_date_sk, evaluation_cost_amount
   FROM fct_evaluations_completed;

   -- Avoid: SELECT * reads all columns
   ```

---

## Deployment Instructions

### Prerequisites

- Snowflake account with appropriate privileges
- SYSADMIN or higher role
- Access to source systems (if loading data)

### Step 1: Deploy Database and Schemas

```bash
# Connect to Snowflake
snowsql -a <account> -u <username>

# Execute setup script
!source snowflake/schema/00_setup_database.sql
```

### Step 2: Deploy All Objects

Option A: Use master deployment script
```bash
!source snowflake/schema/02_master_deployment.sql
```

Option B: Deploy individually
```bash
# Dimensions
!source snowflake/dimensions/01_dim_date.sql
!source snowflake/dimensions/02_dim_veteran.sql
# ... (all dimension scripts)

# Facts
!source snowflake/facts/01_fact_evaluation.sql
# ... (all fact scripts)

# Populate date dimension
!source snowflake/schema/01_populate_date_dimension.sql
```

### Step 3: Verify Deployment

```sql
-- Check all objects created
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    COMMENT
FROM VETERAN_EVALUATION_DW.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('WAREHOUSE')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Verify date dimension populated
SELECT
    MIN(full_date) AS min_date,
    MAX(full_date) AS max_date,
    COUNT(*) AS total_rows
FROM VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates;
```

### Step 4: Grant Permissions

```sql
-- Grant read access to analysts
GRANT SELECT ON ALL TABLES IN SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE TO ROLE ANALYST_ROLE;

-- Grant ETL role full access
GRANT ALL ON SCHEMA VETERAN_EVALUATION_DW.STG TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE TO ROLE ETL_ROLE;
```

---

## Maintenance

### Regular Tasks

1. **Daily**: Load fact tables with previous day's data
2. **Weekly**: Review and resolve data quality issues
3. **Monthly**: Analyze query performance and optimize
4. **Quarterly**: Review dimension changes and SCD history
5. **Annually**: Extend dim_dates for future years

### Monitoring Queries

```sql
-- Check for orphaned fact records
SELECT COUNT(*)
FROM fct_evaluations_completed fe
LEFT JOIN dim_veterans v ON fe.veteran_sk = v.veteran_sk
WHERE v.veteran_sk IS NULL;

-- Check SCD integrity
SELECT veteran_id, COUNT(*)
FROM dim_veterans
WHERE is_current = TRUE
GROUP BY veteran_id
HAVING COUNT(*) > 1;

-- Check fact table growth
SELECT
    TABLE_NAME,
    ROW_COUNT,
    BYTES / (1024 * 1024 * 1024) AS size_gb
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'WAREHOUSE'
ORDER BY ROW_COUNT DESC;
```

---

## Support and Contact

For questions or issues with this dimensional model:
- Review this documentation
- Check Snowflake query history for errors
- Contact the data warehouse team

---

## Version History

- **v1.0** (2024-11-15): Initial dimensional model design
  - 8 dimension tables
  - 4 fact tables
  - Date dimension population procedure
  - Comprehensive documentation

---

*Last Updated: 2024-11-15*
