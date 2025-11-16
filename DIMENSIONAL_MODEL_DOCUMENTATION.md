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
├── DIM (Schema) - Dimension tables
├── FACT (Schema) - Fact tables
├── STG (Schema) - Staging area for ETL
└── UTIL (Schema) - Utility objects (procedures, functions, sequences)
```

### File Formats

- **CSV_FORMAT**: For CSV data loads
- **JSON_FORMAT**: For JSON data loads

---

## Dimension Tables

### 1. DIM_DATE
**Type**: Conformed Dimension (Type 1)
**Purpose**: Standard date dimension for time-based analysis
**Grain**: One row per day
**Business Key**: FULL_DATE

**Key Attributes**:
- Standard calendar attributes (year, quarter, month, week, day)
- VA fiscal year support (starts October 1)
- Business day indicators
- Federal holiday flags

**File**: `snowflake/dimensions/01_dim_date.sql`

**Usage**: Used across all fact tables for time-based filtering and aggregation

---

### 2. DIM_VETERAN
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: Veteran demographic and service information
**Grain**: One row per veteran per version
**Business Key**: VETERAN_ID
**Surrogate Key**: VETERAN_KEY

**Key Attributes**:
- Personal information (name, DOB, contact)
- Military service details (branch, era, rank)
- Disability rating and status
- VA enrollment and priority group
- SCD tracking (EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT)

**File**: `snowflake/dimensions/02_dim_veteran.sql`

**Why Type 2**: Track changes in disability ratings, contact information, and benefit status over time

---

### 3. DIM_EVALUATOR
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: Medical professionals and evaluators
**Grain**: One row per evaluator per version
**Business Key**: EVALUATOR_ID
**Surrogate Key**: EVALUATOR_KEY

**Key Attributes**:
- Professional credentials and specialties
- License and certification information
- Employment details
- Performance metrics
- Active/inactive status

**File**: `snowflake/dimensions/03_dim_evaluator.sql`

**Why Type 2**: Track changes in credentials, employment, and performance over time

---

### 4. DIM_FACILITY
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: VA medical centers and evaluation facilities
**Grain**: One row per facility per version
**Business Key**: FACILITY_ID
**Surrogate Key**: FACILITY_KEY

**Key Attributes**:
- Facility type and characteristics
- Location and VISN (Veterans Integrated Service Network)
- Services offered and capacity
- Accreditation status

**File**: `snowflake/dimensions/04_dim_facility.sql`

**Why Type 2**: Track changes in facility capabilities, accreditation, and status

---

### 5. DIM_EVALUATION_TYPE
**Type**: Type 1 Dimension
**Purpose**: Types of medical evaluations
**Grain**: One row per evaluation type
**Business Key**: EVALUATION_TYPE_ID
**Surrogate Key**: EVALUATION_TYPE_KEY

**Key Attributes**:
- Evaluation category (C&P Exam, IME, etc.)
- Duration and complexity
- Regulatory requirements
- Scheduling requirements

**File**: `snowflake/dimensions/05_dim_evaluation_type.sql`

---

### 6. DIM_MEDICAL_CONDITION
**Type**: Type 1 Dimension
**Purpose**: Medical conditions being evaluated
**Grain**: One row per medical condition
**Business Key**: MEDICAL_CONDITION_ID
**Surrogate Key**: MEDICAL_CONDITION_KEY

**Key Attributes**:
- Condition classification
- ICD-10 and VA diagnostic codes
- DBQ form references
- Rating information
- Service connection indicators

**File**: `snowflake/dimensions/06_dim_medical_condition.sql`

---

### 7. DIM_CLAIM
**Type**: Slowly Changing Dimension (Type 2)
**Purpose**: VA disability claims information
**Grain**: One row per claim per version
**Business Key**: CLAIM_ID
**Surrogate Key**: CLAIM_KEY

**Key Attributes**:
- Claim type and status
- Processing information
- Decision details
- Priority indicators
- Evidence and exam flags

**File**: `snowflake/dimensions/07_dim_claim.sql`

**Why Type 2**: Track claim status changes and processing history

---

### 8. DIM_APPOINTMENT
**Type**: Type 1 Dimension
**Purpose**: Appointment scheduling details
**Grain**: One row per appointment
**Business Key**: APPOINTMENT_ID
**Surrogate Key**: APPOINTMENT_KEY

**Key Attributes**:
- Appointment type and status
- Scheduling details
- Reminder and confirmation information
- Wait time categorization

**File**: `snowflake/dimensions/08_dim_appointment.sql`

---

## Fact Tables

### 1. FACT_EVALUATION
**Type**: Transaction Fact Table
**Purpose**: Core fact table for medical evaluations
**Grain**: One row per evaluation per medical condition
**File**: `snowflake/facts/01_fact_evaluation.sql`

**Dimensions**:
- DIM_VETERAN
- DIM_EVALUATOR
- DIM_FACILITY
- DIM_EVALUATION_TYPE
- DIM_MEDICAL_CONDITION
- DIM_CLAIM
- DIM_APPOINTMENT
- DIM_DATE (multiple role-playing: evaluation, scheduled, claim)

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

### 2. FACT_CLAIM_STATUS
**Type**: Accumulating Snapshot Fact Table
**Purpose**: Track claim status changes over time
**Grain**: One row per claim status change
**File**: `snowflake/facts/02_fact_claim_status.sql`

**Dimensions**:
- DIM_VETERAN
- DIM_CLAIM
- DIM_FACILITY
- DIM_DATE (multiple milestones: filed, received, review, exam, decision, etc.)

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

### 3. FACT_APPOINTMENT
**Type**: Transaction Fact Table
**Purpose**: Track appointment scheduling and attendance
**Grain**: One row per appointment
**File**: `snowflake/facts/03_fact_appointment.sql`

**Dimensions**:
- DIM_VETERAN
- DIM_EVALUATOR
- DIM_FACILITY
- DIM_EVALUATION_TYPE
- DIM_APPOINTMENT
- DIM_CLAIM
- DIM_DATE (multiple: requested, scheduled, appointment, completed, cancelled)

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

### 4. FACT_DAILY_SNAPSHOT
**Type**: Periodic Snapshot Fact Table
**Purpose**: Daily snapshot of key performance indicators
**Grain**: One row per facility per date
**File**: `snowflake/facts/04_fact_daily_snapshot.sql`

**Dimensions**:
- DIM_FACILITY
- DIM_DATE (snapshot date)

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
                    DIM_DATE
                       |
                       |
    DIM_VETERAN ------ |------ DIM_EVALUATOR
           |           |              |
           |           |              |
           |     FACT_EVALUATION      |
           |           |              |
           |           |              |
    DIM_CLAIM -------- |------ DIM_FACILITY
                       |
                       |
              DIM_EVALUATION_TYPE
                       |
              DIM_MEDICAL_CONDITION
```

### Fact Table Relationships

```
FACT_EVALUATION
    ├── DIM_VETERAN (many-to-one)
    ├── DIM_EVALUATOR (many-to-one)
    ├── DIM_FACILITY (many-to-one)
    ├── DIM_EVALUATION_TYPE (many-to-one)
    ├── DIM_MEDICAL_CONDITION (many-to-one)
    ├── DIM_CLAIM (many-to-one)
    ├── DIM_APPOINTMENT (many-to-one)
    └── DIM_DATE (many-to-one, multiple roles)

FACT_CLAIM_STATUS
    ├── DIM_VETERAN (many-to-one)
    ├── DIM_CLAIM (many-to-one)
    ├── DIM_FACILITY (many-to-one)
    └── DIM_DATE (many-to-one, multiple milestones)

FACT_APPOINTMENT
    ├── DIM_VETERAN (many-to-one)
    ├── DIM_EVALUATOR (many-to-one)
    ├── DIM_FACILITY (many-to-one)
    ├── DIM_EVALUATION_TYPE (many-to-one)
    ├── DIM_APPOINTMENT (many-to-one)
    ├── DIM_CLAIM (many-to-one)
    └── DIM_DATE (many-to-one, multiple roles)

FACT_DAILY_SNAPSHOT
    ├── DIM_FACILITY (many-to-one)
    └── DIM_DATE (many-to-one)
```

---

## Common Queries

### 1. Evaluation Completion Rate by Facility

```sql
SELECT
    f.FACILITY_NAME,
    f.STATE,
    d.YEAR_MONTH,
    COUNT(fe.EVALUATION_FACT_KEY) AS total_evaluations,
    SUM(CASE WHEN fe.EVALUATION_COMPLETED_FLAG = TRUE THEN 1 ELSE 0 END) AS completed_evaluations,
    ROUND(completed_evaluations / NULLIF(total_evaluations, 0) * 100, 2) AS completion_rate_pct
FROM VETERAN_EVALUATION_DW.FACT.FACT_EVALUATION fe
JOIN VETERAN_EVALUATION_DW.DIM.DIM_FACILITY f ON fe.FACILITY_KEY = f.FACILITY_KEY
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d ON fe.EVALUATION_DATE_KEY = d.DATE_KEY
WHERE d.YEAR_NUMBER = 2024
  AND f.IS_CURRENT = TRUE
GROUP BY f.FACILITY_NAME, f.STATE, d.YEAR_MONTH
ORDER BY d.YEAR_MONTH, completion_rate_pct DESC;
```

### 2. Average Wait Times by Service Branch

```sql
SELECT
    v.SERVICE_BRANCH,
    ROUND(AVG(fa.TOTAL_WAIT_DAYS), 1) AS avg_wait_days,
    ROUND(AVG(fa.DAYS_FROM_REQUEST_TO_SCHEDULE), 1) AS avg_scheduling_days,
    COUNT(fa.APPOINTMENT_FACT_KEY) AS total_appointments,
    SUM(CASE WHEN fa.MEETS_VA_WAIT_TIME_GOAL = TRUE THEN 1 ELSE 0 END) AS within_goal,
    ROUND(within_goal / NULLIF(total_appointments, 0) * 100, 2) AS compliance_rate_pct
FROM VETERAN_EVALUATION_DW.FACT.FACT_APPOINTMENT fa
JOIN VETERAN_EVALUATION_DW.DIM.DIM_VETERAN v ON fa.VETERAN_KEY = v.VETERAN_KEY
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d ON fa.APPOINTMENT_DATE_KEY = d.DATE_KEY
WHERE d.FISCAL_YEAR = 2024
  AND v.IS_CURRENT = TRUE
  AND fa.ATTENDED_FLAG = TRUE
GROUP BY v.SERVICE_BRANCH
ORDER BY avg_wait_days DESC;
```

### 3. Claim Processing Performance

```sql
SELECT
    fc.CURRENT_STATUS,
    COUNT(DISTINCT fc.CLAIM_KEY) AS claim_count,
    ROUND(AVG(fc.TOTAL_DAYS_PENDING), 1) AS avg_days_pending,
    ROUND(AVG(fc.DAYS_CLAIM_TO_INITIAL_REVIEW), 1) AS avg_days_to_review,
    ROUND(AVG(fc.DAYS_EXAM_TO_DECISION), 1) AS avg_days_exam_to_decision,
    SUM(fc.SERVICE_CONNECTED_GRANTED) AS total_granted,
    SUM(fc.SERVICE_CONNECTED_DENIED) AS total_denied,
    ROUND(total_granted / NULLIF(total_granted + total_denied, 0) * 100, 2) AS grant_rate_pct
FROM VETERAN_EVALUATION_DW.FACT.FACT_CLAIM_STATUS fc
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d ON fc.RATING_DECISION_DATE_KEY = d.DATE_KEY
WHERE d.FISCAL_YEAR = 2024
GROUP BY fc.CURRENT_STATUS
ORDER BY claim_count DESC;
```

### 4. Evaluator Performance Metrics

```sql
SELECT
    e.FULL_NAME,
    e.SPECIALTY,
    f.FACILITY_NAME,
    COUNT(fe.EVALUATION_FACT_KEY) AS total_evaluations,
    ROUND(AVG(fe.EVALUATION_DURATION_MINUTES), 1) AS avg_duration_minutes,
    ROUND(AVG(fe.REPORT_COMPLETENESS_SCORE), 2) AS avg_completeness_score,
    SUM(CASE WHEN fe.SUFFICIENT_EXAM_FLAG = TRUE THEN 1 ELSE 0 END) AS sufficient_exams,
    ROUND(sufficient_exams / NULLIF(total_evaluations, 0) * 100, 2) AS sufficient_exam_rate_pct,
    ROUND(AVG(fe.REPORT_TIMELINESS_DAYS), 1) AS avg_report_turnaround_days
FROM VETERAN_EVALUATION_DW.FACT.FACT_EVALUATION fe
JOIN VETERAN_EVALUATION_DW.DIM.DIM_EVALUATOR e ON fe.EVALUATOR_KEY = e.EVALUATOR_KEY
JOIN VETERAN_EVALUATION_DW.DIM.DIM_FACILITY f ON fe.FACILITY_KEY = f.FACILITY_KEY
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d ON fe.EVALUATION_DATE_KEY = d.DATE_KEY
WHERE d.FISCAL_YEAR = 2024
  AND e.IS_CURRENT = TRUE
  AND e.ACTIVE_FLAG = TRUE
GROUP BY e.FULL_NAME, e.SPECIALTY, f.FACILITY_NAME
HAVING total_evaluations >= 10
ORDER BY avg_completeness_score DESC, total_evaluations DESC;
```

### 5. Telehealth Adoption and Performance

```sql
SELECT
    d.YEAR_MONTH,
    COUNT(fa.APPOINTMENT_FACT_KEY) AS total_appointments,
    SUM(CASE WHEN fa.TELEHEALTH_FLAG = TRUE THEN 1 ELSE 0 END) AS telehealth_appointments,
    ROUND(telehealth_appointments / NULLIF(total_appointments, 0) * 100, 2) AS telehealth_rate_pct,
    AVG(CASE WHEN fa.TELEHEALTH_FLAG = TRUE THEN fa.SATISFACTION_SCORE END) AS telehealth_satisfaction,
    AVG(CASE WHEN fa.TELEHEALTH_FLAG = FALSE THEN fa.SATISFACTION_SCORE END) AS in_person_satisfaction,
    SUM(CASE WHEN fa.TELEHEALTH_FLAG = TRUE AND fa.TECHNICAL_ISSUES_FLAG = TRUE THEN 1 ELSE 0 END) AS technical_issues
FROM VETERAN_EVALUATION_DW.FACT.FACT_APPOINTMENT fa
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d ON fa.APPOINTMENT_DATE_KEY = d.DATE_KEY
WHERE d.YEAR_NUMBER = 2024
  AND fa.ATTENDED_FLAG = TRUE
GROUP BY d.YEAR_MONTH
ORDER BY d.YEAR_MONTH;
```

### 6. Daily Performance Dashboard

```sql
SELECT
    d.FULL_DATE,
    f.FACILITY_NAME,
    fds.EVALUATIONS_COMPLETED_COUNT,
    fds.EVALUATION_COMPLETION_RATE,
    fds.AVERAGE_WAIT_TIME_DAYS,
    fds.WAIT_TIME_COMPLIANCE_RATE,
    fds.CLAIMS_PENDING_COUNT,
    fds.EVALUATION_BACKLOG_COUNT,
    fds.AVERAGE_SATISFACTION_SCORE,
    fds.TOTAL_EVALUATION_COSTS
FROM VETERAN_EVALUATION_DW.FACT.FACT_DAILY_SNAPSHOT fds
JOIN VETERAN_EVALUATION_DW.DIM.DIM_FACILITY f ON fds.FACILITY_KEY = f.FACILITY_KEY
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d ON fds.SNAPSHOT_DATE_KEY = d.DATE_KEY
WHERE d.FULL_DATE >= CURRENT_DATE - 30
  AND f.IS_CURRENT = TRUE
ORDER BY d.FULL_DATE DESC, f.FACILITY_NAME;
```

---

## ETL Considerations

### Loading Sequence

1. **Dimension Tables** (load first)
   - DIM_DATE (populate using procedure)
   - DIM_VETERAN (Type 2 SCD)
   - DIM_EVALUATOR (Type 2 SCD)
   - DIM_FACILITY (Type 2 SCD)
   - DIM_EVALUATION_TYPE (Type 1)
   - DIM_MEDICAL_CONDITION (Type 1)
   - DIM_CLAIM (Type 2 SCD)
   - DIM_APPOINTMENT (Type 1)

2. **Fact Tables** (load after dimensions)
   - FACT_EVALUATION
   - FACT_CLAIM_STATUS
   - FACT_APPOINTMENT
   - FACT_DAILY_SNAPSHOT (calculated from other facts)

### Type 2 SCD Logic

For dimensions using Type 2 SCD (VETERAN, EVALUATOR, FACILITY, CLAIM):

```sql
-- Check for changes
-- If changed:
--   1. Update existing row: SET IS_CURRENT = FALSE, EFFECTIVE_END_DATE = CURRENT_TIMESTAMP
--   2. Insert new row: SET IS_CURRENT = TRUE, EFFECTIVE_START_DATE = CURRENT_TIMESTAMP
-- If no change:
--   3. No action (or update UPDATED_TIMESTAMP)
```

### Data Quality Checks

Implement these checks in your ETL process:

1. **Referential Integrity**: Verify foreign keys exist in dimension tables
2. **Date Consistency**: Ensure date keys exist in DIM_DATE
3. **SCD Validation**: Verify only one IS_CURRENT = TRUE per business key
4. **Metric Validation**: Check for NULL or negative values in key metrics
5. **Duplicate Prevention**: Check for duplicate degenerate dimensions (EVALUATION_ID, CLAIM_ID, etc.)

### Performance Optimization

#### Snowflake-Specific Optimizations

Snowflake does not use traditional indexes. Instead, it uses several optimization techniques:

1. **Clustering Keys** (Already Implemented)

   All fact tables are pre-configured with clustering keys optimized for common query patterns:

   - `FACT_EVALUATION`: Clustered by `(EVALUATION_DATE_KEY, FACILITY_KEY)`
   - `FACT_CLAIM_STATUS`: Clustered by `(CLAIM_KEY, RATING_DECISION_DATE_KEY)`
   - `FACT_APPOINTMENT`: Clustered by `(APPOINTMENT_DATE_KEY, FACILITY_KEY)`
   - `FACT_DAILY_SNAPSHOT`: Clustered by `(SNAPSHOT_DATE_KEY, FACILITY_KEY)`

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
   CREATE MATERIALIZED VIEW MV_MONTHLY_EVAL_SUMMARY AS
   SELECT
       facility_key,
       DATE_TRUNC('month', d.full_date) AS month,
       COUNT(*) AS eval_count,
       AVG(evaluation_duration_minutes) AS avg_duration
   FROM FACT_EVALUATION fe
   JOIN DIM_DATE d ON fe.evaluation_date_key = d.date_key
   GROUP BY facility_key, DATE_TRUNC('month', d.full_date);
   ```

5. **Search Optimization Service** (Optional)

   For dimension tables with high-cardinality string columns:
   ```sql
   ALTER TABLE DIM_VETERAN ADD SEARCH OPTIMIZATION ON EQUALITY(VETERAN_ID);
   ALTER TABLE DIM_EVALUATOR ADD SEARCH OPTIMIZATION ON EQUALITY(EVALUATOR_ID);
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
   SELECT veteran_key, evaluation_date_key, evaluation_cost_amount
   FROM FACT_EVALUATION;

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
WHERE TABLE_SCHEMA IN ('DIM', 'FACT')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Verify date dimension populated
SELECT
    MIN(FULL_DATE) AS min_date,
    MAX(FULL_DATE) AS max_date,
    COUNT(*) AS total_rows
FROM VETERAN_EVALUATION_DW.DIM.DIM_DATE;
```

### Step 4: Grant Permissions

```sql
-- Grant read access to analysts
GRANT SELECT ON ALL TABLES IN SCHEMA VETERAN_EVALUATION_DW.DIM TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA VETERAN_EVALUATION_DW.FACT TO ROLE ANALYST_ROLE;

-- Grant ETL role full access
GRANT ALL ON SCHEMA VETERAN_EVALUATION_DW.STG TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA VETERAN_EVALUATION_DW.DIM TO ROLE ETL_ROLE;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA VETERAN_EVALUATION_DW.FACT TO ROLE ETL_ROLE;
```

---

## Maintenance

### Regular Tasks

1. **Daily**: Load fact tables with previous day's data
2. **Weekly**: Review and resolve data quality issues
3. **Monthly**: Analyze query performance and optimize
4. **Quarterly**: Review dimension changes and SCD history
5. **Annually**: Extend DIM_DATE for future years

### Monitoring Queries

```sql
-- Check for orphaned fact records
SELECT COUNT(*)
FROM FACT_EVALUATION fe
LEFT JOIN DIM_VETERAN v ON fe.VETERAN_KEY = v.VETERAN_KEY
WHERE v.VETERAN_KEY IS NULL;

-- Check SCD integrity
SELECT VETERAN_ID, COUNT(*)
FROM DIM_VETERAN
WHERE IS_CURRENT = TRUE
GROUP BY VETERAN_ID
HAVING COUNT(*) > 1;

-- Check fact table growth
SELECT
    TABLE_NAME,
    ROW_COUNT,
    BYTES / (1024 * 1024 * 1024) AS size_gb
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FACT'
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
