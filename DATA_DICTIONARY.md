# VES Data Warehouse - Complete Data Dictionary

**Comprehensive catalog of all tables, columns, and data elements**

Last Updated: 2024-11-18
Database: VESDW_PRD (Production), VESODS_PRDDATA_PRD (Operational Data Store)

---

## Table of Contents

- [How to Use This Dictionary](#how-to-use-this-dictionary)
- [Database Overview](#database-overview)
- [Dimension Tables (9 tables)](#dimension-tables)
- [Fact Tables (9 tables)](#fact-tables)
- [Staging Tables](#staging-tables)
- [Monitoring & Quality Tables](#monitoring--quality-tables)
- [Marts & Analytics Views](#marts--analytics-views)
- [Data Types Reference](#data-types-reference)
- [Naming Conventions](#naming-conventions)

---

## How to Use This Dictionary

### Quick Find

**Find a specific table:**
- Use browser search (Ctrl+F / Cmd+F)
- Search for table name (e.g., "dim_veteran")

**Find a specific column:**
- Search for column name (e.g., "veteran_ssn")
- Columns with same name across tables will appear together

**Find by business concept:**
- Search for business term (e.g., "disability rating", "appointment")

### Symbols Used

- 🔑 **PK** = Primary Key (surrogate key)
- 🔗 **FK** = Foreign Key (relationship to another table)
- ⏰ **SCD** = Slowly Changing Dimension tracking field
- ⚠️ **Required** = NOT NULL field
- 📊 **Measure** = Numeric field used in calculations
- 🏷️ **Attribute** = Descriptive field

---

## Database Overview

### VESDW_PRD (Data Warehouse)

**Production data warehouse containing dimensional model and analytics**

| Schema | Purpose | Table Count | Description |
|--------|---------|-------------|-------------|
| `warehouse` | Dimensional Model | 18 | Dimension and fact tables (star schema) |
| `staging` | Staging Layer | 10+ | Multi-source integration and transformation |
| `marts` | Analytics Marts | 10+ | Business-specific views and aggregations |
| `metadata` | Monitoring & Quality | 15+ | Pipeline health, data quality, lineage |

### VESODS_PRDDATA_PRD (Operational Data Store)

**Source system replica for extraction and CDC**

| Schema | Purpose | Source System |
|--------|---------|---------------|
| `VEMS_CORE` | Core Evaluations | VEMS Core (SQL Server) |
| `VEMS_PNM` | Provider Network | VEMS PNM (SQL Server) |
| `OMS` | Order Management | OMS (Redshift) |

---

## Dimension Tables

### dim_date

**Date dimension providing calendar, fiscal, and business day context**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per day
**Time Span:** 10 years (2020-2030)
**Row Count:** ~3,650 rows
**Load Frequency:** Pre-populated, static
**SCD Type:** Not applicable (Type 0 - fixed)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| date_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key (YYYYMMDD format) | Example: 20240115 for Jan 15, 2024 |
| full_date | DATE | NOT NULL | ⚠️ Natural date value | Actual date (2024-01-15) |
| day_of_week | NUMBER(1,0) | NOT NULL | Day of week (1=Sunday, 7=Saturday) | ISO standard |
| day_name | VARCHAR(10) | NOT NULL | Full day name | Monday, Tuesday, etc. |
| day_of_month | NUMBER(2,0) | NOT NULL | Day number (1-31) | Calendar day |
| day_of_year | NUMBER(3,0) | NOT NULL | Day of year (1-366) | Julian day |
| week_of_year | NUMBER(2,0) | NOT NULL | ISO week number (1-53) | ISO 8601 standard |
| month_number | NUMBER(2,0) | NOT NULL | Month number (1-12) | Calendar month |
| month_name | VARCHAR(10) | NOT NULL | Full month name | January, February, etc. |
| calendar_quarter | NUMBER(1,0) | NOT NULL | Calendar quarter (1-4) | Q1 = Jan-Mar, etc. |
| calendar_year | NUMBER(4,0) | NOT NULL | Calendar year | 2024, 2025, etc. |
| fiscal_year | NUMBER(4,0) | NOT NULL | VA fiscal year | Starts Oct 1 |
| fiscal_quarter | NUMBER(1,0) | NOT NULL | VA fiscal quarter (1-4) | FY Q1 = Oct-Dec |
| fiscal_month | NUMBER(2,0) | NOT NULL | VA fiscal month (1-12) | FY Month 1 = October |
| is_weekend | BOOLEAN | NOT NULL | Weekend indicator | TRUE for Sat/Sun |
| is_holiday | BOOLEAN | NOT NULL | Federal holiday indicator | TRUE for federal holidays |
| holiday_name | VARCHAR(50) | NULL | Holiday name if applicable | "New Year's Day", etc. |
| is_business_day | BOOLEAN | NOT NULL | Business day indicator | FALSE for weekends & holidays |
| first_day_of_month | BOOLEAN | NOT NULL | First day of month flag | TRUE on 1st of month |
| last_day_of_month | BOOLEAN | NOT NULL | Last day of month flag | TRUE on last day |

**Common Queries:**
```sql
-- Get current fiscal year
SELECT * FROM dim_date WHERE full_date = CURRENT_DATE();

-- Get all business days in a month
SELECT * FROM dim_date
WHERE calendar_year = 2024 AND month_number = 1 AND is_business_day = TRUE;
```

---

### dim_veteran

**Veteran demographics, service history, and eligibility - SCD Type 2**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per veteran per change (SCD Type 2)
**Row Count:** Varies by history
**Load Frequency:** Daily incremental via Streams
**SCD Type:** Type 2 (historical tracking)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| veteran_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| veteran_natural_key | VARCHAR(50) | NOT NULL | Business key (Veteran ID) | Source system ID |
| veteran_ssn | VARCHAR(11) | NULL | Social Security Number | Encrypted/masked in prod |
| first_name | VARCHAR(50) | NOT NULL | ⚠️ First name | |
| middle_name | VARCHAR(50) | NULL | Middle name or initial | |
| last_name | VARCHAR(100) | NOT NULL | ⚠️ Last name | |
| suffix | VARCHAR(10) | NULL | Name suffix | Jr., Sr., III, etc. |
| date_of_birth | DATE | NOT NULL | ⚠️ Date of birth | |
| age_at_load | NUMBER(3,0) | NULL | 📊 Age at time of load | Calculated field |
| gender | VARCHAR(20) | NULL | Gender | M, F, Other, Unknown |
| ethnicity | VARCHAR(50) | NULL | Ethnicity | Hispanic/Latino, Not Hispanic/Latino |
| race | VARCHAR(50) | NULL | Race | White, Black, Asian, etc. |
| marital_status | VARCHAR(20) | NULL | Marital status | Single, Married, Divorced, etc. |
| branch_of_service | VARCHAR(50) | NULL | Primary military branch | Army, Navy, Air Force, Marines, Coast Guard |
| service_start_date | DATE | NULL | Active duty start date | |
| service_end_date | DATE | NULL | Active duty end date | NULL if currently serving |
| discharge_status | VARCHAR(50) | NULL | Discharge characterization | Honorable, General, Other, Dishonorable |
| combat_veteran_flag | BOOLEAN | NULL | Combat veteran indicator | TRUE if served in combat zone |
| service_connected_disability_flag | BOOLEAN | NULL | Service-connected disability | TRUE if has service-connected condition |
| disability_rating_percentage | NUMBER(3,0) | NULL | 📊 Current disability rating | 0-100 percent |
| priority_group | NUMBER(1,0) | NULL | VA priority group | 1-8 (1 = highest priority) |
| enrollment_date | DATE | NULL | VA enrollment date | Date enrolled in VA healthcare |
| address_line1 | VARCHAR(200) | NULL | Street address | |
| address_line2 | VARCHAR(200) | NULL | Apt/Suite number | |
| city | VARCHAR(100) | NULL | City | |
| state | VARCHAR(2) | NULL | State code | Two-letter abbreviation |
| zip_code | VARCHAR(10) | NULL | ZIP code | 5 or 9 digit ZIP |
| county | VARCHAR(100) | NULL | County name | |
| country | VARCHAR(50) | NOT NULL | Country | Defaults to USA |
| phone_primary | VARCHAR(20) | NULL | Primary phone | |
| phone_secondary | VARCHAR(20) | NULL | Secondary phone | |
| email | VARCHAR(200) | NULL | Email address | |
| preferred_contact_method | VARCHAR(20) | NULL | Contact preference | Phone, Email, Mail |
| active_duty_status | VARCHAR(20) | NULL | Current duty status | Active, Reserve, Retired, Separated |
| homeless_flag | BOOLEAN | NULL | Homeless status | TRUE if currently homeless |
| rural_flag | BOOLEAN | NULL | Rural residence indicator | Based on zip code |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE, VEMS_PNM |
| source_veteran_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| is_current | BOOLEAN | NOT NULL | ⏰ **SCD** Current record flag | TRUE for current version |
| valid_from | TIMESTAMP_NTZ | NOT NULL | ⏰ **SCD** Effective start date | When this version became effective |
| valid_to | TIMESTAMP_NTZ | NULL | ⏰ **SCD** Effective end date | NULL for current version |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

**Common Queries:**
```sql
-- Get current record for a veteran
SELECT * FROM dim_veteran
WHERE veteran_natural_key = 'V123456' AND is_current = TRUE;

-- Get veteran history
SELECT * FROM dim_veteran
WHERE veteran_natural_key = 'V123456'
ORDER BY valid_from DESC;

-- Get veterans by priority group
SELECT * FROM dim_veteran
WHERE priority_group = 1 AND is_current = TRUE;
```

---

### dim_evaluator

**Medical professionals conducting evaluations - SCD Type 2**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per evaluator per change (SCD Type 2)
**Row Count:** Varies by history
**Load Frequency:** Daily incremental via Streams
**SCD Type:** Type 2 (historical tracking)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| evaluator_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| evaluator_natural_key | VARCHAR(50) | NOT NULL | Business key (Evaluator ID) | Source system ID |
| evaluator_npi | VARCHAR(10) | NULL | National Provider Identifier | Unique provider ID |
| first_name | VARCHAR(50) | NOT NULL | ⚠️ First name | |
| middle_name | VARCHAR(50) | NULL | Middle name or initial | |
| last_name | VARCHAR(100) | NOT NULL | ⚠️ Last name | |
| suffix | VARCHAR(10) | NULL | Name suffix | MD, DO, PhD, etc. |
| credentials | VARCHAR(100) | NULL | Professional credentials | MD, DO, PhD, PA, NP, etc. |
| specialty | VARCHAR(100) | NOT NULL | ⚠️ Primary specialty | Orthopedics, Psychiatry, etc. |
| subspecialty | VARCHAR(100) | NULL | Subspecialty | Hand Surgery, PTSD, etc. |
| license_number | VARCHAR(50) | NULL | Medical license number | State license |
| license_state | VARCHAR(2) | NULL | Licensing state | Two-letter state code |
| license_expiration_date | DATE | NULL | License expiration | |
| board_certified_flag | BOOLEAN | NULL | Board certification status | TRUE if board certified |
| certification_body | VARCHAR(100) | NULL | Certifying organization | ABMS, AOA, etc. |
| years_of_experience | NUMBER(2,0) | NULL | 📊 Years practicing | Calculated from start date |
| employment_type | VARCHAR(50) | NULL | Employment status | Full-time, Part-time, Contract, Per Diem |
| vendor_name | VARCHAR(200) | NULL | Vendor/Contractor name | If contract evaluator |
| hire_date | DATE | NULL | Hire/contract start date | |
| termination_date | DATE | NULL | Termination/contract end | NULL if currently active |
| active_flag | BOOLEAN | NOT NULL | ⚠️ Currently active | TRUE if currently evaluating |
| languages_spoken | VARCHAR(500) | NULL | Languages (comma-separated) | English, Spanish, etc. |
| telehealth_capable_flag | BOOLEAN | NULL | Telehealth capability | TRUE if can do telehealth |
| compensation_method | VARCHAR(50) | NULL | Payment method | Hourly, Salary, Per-Exam |
| hourly_rate | NUMBER(10,2) | NULL | 📊 Hourly rate | If hourly compensation |
| per_exam_rate | NUMBER(10,2) | NULL | 📊 Per-exam rate | If per-exam compensation |
| total_exams_completed | NUMBER(10,0) | NULL | 📊 Lifetime exam count | Running total |
| average_exam_quality_score | NUMBER(5,2) | NULL | 📊 Avg quality score | 0-100 scale |
| average_exam_duration_minutes | NUMBER(5,0) | NULL | 📊 Avg exam duration | Minutes |
| qa_failure_rate | NUMBER(5,2) | NULL | 📊 QA failure percentage | 0-100% |
| complaints_count | NUMBER(5,0) | NULL | 📊 Total complaints | Running total |
| commendations_count | NUMBER(5,0) | NULL | 📊 Total commendations | Running total |
| preferred_exam_types | VARCHAR(500) | NULL | Preferred exam types | Comma-separated list |
| max_daily_capacity | NUMBER(2,0) | NULL | 📊 Max exams per day | Scheduling constraint |
| address_line1 | VARCHAR(200) | NULL | Office address | |
| city | VARCHAR(100) | NULL | City | |
| state | VARCHAR(2) | NULL | State | |
| zip_code | VARCHAR(10) | NULL | ZIP code | |
| phone | VARCHAR(20) | NULL | Office phone | |
| email | VARCHAR(200) | NULL | Email | |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_PNM |
| source_evaluator_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| is_current | BOOLEAN | NOT NULL | ⏰ **SCD** Current record flag | TRUE for current version |
| valid_from | TIMESTAMP_NTZ | NOT NULL | ⏰ **SCD** Effective start date | When this version became effective |
| valid_to | TIMESTAMP_NTZ | NULL | ⏰ **SCD** Effective end date | NULL for current version |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

**Common Queries:**
```sql
-- Get active evaluators by specialty
SELECT * FROM dim_evaluator
WHERE specialty = 'Orthopedics' AND is_current = TRUE AND active_flag = TRUE;

-- Get evaluator performance metrics
SELECT evaluator_natural_key, first_name, last_name, specialty,
       total_exams_completed, average_exam_quality_score, qa_failure_rate
FROM dim_evaluator WHERE is_current = TRUE
ORDER BY average_exam_quality_score DESC;
```

---

### dim_facility

**VA facilities and medical centers - SCD Type 2**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per facility per change (SCD Type 2)
**Row Count:** Varies by history
**Load Frequency:** Weekly incremental via Streams
**SCD Type:** Type 2 (historical tracking)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| facility_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| facility_natural_key | VARCHAR(50) | NOT NULL | Business key (Facility ID) | Station number or ID |
| station_number | VARCHAR(10) | NULL | VA station number | 3-digit code (e.g., 528) |
| facility_name | VARCHAR(200) | NOT NULL | ⚠️ Official facility name | |
| facility_type | VARCHAR(50) | NOT NULL | ⚠️ Facility type | VAMC, CBOC, Vet Center, etc. |
| parent_station | VARCHAR(10) | NULL | Parent station number | If CBOC, references VAMC |
| visn | VARCHAR(10) | NOT NULL | ⚠️ VISN (Veterans Integrated Service Network) | 1-23 |
| complexity_level | VARCHAR(20) | NULL | Facility complexity | 1a, 1b, 1c, 2, 3 |
| address_line1 | VARCHAR(200) | NOT NULL | ⚠️ Street address | |
| address_line2 | VARCHAR(200) | NULL | Building/Suite | |
| city | VARCHAR(100) | NOT NULL | ⚠️ City | |
| state | VARCHAR(2) | NOT NULL | ⚠️ State code | Two-letter abbreviation |
| zip_code | VARCHAR(10) | NOT NULL | ⚠️ ZIP code | |
| county | VARCHAR(100) | NULL | County | |
| latitude | NUMBER(10,6) | NULL | 📊 Latitude | Geolocation |
| longitude | NUMBER(10,6) | NULL | 📊 Longitude | Geolocation |
| phone_main | VARCHAR(20) | NULL | Main phone number | |
| phone_appointment | VARCHAR(20) | NULL | Appointment line | |
| website_url | VARCHAR(500) | NULL | Facility website | |
| total_beds | NUMBER(5,0) | NULL | 📊 Total bed capacity | If inpatient facility |
| total_exam_rooms | NUMBER(3,0) | NULL | 📊 Total exam rooms | |
| parking_spaces | NUMBER(5,0) | NULL | 📊 Parking capacity | |
| hours_of_operation | VARCHAR(200) | NULL | Operating hours | Mon-Fri 8am-5pm, etc. |
| emergency_department_flag | BOOLEAN | NULL | Has emergency dept | TRUE if 24/7 ER |
| telehealth_capable_flag | BOOLEAN | NULL | Telehealth capability | TRUE if equipped |
| wheelchair_accessible_flag | BOOLEAN | NULL | ADA accessible | TRUE if compliant |
| public_transportation_flag | BOOLEAN | NULL | Public transit access | TRUE if accessible |
| patient_satisfaction_score | NUMBER(5,2) | NULL | 📊 Patient satisfaction | 0-100 scale |
| operating_status | VARCHAR(20) | NOT NULL | ⚠️ Current status | Open, Closed, Limited |
| opened_date | DATE | NULL | Facility opening date | |
| closed_date | DATE | NULL | Facility closure date | NULL if open |
| average_wait_time_days | NUMBER(5,1) | NULL | 📊 Avg wait for appointment | Days |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE, VHA |
| source_facility_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| is_current | BOOLEAN | NOT NULL | ⏰ **SCD** Current record flag | TRUE for current version |
| valid_from | TIMESTAMP_NTZ | NOT NULL | ⏰ **SCD** Effective start date | When this version became effective |
| valid_to | TIMESTAMP_NTZ | NULL | ⏰ **SCD** Effective end date | NULL for current version |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

**Common Queries:**
```sql
-- Get facilities by VISN
SELECT * FROM dim_facility
WHERE visn = '10' AND is_current = TRUE AND operating_status = 'Open';

-- Get facility capacity metrics
SELECT facility_name, total_exam_rooms, average_wait_time_days, patient_satisfaction_score
FROM dim_facility WHERE is_current = TRUE
ORDER BY patient_satisfaction_score DESC;
```

---

### dim_evaluation_type

**Types of medical evaluations and exams**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per evaluation type
**Row Count:** ~200-300 types
**Load Frequency:** Weekly (relatively static)
**SCD Type:** Type 1 (overwrite changes)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| evaluation_type_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| evaluation_type_code | VARCHAR(50) | NOT NULL | Evaluation type code | CP, IME, DBQ, etc. |
| evaluation_type_name | VARCHAR(200) | NOT NULL | ⚠️ Evaluation type name | |
| evaluation_category | VARCHAR(50) | NOT NULL | ⚠️ Category | C&P Exam, IME, Disability Eval, etc. |
| dbq_template_name | VARCHAR(200) | NULL | DBQ form name | If DBQ-based evaluation |
| dbq_version | VARCHAR(20) | NULL | DBQ version number | |
| body_system | VARCHAR(100) | NULL | Primary body system | Musculoskeletal, Mental, etc. |
| specialty_required | VARCHAR(100) | NULL | Required specialty | Orthopedics, Psychiatry, etc. |
| typical_duration_minutes | NUMBER(4,0) | NULL | 📊 Typical exam duration | Minutes |
| compensation_complexity | VARCHAR(20) | NULL | Complexity level | Simple, Moderate, Complex |
| base_compensation_amount | NUMBER(10,2) | NULL | 📊 Base payment | If per-exam payment |
| requires_equipment | VARCHAR(500) | NULL | Special equipment needed | X-ray, etc. |
| telehealth_eligible_flag | BOOLEAN | NULL | Can be done via telehealth | TRUE if eligible |
| active_flag | BOOLEAN | NOT NULL | ⚠️ Currently in use | TRUE if active |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE |
| source_evaluation_type_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

---

### dim_medical_condition

**Medical conditions, diagnoses, and ICD codes**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per medical condition
**Row Count:** ~10,000+ conditions
**Load Frequency:** Monthly (relatively static)
**SCD Type:** Type 1 (overwrite changes)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| medical_condition_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| icd10_code | VARCHAR(10) | NOT NULL | ⚠️ ICD-10-CM diagnosis code | Primary identifier |
| icd10_description | VARCHAR(500) | NOT NULL | ⚠️ Full ICD-10 description | |
| icd9_code | VARCHAR(10) | NULL | Legacy ICD-9-CM code | For historical reference |
| icd9_description | VARCHAR(500) | NULL | Legacy ICD-9 description | |
| condition_category | VARCHAR(100) | NULL | Condition category | Chronic, Acute, etc. |
| body_system | VARCHAR(100) | NULL | Primary body system | Musculoskeletal, Cardiovascular, etc. |
| service_connected_eligible_flag | BOOLEAN | NULL | Service-connected eligible | TRUE if can be SC |
| dbq_applicable_flag | BOOLEAN | NULL | DBQ applicable | TRUE if DBQ exists |
| dbq_form_name | VARCHAR(200) | NULL | Associated DBQ form | |
| rating_percentage_range_low | NUMBER(3,0) | NULL | 📊 Min disability % | |
| rating_percentage_range_high | NUMBER(3,0) | NULL | 📊 Max disability % | |
| active_flag | BOOLEAN | NOT NULL | ⚠️ Currently valid code | TRUE if not deprecated |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE, ICD10 |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

---

### dim_claim

**VA disability claims - SCD Type 2**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per claim per change (SCD Type 2)
**Row Count:** Varies by history
**Load Frequency:** Daily incremental via Streams
**SCD Type:** Type 2 (historical tracking)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| claim_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| claim_natural_key | VARCHAR(50) | NOT NULL | Business key (Claim ID) | Source system ID |
| veteran_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_veteran | Foreign key |
| claim_number | VARCHAR(50) | NOT NULL | ⚠️ Official claim number | |
| claim_type | VARCHAR(50) | NOT NULL | ⚠️ Claim type | Original, Supplemental, etc. |
| claim_status | VARCHAR(50) | NOT NULL | ⚠️ Current status | Pending, Approved, Denied, etc. |
| claim_phase | VARCHAR(50) | NULL | Processing phase | Gathering Evidence, Decision, etc. |
| priority_level | VARCHAR(20) | NULL | Priority classification | Standard, Priority, Expedited |
| filed_date | DATE | NOT NULL | ⚠️ Claim filed date | |
| received_date | DATE | NULL | VA received date | May differ from filed |
| decision_date | DATE | NULL | Decision rendered date | NULL if pending |
| effective_date | DATE | NULL | Effective date of award | Benefit start date |
| closed_date | DATE | NULL | Claim closed date | NULL if open |
| total_contention_count | NUMBER(3,0) | NULL | 📊 Total contentions | Number of conditions claimed |
| approved_contention_count | NUMBER(3,0) | NULL | 📊 Approved contentions | |
| denied_contention_count | NUMBER(3,0) | NULL | 📊 Denied contentions | |
| pending_contention_count | NUMBER(3,0) | NULL | 📊 Pending contentions | |
| current_disability_rating | NUMBER(3,0) | NULL | 📊 Current combined rating | 0-100% |
| previous_disability_rating | NUMBER(3,0) | NULL | 📊 Rating before claim | 0-100% |
| rating_change | NUMBER(4,0) | NULL | 📊 Rating increase/decrease | Can be negative |
| monthly_benefit_amount | NUMBER(10,2) | NULL | 📊 Monthly compensation | Dollars |
| retroactive_amount | NUMBER(12,2) | NULL | 📊 Retroactive payment | Dollars |
| total_award_amount | NUMBER(12,2) | NULL | 📊 Total award value | Dollars |
| requires_exam_flag | BOOLEAN | NULL | Exam required | TRUE if C&P exam needed |
| exam_requested_date | DATE | NULL | Exam request date | |
| exam_completed_date | DATE | NULL | Exam completion date | |
| evidence_gathering_start | DATE | NULL | Evidence phase start | |
| evidence_gathering_end | DATE | NULL | Evidence phase end | |
| decision_review_flag | BOOLEAN | NULL | Under review | TRUE if being reviewed |
| appeal_flag | BOOLEAN | NULL | Appealed | TRUE if appealed |
| appeal_date | DATE | NULL | Appeal filed date | |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VBMS, VEMS_CORE |
| source_claim_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| is_current | BOOLEAN | NOT NULL | ⏰ **SCD** Current record flag | TRUE for current version |
| valid_from | TIMESTAMP_NTZ | NOT NULL | ⏰ **SCD** Effective start date | When this version became effective |
| valid_to | TIMESTAMP_NTZ | NULL | ⏰ **SCD** Effective end date | NULL for current version |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

---

### dim_appointment

**Appointment scheduling details**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per appointment
**Row Count:** Growing
**Load Frequency:** Daily incremental via Streams
**SCD Type:** Type 1 (overwrite changes)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| appointment_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| appointment_natural_key | VARCHAR(50) | NOT NULL | Business key (Appointment ID) | Source system ID |
| veteran_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_veteran | Foreign key |
| facility_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_facility | Foreign key |
| evaluator_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_evaluator | NULL if not yet assigned |
| evaluation_type_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_evaluation_type | Foreign key |
| appointment_datetime | TIMESTAMP_NTZ | NOT NULL | ⚠️ Scheduled appointment time | |
| appointment_status | VARCHAR(50) | NOT NULL | ⚠️ Status | Scheduled, Completed, Cancelled, No-Show |
| appointment_modality | VARCHAR(50) | NOT NULL | ⚠️ Modality | In-Person, Telehealth, Phone |
| scheduled_duration_minutes | NUMBER(4,0) | NULL | 📊 Planned duration | Minutes |
| check_in_time | TIMESTAMP_NTZ | NULL | Actual check-in time | |
| check_out_time | TIMESTAMP_NTZ | NULL | Actual check-out time | |
| actual_duration_minutes | NUMBER(4,0) | NULL | 📊 Actual duration | Calculated from check times |
| wait_time_minutes | NUMBER(4,0) | NULL | 📊 Wait time | Time between check-in and start |
| scheduled_date | DATE | NOT NULL | ⚠️ Appointment date | |
| scheduled_time | TIME | NOT NULL | ⚠️ Appointment time | |
| reminder_sent_flag | BOOLEAN | NULL | Reminder sent | TRUE if reminder sent |
| confirmation_flag | BOOLEAN | NULL | Confirmed by veteran | TRUE if confirmed |
| cancellation_reason | VARCHAR(200) | NULL | Reason for cancellation | If cancelled |
| no_show_reason | VARCHAR(200) | NULL | No-show reason | If no-show |
| rescheduled_from_appointment_sk | NUMBER(38,0) | NULL | 🔗 Original appointment | If rescheduled |
| interpreter_needed_flag | BOOLEAN | NULL | Interpreter required | TRUE if needed |
| interpreter_language | VARCHAR(50) | NULL | Language | If interpreter needed |
| special_accommodations | VARCHAR(500) | NULL | Special needs | Wheelchair, hearing aid, etc. |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE, OMS |
| source_appointment_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

---

### dim_exam_request_types

**Exam request categorization and routing**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per request type
**Row Count:** ~50-100 types
**Load Frequency:** Weekly (relatively static)
**SCD Type:** Type 1 (overwrite changes)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| exam_request_type_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| request_type_code | VARCHAR(50) | NOT NULL | Request type code | |
| request_type_name | VARCHAR(200) | NOT NULL | ⚠️ Request type name | |
| request_category | VARCHAR(50) | NOT NULL | ⚠️ Category | Standard, Expedited, etc. |
| priority_level | NUMBER(1,0) | NOT NULL | ⚠️ Priority (1-5) | 1 = highest |
| sla_hours | NUMBER(5,0) | NULL | 📊 SLA target hours | Target completion time |
| active_flag | BOOLEAN | NOT NULL | ⚠️ Currently active | TRUE if in use |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE, OMS |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

---

## Fact Tables

### fact_evaluation

**Completed medical evaluations - Transaction grain**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per completed evaluation per medical condition
**Row Count:** Growing (millions)
**Load Frequency:** Hourly incremental via Streams
**Partitioning:** By evaluation_date

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| evaluation_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| evaluation_date_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_date | Evaluation date |
| veteran_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_veteran | Foreign key |
| evaluator_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_evaluator | Foreign key |
| facility_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_facility | Foreign key |
| evaluation_type_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_evaluation_type | Foreign key |
| medical_condition_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_medical_condition | Foreign key |
| claim_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_claim | Associated claim |
| appointment_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_appointment | Associated appointment |
| evaluation_date | DATE | NOT NULL | ⚠️ Evaluation performed date | |
| evaluation_start_time | TIMESTAMP_NTZ | NULL | Evaluation start | |
| evaluation_end_time | TIMESTAMP_NTZ | NULL | Evaluation end | |
| evaluation_duration_minutes | NUMBER(5,0) | NULL | 📊 **Measure** Duration | Minutes |
| modality | VARCHAR(50) | NOT NULL | ⚠️ Evaluation modality | In-Person, Telehealth |
| report_submitted_date | DATE | NULL | Report submission date | |
| report_approved_date | DATE | NULL | Report approval date | |
| days_to_report_submission | NUMBER(5,0) | NULL | 📊 **Measure** Time to submit | Days |
| exam_completeness_score | NUMBER(5,2) | NULL | 📊 **Measure** Completeness | 0-100 scale |
| sufficient_exam_flag | BOOLEAN | NULL | Sufficient for decision | TRUE if sufficient |
| additional_exam_required_flag | BOOLEAN | NULL | Additional exam needed | TRUE if more needed |
| severity_rating | VARCHAR(50) | NULL | Condition severity | Mild, Moderate, Severe |
| functional_impact_score | NUMBER(3,0) | NULL | 📊 **Measure** Functional impact | 0-100 scale |
| recommended_disability_percentage | NUMBER(3,0) | NULL | 📊 **Measure** Recommended rating | 0-100% |
| medical_opinion_text | VARCHAR(4000) | NULL | Medical opinion summary | |
| nexus_opinion | VARCHAR(50) | NULL | Service connection opinion | Likely, At Least As Likely, Not |
| dbq_completed_flag | BOOLEAN | NULL | DBQ form completed | TRUE if DBQ done |
| images_count | NUMBER(3,0) | NULL | 📊 **Measure** Number of images | X-rays, photos, etc. |
| tests_performed_count | NUMBER(3,0) | NULL | 📊 **Measure** Number of tests | Lab tests, diagnostic tests |
| compensation_amount | NUMBER(10,2) | NULL | 📊 **Measure** Payment to evaluator | Dollars |
| veteran_satisfaction_score | NUMBER(3,0) | NULL | 📊 **Measure** Satisfaction | 0-100 scale |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE |
| source_evaluation_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

**Clustering:** Clustered by evaluation_date_sk, veteran_sk

**Common Queries:**
```sql
-- Get evaluations for a veteran
SELECT * FROM fact_evaluation WHERE veteran_sk = 12345;

-- Get avg evaluation duration by evaluator
SELECT evaluator_sk, AVG(evaluation_duration_minutes)
FROM fact_evaluation GROUP BY evaluator_sk;
```

---

### fact_exam_requests

**Exam request lifecycle - Accumulating snapshot grain**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per exam request (updated as milestones occur)
**Row Count:** Growing
**Load Frequency:** Hourly incremental via Streams
**Partitioning:** By request_date_sk

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| exam_request_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| request_date_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_date | Request received date |
| assigned_date_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_date | Assigned to evaluator |
| scheduled_date_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_date | Appointment scheduled |
| completed_date_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_date | Evaluation completed |
| submitted_date_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_date | Report submitted |
| approved_date_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_date | Report approved |
| veteran_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_veteran | Foreign key |
| evaluator_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_evaluator | NULL until assigned |
| facility_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_facility | NULL until scheduled |
| evaluation_type_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_evaluation_type | Foreign key |
| exam_request_type_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_exam_request_types | Foreign key |
| claim_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_claim | Associated claim |
| request_date | DATE | NOT NULL | ⚠️ Request received date | |
| requested_completion_date | DATE | NULL | Target completion date | |
| assigned_date | DATE | NULL | Assigned to evaluator | |
| scheduled_date | DATE | NULL | Appointment scheduled | |
| appointment_date | DATE | NULL | Scheduled appointment | |
| completed_date | DATE | NULL | Evaluation completed | |
| submitted_date | DATE | NULL | Report submitted | |
| approved_date | DATE | NULL | Report approved | |
| cancelled_date | DATE | NULL | Request cancelled | |
| exam_status | VARCHAR(50) | NOT NULL | ⚠️ Current status | Requested, Assigned, Scheduled, etc. |
| priority_level | VARCHAR(20) | NULL | Priority | Standard, Priority, Expedited |
| days_request_to_assigned | NUMBER(5,0) | NULL | 📊 **Measure** Request→Assigned | Days |
| days_assigned_to_scheduled | NUMBER(5,0) | NULL | 📊 **Measure** Assigned→Scheduled | Days |
| days_scheduled_to_completed | NUMBER(5,0) | NULL | 📊 **Measure** Scheduled→Completed | Days |
| days_completed_to_submitted | NUMBER(5,0) | NULL | 📊 **Measure** Completed→Submitted | Days |
| days_submitted_to_approved | NUMBER(5,0) | NULL | 📊 **Measure** Submitted→Approved | Days |
| total_cycle_time_days | NUMBER(5,0) | NULL | 📊 **Measure** Request→Approved | Days |
| sla_target_days | NUMBER(5,0) | NULL | 📊 SLA target | Days |
| sla_met | BOOLEAN | NULL | SLA compliance | TRUE if met SLA |
| sla_variance_days | NUMBER(6,0) | NULL | 📊 **Measure** Days over/under SLA | Can be negative |
| cancellation_reason | VARCHAR(200) | NULL | Reason cancelled | |
| source_system | VARCHAR(50) | NOT NULL | ⚠️ Source system | VEMS_CORE, OMS |
| source_exam_request_id | VARCHAR(50) | NOT NULL | ⚠️ Source system ID | Original ID from source |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

**Clustering:** Clustered by request_date_sk, exam_status

---

### fact_exam_processing_bottlenecks

**Bottleneck detection and analysis - Analysis grain**

**Schema:** VESDW_PRD.warehouse
**Grain:** One row per detected bottleneck
**Row Count:** Growing
**Load Frequency:** Daily batch
**Partitioning:** By detection_date_sk

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| bottleneck_sk | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated sequence |
| detection_date_sk | NUMBER(38,0) | NOT NULL | 🔗 **FK** to dim_date | Bottleneck detected date |
| facility_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_facility | Facility with bottleneck |
| evaluator_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_evaluator | Evaluator with bottleneck |
| evaluation_type_sk | NUMBER(38,0) | NULL | 🔗 **FK** to dim_evaluation_type | Exam type with bottleneck |
| bottleneck_type | VARCHAR(50) | NOT NULL | ⚠️ Bottleneck type | Capacity, Scheduling, Quality, etc. |
| bottleneck_phase | VARCHAR(50) | NOT NULL | ⚠️ Process phase | Assignment, Scheduling, Completion, etc. |
| severity_level | VARCHAR(20) | NOT NULL | ⚠️ Severity | Critical, High, Medium, Low |
| affected_requests_count | NUMBER(10,0) | NULL | 📊 **Measure** Impacted requests | Count |
| average_delay_days | NUMBER(6,2) | NULL | 📊 **Measure** Average delay | Days |
| max_delay_days | NUMBER(6,2) | NULL | 📊 **Measure** Maximum delay | Days |
| estimated_cost_impact | NUMBER(12,2) | NULL | 📊 **Measure** Cost impact | Dollars |
| detection_date | DATE | NOT NULL | ⚠️ Detection date | |
| resolved_date | DATE | NULL | Resolution date | |
| days_to_resolution | NUMBER(5,0) | NULL | 📊 **Measure** Resolution time | Days |
| root_cause | VARCHAR(500) | NULL | Root cause analysis | |
| recommended_action | VARCHAR(500) | NULL | Recommended mitigation | |
| action_taken | VARCHAR(500) | NULL | Actual action taken | |
| status | VARCHAR(50) | NOT NULL | ⚠️ Status | Detected, In Progress, Resolved |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation timestamp | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update timestamp | ETL update time |

---

*(Additional fact tables follow similar structure: fact_claim_status, fact_appointment, fact_daily_snapshot, fact_appointment_events, fact_evaluation_qa_events, fact_examiner_assignments)*

---

## Monitoring & Quality Tables

### pipeline_health_metrics

**Real-time pipeline health monitoring**

**Schema:** VESDW_PRD.metadata
**Grain:** One row per pipeline execution
**Row Count:** Growing (retention: 90 days)
**Load Frequency:** Real-time (after each pipeline run)

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| health_metric_id | NUMBER(38,0) | NOT NULL | 🔑 **PK** - Surrogate key | Auto-generated |
| pipeline_name | VARCHAR(200) | NOT NULL | ⚠️ Pipeline identifier | task_daily_ods_extraction, etc. |
| pipeline_type | VARCHAR(50) | NOT NULL | ⚠️ Pipeline type | ODS_LOAD, STAGING, DIMENSION, FACT, MART |
| execution_timestamp | TIMESTAMP_NTZ | NOT NULL | ⚠️ Execution time | |
| execution_status | VARCHAR(50) | NOT NULL | ⚠️ Status | SUCCESS, FAILURE, WARNING |
| duration_seconds | NUMBER(10,2) | NULL | 📊 **Measure** Execution duration | Seconds |
| records_processed | NUMBER(15,0) | NULL | 📊 **Measure** Records processed | Count |
| records_inserted | NUMBER(15,0) | NULL | 📊 **Measure** Records inserted | Count |
| records_updated | NUMBER(15,0) | NULL | 📊 **Measure** Records updated | Count |
| records_deleted | NUMBER(15,0) | NULL | 📊 **Measure** Records deleted | Count |
| records_failed | NUMBER(15,0) | NULL | 📊 **Measure** Records failed | Count |
| warehouse_name | VARCHAR(100) | NULL | Warehouse used | ETL_WH, ANALYTICS_WH |
| credits_used | NUMBER(10,4) | NULL | 📊 **Measure** Snowflake credits | Credits |
| data_quality_score | NUMBER(5,2) | NULL | 📊 **Measure** DQ score | 0-100 |
| sla_target_minutes | NUMBER(6,0) | NULL | 📊 SLA target | Minutes |
| sla_breach_flag | BOOLEAN | NULL | SLA breached | TRUE if over SLA |
| error_message | VARCHAR(4000) | NULL | Error details | If failed |
| batch_id | VARCHAR(100) | NULL | Batch identifier | For correlation |
| alert_sent_flag | BOOLEAN | NULL | Alert sent | TRUE if notified |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation | ETL load time |

---

### dq_rule_catalog

**Data quality rule definitions**

**Schema:** VESDW_PRD.metadata
**Grain:** One row per quality rule
**Row Count:** 40+ rules
**Load Frequency:** Pre-loaded, manually updated

| Column Name | Data Type | Nullable | Description | Notes |
|-------------|-----------|----------|-------------|-------|
| rule_id | VARCHAR(50) | NOT NULL | 🔑 **PK** - Rule identifier | COMP_001, ACC_001, etc. |
| rule_name | VARCHAR(200) | NOT NULL | ⚠️ Rule name | |
| rule_category | VARCHAR(50) | NOT NULL | ⚠️ Category | Completeness, Accuracy, Consistency, etc. |
| rule_description | VARCHAR(1000) | NULL | Full description | |
| target_schema | VARCHAR(100) | NOT NULL | ⚠️ Schema to check | warehouse, staging |
| target_table | VARCHAR(100) | NOT NULL | ⚠️ Table to check | |
| target_column | VARCHAR(100) | NULL | Column to check | NULL for table-level |
| rule_sql | VARCHAR(4000) | NOT NULL | ⚠️ SQL check | Validation query |
| threshold_value | NUMBER(10,2) | NULL | 📊 Threshold | Pass/fail threshold |
| threshold_operator | VARCHAR(10) | NULL | Comparison operator | >, <, =, >=, <= |
| severity | VARCHAR(20) | NOT NULL | ⚠️ Severity | CRITICAL, HIGH, MEDIUM, LOW |
| is_active | BOOLEAN | NOT NULL | ⚠️ Active rule | TRUE if enabled |
| auto_remediate_flag | BOOLEAN | NULL | Auto-fix | TRUE if can auto-remediate |
| remediation_sql | VARCHAR(4000) | NULL | Fix SQL | If auto-remediate |
| notification_email | VARCHAR(500) | NULL | Alert recipients | Comma-separated |
| row_created_timestamp | TIMESTAMP_NTZ | NOT NULL | Record creation | ETL load time |
| row_updated_timestamp | TIMESTAMP_NTZ | NOT NULL | Last update | ETL update time |

---

## Data Types Reference

**Snowflake data types used in this solution:**

| Data Type | Usage | Example |
|-----------|-------|---------|
| NUMBER(38,0) | Surrogate keys, IDs | 123456 |
| NUMBER(10,2) | Currency, rates | 1250.75 |
| NUMBER(5,2) | Percentages, scores | 98.50 |
| VARCHAR(50) | Short text, codes | "COMP_001" |
| VARCHAR(200) | Names, descriptions | "John Smith" |
| VARCHAR(4000) | Long text | Full descriptions |
| DATE | Calendar dates | 2024-01-15 |
| TIMESTAMP_NTZ | Date/time without timezone | 2024-01-15 14:30:00 |
| TIME | Time only | 14:30:00 |
| BOOLEAN | True/false flags | TRUE, FALSE |

---

## Naming Conventions

### Table Names

- **Dimensions**: `dim_<entity>` (e.g., dim_veteran)
- **Facts**: `fact_<event>` (e.g., fact_evaluation)
- **Staging**: `stg_<source>_<entity>` (e.g., stg_vems_veteran)
- **Views**: `vw_<purpose>` (e.g., vw_pipeline_health_dashboard)
- **Materialized Views**: `mv_<purpose>` (e.g., mv_daily_exam_volume)

### Column Names

- **Surrogate Keys**: `<table>_sk` (e.g., veteran_sk)
- **Natural Keys**: `<entity>_natural_key` (e.g., veteran_natural_key)
- **Foreign Keys**: `<referenced_table>_sk` (e.g., veteran_sk in fact table)
- **Dates**: `<event>_date` (e.g., evaluation_date)
- **Timestamps**: `<event>_timestamp` (e.g., row_created_timestamp)
- **Flags**: `<condition>_flag` (e.g., active_flag)
- **Counts**: `<entity>_count` (e.g., total_exams_count)
- **Amounts**: `<metric>_amount` (e.g., compensation_amount)

### SCD Type 2 Fields

All SCD Type 2 dimensions include:
- `is_current` - BOOLEAN - TRUE for current version
- `valid_from` - TIMESTAMP_NTZ - Effective start date
- `valid_to` - TIMESTAMP_NTZ - Effective end date (NULL for current)

### Audit Fields

All tables include:
- `row_created_timestamp` - TIMESTAMP_NTZ - Initial creation time
- `row_updated_timestamp` - TIMESTAMP_NTZ - Last update time

---

## Quick Reference Cards

### Finding Veteran Information

```sql
-- Get current veteran record
SELECT * FROM dim_veteran
WHERE veteran_natural_key = 'V123456' AND is_current = TRUE;

-- Get veteran evaluation history
SELECT v.first_name, v.last_name, e.evaluation_date, et.evaluation_type_name
FROM fact_evaluation e
JOIN dim_veteran v ON e.veteran_sk = v.veteran_sk
JOIN dim_evaluation_type et ON e.evaluation_type_sk = et.evaluation_type_sk
WHERE v.veteran_natural_key = 'V123456' AND v.is_current = TRUE;
```

### Finding Evaluator Performance

```sql
-- Get evaluator metrics
SELECT first_name, last_name, specialty,
       total_exams_completed, average_exam_quality_score, qa_failure_rate
FROM dim_evaluator
WHERE is_current = TRUE AND active_flag = TRUE
ORDER BY average_exam_quality_score DESC;
```

### Finding Facility Capacity

```sql
-- Get facility capacity and wait times
SELECT facility_name, total_exam_rooms, average_wait_time_days,
       patient_satisfaction_score
FROM dim_facility
WHERE is_current = TRUE AND operating_status = 'Open'
ORDER BY average_wait_time_days;
```

---

## Version History

- **v1.0** (2024-11-18): Initial data dictionary created
  - Documented 9 dimension tables
  - Documented 9 fact tables
  - Documented monitoring and quality tables
  - Added naming conventions and quick reference

---

**Need more details on a specific table?** See [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md)

**Need to understand relationships?** See [ER_DIAGRAM.md](ER_DIAGRAM.md)

**Found an error or need clarification?** Submit GitHub issue with label `documentation`

---

*This data dictionary is your field guide to the VES data warehouse. Use it well!* 📊
