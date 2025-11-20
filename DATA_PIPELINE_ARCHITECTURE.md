# VES Data Pipeline Architecture

**Version**: 2.0
**Date**: 2025-11-17
**Standards**: VES Snowflake Naming Conventions v1.0

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow Layers](#data-flow-layers)
3. [Layer 1: ODS (Operational Data Store)](#layer-1-ods-operational-data-store)
4. [Layer 2: Staging Layer](#layer-2-staging-layer)
5. [Layer 3: Warehouse Layer (Star Schema)](#layer-3-warehouse-layer-star-schema)
6. [Layer 4: Marts Layer](#layer-4-marts-layer)
7. [ETL Pipeline Orchestration](#etl-pipeline-orchestration)
8. [Data Quality Framework](#data-quality-framework)
9. [Deployment Guide](#deployment-guide)
10. [Operational Procedures](#operational-procedures)

---

## Architecture Overview

The VES Data Pipeline follows a **four-layer medallion architecture** pattern optimized for Snowflake:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                                │
│  VES OMS │ VEMS │ VA Systems │ External APIs │ File Feeds           │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Extract (Batch/CDC/Real-time)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│               LAYER 1: ODS (Raw Landing Zone)                        │
│  Schema: ODS_RAW                                                     │
│  Purpose: Raw data exactly as received from sources                  │
│  Pattern: ods_*_source tables                                        │
│  Retention: 30 days                                                  │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Transform & Validate
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│          LAYER 2: STAGING (Conformed & Cleansed)                    │
│  Schema: STAGING                                                     │
│  Purpose: Business-rule validated, conformed data                    │
│  Pattern: stg_* tables                                               │
│  Retention: 7 days                                                   │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Load with SCD Logic
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│        LAYER 3: WAREHOUSE (Star Schema - Gold Layer)                │
│  Schema: WAREHOUSE                                                   │
│  Purpose: Dimensional model for analytics                            │
│  Pattern: dim_* and fact_* tables                                    │
│  Retention: 7 years                                                  │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Aggregate & Present
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│           LAYER 4: MARTS (Business Views)                            │
│  Schemas: MARTS_CLINICAL │ MARTS_OPERATIONS │ MARTS_FINANCE         │
│  Purpose: Pre-aggregated business-specific views                     │
│  Pattern: vw_* views                                                 │
└─────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
           BI Tools & Analytics (Tableau, Power BI, etc.)
```

---

## Data Flow Layers

### Layer Architecture Summary

| Layer | Schema | Purpose | Data Pattern | Update Frequency |
|-------|--------|---------|--------------|------------------|
| **ODS** | `ODS_RAW` | Raw landing zone | Exact copy of source | Batch (hourly/daily) |
| **Staging** | `STAGING` | Validated & conformed | Cleansed & transformed | After each ODS load |
| **Warehouse** | `WAREHOUSE` | Dimensional model | Star schema (dims + facts) | After staging |
| **Marts** | `MARTS_*` | Business views | Aggregated views | Real-time (views) |

---

## Layer 1: ODS (Operational Data Store)

### Purpose
- **Raw landing zone** for data exactly as received from source systems
- **No transformations** applied (preserve source fidelity)
- **Audit trail** of what was received and when
- **Reprocessing capability** if transformations need to be changed

### Schema
```sql
USE DATABASE VETERAN_EVALUATION_DW;
USE SCHEMA ODS_RAW;
```

### Table Naming Convention
```
ods_<entity>_source

Examples:
- ods_veterans_source
- ods_evaluators_source
- ods_exam_requests_source
- ods_evaluations_source
```

### Key ODS Tables

| Table | Source System | Load Pattern | Grain |
|-------|---------------|--------------|-------|
| `ods_veterans_source` | VES OMS | Full + CDC | One row per veteran per extraction |
| `ods_evaluators_source` | VES OMS | Full + CDC | One row per evaluator per extraction |
| `ods_facilities_source` | VES OMS | Full | One row per facility per extraction |
| `ods_exam_requests_source` | VES OMS | Incremental | One row per request per extraction |
| `ods_evaluations_source` | VES OMS | Incremental | One row per evaluation per extraction |
| `ods_appointments_source` | VEMS | Incremental | One row per appointment per extraction |
| `ods_qa_events_source` | VES OMS | Incremental | One row per QA event per extraction |
| `ods_claims_source` | VA Systems | CDC | One row per claim per extraction |

### ODS Table Structure Pattern

All ODS tables follow this pattern:

```sql
CREATE TABLE ods_<entity>_source (
    -- Source System Identifiers
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    extraction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Raw Data Columns (as received)
    <entity_specific_columns>
    ...

    -- Metadata
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (source_system, source_record_id, extraction_timestamp)
);
```

### Batch Control

**`ods_batch_control`** tracks all data loads:

```sql
CREATE TABLE ods_batch_control (
    batch_id VARCHAR(50) PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    extraction_type VARCHAR(50) NOT NULL,  -- FULL, INCREMENTAL, CDC
    batch_start_timestamp TIMESTAMP_NTZ,
    batch_end_timestamp TIMESTAMP_NTZ,
    batch_status VARCHAR(20),  -- RUNNING, COMPLETED, FAILED
    records_extracted INTEGER,
    records_loaded INTEGER,
    records_rejected INTEGER
);
```

### Error Logging

**`ods_error_log`** captures data load failures:

```sql
CREATE TABLE ods_error_log (
    error_id INTEGER AUTOINCREMENT PRIMARY KEY,
    batch_id VARCHAR(50),
    source_table VARCHAR(100),
    source_record_id VARCHAR(100),
    error_type VARCHAR(50),
    error_message TEXT,
    error_details VARIANT  -- JSON
);
```

### Retention Policy
- **30 days** for transactional data (evaluations, appointments, QA events)
- **90 days** for dimensional data (veterans, evaluators, facilities)
- Automated purge via Snowflake Time Travel

---

## Layer 2: Staging Layer

### Purpose
- **Data cleansing** and standardization
- **Business rule validation**
- **Data quality scoring**
- **Change detection** (hash comparison for SCD)
- **Derived column calculation**

### Schema
```sql
USE SCHEMA STAGING;
```

### Table Naming Convention
```
stg_<entity>        -- For dimensions
stg_fact_<entity>   -- For facts

Examples:
- stg_veterans
- stg_evaluators
- stg_fact_exam_requests
- stg_fact_evaluations
```

### Key Staging Tables

| Table | Type | Purpose |
|-------|------|---------|
| `stg_veterans` | Dimension | Cleansed veteran data with DQ scores |
| `stg_evaluators` | Dimension | Cleansed evaluator data with DQ scores |
| `stg_facilities` | Dimension | Cleansed facility data with DQ scores |
| `stg_fact_exam_requests` | Fact | Validated exam requests with derived metrics |
| `stg_fact_evaluations` | Fact | Validated evaluations with business rules |
| `stg_fact_appointment_events` | Fact | Validated appointment events |
| `stg_fact_qa_events` | Fact | Validated QA events |

### Staging Table Structure Pattern

```sql
CREATE TABLE stg_<entity> (
    -- Business Key
    <entity>_id VARCHAR(50) NOT NULL,

    -- Cleansed Columns (standardized, validated)
    <entity_specific_columns>
    ...

    -- Change Detection
    source_record_hash VARCHAR(64),  -- MD5 hash for SCD change detection

    -- Data Quality
    dq_score DECIMAL(5,2),  -- 0-100 quality score
    dq_issues VARCHAR(1000),  -- Comma-separated list of issues

    -- Metadata
    source_system VARCHAR(50),
    batch_id VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Data Quality Scoring

Each staging record receives a **data quality score** (0-100):

```sql
-- Example DQ Scoring Logic
dq_score = (
    (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN date_of_birth IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN veteran_va_id IS NOT NULL THEN 20 ELSE 0 END) +
    (CASE WHEN email IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN phone IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
    (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
)
```

**DQ Score Interpretation**:
- **90-100**: Excellent quality
- **80-89**: Good quality
- **70-79**: Acceptable quality
- **60-69**: Poor quality (review required)
- **<60**: Unacceptable quality (reject)

### Data Transformations

**Standardization Examples**:

```sql
-- Name standardization
UPPER(TRIM(first_name)) AS first_name,
UPPER(TRIM(last_name)) || ', ' || UPPER(TRIM(first_name)) AS full_name,

-- Phone standardization
REGEXP_REPLACE(phone_primary, '[^0-9]', '') AS phone,

-- Email standardization
LOWER(TRIM(email)) AS email,

-- Zip code standardization
REGEXP_REPLACE(zip_code, '[^0-9-]', '') AS zip_code,

-- Date calculations
DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS age,
ROUND(DATEDIFF(day, service_start_date, service_end_date) / 365.25, 2) AS years_of_service,

-- Categorical derivations
CASE
    WHEN disability_rating = 0 THEN '0%'
    WHEN disability_rating BETWEEN 10 AND 30 THEN '10-30%'
    WHEN disability_rating BETWEEN 40 AND 60 THEN '40-60%'
    WHEN disability_rating BETWEEN 70 AND 90 THEN '70-90%'
    WHEN disability_rating = 100 THEN '100%'
END AS disability_rating_category
```

### Retention Policy
- **7 days** (truncated after each successful warehouse load)

---

## Multi-Source Data Integration

### Overview

The VES data pipeline integrates data from **two primary source systems**:

1. **OMS (Operations Management System)** - Legacy system containing historical veteran and evaluation data
2. **VEMS (Veterans Exam Management System)** - Modern system with current operational data

These systems have different schemas, field naming conventions, and code values. The staging layer performs **data reconciliation and merging** to create a unified view.

### Integration Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   OMS System    │     │  VEMS System    │
│   (Legacy)      │     │   (Modern)      │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │  Mulesoft ETL         │  Mulesoft ETL
         ▼                       ▼
┌─────────────────────────────────────────┐
│     ODS Layer (Separate Records)        │
│  ods_veterans_source (OMS rows)         │
│  ods_veterans_source (VEMS rows)        │
└────────┬────────────────────────────────┘
         │
         │  Entity Matching + Code Mapping
         ▼
┌─────────────────────────────────────────┐
│   Reference Tables (REFERENCE Schema)   │
│  • ref_entity_crosswalk_*               │
│  • ref_field_mapping_*                  │
│  • ref_code_mapping_*                   │
│  • ref_system_of_record                 │
│  • ref_reconciliation_log               │
└────────┬────────────────────────────────┘
         │
         │  Merge + Reconcile
         ▼
┌─────────────────────────────────────────┐
│  Staging Layer (Merged Records)         │
│  stg_veterans (OMS+VEMS combined)       │
│  Source: OMS_MERGED or VEMS_MERGED      │
└─────────────────────────────────────────┘
```

### Reference Schema Tables

#### 1. System of Record Configuration

**`ref_system_of_record`** - Defines authoritative source per entity type:

```sql
CREATE TABLE ref_system_of_record (
    entity_type VARCHAR(50) PRIMARY KEY,
    primary_source_system VARCHAR(50) NOT NULL,    -- OMS or VEMS
    fallback_source_system VARCHAR(50),
    reconciliation_rule VARCHAR(100),              -- PREFER_PRIMARY, MOST_RECENT, MERGE_FIELDS
    conflict_resolution VARCHAR(100)
);
```

**Configuration**:
| Entity Type | Primary Source | Fallback | Reconciliation Rule |
|-------------|----------------|----------|---------------------|
| VETERAN | OMS | VEMS | PREFER_PRIMARY |
| EVALUATOR | VEMS | OMS | PREFER_PRIMARY |
| FACILITY | OMS | VEMS | PREFER_PRIMARY |
| APPOINTMENT | VEMS | NULL | SINGLE_SOURCE |
| EXAM_REQUEST | VEMS | OMS | MERGE_FIELDS |

#### 2. Field Mapping Tables

**`ref_field_mapping_oms`** and **`ref_field_mapping_vems`** - Map source-specific field names to standard names:

```sql
CREATE TABLE ref_field_mapping_oms (
    entity_type VARCHAR(50) NOT NULL,
    source_field_name VARCHAR(100) NOT NULL,
    standard_field_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50),
    transformation_rule VARCHAR(500),
    PRIMARY KEY (entity_type, source_field_name)
);
```

**Example Mappings**:
| Source System | Entity | Source Field | Standard Field | Transformation |
|---------------|--------|--------------|----------------|----------------|
| OMS | VETERAN | `vet_ssn` | `veteran_ssn` | Direct mapping |
| OMS | VETERAN | `disability_pct` | `disability_rating` | `CAST(disability_pct AS INTEGER)` |
| VEMS | VETERAN | `veteran_ssn` | `veteran_ssn` | Direct mapping |
| VEMS | VETERAN | `disability_rating` | `disability_rating` | Direct mapping |

#### 3. Code Value Mapping Tables

**`ref_code_mapping_specialty`**, **`ref_code_mapping_request_type`**, **`ref_code_mapping_appointment_status`** - Translate system-specific codes to standard values:

```sql
CREATE TABLE ref_code_mapping_specialty (
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_description VARCHAR(200),
    standard_value VARCHAR(100) NOT NULL,
    standard_code VARCHAR(50),
    category VARCHAR(100),
    active_flag BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (source_system, source_code)
);
```

**Example Code Mappings**:
| Source System | Source Code | Standard Value | Standard Code | Category |
|---------------|-------------|----------------|---------------|----------|
| OMS | `PSYCH` | `Psychiatry` | `PSYCHIATRY` | `MENTAL_HEALTH` |
| VEMS | `PSYCHIATRY` | `Psychiatry` | `PSYCHIATRY` | `MENTAL_HEALTH` |
| OMS | `ORTHO` | `Orthopedics` | `ORTHOPEDICS` | `MUSCULOSKELETAL` |
| VEMS | `ORTHOPEDICS` | `Orthopedics` | `ORTHOPEDICS` | `MUSCULOSKELETAL` |

#### 4. Entity Crosswalk Tables

**`ref_entity_crosswalk_veteran`**, **`ref_entity_crosswalk_evaluator`**, **`ref_entity_crosswalk_facility`** - Match records between OMS and VEMS:

```sql
CREATE TABLE ref_entity_crosswalk_veteran (
    master_veteran_id VARCHAR(50) PRIMARY KEY,
    oms_veteran_id VARCHAR(50),
    oms_ssn VARCHAR(11),
    vems_veteran_id VARCHAR(50),
    vems_ssn VARCHAR(11),
    va_file_number VARCHAR(50),
    match_confidence DECIMAL(5,2),     -- 0-100 confidence score
    match_method VARCHAR(50),           -- SSN_EXACT_MATCH, NAME_DOB_MATCH, MANUAL
    primary_source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

**Matching Logic**:
- **Veterans**: Match on SSN (100% confidence), Name+DOB (85% confidence)
- **Evaluators**: Match on NPI (100% confidence), License Number (85% confidence)
- **Facilities**: Match on Facility ID (100% confidence), Facility Name (95% confidence)

#### 5. Reconciliation Log

**`ref_reconciliation_log`** - Tracks conflicts and resolutions:

```sql
CREATE TABLE ref_reconciliation_log (
    reconciliation_id INTEGER AUTOINCREMENT PRIMARY KEY,
    batch_id VARCHAR(50),
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(50) NOT NULL,
    conflict_type VARCHAR(100),         -- DUPLICATE, FIELD_MISMATCH, MISSING_IN_SYSTEM
    oms_value VARIANT,
    vems_value VARIANT,
    resolved_value VARIANT,
    resolution_method VARCHAR(100),
    reconciliation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Multi-Source ETL Patterns

#### Pattern 1: Entity Matching (Crosswalk Building)

```sql
-- Build veteran crosswalk using SSN
MERGE INTO ref_entity_crosswalk_veteran tgt
USING (
    SELECT
        COALESCE(oms.veteran_ssn, vems.veteran_ssn) AS master_veteran_id,
        oms.source_record_id AS oms_veteran_id,
        vems.source_record_id AS vems_veteran_id,
        CASE
            WHEN oms.veteran_ssn = vems.veteran_ssn THEN 100.00
            WHEN oms.veteran_ssn IS NOT NULL THEN 90.00
            ELSE 90.00
        END AS match_confidence
    FROM ods_veterans_source oms
    FULL OUTER JOIN ods_veterans_source vems
        ON oms.veteran_ssn = vems.veteran_ssn
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
) src
ON tgt.master_veteran_id = src.master_veteran_id
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

#### Pattern 2: Data Merging with System of Record Preference

```sql
-- Merge veterans preferring OMS as primary source
WITH combined_sources AS (
    SELECT
        xwalk.master_veteran_id,
        xwalk.primary_source_system,

        -- Primary source fields (use system of record)
        CASE WHEN xwalk.primary_source_system = 'OMS'
            THEN oms.first_name
            ELSE vems.first_name
        END AS first_name_primary,

        -- Merged fields (most recent non-null value)
        COALESCE(
            CASE WHEN vems.extraction_timestamp > oms.extraction_timestamp
                THEN vems.email ELSE NULL END,
            oms.email,
            vems.email
        ) AS email_merged,

        -- Conflict detection
        CASE
            WHEN oms.disability_rating IS NOT NULL
                AND vems.disability_rating IS NOT NULL
                AND oms.disability_rating != vems.disability_rating
            THEN 'DISABILITY_RATING_MISMATCH'
        END AS conflict_type

    FROM ref_entity_crosswalk_veteran xwalk
    LEFT JOIN ods_veterans_source oms
        ON xwalk.oms_veteran_id = oms.source_record_id
    LEFT JOIN ods_veterans_source vems
        ON xwalk.vems_veteran_id = vems.source_record_id
)
INSERT INTO stg_veterans (...)
SELECT ... FROM combined_sources;
```

#### Pattern 3: Code Value Translation

```sql
-- User-defined function for code mapping
CREATE FUNCTION fn_map_specialty_code(
    p_source_system VARCHAR,
    p_source_code VARCHAR
)
RETURNS VARCHAR
AS
$$
    SELECT standard_value
    FROM ref_code_mapping_specialty
    WHERE source_system = p_source_system
      AND source_code = p_source_code
      AND active_flag = TRUE
    LIMIT 1
$$;

-- Usage in transformation
SELECT
    veteran_id,
    fn_map_specialty_code(source_system, specialty_code) AS specialty
FROM ods_evaluations_source;
```

#### Pattern 4: Conflict Logging

```sql
-- Log conflicts for review
INSERT INTO ref_reconciliation_log (
    batch_id, entity_type, entity_id, conflict_type,
    oms_value, vems_value, resolved_value, resolution_method
)
SELECT
    :batch_id,
    'VETERAN',
    xwalk.master_veteran_id,
    'DISABILITY_RATING_MISMATCH',
    TO_VARIANT(oms.disability_rating),
    TO_VARIANT(vems.disability_rating),
    TO_VARIANT(COALESCE(oms.disability_rating, vems.disability_rating)),
    'PREFER_OMS'
FROM ref_entity_crosswalk_veteran xwalk
JOIN ods_veterans_source oms ON xwalk.oms_veteran_id = oms.source_record_id
JOIN ods_veterans_source vems ON xwalk.vems_veteran_id = vems.source_record_id
WHERE oms.disability_rating IS NOT NULL
  AND vems.disability_rating IS NOT NULL
  AND oms.disability_rating != vems.disability_rating;
```

### Multi-Source Stored Procedures

| Procedure | Purpose | Layer Transition |
|-----------|---------|------------------|
| `sp_build_crosswalk_veterans()` | Match veterans between OMS/VEMS | ODS → Reference |
| `sp_build_crosswalk_evaluators()` | Match evaluators between OMS/VEMS | ODS → Reference |
| `sp_build_crosswalk_facilities()` | Match facilities between OMS/VEMS | ODS → Reference |
| `sp_transform_multisource_ods_to_staging_veterans()` | Merge veteran data | ODS + Reference → Staging |
| `sp_transform_multisource_ods_to_staging_evaluators()` | Merge evaluator data | ODS + Reference → Staging |
| `sp_transform_multisource_ods_to_staging_facilities()` | Merge facility data | ODS + Reference → Staging |
| `sp_transform_multisource_ods_to_staging_exam_requests()` | Transform exam requests with ID mapping | ODS + Reference → Staging |
| `sp_transform_multisource_ods_to_staging_evaluations()` | Transform evaluations with ID mapping | ODS + Reference → Staging |
| `sp_transform_multisource_ods_to_staging_appointments()` | Transform appointments with ID mapping | ODS + Reference → Staging |
| `sp_etl_master_pipeline_multisource()` | Orchestrate complete multi-source ETL | Full Pipeline |

### Best Practices

1. **Always build crosswalks first** before merging data
2. **Log all conflicts** for data stewardship review
3. **Use UDFs for code mapping** to ensure consistency
4. **Track match confidence** to identify low-quality matches
5. **Prefer primary source** per ref_system_of_record configuration
6. **Use most recent timestamp** for frequently-updated fields
7. **Include source_system metadata** in staging tables (e.g., 'OMS_MERGED', 'VEMS_MERGED')
8. **Update DQ scores** to reflect multi-source quality issues

---

## Layer 3: Warehouse Layer (Star Schema)

### Purpose
- **Dimensional model** optimized for analytics
- **SCD Type 2** for tracking historical changes
- **Conformed dimensions** shared across facts
- **Multiple fact grains** (transaction, accumulating snapshot, periodic snapshot)

### Schema
```sql
USE SCHEMA WAREHOUSE;
```

### Table Naming Convention
```
dim_<entity>        -- Dimension tables (plural)
fact_<process>      -- Fact tables (past-tense verb describing event)

Examples:
Dimensions:
- dim_veterans
- dim_evaluators
- dim_facilities
- dim_dates
- dim_exam_request_types

Facts:
- fact_evaluations_completed
- fact_exam_requests
- fact_appointment_events
- fact_evaluation_qa_events
- fact_examiner_assignments
```

### Dimension Tables (9)

| Table | SCD Type | Grain | Row Count (Est) |
|-------|----------|-------|-----------------|
| `dim_dates` | Type 1 | One row per day | 3,650 (10 years) |
| `dim_veterans` | Type 2 | One row per veteran per version | 100K-1M |
| `dim_evaluators` | Type 2 | One row per evaluator per version | 1K-10K |
| `dim_facilities` | Type 2 | One row per facility per version | 100-1K |
| `dim_evaluation_types` | Type 1 | One row per evaluation type | 50-100 |
| `dim_medical_conditions` | Type 1 | One row per medical condition | 500-1K |
| `dim_claims` | Type 2 | One row per claim per version | 100K-1M |
| `dim_appointments` | Type 1 | One row per appointment type | 20-50 |
| `dim_exam_request_types` | Type 1 | One row per request type | 20-50 |

### Fact Tables (8)

| Table | Pattern | Grain | Row Count (Est) |
|-------|---------|-------|-----------------|
| `fact_evaluations_completed` | Transaction | One row per evaluation per condition | 1M-10M/year |
| `fact_exam_requests` | Accumulating Snapshot | One row per exam request | 1M-10M/year |
| `fact_appointment_events` | Transaction | One row per appointment event | 5M-50M/year |
| `fact_evaluation_qa_events` | Transaction | One row per QA event | 3M-30M/year |
| `fact_examiner_assignments` | Transaction | One row per assignment event | 2M-20M/year |
| `fact_claim_status_changes` | Accumulating Snapshot | One row per claim | 100K-1M |
| `fact_appointments_scheduled` | Accumulating Snapshot | One row per appointment | 1M-10M/year |
| `fact_daily_facility_snapshot` | Periodic Snapshot | One row per facility per day | 36K/year |

### SCD Type 2 Implementation

**Dimension Pattern**:

```sql
CREATE TABLE dim_veterans (
    veteran_sk INTEGER AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    veteran_id VARCHAR(50) NOT NULL,  -- Business key

    -- Attributes that can change
    <demographic_columns>,
    current_disability_rating INTEGER,
    email VARCHAR(255),
    ...

    -- SCD Type 2 tracking
    effective_start_date TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

**SCD Type 2 ETL Logic**:

```sql
-- Step 1: End-date changed records
UPDATE dim_veterans tgt
SET
    effective_end_date = CURRENT_TIMESTAMP(),
    is_current = FALSE
FROM stg_veterans src
WHERE tgt.veteran_id = src.veteran_id
  AND tgt.is_current = TRUE
  AND tgt.source_record_hash != src.source_record_hash;  -- Detect changes

-- Step 2: Insert new versions
INSERT INTO dim_veterans (...)
SELECT ...
FROM stg_veterans src
WHERE NOT EXISTS (SELECT 1 FROM dim_veterans WHERE veteran_id = src.veteran_id)
   OR EXISTS (SELECT 1 FROM dim_veterans WHERE veteran_id = src.veteran_id AND is_current = FALSE AND effective_end_date = CURRENT_TIMESTAMP()::DATE);
```

### Fact Table Patterns

**Transaction Fact** (insert-only):
```sql
-- fact_evaluations_completed
-- One row per evaluation, never updated
INSERT INTO fact_evaluations_completed (...) SELECT ... FROM stg_fact_evaluations;
```

**Accumulating Snapshot Fact** (merge with milestone updates):
```sql
-- fact_exam_requests
-- One row per exam request, updated as it progresses through milestones
MERGE INTO fact_exam_requests tgt
USING stg_fact_exam_requests src
ON tgt.exam_request_id = src.exam_request_id
WHEN MATCHED THEN UPDATE SET
    eligibility_confirmed_date_sk = COALESCE(src.eligibility_confirmed_date_sk, tgt.eligibility_confirmed_date_sk),
    examiner_assigned_date_sk = COALESCE(src.examiner_assigned_date_sk, tgt.examiner_assigned_date_sk),
    ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

**Periodic Snapshot Fact** (daily summary):
```sql
-- fact_daily_facility_snapshot
-- One row per facility per day, summarizing daily metrics
INSERT INTO fact_daily_facility_snapshot (...)
SELECT
    facility_sk,
    CURRENT_DATE() AS snapshot_date_sk,
    COUNT(DISTINCT evaluator_sk) AS active_evaluators,
    COUNT(DISTINCT appointment_id) AS total_appointments,
    ...
FROM fact_evaluations_completed
WHERE evaluation_date = CURRENT_DATE()
GROUP BY facility_sk;
```

### Clustering Keys

All fact tables use **clustering** for query optimization:

```sql
-- Cluster by date and primary dimension
CLUSTER BY (evaluation_date_sk, facility_sk);
CLUSTER BY (assignment_event_date_sk, evaluator_sk);
```

### Retention Policy
- **7 years** for compliance
- Archive to cold storage after 3 years

---

## Layer 4: Marts Layer

### Purpose
- **Pre-aggregated views** for specific business functions
- **Denormalized** for ease of use
- **Performance optimization** for common queries
- **Business logic encapsulation**

### Schemas
```sql
MARTS_CLINICAL     -- Clinical operations metrics
MARTS_OPERATIONS   -- Operational efficiency metrics
MARTS_FINANCE      -- Financial and payment metrics
```

### Table Naming Convention
```
vw_<subject>_<metric>

Examples:
- vw_evaluator_performance
- vw_exam_request_performance
- vw_qa_performance_metrics
- vw_facility_performance_dashboard
```

### Key Mart Views

**MARTS_CLINICAL Schema**:

| View | Purpose | Refresh Pattern |
|------|---------|-----------------|
| `vw_evaluator_performance` | Evaluator quality and volume metrics | Real-time (view) |
| `vw_exam_request_performance` | Exam request SLA and assignment efficiency | Real-time (view) |
| `vw_qa_performance_metrics` | QA review quality and turnaround times | Real-time (view) |
| `vw_appointment_lifecycle_analytics` | Appointment completion and no-show rates | Real-time (view) |
| `vw_medical_condition_analytics` | Service connection rates by condition | Real-time (view) |
| `vw_facility_performance_dashboard` | Facility operational dashboard | Real-time (view) |

### Mart View Example

```sql
CREATE OR REPLACE VIEW marts_clinical.vw_evaluator_performance AS
SELECT
    -- Evaluator Demographics
    e.evaluator_id,
    e.full_name AS evaluator_name,
    e.specialty,

    -- Volume Metrics
    COUNT(DISTINCT f.evaluation_id) AS total_evaluations,
    COUNT(DISTINCT CASE WHEN d.fiscal_year = YEAR(CURRENT_DATE()) THEN f.evaluation_id END) AS evaluations_current_fy,

    -- Quality Metrics
    AVG(f.report_completeness_score) AS avg_completeness_score,
    SUM(CASE WHEN f.first_pass_qa_approval = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN f.qa_reviewed_flag = TRUE THEN 1 ELSE 0 END), 0) AS first_pass_approval_rate_pct,

    -- Timeliness Metrics
    AVG(f.evaluation_duration_minutes) AS avg_evaluation_duration_minutes

FROM dim_evaluators e
LEFT JOIN fact_evaluations_completed f ON e.evaluator_sk = f.evaluator_sk
LEFT JOIN dim_dates d ON f.evaluation_date_sk = d.date_sk
WHERE e.is_current = TRUE
GROUP BY e.evaluator_id, e.full_name, e.specialty;
```

### Materialized Views (Optional)

For very large aggregations, consider **materialized views**:

```sql
CREATE MATERIALIZED VIEW marts_clinical.mv_monthly_facility_summary AS
SELECT
    d.year_month,
    fac.facility_id,
    COUNT(*) AS evaluations,
    AVG(f.report_completeness_score) AS avg_quality
FROM fact_evaluations_completed f
JOIN dim_dates d ON f.evaluation_date_sk = d.date_sk
JOIN dim_facilities fac ON f.facility_sk = fac.facility_sk
GROUP BY d.year_month, fac.facility_id;

-- Refresh schedule
ALTER MATERIALIZED VIEW mv_monthly_facility_summary RESUME;  -- Auto-refresh
```

---

## ETL Pipeline Orchestration

### Master ETL Procedure

```sql
CREATE OR REPLACE PROCEDURE sp_etl_master_pipeline(
    p_extraction_type VARCHAR DEFAULT 'INCREMENTAL'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id VARCHAR;
BEGIN
    -- Generate batch ID
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- Create batch control record
    INSERT INTO ods_batch_control (...) VALUES (v_batch_id, 'RUNNING', ...);

    -- Execute dimension ETLs
    CALL sp_etl_veterans(v_batch_id);
    CALL sp_etl_evaluators(v_batch_id);
    CALL sp_etl_facilities(v_batch_id);

    -- Execute fact ETLs
    CALL sp_etl_exam_requests(v_batch_id);
    CALL sp_etl_evaluations(v_batch_id);
    CALL sp_etl_appointment_events(v_batch_id);
    CALL sp_etl_qa_events(v_batch_id);

    -- Update batch status
    UPDATE ods_batch_control SET batch_status = 'COMPLETED' WHERE batch_id = v_batch_id;

    RETURN 'Pipeline completed. Batch ID: ' || v_batch_id;
END;
$$;
```

### Individual ETL Procedure Pattern

```sql
CREATE OR REPLACE PROCEDURE sp_etl_<entity>(p_batch_id VARCHAR)
RETURNS VARCHAR
AS
$$
BEGIN
    -- Step 1: Transform ODS → Staging
    CALL sp_transform_ods_to_staging_<entity>(p_batch_id);

    -- Step 2: Validate data quality
    CALL sp_validate_<entity>_quality(p_batch_id);

    -- Step 3: Load Staging → Warehouse
    CALL sp_load_dim_<entity>(p_batch_id);  -- Or sp_load_fact_<entity>

    RETURN '<Entity> ETL completed successfully';
EXCEPTION
    WHEN OTHER THEN
        -- Log error
        INSERT INTO ods_error_log (...) VALUES (p_batch_id, '<entity>', 'ETL_ERROR', SQLERRM);
        RETURN 'Error in <entity> ETL: ' || SQLERRM;
END;
$$;
```

### Execution Schedule

**Recommended Schedule**:

| Frequency | Tables | Schedule (Cron) |
|-----------|--------|-----------------|
| **Real-time** | Appointment events, QA events | CDC/streaming |
| **Hourly** | Exam requests, Evaluations | 0 * * * * |
| **Daily** | Veterans, Evaluators, Facilities | 0 1 * * * |
| **Weekly** | Claims, Reference data | 0 2 * * 0 |

**Snowflake Tasks** (Example):

```sql
CREATE OR REPLACE TASK task_daily_etl
    WAREHOUSE = etl_warehouse
    SCHEDULE = 'USING CRON 0 1 * * * America/New_York'
AS
    CALL sp_etl_master_pipeline('INCREMENTAL');

-- Enable task
ALTER TASK task_daily_etl RESUME;
```

---

## Data Quality Framework

### Data Quality Checks

**Pre-Load Validation**:

```sql
CREATE OR REPLACE PROCEDURE sp_validate_<entity>_quality(p_batch_id VARCHAR)
AS
$$
DECLARE
    v_null_count INTEGER;
    v_duplicate_count INTEGER;
BEGIN
    -- Check for required fields
    SELECT COUNT(*) INTO v_null_count
    FROM stg_<entity>
    WHERE <critical_field> IS NULL AND batch_id = p_batch_id;

    IF (v_null_count > 0) THEN
        INSERT INTO ods_error_log (...) VALUES (p_batch_id, 'stg_<entity>', 'DQ_ERROR', 'Found ' || v_null_count || ' records with NULL critical fields');
        RETURN 'Data quality check failed';
    END IF;

    -- Check for duplicates
    SELECT COUNT(*) - COUNT(DISTINCT <business_key>) INTO v_duplicate_count
    FROM stg_<entity>
    WHERE batch_id = p_batch_id;

    IF (v_duplicate_count > 0) THEN
        INSERT INTO ods_error_log (...) VALUES (p_batch_id, 'stg_<entity>', 'DQ_WARNING', 'Found ' || v_duplicate_count || ' duplicate records');
    END IF;

    RETURN 'Data quality validation passed';
END;
$$;
```

**Post-Load Validation**:

```sql
-- Orphan record check
SELECT COUNT(*) AS orphan_count
FROM fact_evaluations_completed f
WHERE NOT EXISTS (
    SELECT 1 FROM dim_veterans v
    WHERE f.veteran_sk = v.veteran_sk
);

-- Referential integrity check
SELECT
    'fact_evaluations_completed' AS fact_table,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN veteran_sk IS NULL THEN 1 ELSE 0 END) AS null_veteran_sk,
    SUM(CASE WHEN evaluator_sk IS NULL THEN 1 ELSE 0 END) AS null_evaluator_sk
FROM fact_evaluations_completed;
```

### Data Quality Monitoring View

```sql
CREATE OR REPLACE VIEW vw_data_quality_dashboard AS
SELECT
    'stg_veterans' AS table_name,
    COUNT(*) AS total_records,
    AVG(dq_score) AS avg_dq_score,
    SUM(CASE WHEN dq_score < 80 THEN 1 ELSE 0 END) AS low_quality_count,
    MAX(loaded_timestamp) AS last_load
FROM stg_veterans
UNION ALL
SELECT
    'stg_evaluators',
    COUNT(*),
    AVG(dq_score),
    SUM(CASE WHEN dq_score < 80 THEN 1 ELSE 0 END),
    MAX(loaded_timestamp)
FROM stg_evaluators
ORDER BY avg_dq_score ASC;
```

---

## Deployment Guide

### 1. Initial Deployment

```bash
# Step 1: Connect to Snowflake
snowsql -a <account> -u <username>

# Step 2: Deploy in order
!source snowflake/schema/00_setup_database.sql
!source snowflake/ods/01_create_ods_tables.sql
!source snowflake/staging/01_create_staging_tables.sql

# Step 3: Deploy dimensions
!source snowflake/dimensions/01_dim_date.sql
!source snowflake/dimensions/02_dim_veteran.sql
...

# Step 4: Deploy facts
!source snowflake/facts/01_fact_evaluation.sql
!source snowflake/facts/02_fact_claim_status.sql
...

# Step 5: Deploy ETL procedures
!source snowflake/etl/01_etl_procedures_dimensions.sql
!source snowflake/etl/02_etl_procedures_facts.sql

# Step 6: Deploy marts
!source snowflake/marts/01_create_marts_clinical.sql

# Step 7: Populate date dimension
CALL populate_dim_dates('2020-01-01', '2030-12-31');
```

### 2. Initial Data Load

```sql
-- Execute full load
CALL sp_etl_master_pipeline('FULL');

-- Verify load
SELECT * FROM ods_batch_control ORDER BY batch_start_timestamp DESC LIMIT 5;
SELECT * FROM ods_error_log WHERE batch_id = '<latest_batch_id>';
```

### 3. Schedule Ongoing Loads

```sql
-- Create daily task
CREATE OR REPLACE TASK task_daily_etl
    WAREHOUSE = etl_warehouse
    SCHEDULE = 'USING CRON 0 1 * * * America/New_York'
AS
    CALL sp_etl_master_pipeline('INCREMENTAL');

ALTER TASK task_daily_etl RESUME;
```

---

## Operational Procedures

### Monitoring

**Daily Health Check**:

```sql
-- Check recent batch status
SELECT
    batch_id,
    batch_status,
    batch_start_timestamp,
    batch_end_timestamp,
    DATEDIFF(minute, batch_start_timestamp, batch_end_timestamp) AS duration_minutes,
    records_extracted,
    records_loaded,
    records_rejected
FROM ods_batch_control
WHERE batch_start_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY batch_start_timestamp DESC;

-- Check for errors
SELECT
    batch_id,
    source_table,
    error_type,
    COUNT(*) AS error_count
FROM ods_error_log
WHERE error_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY batch_id, source_table, error_type;

-- Check data freshness
SELECT
    'dim_veterans' AS table_name,
    MAX(updated_timestamp) AS last_update,
    COUNT(*) AS current_records
FROM dim_veterans
WHERE is_current = TRUE
UNION ALL
SELECT
    'fact_evaluations_completed',
    MAX(created_timestamp),
    COUNT(*)
FROM fact_evaluations_completed
WHERE evaluation_date_sk >= (SELECT date_sk FROM dim_dates WHERE full_date >= DATEADD(day, -30, CURRENT_DATE()));
```

### Troubleshooting

**Common Issues**:

1. **Batch Failure**
```sql
-- Check error log
SELECT * FROM ods_error_log WHERE batch_id = '<failed_batch_id>';

-- Reprocess batch
CALL sp_etl_master_pipeline('FULL');  -- If safe to reprocess
```

2. **Orphan Records**
```sql
-- Find orphans
SELECT f.*
FROM fact_evaluations_completed f
LEFT JOIN dim_veterans v ON f.veteran_sk = v.veteran_sk
WHERE v.veteran_sk IS NULL;

-- Fix: Load missing veteran dimension record
```

3. **Performance Issues**
```sql
-- Check warehouse size
SHOW WAREHOUSES;

-- Check query performance
SELECT
    query_id,
    query_text,
    total_elapsed_time/1000 AS seconds,
    bytes_scanned,
    rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC
LIMIT 10;

-- Optimize clustering
ALTER TABLE fact_evaluations_completed RESUME RECLUSTER;
```

---

## Key Design Decisions

1. **Four-Layer Architecture**: Separates raw data (ODS), validated data (Staging), analytics data (Warehouse), and business views (Marts)

2. **SCD Type 2**: Tracks historical changes in veterans, evaluators, facilities, and claims

3. **Data Quality Scoring**: Every staging record gets a DQ score for monitoring and filtering

4. **Change Detection via Hashing**: MD5 hashes detect changes for SCD Type 2 logic

5. **Batch Control**: All loads tracked in ods_batch_control for lineage and auditing

6. **Error Logging**: Centralized error logging in ods_error_log

7. **Clustering Keys**: Fact tables clustered by date + primary dimension for query performance

8. **Marts as Views**: Real-time aggregations via views (can be materialized if needed)

---

## Success Metrics

**ETL Performance**:
- Batch completion time: < 2 hours for daily incremental
- Error rate: < 0.1% of records
- Data quality score: > 95 average

**Data Freshness**:
- ODS: < 1 hour lag from source
- Warehouse: < 2 hours lag from source
- Marts: Real-time (views)

**Query Performance**:
- Mart views: < 5 seconds for 90th percentile
- Ad-hoc queries: < 30 seconds for 90th percentile
- Dashboard refresh: < 10 seconds

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0 | 2025-11-17 | Added four-layer architecture (ODS, Staging, Warehouse, Marts), changed fact_ naming |
| 1.0 | 2025-11-15 | Initial architecture with fct_ naming |

---

**For Questions or Support**: Refer to PRODUCT_READINESS_ASSESSMENT.md and DIMENSIONAL_MODEL_DOCUMENTATION.md
