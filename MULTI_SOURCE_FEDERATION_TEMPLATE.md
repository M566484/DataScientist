# Multi-Source Federation Template

## Overview

This template provides a comprehensive, reusable framework for federating data from **OMS (Operations Management System)** and **VEMS (Veterans Exam Management System)** source systems. The template follows the VES Data Warehouse's established medallion architecture and multi-source integration patterns.

**Purpose:** Enable consistent, high-quality data federation across multiple source systems with built-in conflict resolution, data quality scoring, and comprehensive validation.

**Key Capabilities:**
- Entity matching and master ID generation
- System-of-record rule enforcement
- Code value standardization and mapping
- Conflict detection and resolution logging
- Data quality scoring (0-100)
- Full auditability and traceability

---

## Table of Contents

1. [Federation Components](#federation-components)
2. [Template Structure](#template-structure)
3. [Step 1: Define System-of-Record Rules](#step-1-define-system-of-record-rules)
4. [Step 2: Create Entity Crosswalk Tables](#step-2-create-entity-crosswalk-tables)
5. [Step 3: Create Code Mapping Tables](#step-3-create-code-mapping-tables)
6. [Step 4: Create Field Mapping Configuration](#step-4-create-field-mapping-configuration)
7. [Step 5: Build Entity Matching Procedures](#step-5-build-entity-matching-procedures)
8. [Step 6: Build Data Merge Procedures](#step-6-build-data-merge-procedures)
9. [Step 7: Create Validation Queries](#step-7-create-validation-queries)
10. [Step 8: Execution Workflow](#step-8-execution-workflow)
11. [Validation Checkpoints](#validation-checkpoints)
12. [Customization Guide](#customization-guide)

---

## Federation Components

A complete multi-source federation requires these components:

| Component | Purpose | Location |
|-----------|---------|----------|
| **ODS Tables** | Raw data from both sources lands here | `/snowflake/ods/` |
| **System-of-Record Rules** | Defines which source is authoritative per entity | `/snowflake/reference/` |
| **Entity Crosswalk Tables** | Maps source-specific IDs to master IDs | `/snowflake/reference/` |
| **Code Mapping Tables** | Standardizes codes across sources | `/snowflake/reference/` |
| **Field Mapping Tables** | Documents field transformations | `/snowflake/reference/` |
| **Crosswalk Procedures** | Builds entity matching/master IDs | `/snowflake/staging/` or `/snowflake/etl/` |
| **Merge Procedures** | Merges OMS+VEMS data to staging | `/snowflake/staging/` |
| **Validation Queries** | Validates federation quality | `/snowflake/monitoring/` |
| **Reconciliation Log** | Tracks all conflicts and resolutions | `/snowflake/reference/` |

---

## Template Structure

```
📁 Multi-Source Federation Implementation
│
├── 📄 Step 1: System-of-Record Configuration (ref_system_of_record)
│   └── Defines which source (OMS/VEMS) is authoritative per entity
│
├── 📄 Step 2: Entity Crosswalk Tables (ref_entity_crosswalk_*)
│   └── Maps OMS IDs + VEMS IDs → Master ID (with match confidence)
│
├── 📄 Step 3: Code Mapping Tables (ref_code_mapping_*)
│   └── Translates OMS/VEMS codes to standardized values
│
├── 📄 Step 4: Field Mapping Configuration (ref_field_mapping_*)
│   └── Documents field name translations and transformations
│
├── 📄 Step 5: Entity Matching Procedures (sp_build_crosswalk_*)
│   └── Performs entity matching and generates master IDs
│
├── 📄 Step 6: Data Merge Procedures (sp_merge_*_to_staging)
│   └── Merges OMS+VEMS data applying SOR rules
│
├── 📄 Step 7: Validation Queries
│   └── Quality checks, conflict detection, volume reconciliation
│
└── 📄 Step 8: Master Orchestration Procedure
    └── Executes complete federation pipeline
```

---

## Step 1: Define System-of-Record Rules

**Purpose:** Establish which source system is authoritative for each entity type.

### SQL Template: System-of-Record Configuration

```sql
-- Table: ref_system_of_record
-- Location: /snowflake/reference/01_create_reference_tables.sql

CREATE OR REPLACE TABLE ref_system_of_record (
    entity_type VARCHAR(50) PRIMARY KEY,           -- Entity being federated
    primary_source_system VARCHAR(50) NOT NULL,     -- OMS or VEMS (authoritative)
    fallback_source_system VARCHAR(50),             -- Fallback if primary is NULL
    reconciliation_rule VARCHAR(100) NOT NULL,      -- How conflicts are resolved
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed Data: Configure rules per entity
INSERT INTO ref_system_of_record (
    entity_type,
    primary_source_system,
    fallback_source_system,
    reconciliation_rule
) VALUES
    -- Core Entities
    ('VETERAN',        'OMS',  'VEMS', 'PREFER_PRIMARY'),        -- OMS is authoritative
    ('EVALUATOR',      'VEMS', 'OMS',  'PREFER_PRIMARY'),        -- VEMS is authoritative
    ('FACILITY',       'OMS',  'VEMS', 'PREFER_PRIMARY'),        -- OMS is authoritative

    -- Transactional Entities
    ('EXAM_REQUEST',   'OMS',  'VEMS', 'MOST_RECENT'),           -- Use most recent
    ('APPOINTMENT',    'VEMS', NULL,   'SINGLE_SOURCE'),         -- VEMS only
    ('EVALUATION',     'OMS',  'VEMS', 'MERGE_FIELDS'),          -- Merge field-by-field
    ('QA_EVENT',       'OMS',  NULL,   'SINGLE_SOURCE'),         -- OMS only
    ('CLAIM',          'OMS',  NULL,   'SINGLE_SOURCE');         -- OMS only

-- Add your custom entities here:
-- INSERT INTO ref_system_of_record VALUES ('<ENTITY>', '<PRIMARY_SOURCE>', '<FALLBACK>', '<RULE>');
```

### Reconciliation Rules Reference

| Rule | Description | Use Case |
|------|-------------|----------|
| `PREFER_PRIMARY` | Always use primary source; fallback only if NULL | Master data entities |
| `MOST_RECENT` | Use record with latest timestamp | Transactional data |
| `MERGE_FIELDS` | Merge field-by-field (primary first, then fallback) | Contact info, attributes |
| `SINGLE_SOURCE` | Only one source has this entity | Source-specific entities |
| `MANUAL_REVIEW` | Flag for manual reconciliation | High-risk conflicts |

---

## Step 2: Create Entity Crosswalk Tables

**Purpose:** Map source-specific IDs (OMS ID, VEMS ID) to a universal Master ID.

### SQL Template: Entity Crosswalk Table

```sql
-- Template: Entity Crosswalk Table
-- Customize: Replace {ENTITY} with your entity name (veteran, evaluator, facility, etc.)

CREATE OR REPLACE TABLE ref_entity_crosswalk_{ENTITY} (
    -- Primary Key: Master ID (universal identifier)
    master_{ENTITY}_id VARCHAR(50) PRIMARY KEY,

    -- Source System IDs
    oms_{ENTITY}_id VARCHAR(50),                    -- OMS system ID
    oms_{BUSINESS_KEY} VARCHAR(100),                -- OMS business key (SSN, NPI, etc.)
    vems_{ENTITY}_id VARCHAR(50),                   -- VEMS system ID
    vems_{BUSINESS_KEY} VARCHAR(100),               -- VEMS business key

    -- Match Metadata
    match_confidence DECIMAL(5,2) NOT NULL,         -- 0-100 confidence score
    match_method VARCHAR(50) NOT NULL,              -- Matching technique used
    primary_source_system VARCHAR(50) NOT NULL,     -- OMS or VEMS

    -- Audit Fields
    batch_id VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_crosswalk_{ENTITY}_oms ON ref_entity_crosswalk_{ENTITY}(oms_{ENTITY}_id);
CREATE INDEX idx_crosswalk_{ENTITY}_vems ON ref_entity_crosswalk_{ENTITY}(vems_{ENTITY}_id);
```

### Example: Veteran Crosswalk (Real Implementation)

```sql
CREATE OR REPLACE TABLE ref_entity_crosswalk_veteran (
    master_veteran_id VARCHAR(50) PRIMARY KEY,      -- Master ID = SSN (or synthetic)

    oms_veteran_id VARCHAR(50),                     -- OMS-specific ID
    oms_ssn VARCHAR(11),                            -- Business key in OMS

    vems_veteran_id VARCHAR(50),                    -- VEMS-specific ID
    vems_ssn VARCHAR(11),                           -- Business key in VEMS

    match_confidence DECIMAL(5,2) NOT NULL,         -- 100 = exact, 90 = one source
    match_method VARCHAR(50) NOT NULL,              -- SSN_EXACT_MATCH, SSN_OMS_ONLY, etc.
    primary_source_system VARCHAR(50) NOT NULL,     -- OMS (per SOR rules)

    batch_id VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_crosswalk_veteran_oms ON ref_entity_crosswalk_veteran(oms_veteran_id);
CREATE INDEX idx_crosswalk_veteran_vems ON ref_entity_crosswalk_veteran(vems_veteran_id);
```

### Example: Evaluator Crosswalk

```sql
CREATE OR REPLACE TABLE ref_entity_crosswalk_evaluator (
    master_evaluator_id VARCHAR(50) PRIMARY KEY,    -- Master ID = NPI (or synthetic)

    oms_evaluator_id VARCHAR(50),                   -- OMS-specific ID
    oms_provider_id VARCHAR(50),                    -- OMS provider ID

    vems_evaluator_id VARCHAR(50),                  -- VEMS-specific ID
    npi_number VARCHAR(10),                         -- Common business key (NPI)

    match_confidence DECIMAL(5,2) NOT NULL,
    match_method VARCHAR(50) NOT NULL,              -- NPI_EXACT_MATCH, NPI_OMS_ONLY, etc.
    primary_source_system VARCHAR(50) NOT NULL,     -- VEMS (per SOR rules)

    batch_id VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Match Confidence Scoring Guidelines

| Confidence | Criteria | Example |
|-----------|----------|---------|
| 100% | Exact match on business key in both sources | Same SSN in OMS and VEMS |
| 90% | Exists in one source only (authoritative source) | SSN only in OMS (OMS is primary) |
| 85% | Exists in one source only (fallback source) | SSN only in VEMS (OMS is primary) |
| 75% | Fuzzy match (name + DOB) | Similar names, matching DOB |
| 50% | Weak match (manual review needed) | Partial name match |
| <50% | No match confidence / new entity | Manual intervention required |

---

## Step 3: Create Code Mapping Tables

**Purpose:** Standardize code values that differ between OMS and VEMS.

### SQL Template: Code Mapping Table

```sql
-- Template: Code Mapping Table
-- Customize: Replace {DOMAIN} with your domain (specialty, request_type, status, etc.)

CREATE OR REPLACE TABLE ref_code_mapping_{DOMAIN} (
    mapping_id INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Source System Code
    source_system VARCHAR(50) NOT NULL,             -- OMS, VEMS, etc.
    source_code VARCHAR(50) NOT NULL,               -- System-specific code
    source_value VARCHAR(100),                      -- Human-readable description

    -- Standardized Code
    standard_code VARCHAR(50) NOT NULL,             -- Standardized code
    standard_value VARCHAR(100) NOT NULL,           -- Standard description
    category VARCHAR(50),                           -- Optional category grouping

    -- Metadata
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    UNIQUE (source_system, source_code)
);

CREATE INDEX idx_code_mapping_{DOMAIN} ON ref_code_mapping_{DOMAIN}(source_system, source_code);
```

### Example: Specialty Code Mapping

```sql
CREATE OR REPLACE TABLE ref_code_mapping_specialty (
    mapping_id INTEGER AUTOINCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_value VARCHAR(100),
    standard_code VARCHAR(50) NOT NULL,
    standard_value VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (source_system, source_code)
);

-- Seed Data: Map OMS and VEMS specialty codes to standard values
INSERT INTO ref_code_mapping_specialty (
    source_system, source_code, source_value,
    standard_code, standard_value, category
) VALUES
    -- Psychiatry
    ('OMS',  'PSYCH',      'Psychiatry',    'PSYCHIATRY',  'PSYCHIATRY',  'MENTAL_HEALTH'),
    ('VEMS', 'PSYCHIATRY', 'Psychiatry',    'PSYCHIATRY',  'PSYCHIATRY',  'MENTAL_HEALTH'),

    -- Orthopedics
    ('OMS',  'ORTHO',       'Orthopedics',  'ORTHOPEDICS', 'ORTHOPEDICS', 'MUSCULOSKELETAL'),
    ('VEMS', 'ORTHOPEDICS', 'Orthopedics',  'ORTHOPEDICS', 'ORTHOPEDICS', 'MUSCULOSKELETAL'),

    -- Cardiology
    ('OMS',  'CARD',        'Cardiology',   'CARDIOLOGY',  'CARDIOLOGY',  'CARDIOVASCULAR'),
    ('VEMS', 'CARDIOLOGY',  'Cardiology',   'CARDIOLOGY',  'CARDIOLOGY',  'CARDIOVASCULAR'),

    -- Add more mappings as needed
    ('OMS',  'NEURO',       'Neurology',    'NEUROLOGY',   'NEUROLOGY',   'NEUROLOGICAL'),
    ('VEMS', 'NEUROLOGY',   'Neurology',    'NEUROLOGY',   'NEUROLOGY',   'NEUROLOGICAL');

-- Add your custom code mappings here:
-- INSERT INTO ref_code_mapping_specialty VALUES (...);
```

### Example: Request Type Mapping

```sql
CREATE OR REPLACE TABLE ref_code_mapping_request_type (
    mapping_id INTEGER AUTOINCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_value VARCHAR(100),
    standard_code VARCHAR(50) NOT NULL,
    standard_value VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (source_system, source_code)
);

INSERT INTO ref_code_mapping_request_type VALUES
    ('OMS',  'CP',       'C&P Exam',         'CP_EXAM',    'C&P EXAM',         'COMPENSATION'),
    ('VEMS', 'C&P_EXAM', 'C&P Exam',         'CP_EXAM',    'C&P EXAM',         'COMPENSATION'),
    ('OMS',  'REEX',     'Re-examination',   'REEXAM',     'RE-EXAMINATION',   'FOLLOWUP'),
    ('VEMS', 'RE_EXAM',  'Re-examination',   'REEXAM',     'RE-EXAMINATION',   'FOLLOWUP'),
    ('OMS',  'INIT',     'Initial Exam',     'INITIAL',    'INITIAL EXAM',     'INITIAL'),
    ('VEMS', 'INITIAL',  'Initial Exam',     'INITIAL',    'INITIAL EXAM',     'INITIAL');
```

### Code Translation UDF Template

```sql
-- Template: Code Translation Function
-- Customize: Replace {DOMAIN} with your domain

CREATE OR REPLACE FUNCTION fn_map_{DOMAIN}_code(
    p_source_system VARCHAR,
    p_source_code VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT standard_value
    FROM ref_code_mapping_{DOMAIN}
    WHERE source_system = p_source_system
      AND source_code = p_source_code
      AND active_flag = TRUE
    LIMIT 1
$$;

-- Usage Example:
-- SELECT fn_map_specialty_code('OMS', 'PSYCH') AS standard_specialty;
-- Returns: 'PSYCHIATRY'
```

---

## Step 4: Create Field Mapping Configuration

**Purpose:** Document field name translations and transformation rules between OMS and VEMS.

### SQL Template: Field Mapping Tables

```sql
-- OMS Field Mapping
CREATE OR REPLACE TABLE ref_field_mapping_oms (
    mapping_id INTEGER AUTOINCREMENT PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,               -- VETERAN, EVALUATOR, etc.
    oms_field_name VARCHAR(100) NOT NULL,           -- Original OMS field name
    standard_field_name VARCHAR(100) NOT NULL,      -- Standardized field name
    transformation_rule VARCHAR(500),               -- SQL transformation logic
    data_type VARCHAR(50),                          -- VARCHAR, INTEGER, DATE, etc.
    is_required BOOLEAN DEFAULT FALSE,
    notes VARCHAR(500),
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (entity_type, oms_field_name)
);

-- VEMS Field Mapping
CREATE OR REPLACE TABLE ref_field_mapping_vems (
    mapping_id INTEGER AUTOINCREMENT PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    vems_field_name VARCHAR(100) NOT NULL,          -- Original VEMS field name
    standard_field_name VARCHAR(100) NOT NULL,      -- Standardized field name
    transformation_rule VARCHAR(500),
    data_type VARCHAR(50),
    is_required BOOLEAN DEFAULT FALSE,
    notes VARCHAR(500),
    active_flag BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (entity_type, vems_field_name)
);
```

### Example: Veteran Field Mappings

```sql
-- OMS Veteran Field Mappings
INSERT INTO ref_field_mapping_oms (
    entity_type, oms_field_name, standard_field_name,
    transformation_rule, data_type, is_required, notes
) VALUES
    ('VETERAN', 'vet_ssn',         'veteran_ssn',        'Direct mapping',                   'VARCHAR(11)',  TRUE,  'Business key'),
    ('VETERAN', 'vet_first',       'first_name',         'UPPER(TRIM(vet_first))',           'VARCHAR(50)',  TRUE,  'Standardize case'),
    ('VETERAN', 'vet_last',        'last_name',          'UPPER(TRIM(vet_last))',            'VARCHAR(50)',  TRUE,  'Standardize case'),
    ('VETERAN', 'vet_dob',         'date_of_birth',      'TO_DATE(vet_dob, ''YYYY-MM-DD'')', 'DATE',         TRUE,  'Format conversion'),
    ('VETERAN', 'disability_pct',  'disability_rating',  'CAST(disability_pct AS INTEGER)',  'INTEGER',      FALSE, 'Type conversion'),
    ('VETERAN', 'email_addr',      'email',              'LOWER(TRIM(email_addr))',          'VARCHAR(100)', FALSE, 'Normalize email');

-- VEMS Veteran Field Mappings
INSERT INTO ref_field_mapping_vems (
    entity_type, vems_field_name, standard_field_name,
    transformation_rule, data_type, is_required, notes
) VALUES
    ('VETERAN', 'veteran_ssn',      'veteran_ssn',       'Direct mapping',                    'VARCHAR(11)',  TRUE,  'Business key'),
    ('VETERAN', 'first_name',       'first_name',        'UPPER(TRIM(first_name))',           'VARCHAR(50)',  TRUE,  'Standardize case'),
    ('VETERAN', 'last_name',        'last_name',         'UPPER(TRIM(last_name))',            'VARCHAR(50)',  TRUE,  'Standardize case'),
    ('VETERAN', 'date_of_birth',    'date_of_birth',     'TO_DATE(date_of_birth)',            'DATE',         TRUE,  'Already in DATE format'),
    ('VETERAN', 'disability_rating', 'disability_rating', 'Direct mapping',                    'INTEGER',      FALSE, 'Already INTEGER'),
    ('VETERAN', 'email_address',    'email',             'LOWER(TRIM(email_address))',        'VARCHAR(100)', FALSE, 'Normalize email');
```

---

## Step 5: Build Entity Matching Procedures

**Purpose:** Perform entity matching between OMS and VEMS to generate master IDs and populate crosswalk tables.

### SQL Template: Entity Matching Procedure

```sql
-- Template: Build Entity Crosswalk Procedure
-- Customize: Replace {ENTITY} and {BUSINESS_KEY} with your values

CREATE OR REPLACE PROCEDURE sp_build_crosswalk_{ENTITY}(
    p_batch_id VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Clear existing crosswalk for this batch (if reprocessing)
    DELETE FROM ref_entity_crosswalk_{ENTITY}
    WHERE batch_id = :p_batch_id;

    -- Step 2: Perform entity matching using FULL OUTER JOIN on business key
    INSERT INTO ref_entity_crosswalk_{ENTITY} (
        batch_id,
        master_{ENTITY}_id,
        oms_{ENTITY}_id,
        oms_{BUSINESS_KEY},
        vems_{ENTITY}_id,
        vems_{BUSINESS_KEY},
        match_confidence,
        match_method,
        primary_source_system,
        created_timestamp
    )
    SELECT
        :p_batch_id AS batch_id,

        -- Master ID = COALESCE of business keys (prefer primary source per SOR)
        COALESCE(
            CASE WHEN sor.primary_source_system = 'OMS' THEN oms.{BUSINESS_KEY} END,
            CASE WHEN sor.primary_source_system = 'VEMS' THEN vems.{BUSINESS_KEY} END,
            oms.{BUSINESS_KEY},
            vems.{BUSINESS_KEY}
        ) AS master_{ENTITY}_id,

        -- OMS fields
        oms.source_record_id AS oms_{ENTITY}_id,
        oms.{BUSINESS_KEY} AS oms_{BUSINESS_KEY},

        -- VEMS fields
        vems.source_record_id AS vems_{ENTITY}_id,
        vems.{BUSINESS_KEY} AS vems_{BUSINESS_KEY},

        -- Match confidence scoring
        CASE
            WHEN oms.{BUSINESS_KEY} IS NOT NULL
             AND vems.{BUSINESS_KEY} IS NOT NULL
             AND oms.{BUSINESS_KEY} = vems.{BUSINESS_KEY} THEN 100.00  -- Exact match
            WHEN oms.{BUSINESS_KEY} IS NOT NULL
             AND vems.{BUSINESS_KEY} IS NULL
             AND sor.primary_source_system = 'OMS' THEN 90.00           -- OMS only (OMS primary)
            WHEN vems.{BUSINESS_KEY} IS NOT NULL
             AND oms.{BUSINESS_KEY} IS NULL
             AND sor.primary_source_system = 'VEMS' THEN 90.00          -- VEMS only (VEMS primary)
            WHEN oms.{BUSINESS_KEY} IS NOT NULL
             AND vems.{BUSINESS_KEY} IS NULL
             AND sor.primary_source_system = 'VEMS' THEN 85.00          -- OMS only (VEMS primary)
            WHEN vems.{BUSINESS_KEY} IS NOT NULL
             AND oms.{BUSINESS_KEY} IS NULL
             AND sor.primary_source_system = 'OMS' THEN 85.00           -- VEMS only (OMS primary)
            ELSE 50.00                                                  -- Uncertain match
        END AS match_confidence,

        -- Match method description
        CASE
            WHEN oms.{BUSINESS_KEY} = vems.{BUSINESS_KEY} THEN '{BUSINESS_KEY}_EXACT_MATCH'
            WHEN oms.{BUSINESS_KEY} IS NOT NULL AND vems.{BUSINESS_KEY} IS NULL THEN '{BUSINESS_KEY}_OMS_ONLY'
            WHEN vems.{BUSINESS_KEY} IS NOT NULL AND oms.{BUSINESS_KEY} IS NULL THEN '{BUSINESS_KEY}_VEMS_ONLY'
            ELSE '{BUSINESS_KEY}_NO_MATCH'
        END AS match_method,

        -- Primary source from system-of-record configuration
        sor.primary_source_system,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM ods_{ENTITY}s_source oms
    FULL OUTER JOIN ods_{ENTITY}s_source vems
        ON oms.{BUSINESS_KEY} = vems.{BUSINESS_KEY}
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
    INNER JOIN ref_system_of_record sor
        ON sor.entity_type = UPPER('{ENTITY}')
        AND sor.active_flag = TRUE
    WHERE (oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id)
      AND (oms.{BUSINESS_KEY} IS NOT NULL OR vems.{BUSINESS_KEY} IS NOT NULL);

    -- Step 3: Return success message with statistics
    LET rows_inserted INTEGER := (SELECT COUNT(*) FROM ref_entity_crosswalk_{ENTITY} WHERE batch_id = :p_batch_id);
    LET exact_matches INTEGER := (SELECT COUNT(*) FROM ref_entity_crosswalk_{ENTITY} WHERE batch_id = :p_batch_id AND match_confidence = 100.00);

    RETURN 'Crosswalk built successfully. Rows: ' || :rows_inserted || ', Exact matches: ' || :exact_matches;
END;
$$;
```

### Example: Veteran Crosswalk Procedure (Real Implementation)

```sql
CREATE OR REPLACE PROCEDURE sp_build_crosswalk_veteran(
    p_batch_id VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM ref_entity_crosswalk_veteran WHERE batch_id = :p_batch_id;

    INSERT INTO ref_entity_crosswalk_veteran (
        batch_id, master_veteran_id,
        oms_veteran_id, oms_ssn,
        vems_veteran_id, vems_ssn,
        match_confidence, match_method, primary_source_system, created_timestamp
    )
    SELECT
        :p_batch_id,
        COALESCE(
            CASE WHEN sor.primary_source_system = 'OMS' THEN oms.veteran_ssn END,
            CASE WHEN sor.primary_source_system = 'VEMS' THEN vems.veteran_ssn END,
            oms.veteran_ssn,
            vems.veteran_ssn
        ) AS master_veteran_id,

        oms.source_record_id AS oms_veteran_id,
        oms.veteran_ssn AS oms_ssn,
        vems.source_record_id AS vems_veteran_id,
        vems.veteran_ssn AS vems_ssn,

        CASE
            WHEN oms.veteran_ssn = vems.veteran_ssn THEN 100.00
            WHEN oms.veteran_ssn IS NOT NULL AND vems.veteran_ssn IS NULL AND sor.primary_source_system = 'OMS' THEN 90.00
            WHEN vems.veteran_ssn IS NOT NULL AND oms.veteran_ssn IS NULL AND sor.primary_source_system = 'VEMS' THEN 90.00
            WHEN oms.veteran_ssn IS NOT NULL AND vems.veteran_ssn IS NULL AND sor.primary_source_system = 'VEMS' THEN 85.00
            WHEN vems.veteran_ssn IS NOT NULL AND oms.veteran_ssn IS NULL AND sor.primary_source_system = 'OMS' THEN 85.00
            ELSE 50.00
        END AS match_confidence,

        CASE
            WHEN oms.veteran_ssn = vems.veteran_ssn THEN 'SSN_EXACT_MATCH'
            WHEN oms.veteran_ssn IS NOT NULL AND vems.veteran_ssn IS NULL THEN 'SSN_OMS_ONLY'
            WHEN vems.veteran_ssn IS NOT NULL AND oms.veteran_ssn IS NULL THEN 'SSN_VEMS_ONLY'
            ELSE 'SSN_NO_MATCH'
        END AS match_method,

        sor.primary_source_system,
        CURRENT_TIMESTAMP()

    FROM ods_veterans_source oms
    FULL OUTER JOIN ods_veterans_source vems
        ON oms.veteran_ssn = vems.veteran_ssn
        AND oms.source_system = 'OMS'
        AND vems.source_system = 'VEMS'
    INNER JOIN ref_system_of_record sor
        ON sor.entity_type = 'VETERAN'
        AND sor.active_flag = TRUE
    WHERE (oms.batch_id = :p_batch_id OR vems.batch_id = :p_batch_id)
      AND (oms.veteran_ssn IS NOT NULL OR vems.veteran_ssn IS NOT NULL);

    LET rows_inserted INTEGER := (SELECT COUNT(*) FROM ref_entity_crosswalk_veteran WHERE batch_id = :p_batch_id);
    LET exact_matches INTEGER := (SELECT COUNT(*) FROM ref_entity_crosswalk_veteran WHERE batch_id = :p_batch_id AND match_confidence = 100.00);

    RETURN 'Veteran crosswalk built. Rows: ' || :rows_inserted || ', Exact matches: ' || :exact_matches;
END;
$$;
```

---

## Step 6: Build Data Merge Procedures

**Purpose:** Merge OMS and VEMS data into staging tables, applying system-of-record rules and detecting conflicts.

### SQL Template: Data Merge Procedure

```sql
-- Template: Merge OMS + VEMS Data to Staging
-- Customize: Replace {ENTITY} with your entity name

CREATE OR REPLACE PROCEDURE sp_merge_{ENTITY}_to_staging(
    p_batch_id VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Clear staging for this batch (if reprocessing)
    DELETE FROM stg_{ENTITY}s WHERE batch_id = :p_batch_id;

    -- Step 2: Merge OMS and VEMS data using crosswalk
    INSERT INTO stg_{ENTITY}s (
        {ENTITY}_id,                        -- Master ID from crosswalk
        batch_id,
        source_system,                       -- Which source was used per field

        -- Business key
        {BUSINESS_KEY},

        -- Core attributes (apply PREFER_PRIMARY rule)
        field1,
        field2,
        field3,

        -- Contact attributes (apply MOST_RECENT rule)
        email,
        phone,
        address,

        -- Metadata
        oms_source_record_id,
        vems_source_record_id,
        extraction_timestamp,
        dq_score,                            -- Data quality score (0-100)
        dq_issues,                           -- Comma-separated DQ issues
        created_timestamp
    )
    SELECT
        -- Master ID from crosswalk
        xwalk.master_{ENTITY}_id AS {ENTITY}_id,
        :p_batch_id AS batch_id,

        -- Source system indicator
        CASE
            WHEN sor.primary_source_system = 'OMS' THEN 'OMS'
            WHEN sor.primary_source_system = 'VEMS' THEN 'VEMS'
            ELSE 'MERGED'
        END AS source_system,

        -- Business key (prefer primary source)
        COALESCE(
            CASE WHEN sor.primary_source_system = 'OMS' THEN oms.{BUSINESS_KEY} END,
            CASE WHEN sor.primary_source_system = 'VEMS' THEN vems.{BUSINESS_KEY} END,
            oms.{BUSINESS_KEY},
            vems.{BUSINESS_KEY}
        ) AS {BUSINESS_KEY},

        -- Core attributes (PREFER_PRIMARY)
        COALESCE(
            CASE WHEN sor.primary_source_system = 'OMS' THEN oms.field1 END,
            CASE WHEN sor.primary_source_system = 'VEMS' THEN vems.field1 END,
            oms.field1,
            vems.field1
        ) AS field1,

        -- Add similar logic for field2, field3, etc.

        -- Contact attributes (MOST_RECENT non-null value)
        COALESCE(
            CASE
                WHEN vems.extraction_timestamp >= oms.extraction_timestamp
                 AND vems.email IS NOT NULL THEN vems.email
            END,
            oms.email,
            vems.email
        ) AS email,

        -- Metadata
        oms.source_record_id AS oms_source_record_id,
        vems.source_record_id AS vems_source_record_id,
        GREATEST(oms.extraction_timestamp, vems.extraction_timestamp) AS extraction_timestamp,

        -- Data quality score (computed based on DQ rules)
        100 - (
            CASE WHEN COALESCE(oms.field1, vems.field1) IS NULL THEN 20 ELSE 0 END +
            CASE WHEN COALESCE(oms.field2, vems.field2) IS NULL THEN 15 ELSE 0 END
            -- Add more DQ deductions as needed
        ) AS dq_score,

        -- DQ issues (comma-separated)
        NULLIF(
            TRIM(BOTH ',' FROM
                CASE WHEN COALESCE(oms.field1, vems.field1) IS NULL THEN 'FIELD1_MISSING,' ELSE '' END ||
                CASE WHEN COALESCE(oms.field2, vems.field2) IS NULL THEN 'FIELD2_MISSING,' ELSE '' END
            ),
            ''
        ) AS dq_issues,

        CURRENT_TIMESTAMP() AS created_timestamp

    FROM ref_entity_crosswalk_{ENTITY} xwalk
    LEFT JOIN ods_{ENTITY}s_source oms
        ON xwalk.oms_{ENTITY}_id = oms.source_record_id
        AND oms.source_system = 'OMS'
    LEFT JOIN ods_{ENTITY}s_source vems
        ON xwalk.vems_{ENTITY}_id = vems.source_record_id
        AND vems.source_system = 'VEMS'
    INNER JOIN ref_system_of_record sor
        ON sor.entity_type = UPPER('{ENTITY}')
        AND sor.active_flag = TRUE
    WHERE xwalk.batch_id = :p_batch_id;

    -- Step 3: Log conflicts to reconciliation log
    INSERT INTO ref_reconciliation_log (
        batch_id, entity_type, entity_id,
        conflict_type, oms_value, vems_value,
        resolved_value, resolution_method, created_timestamp
    )
    SELECT
        :p_batch_id,
        UPPER('{ENTITY}'),
        xwalk.master_{ENTITY}_id,
        'FIELD1_MISMATCH',
        oms.field1,
        vems.field1,
        COALESCE(oms.field1, vems.field1),
        sor.reconciliation_rule,
        CURRENT_TIMESTAMP()
    FROM ref_entity_crosswalk_{ENTITY} xwalk
    INNER JOIN ods_{ENTITY}s_source oms ON xwalk.oms_{ENTITY}_id = oms.source_record_id AND oms.source_system = 'OMS'
    INNER JOIN ods_{ENTITY}s_source vems ON xwalk.vems_{ENTITY}_id = vems.source_record_id AND vems.source_system = 'VEMS'
    INNER JOIN ref_system_of_record sor ON sor.entity_type = UPPER('{ENTITY}')
    WHERE xwalk.batch_id = :p_batch_id
      AND oms.field1 IS NOT NULL
      AND vems.field1 IS NOT NULL
      AND oms.field1 != vems.field1;

    -- Step 4: Return success message
    LET rows_merged INTEGER := (SELECT COUNT(*) FROM stg_{ENTITY}s WHERE batch_id = :p_batch_id);
    LET conflicts_logged INTEGER := (SELECT COUNT(*) FROM ref_reconciliation_log WHERE batch_id = :p_batch_id AND entity_type = UPPER('{ENTITY}'));

    RETURN 'Merge completed. Rows: ' || :rows_merged || ', Conflicts: ' || :conflicts_logged;
END;
$$;
```

### Example: Veteran Merge Procedure (Simplified)

```sql
CREATE OR REPLACE PROCEDURE sp_merge_veteran_to_staging(
    p_batch_id VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM stg_veterans WHERE batch_id = :p_batch_id;

    INSERT INTO stg_veterans (
        veteran_id, batch_id, source_system,
        veteran_ssn, first_name, last_name, date_of_birth,
        disability_rating, email, phone,
        oms_source_record_id, vems_source_record_id,
        extraction_timestamp, dq_score, dq_issues, created_timestamp
    )
    SELECT
        xwalk.master_veteran_id AS veteran_id,
        :p_batch_id,
        CASE WHEN sor.primary_source_system = 'OMS' THEN 'OMS' ELSE 'VEMS' END AS source_system,

        -- Business key
        COALESCE(oms.veteran_ssn, vems.veteran_ssn) AS veteran_ssn,

        -- Core attributes (PREFER_PRIMARY = OMS)
        COALESCE(oms.first_name, vems.first_name) AS first_name,
        COALESCE(oms.last_name, vems.last_name) AS last_name,
        COALESCE(oms.date_of_birth, vems.date_of_birth) AS date_of_birth,
        COALESCE(oms.disability_rating, vems.disability_rating) AS disability_rating,

        -- Contact info (MOST_RECENT)
        COALESCE(
            CASE WHEN vems.extraction_timestamp >= oms.extraction_timestamp THEN vems.email END,
            oms.email,
            vems.email
        ) AS email,
        COALESCE(
            CASE WHEN vems.extraction_timestamp >= oms.extraction_timestamp THEN vems.phone END,
            oms.phone,
            vems.phone
        ) AS phone,

        -- Metadata
        oms.source_record_id AS oms_source_record_id,
        vems.source_record_id AS vems_source_record_id,
        GREATEST(COALESCE(oms.extraction_timestamp, '1900-01-01'), COALESCE(vems.extraction_timestamp, '1900-01-01')) AS extraction_timestamp,

        -- Data quality score
        100 - (
            CASE WHEN veteran_ssn IS NULL THEN 20 ELSE 0 END +
            CASE WHEN first_name IS NULL THEN 15 ELSE 0 END +
            CASE WHEN last_name IS NULL THEN 15 ELSE 0 END
        ) AS dq_score,

        NULL AS dq_issues,
        CURRENT_TIMESTAMP()

    FROM ref_entity_crosswalk_veteran xwalk
    LEFT JOIN ods_veterans_source oms
        ON xwalk.oms_veteran_id = oms.source_record_id AND oms.source_system = 'OMS'
    LEFT JOIN ods_veterans_source vems
        ON xwalk.vems_veteran_id = vems.source_record_id AND vems.source_system = 'VEMS'
    INNER JOIN ref_system_of_record sor ON sor.entity_type = 'VETERAN'
    WHERE xwalk.batch_id = :p_batch_id;

    -- Log conflicts
    INSERT INTO ref_reconciliation_log (
        batch_id, entity_type, entity_id,
        conflict_type, oms_value, vems_value,
        resolved_value, resolution_method, created_timestamp
    )
    SELECT
        :p_batch_id, 'VETERAN', xwalk.master_veteran_id,
        'DISABILITY_RATING_MISMATCH',
        oms.disability_rating, vems.disability_rating,
        COALESCE(oms.disability_rating, vems.disability_rating),
        'PREFER_PRIMARY',
        CURRENT_TIMESTAMP()
    FROM ref_entity_crosswalk_veteran xwalk
    INNER JOIN ods_veterans_source oms ON xwalk.oms_veteran_id = oms.source_record_id AND oms.source_system = 'OMS'
    INNER JOIN ods_veterans_source vems ON xwalk.vems_veteran_id = vems.source_record_id AND vems.source_system = 'VEMS'
    WHERE xwalk.batch_id = :p_batch_id
      AND oms.disability_rating IS NOT NULL
      AND vems.disability_rating IS NOT NULL
      AND oms.disability_rating != vems.disability_rating;

    LET rows_merged INTEGER := (SELECT COUNT(*) FROM stg_veterans WHERE batch_id = :p_batch_id);
    LET conflicts INTEGER := (SELECT COUNT(*) FROM ref_reconciliation_log WHERE batch_id = :p_batch_id AND entity_type = 'VETERAN');

    RETURN 'Veteran merge completed. Rows: ' || :rows_merged || ', Conflicts: ' || :conflicts;
END;
$$;
```

---

## Step 7: Create Validation Queries

**Purpose:** Validate federation quality through multiple checkpoints.

### Validation Query Template

```sql
-- ============================================================
-- MULTI-SOURCE FEDERATION VALIDATION QUERIES
-- Entity: {ENTITY}
-- ============================================================

-- ------------------------------------------------------------
-- CHECKPOINT 1: Crosswalk Match Summary
-- ------------------------------------------------------------
-- Validates entity matching quality and distribution

SELECT
    'Crosswalk Match Summary' AS checkpoint,
    match_method,
    COUNT(*) AS record_count,
    ROUND(AVG(match_confidence), 2) AS avg_confidence,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM ref_entity_crosswalk_{ENTITY}
WHERE batch_id = '{BATCH_ID}'
GROUP BY match_method
ORDER BY record_count DESC;

-- Expected Results:
-- - {BUSINESS_KEY}_EXACT_MATCH: 70-90% (100.00 confidence)
-- - {BUSINESS_KEY}_OMS_ONLY: 5-15% (90.00 confidence)
-- - {BUSINESS_KEY}_VEMS_ONLY: 5-15% (85-90 confidence)
-- - Overall avg_confidence: >95%

-- ------------------------------------------------------------
-- CHECKPOINT 2: Data Quality Score Distribution
-- ------------------------------------------------------------
-- Validates merged data quality in staging

SELECT
    'DQ Score Distribution' AS checkpoint,
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        ELSE 'Poor (<70)'
    END AS dq_category,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
    ROUND(AVG(dq_score), 2) AS avg_score
FROM stg_{ENTITY}s
WHERE batch_id = '{BATCH_ID}'
GROUP BY dq_category
ORDER BY dq_category;

-- Expected Results:
-- - Excellent (90-100): >70% of records
-- - Good (80-89): Most remaining records
-- - Acceptable (70-79): <10%
-- - Poor (<70): Minimal (investigate these)

-- ------------------------------------------------------------
-- CHECKPOINT 3: Conflict Detection Summary
-- ------------------------------------------------------------
-- Shows conflicts between OMS and VEMS

SELECT
    'Conflict Summary' AS checkpoint,
    conflict_type,
    COUNT(*) AS conflict_count,
    resolution_method,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_conflicts
FROM ref_reconciliation_log
WHERE batch_id = '{BATCH_ID}'
  AND entity_type = UPPER('{ENTITY}')
GROUP BY conflict_type, resolution_method
ORDER BY conflict_count DESC;

-- Expected Results:
-- - Conflicts are NORMAL during system transitions
-- - Common conflicts: contact info changes, status mismatches
-- - All conflicts should have a resolution_method applied

-- ------------------------------------------------------------
-- CHECKPOINT 4: Volume Reconciliation
-- ------------------------------------------------------------
-- Validates no data loss during federation

WITH source_counts AS (
    SELECT
        source_system,
        COUNT(*) AS ods_count
    FROM ods_{ENTITY}s_source
    WHERE batch_id = '{BATCH_ID}'
    GROUP BY source_system
),
staging_count AS (
    SELECT COUNT(*) AS stg_count
    FROM stg_{ENTITY}s
    WHERE batch_id = '{BATCH_ID}'
)
SELECT
    'Volume Reconciliation' AS checkpoint,
    (SELECT ods_count FROM source_counts WHERE source_system = 'OMS') AS oms_count,
    (SELECT ods_count FROM source_counts WHERE source_system = 'VEMS') AS vems_count,
    (SELECT stg_count FROM staging_count) AS staging_count,
    (SELECT ods_count FROM source_counts WHERE source_system = 'OMS') +
    (SELECT ods_count FROM source_counts WHERE source_system = 'VEMS') AS total_ods_count,
    (SELECT stg_count FROM staging_count) -
    ((SELECT ods_count FROM source_counts WHERE source_system = 'OMS') +
     (SELECT ods_count FROM source_counts WHERE source_system = 'VEMS')) AS records_lost
FROM dual;

-- Expected Results:
-- - staging_count should be <= total_ods_count (due to deduplication)
-- - records_lost should be small (<1% due to exact matches reducing count)
-- - Validate: oms_count + vems_count - exact_match_count ≈ staging_count

-- ------------------------------------------------------------
-- CHECKPOINT 5: System-of-Record Validation
-- ------------------------------------------------------------
-- Confirms SOR rules were applied correctly

SELECT
    'SOR Validation' AS checkpoint,
    source_system,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM stg_{ENTITY}s
WHERE batch_id = '{BATCH_ID}'
GROUP BY source_system
ORDER BY record_count DESC;

-- Expected Results (example for VETERAN entity):
-- - OMS: Should be highest % (OMS is primary for veterans)
-- - VEMS: Lower % (fallback source)
-- - MERGED: Records that required field-level merging

-- ------------------------------------------------------------
-- CHECKPOINT 6: Field Population Analysis
-- ------------------------------------------------------------
-- Shows completeness per field

SELECT
    'Field Population' AS checkpoint,
    COUNT(*) AS total_records,
    COUNT({BUSINESS_KEY}) AS {BUSINESS_KEY}_populated,
    COUNT(field1) AS field1_populated,
    COUNT(field2) AS field2_populated,
    COUNT(email) AS email_populated,
    COUNT(phone) AS phone_populated,
    ROUND(100.0 * COUNT({BUSINESS_KEY}) / COUNT(*), 2) AS {BUSINESS_KEY}_pct,
    ROUND(100.0 * COUNT(field1) / COUNT(*), 2) AS field1_pct,
    ROUND(100.0 * COUNT(email) / COUNT(*), 2) AS email_pct
FROM stg_{ENTITY}s
WHERE batch_id = '{BATCH_ID}';

-- Expected Results:
-- - Required fields (SSN, name): 100% populated
-- - Core attributes: >95% populated
-- - Contact fields: 70-90% populated
```

### Example: Veteran Validation Queries

```sql
-- CHECKPOINT 1: Veteran Crosswalk Match Summary
SELECT
    match_method,
    COUNT(*) AS record_count,
    ROUND(AVG(match_confidence), 2) AS avg_confidence,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM ref_entity_crosswalk_veteran
WHERE batch_id = 'BATCH_001'
GROUP BY match_method
ORDER BY record_count DESC;

-- CHECKPOINT 2: Veteran DQ Score Distribution
SELECT
    CASE
        WHEN dq_score >= 90 THEN 'Excellent (90-100)'
        WHEN dq_score >= 80 THEN 'Good (80-89)'
        WHEN dq_score >= 70 THEN 'Acceptable (70-79)'
        ELSE 'Poor (<70)'
    END AS dq_category,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM stg_veterans
WHERE batch_id = 'BATCH_001'
GROUP BY dq_category;

-- CHECKPOINT 3: Veteran Conflicts
SELECT
    conflict_type,
    COUNT(*) AS conflict_count,
    resolution_method
FROM ref_reconciliation_log
WHERE batch_id = 'BATCH_001'
  AND entity_type = 'VETERAN'
GROUP BY conflict_type, resolution_method
ORDER BY conflict_count DESC;
```

---

## Step 8: Execution Workflow

### Master Orchestration Procedure

```sql
-- Master Procedure: Orchestrate Complete Multi-Source Federation
CREATE OR REPLACE PROCEDURE sp_multisource_federation_master(
    p_batch_id VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET result STRING;

    -- Step 1: Build entity crosswalks
    CALL sp_build_crosswalk_veteran(:p_batch_id);
    CALL sp_build_crosswalk_evaluator(:p_batch_id);
    CALL sp_build_crosswalk_facility(:p_batch_id);
    -- Add more entities as needed

    -- Step 2: Merge data to staging
    CALL sp_merge_veteran_to_staging(:p_batch_id);
    CALL sp_merge_evaluator_to_staging(:p_batch_id);
    CALL sp_merge_facility_to_staging(:p_batch_id);
    -- Add more entities as needed

    -- Step 3: Validate results (optional - can be run separately)
    -- Run validation queries from Step 7

    RETURN 'Multi-source federation completed for batch: ' || :p_batch_id;
END;
$$;
```

### Execution Steps

```sql
-- ===========================================================
-- MULTI-SOURCE FEDERATION EXECUTION WORKFLOW
-- ===========================================================

-- PHASE 1: SETUP (One-Time)
-- -----------------------------------------------------------
-- Deploy all reference tables, procedures, and functions

-- PHASE 2: FIRST EXECUTION (Testing)
-- -----------------------------------------------------------

-- Step 1: Generate a unique batch ID
SET batch_id = 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

-- Step 2: Verify ODS data is loaded
SELECT
    source_system,
    COUNT(*) AS record_count
FROM ods_veterans_source
WHERE batch_id = $batch_id
GROUP BY source_system;

-- Step 3: Run master orchestration procedure
CALL sp_multisource_federation_master($batch_id);

-- Step 4: Run validation queries (Checkpoint 1-6)
-- Copy validation queries from Step 7 and replace {BATCH_ID} with $batch_id

-- Step 5: Review results
-- - Crosswalk quality (>95% avg confidence expected)
-- - DQ scores (>80% avg expected)
-- - Conflicts (review and validate resolution)
-- - Volume reconciliation (no data loss)

-- Step 6: If issues found, investigate and reprocess
-- DELETE FROM stg_{entity}s WHERE batch_id = $batch_id;
-- DELETE FROM ref_entity_crosswalk_{entity} WHERE batch_id = $batch_id;
-- [Fix data issues in ODS]
-- CALL sp_multisource_federation_master($batch_id);

-- PHASE 3: PRODUCTION DEPLOYMENT
-- -----------------------------------------------------------

-- Step 7: Schedule master procedure in production
-- - Daily, hourly, or event-driven based on SLA
-- - Monitor execution logs
-- - Alert on validation failures

-- Step 8: Ongoing monitoring
-- - Track DQ score trends
-- - Monitor conflict rates
-- - Volume reconciliation
-- - Performance metrics
```

---

## Validation Checkpoints

### Pre-Flight Checks (Before Execution)

- [ ] ODS tables contain data for both OMS and VEMS
- [ ] `ref_system_of_record` configured for all entities
- [ ] Code mapping tables populated (specialty, request_type, status, etc.)
- [ ] Field mapping tables documented (OMS and VEMS)
- [ ] Crosswalk procedures deployed
- [ ] Merge procedures deployed
- [ ] Validation queries tested

### Post-Execution Validation (After Running)

- [ ] **Checkpoint 1**: Crosswalk match summary shows >70% exact matches
- [ ] **Checkpoint 2**: DQ score distribution >70% in "Excellent" range
- [ ] **Checkpoint 3**: All conflicts logged with resolution method
- [ ] **Checkpoint 4**: Volume reconciliation shows <1% data loss
- [ ] **Checkpoint 5**: System-of-record rules applied correctly
- [ ] **Checkpoint 6**: Required fields 100% populated

### Quality Gates

| Metric | Threshold | Action if Below Threshold |
|--------|-----------|---------------------------|
| Avg Match Confidence | >95% | Investigate matching logic |
| Exact Match Rate | >70% | Check business key quality in ODS |
| Avg DQ Score | >80 | Review DQ rules and source data quality |
| Records Lost | <1% | Investigate merge logic |
| Conflicts Resolved | 100% | Ensure all conflicts have resolution_method |

---

## Customization Guide

### Quick Start: Federate a New Entity

1. **Define entity name and business key**
   - Entity: `{ENTITY}` (e.g., appointment, claim, evaluation)
   - Business key: `{BUSINESS_KEY}` (e.g., appointment_id, claim_number)

2. **Configure system-of-record**
   ```sql
   INSERT INTO ref_system_of_record VALUES (
       '{ENTITY}', 'OMS', 'VEMS', 'PREFER_PRIMARY'
   );
   ```

3. **Create crosswalk table**
   - Use template from Step 2
   - Replace `{ENTITY}` and `{BUSINESS_KEY}` placeholders

4. **Create code mapping tables** (if needed)
   - Use template from Step 3
   - Populate with OMS/VEMS code mappings

5. **Document field mappings**
   - Use template from Step 4
   - Map OMS and VEMS field names to standard

6. **Build crosswalk procedure**
   - Use template from Step 5
   - Customize matching logic for your business key

7. **Build merge procedure**
   - Use template from Step 6
   - Apply appropriate reconciliation rules

8. **Create validation queries**
   - Use template from Step 7
   - Customize for your entity

9. **Test and validate**
   - Run with test batch_id
   - Validate all checkpoints pass

10. **Add to master orchestration**
    - Include in `sp_multisource_federation_master`

### Advanced Customization

**Custom Matching Logic:**
- Fuzzy matching (SOUNDEX, Levenshtein distance)
- Multi-field matching (name + DOB + address)
- Confidence scoring based on multiple criteria

**Custom Reconciliation Rules:**
- Business-specific logic (e.g., prefer most complete record)
- Time-based rules (e.g., prefer data from last 30 days)
- Source-specific trust scores

**Code Translation with Fallbacks:**
```sql
CREATE FUNCTION fn_map_custom_code(p_source VARCHAR, p_code VARCHAR)
RETURNS VARCHAR AS
$$
    COALESCE(
        (SELECT standard_value FROM ref_code_mapping WHERE source_system = p_source AND source_code = p_code),
        p_code  -- Fallback: return original if no mapping found
    )
$$;
```

---

## Reference Documentation

For detailed implementation guidance, refer to:

1. **STAGING_LAYER_IMPLEMENTATION_GUIDE.md** - Step-by-step execution guide
2. **DATA_PIPELINE_ARCHITECTURE.md** - Architecture and design patterns (lines 315-624)
3. **VES_Multi_Source_Integration_Guide.html** - Visual guide for multi-source patterns
4. **README.md** - Project overview and multi-source integration summary

### Existing Implementation Files

- `/snowflake/ods/01_create_ods_tables.sql` - ODS tables (source landing)
- `/snowflake/reference/01_create_reference_tables.sql` - All reference/mapping tables
- `/snowflake/staging/02_staging_layer_oms_vems_merge_simplified.sql` - Multi-source procedures
- `/snowflake/etl/03_etl_procedures_multi_source.sql` - Code translation UDFs
- `/snowflake/monitoring/staging_layer_validation_queries.sql` - Validation queries

---

## Support and Troubleshooting

### Common Issues

**Issue: Low match confidence (<95%)**
- **Cause**: Business key quality issues in ODS (NULLs, format inconsistencies)
- **Solution**: Clean business keys before loading to ODS

**Issue: High conflict rate (>20%)**
- **Cause**: Divergent data in OMS and VEMS during transition
- **Solution**: Review reconciliation rules; may need MERGE_FIELDS instead of PREFER_PRIMARY

**Issue: Data loss (>1%)**
- **Cause**: Join logic excluding records with NULL business keys
- **Solution**: Use FULL OUTER JOIN and handle NULLs explicitly

**Issue: Poor DQ scores (<70)**
- **Cause**: Missing required fields in source systems
- **Solution**: Review DQ rules; adjust scoring weights

### Best Practices

1. **Always use batch_id for traceability**
2. **Run validation queries after every execution**
3. **Log all conflicts to ref_reconciliation_log**
4. **Use system-of-record configuration table (don't hardcode)**
5. **Document all code mappings in reference tables**
6. **Test with small batch first, then scale**
7. **Monitor DQ score trends over time**
8. **Review conflicts regularly for pattern detection**

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-20 | Initial template created based on VES Data Warehouse patterns |

---

**Template maintained by:** VES Data Engineering Team
**Last updated:** 2025-11-20
**Project:** VES Data Warehouse - Multi-Source Federation Framework
