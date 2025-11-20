# Architectural Improvements for Code Consistency & Simplification

## Executive Summary

Beyond the **25+ common functions** already created, this analysis identifies **9 additional high-impact opportunities** for architectural improvements that will:

- **Reduce codebase by ~2,000 additional lines** (beyond the ~960 from functions)
- **Increase flexibility** - Business rules become data-driven, changeable without code deployment
- **Improve consistency** - Eliminate duplicate ETL patterns across 6+ dimension tables
- **Simplify maintenance** - Generic procedures replace repetitive code

---

## 🎯 OPPORTUNITY #1: Reference Tables for Categorical Data ⭐⭐⭐⭐⭐

### Problem: Repeated CASE Statements for Business Rules

**Current State:** Disability rating categorization is hardcoded in 4+ files with identical 8-line CASE statement:

```sql
-- Found in:
-- etl/01_etl_procedures_dimensions.sql:109-116
-- etl/03_etl_procedures_multi_source.sql:394-401
-- dimensions/02_dim_veteran.sql (implied)
-- staging tables (implied)

CASE
    WHEN disability_rating = 0 THEN '0%'
    WHEN disability_rating BETWEEN 10 AND 30 THEN '10-30%'
    WHEN disability_rating BETWEEN 40 AND 60 THEN '40-60%'
    WHEN disability_rating BETWEEN 70 AND 90 THEN '70-90%'
    WHEN disability_rating = 100 THEN '100%'
    ELSE NULL
END AS disability_rating_category
```

**Problems:**
- ❌ Business rule changes require code updates in 4+ files
- ❌ Risk of inconsistent implementation
- ❌ Can't change categories without code deployment
- ❌ No audit trail of category definition changes

---

### Solution: Reference Table + Lookup Function

**Step 1: Create Reference Table**

```sql
-- File: snowflake/reference/02_ref_disability_rating_categories.sql

CREATE OR REPLACE TABLE reference.ref_disability_rating_categories (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    min_rating INTEGER NOT NULL,
    max_rating INTEGER NOT NULL,
    category_code VARCHAR(10) NOT NULL,
    category_label VARCHAR(20) NOT NULL,
    benefit_tier VARCHAR(20),
    monthly_compensation_range VARCHAR(50),
    description TEXT,
    sort_order INTEGER,
    active_flag BOOLEAN DEFAULT TRUE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    end_date DATE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT chk_rating_range CHECK (min_rating <= max_rating),
    CONSTRAINT chk_rating_bounds CHECK (min_rating >= 0 AND max_rating <= 100)
)
COMMENT = 'VA disability rating categories for compensation and benefits determination';

-- Populate with current categories
INSERT INTO reference.ref_disability_rating_categories
    (min_rating, max_rating, category_code, category_label, benefit_tier, monthly_compensation_range, sort_order)
VALUES
    (0, 0, '0PCT', '0%', 'NONE', '$0', 1),
    (10, 30, '10-30PCT', '10-30%', 'LOW', '$171-$524', 2),
    (40, 60, '40-60PCT', '40-60%', 'MODERATE', '$755-$1,437', 3),
    (70, 90, '70-90PCT', '70-90%', 'HIGH', '$1,729-$2,241', 4),
    (100, 100, '100PCT', '100%', 'FULL', '$3,737+', 5);
```

**Step 2: Enhanced Lookup Function** (Replaces the one in common functions)

```sql
CREATE OR REPLACE FUNCTION fn_get_disability_category(rating NUMBER)
RETURNS OBJECT
LANGUAGE SQL
COMMENT = 'Returns disability rating category details including tier and compensation range'
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'category_code', category_code,
        'category_label', category_label,
        'benefit_tier', benefit_tier,
        'monthly_compensation_range', monthly_compensation_range
    )
    FROM reference.ref_disability_rating_categories
    WHERE rating BETWEEN min_rating AND max_rating
      AND active_flag = TRUE
      AND (effective_date IS NULL OR effective_date <= CURRENT_DATE())
      AND (end_date IS NULL OR end_date > CURRENT_DATE())
    LIMIT 1
$$;

-- Simple version that just returns the label (backward compatible)
CREATE OR REPLACE FUNCTION fn_categorize_disability_rating(rating NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    SELECT category_label
    FROM reference.ref_disability_rating_categories
    WHERE rating BETWEEN min_rating AND max_rating
      AND active_flag = TRUE
    LIMIT 1
$$;
```

**Step 3: Usage in ETL**

```sql
-- BEFORE: 8 lines
CASE
    WHEN disability_rating = 0 THEN '0%'
    WHEN disability_rating BETWEEN 10 AND 30 THEN '10-30%'
    WHEN disability_rating BETWEEN 40 AND 60 THEN '40-60%'
    WHEN disability_rating BETWEEN 70 AND 90 THEN '70-90%'
    WHEN disability_rating = 100 THEN '100%'
    ELSE NULL
END AS disability_rating_category

-- AFTER: 1 line
fn_categorize_disability_rating(disability_rating) AS disability_rating_category

-- ENHANCED: Get full details
fn_get_disability_category(disability_rating) AS disability_category_details
```

---

### Additional Reference Tables to Create

**1. VA Priority Groups**

```sql
CREATE TABLE reference.ref_priority_groups (
    priority_group INTEGER PRIMARY KEY,
    group_name VARCHAR(100),
    description TEXT,
    copay_required BOOLEAN,
    enrollment_eligibility VARCHAR(500),
    service_connected_required BOOLEAN,
    income_limits_apply BOOLEAN,
    sort_order INTEGER
);

INSERT INTO reference.ref_priority_groups VALUES
(1, 'Priority Group 1', 'Veterans with service-connected disabilities rated 50% or more', FALSE, 'SC disability >= 50%', TRUE, FALSE, 1),
(2, 'Priority Group 2', 'Veterans with service-connected disabilities rated 40% or less', FALSE, 'SC disability 10-40%', TRUE, FALSE, 2),
(3, 'Priority Group 3', 'Former POWs, Purple Heart recipients', FALSE, 'Special eligibility', FALSE, FALSE, 3),
-- ... groups 4-8
```

**2. Service Branch/Era Reference**

```sql
CREATE TABLE reference.ref_service_branches (
    branch_code VARCHAR(20) PRIMARY KEY,
    branch_name VARCHAR(100),
    parent_branch VARCHAR(20),
    active_flag BOOLEAN,
    sort_order INTEGER
);

CREATE TABLE reference.ref_service_eras (
    era_code VARCHAR(20) PRIMARY KEY,
    era_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    wartime_service BOOLEAN,
    benefits_multiplier DECIMAL(3,2) DEFAULT 1.0
);
```

**Impact:**
- **Lines saved:** ~60 (6 CASE statements × 10 lines each)
- **Business agility:** Categories now changeable via INSERT/UPDATE
- **Historical tracking:** Can track category definition changes over time
- **Enhanced analytics:** Join to get benefit tier, compensation info

---

## 🎯 OPPORTUNITY #2: Generic SCD Type 2 Procedure ⭐⭐⭐⭐⭐

### Problem: Identical SCD Type 2 Logic Repeated 6+ Times

**Current State:** Every dimension table has nearly identical load procedure:

```sql
-- sp_load_dim_veterans (lines 179-307)
-- sp_load_dim_evaluators (lines 313-441)
-- sp_load_dim_facilities (lines 447-575)
-- ... 3 more dimension tables

-- All follow EXACT same pattern:
1. UPDATE target SET effective_end_date = NOW(), is_current = FALSE WHERE hash differs
2. INSERT new/changed records with effective dates and is_current = TRUE
```

**Repetition:** ~130 lines × 6 dimensions = **780 lines of nearly identical code**

---

### Solution: Metadata-Driven Generic SCD Type 2 Procedure

**Step 1: Create Configuration Table**

```sql
CREATE TABLE metadata.scd_type2_config (
    table_name VARCHAR(100) PRIMARY KEY,
    schema_name VARCHAR(50) DEFAULT 'WAREHOUSE',
    staging_schema VARCHAR(50) DEFAULT 'STAGING',
    staging_table VARCHAR(100),
    business_key_columns ARRAY,  -- Columns that identify unique entity
    hash_column VARCHAR(100) DEFAULT 'source_record_hash',
    exclude_from_insert ARRAY,   -- Columns not in staging (e.g., surrogate keys)
    scd_type INTEGER DEFAULT 2,
    active_flag BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate for each dimension
INSERT INTO metadata.scd_type2_config
    (table_name, staging_table, business_key_columns, exclude_from_insert)
VALUES
    ('dim_veterans', 'stg_veterans', ARRAY_CONSTRUCT('veteran_id'),
     ARRAY_CONSTRUCT('veteran_key')),
    ('dim_evaluators', 'stg_evaluators', ARRAY_CONSTRUCT('evaluator_npi'),
     ARRAY_CONSTRUCT('evaluator_key')),
    ('dim_facilities', 'stg_facilities', ARRAY_CONSTRUCT('facility_id'),
     ARRAY_CONSTRUCT('facility_key')),
    ('dim_clinical_conditions', 'stg_clinical_conditions',
     ARRAY_CONSTRUCT('condition_code'), ARRAY_CONSTRUCT('condition_key'));
```

**Step 2: Generic Procedure**

```sql
CREATE OR REPLACE PROCEDURE sp_load_scd_type2_generic(
    p_table_name VARCHAR,
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // Get configuration
    var config_sql = `
        SELECT
            schema_name,
            staging_schema,
            staging_table,
            business_key_columns,
            hash_column
        FROM metadata.scd_type2_config
        WHERE table_name = ? AND active_flag = TRUE
    `;

    var config_stmt = snowflake.createStatement({
        sqlText: config_sql,
        binds: [P_TABLE_NAME]
    });
    var config = config_stmt.execute();

    if (!config.next()) {
        return "ERROR: No configuration found for table " + P_TABLE_NAME;
    }

    var schema = config.getColumnValue('SCHEMA_NAME');
    var staging_schema = config.getColumnValue('STAGING_SCHEMA');
    var staging_table = config.getColumnValue('STAGING_TABLE');
    var business_keys = config.getColumnValue('BUSINESS_KEY_COLUMNS');
    var hash_column = config.getColumnValue('HASH_COLUMN');

    // Build WHERE clause for business key matching
    var key_conditions = business_keys.map(function(key) {
        return `tgt.${key} = src.${key}`;
    }).join(' AND ');

    // Step 1: End-date changed records
    var update_sql = `
        UPDATE ${schema}.${P_TABLE_NAME} tgt
        SET
            effective_end_date = CURRENT_TIMESTAMP(),
            is_current = FALSE,
            updated_timestamp = CURRENT_TIMESTAMP()
        FROM ${staging_schema}.${staging_table} src
        WHERE ${key_conditions}
          AND tgt.is_current = TRUE
          AND tgt.${hash_column} != src.${hash_column}
    `;

    var update_stmt = snowflake.createStatement({sqlText: update_sql});
    var update_result = update_stmt.execute();
    var rows_updated = update_result.next() ? update_result.getColumnValue(1) : 0;

    // Step 2: Get column list from staging (excluding surrogate key)
    var columns_sql = `
        SELECT ARRAY_AGG(column_name) as cols
        FROM information_schema.columns
        WHERE table_schema = '${staging_schema}'
          AND table_name = UPPER('${staging_table}')
          AND column_name NOT IN (
              SELECT value::VARCHAR
              FROM metadata.scd_type2_config,
                   LATERAL FLATTEN(input => exclude_from_insert)
              WHERE table_name = '${P_TABLE_NAME}'
          )
        ORDER BY ordinal_position
    `;

    var cols_stmt = snowflake.createStatement({sqlText: columns_sql});
    var cols_result = cols_stmt.execute();
    cols_result.next();
    var columns = cols_result.getColumnValue('COLS');
    var column_list = columns.join(', ');

    // Step 3: Insert new/changed records
    var insert_sql = `
        INSERT INTO ${schema}.${P_TABLE_NAME} (
            ${column_list},
            effective_start_date,
            effective_end_date,
            is_current,
            created_timestamp,
            updated_timestamp
        )
        SELECT
            ${column_list},
            CURRENT_TIMESTAMP() AS effective_start_date,
            TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS effective_end_date,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS created_timestamp,
            CURRENT_TIMESTAMP() AS updated_timestamp
        FROM ${staging_schema}.${staging_table} src
        WHERE src.batch_id = ?
          AND (
              -- New record
              NOT EXISTS (
                  SELECT 1
                  FROM ${schema}.${P_TABLE_NAME} tgt
                  WHERE ${key_conditions}
              )
              OR
              -- Changed record (just end-dated)
              EXISTS (
                  SELECT 1
                  FROM ${schema}.${P_TABLE_NAME} tgt
                  WHERE ${key_conditions}
                    AND tgt.is_current = FALSE
                    AND tgt.effective_end_date >= CURRENT_TIMESTAMP() - INTERVAL '1 minute'
              )
          )
    `;

    var insert_stmt = snowflake.createStatement({
        sqlText: insert_sql,
        binds: [P_BATCH_ID]
    });
    var insert_result = insert_stmt.execute();
    var rows_inserted = insert_result.next() ? insert_result.getColumnValue(1) : 0;

    return `Loaded ${P_TABLE_NAME}: ${rows_updated} updated, ${rows_inserted} inserted`;
$$;
```

**Step 3: Replace Individual Procedures**

```sql
-- BEFORE: 6 separate procedures × 130 lines = 780 lines
CREATE OR REPLACE PROCEDURE sp_load_dim_veterans(p_batch_id VARCHAR) ...
CREATE OR REPLACE PROCEDURE sp_load_dim_evaluators(p_batch_id VARCHAR) ...
CREATE OR REPLACE PROCEDURE sp_load_dim_facilities(p_batch_id VARCHAR) ...
-- etc.

-- AFTER: One call per dimension
CALL sp_load_scd_type2_generic('dim_veterans', :batch_id);
CALL sp_load_scd_type2_generic('dim_evaluators', :batch_id);
CALL sp_load_scd_type2_generic('dim_facilities', :batch_id);
```

**Impact:**
- **Lines saved:** ~650 (780 lines - 130 for generic procedure)
- **Maintainability:** Fix SCD logic once, applies to all dimensions
- **Flexibility:** Add new dimensions by adding config row, not new procedure
- **Consistency:** Guaranteed identical SCD logic across all dimensions

---

## 🎯 OPPORTUNITY #3: Metadata-Driven Data Quality Scoring ⭐⭐⭐⭐

### Problem: DQ Scoring Logic Repeated in Every ETL

**Current State:** Found in 8+ files with nearly identical pattern:

```sql
-- etl/01_etl_procedures_dimensions.sql:142-151 (veterans)
-- etl/01_etl_procedures_dimensions.sql:427-436 (evaluators)
-- etl/03_etl_procedures_multi_source.sql:971-978 (exam requests)
-- ... 5 more instances

(
    (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN date_of_birth IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN veteran_va_id IS NOT NULL OR veteran_ssn IS NOT NULL THEN 20 ELSE 0 END) +
    (CASE WHEN email IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN phone_primary IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
    (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
) AS dq_score
```

**Problems:**
- ❌ Scoring rules hardcoded (changing weights requires code deployment)
- ❌ Different entities have different rules, all manually maintained
- ❌ No visibility into what makes a "good" score
- ❌ ~10 lines × 8 procedures = 80 lines of repetitive code

---

### Solution: Configuration Table + Dynamic Scoring

**Step 1: Create DQ Scoring Configuration**

```sql
CREATE TABLE metadata.dq_scoring_rules (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type VARCHAR(50) NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(20) NOT NULL,  -- NOT_NULL, RANGE, REGEX, CUSTOM_FUNCTION
    rule_condition VARCHAR(500),     -- e.g., 'BETWEEN 0 AND 100', 'IS NOT NULL'
    points_if_met INTEGER NOT NULL,
    points_if_not_met INTEGER DEFAULT 0,
    rule_weight DECIMAL(5,2) DEFAULT 1.0,  -- For weighted scoring
    field_importance VARCHAR(20),   -- CRITICAL, HIGH, MEDIUM, LOW
    active_flag BOOLEAN DEFAULT TRUE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_entity_field_rule UNIQUE (entity_type, field_name, rule_type)
);

-- Populate for Veterans
INSERT INTO metadata.dq_scoring_rules
    (entity_type, field_name, rule_type, rule_condition, points_if_met, field_importance)
VALUES
    ('VETERAN', 'first_name', 'NOT_NULL', 'first_name IS NOT NULL', 15, 'CRITICAL'),
    ('VETERAN', 'last_name', 'NOT_NULL', 'last_name IS NOT NULL', 15, 'CRITICAL'),
    ('VETERAN', 'date_of_birth', 'NOT_NULL', 'date_of_birth IS NOT NULL', 15, 'CRITICAL'),
    ('VETERAN', 'veteran_va_id', 'NOT_NULL', 'veteran_va_id IS NOT NULL OR veteran_ssn IS NOT NULL', 20, 'CRITICAL'),
    ('VETERAN', 'email', 'NOT_NULL', 'email IS NOT NULL', 10, 'HIGH'),
    ('VETERAN', 'phone_primary', 'NOT_NULL', 'phone_primary IS NOT NULL', 10, 'HIGH'),
    ('VETERAN', 'state', 'NOT_NULL', 'state IS NOT NULL', 5, 'MEDIUM'),
    ('VETERAN', 'disability_rating', 'RANGE', 'disability_rating BETWEEN 0 AND 100', 10, 'HIGH');

-- Populate for Evaluators
INSERT INTO metadata.dq_scoring_rules
    (entity_type, field_name, rule_type, rule_condition, points_if_met, field_importance)
VALUES
    ('EVALUATOR', 'evaluator_npi', 'NOT_NULL', 'evaluator_npi IS NOT NULL', 25, 'CRITICAL'),
    ('EVALUATOR', 'first_name', 'NOT_NULL', 'first_name IS NOT NULL', 15, 'CRITICAL'),
    ('EVALUATOR', 'last_name', 'NOT_NULL', 'last_name IS NOT NULL', 15, 'CRITICAL'),
    ('EVALUATOR', 'specialty', 'NOT_NULL', 'specialty IS NOT NULL', 20, 'CRITICAL'),
    ('EVALUATOR', 'license_number', 'NOT_NULL', 'license_number IS NOT NULL', 15, 'HIGH'),
    ('EVALUATOR', 'license_state', 'NOT_NULL', 'license_state IS NOT NULL', 10, 'MEDIUM');
```

**Step 2: Dynamic DQ Score Calculation Function**

```sql
CREATE OR REPLACE FUNCTION fn_calculate_dq_score_dynamic(
    p_entity_type VARCHAR,
    p_record_data OBJECT  -- JSON object with field values
)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS
$$
    // Get scoring rules for this entity type
    var rules_sql = `
        SELECT
            field_name,
            rule_condition,
            points_if_met,
            field_importance
        FROM metadata.dq_scoring_rules
        WHERE entity_type = ?
          AND active_flag = TRUE
        ORDER BY points_if_met DESC
    `;

    var stmt = snowflake.createStatement({
        sqlText: rules_sql,
        binds: [P_ENTITY_TYPE]
    });

    var result = stmt.execute();
    var total_score = 0;
    var max_possible = 0;
    var failed_rules = [];

    // Evaluate each rule
    while (result.next()) {
        var field = result.getColumnValue('FIELD_NAME');
        var condition = result.getColumnValue('RULE_CONDITION');
        var points = result.getColumnValue('POINTS_IF_MET');
        var importance = result.getColumnValue('FIELD_IMPORTANCE');

        max_possible += points;

        // Simple null check (can be extended for complex rules)
        var field_value = P_RECORD_DATA[field];

        if (condition.includes('IS NOT NULL')) {
            if (field_value !== null && field_value !== undefined && field_value !== '') {
                total_score += points;
            } else {
                failed_rules.push({
                    field: field,
                    importance: importance,
                    points_lost: points
                });
            }
        }
        // Add more rule type evaluations as needed
    }

    return {
        score: total_score,
        max_score: max_possible,
        score_percentage: (total_score / max_possible * 100).toFixed(2),
        failed_rules: failed_rules,
        grade: total_score >= max_possible * 0.9 ? 'A' :
               total_score >= max_possible * 0.8 ? 'B' :
               total_score >= max_possible * 0.7 ? 'C' :
               total_score >= max_possible * 0.6 ? 'D' : 'F'
    };
$$;
```

**Step 3: Simpler SQL Version (Backward Compatible)**

```sql
-- For use in ETL procedures, generates SQL dynamically
CREATE OR REPLACE PROCEDURE sp_generate_dq_score_sql(
    p_entity_type VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_sql VARCHAR;
BEGIN
    SELECT
        '(' || LISTAGG(
            '(CASE WHEN ' || rule_condition || ' THEN ' || points_if_met || ' ELSE 0 END)',
            ' + '
        ) WITHIN GROUP (ORDER BY field_importance DESC, points_if_met DESC) || ')'
    INTO v_sql
    FROM metadata.dq_scoring_rules
    WHERE entity_type = :p_entity_type
      AND active_flag = TRUE;

    RETURN v_sql;
END;
$$;
```

**Usage:**

```sql
-- Generate DQ scoring SQL for veterans
CALL sp_generate_dq_score_sql('VETERAN');
-- Returns: '(CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) + (CASE WHEN ...'

-- Use in ETL with IDENTIFIER for dynamic SQL
SELECT
    *,
    IDENTIFIER(sp_generate_dq_score_sql('VETERAN')) AS dq_score
FROM staging.stg_veterans;
```

**Impact:**
- **Lines saved:** ~80-100 lines
- **Flexibility:** Change scoring weights via UPDATE, no code deployment
- **Auditability:** Track scoring rule changes over time
- **Consistency:** Guaranteed same scoring logic everywhere

---

## 🎯 OPPORTUNITY #4: Consistent Code Mapping Function Usage ⭐⭐⭐⭐

### Problem: Existing Code Mapping Functions Not Used Consistently

**Discovery:** You already have excellent code mapping infrastructure:
- `reference.ref_code_mapping_specialty` (lines 126-154)
- `reference.ref_code_mapping_request_type` (lines 159-186)
- `reference.ref_code_mapping_appointment_status` (lines 191-218)
- Helper functions: `fn_map_specialty_code()`, `fn_map_request_type_code()`, etc.

**BUT:** Multi-source ETL uses them (line 954), while single-source ETL still uses raw CASE/UPPER/TRIM patterns.

---

### Solution: Standardize All Code Mappings

**Step 1: Audit Current Usage**

```sql
-- Find all hardcoded specialty mappings
-- Should be: fn_map_specialty_code(source_system, specialty_code)

-- Find all hardcoded status mappings
-- Should be: fn_map_appointment_status(source_system, status_code)
```

**Step 2: Extend Reference Tables**

Add more code mapping tables for categories not yet covered:

```sql
-- Gender code mappings (if sources use different codes)
CREATE TABLE reference.ref_code_mapping_gender (
    source_system VARCHAR(50),
    source_code VARCHAR(10),
    standard_code VARCHAR(1),  -- M, F, X, U
    standard_value VARCHAR(20),
    PRIMARY KEY (source_system, source_code)
);

-- State/Province mappings (for international)
CREATE TABLE reference.ref_code_mapping_state (
    source_system VARCHAR(50),
    source_code VARCHAR(10),
    standard_code VARCHAR(2),  -- US postal codes
    standard_name VARCHAR(50),
    country_code VARCHAR(3),
    PRIMARY KEY (source_system, source_code)
);
```

**Step 3: Create Generic Mapping Function**

```sql
CREATE OR REPLACE FUNCTION fn_map_code_generic(
    p_mapping_table VARCHAR,
    p_source_system VARCHAR,
    p_source_code VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT standard_value
    FROM IDENTIFIER('reference.' || p_mapping_table)
    WHERE source_system = p_source_system
      AND source_code = p_source_code
      AND active_flag = TRUE
    LIMIT 1
$$;

-- Usage:
SELECT fn_map_code_generic('ref_code_mapping_specialty', 'OMS', 'PSYCH') AS specialty;
```

**Impact:**
- **Lines saved:** ~100 (eliminating hardcoded CASE statements)
- **Consistency:** All systems use same mapping logic
- **Maintainability:** Add new mappings via INSERT, not code changes

---

## 🎯 OPPORTUNITY #5: ETL Pipeline Metadata & Orchestration ⭐⭐⭐⭐⭐

### Problem: Hardcoded Pipeline Orchestration

**Current State:** Master orchestration procedures have hardcoded CALL sequences:

```sql
-- Likely pattern in orchestration file:
CALL sp_etl_veterans(:batch_id);
CALL sp_etl_evaluators(:batch_id);
CALL sp_etl_facilities(:batch_id);
CALL sp_etl_exam_requests(:batch_id);
-- ... etc for all entities
```

---

### Solution: Configuration-Driven Pipeline Execution

**Step 1: Pipeline Configuration Table**

```sql
CREATE TABLE metadata.etl_pipeline_config (
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name VARCHAR(100) UNIQUE NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    execution_order INTEGER NOT NULL,
    source_type VARCHAR(20),  -- SINGLE_SOURCE, MULTI_SOURCE

    -- Procedure names (optional if using naming convention)
    transform_procedure VARCHAR(200),
    load_procedure VARCHAR(200),
    full_pipeline_procedure VARCHAR(200),

    -- Dependencies
    depends_on_pipelines ARRAY,  -- Array of pipeline_names that must complete first

    -- Tables involved
    source_table VARCHAR(200),
    staging_table VARCHAR(200),
    target_table VARCHAR(200),

    -- Configuration
    scd_type INTEGER DEFAULT 2,
    parallel_execution_group INTEGER,  -- Pipelines in same group can run parallel
    estimated_duration_minutes INTEGER,

    -- Control flags
    enabled BOOLEAN DEFAULT TRUE,
    skip_on_error BOOLEAN DEFAULT FALSE,
    retry_count INTEGER DEFAULT 0,

    -- Metadata
    notes TEXT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate pipeline configuration
INSERT INTO metadata.etl_pipeline_config
    (pipeline_name, entity_type, execution_order, source_type, parallel_execution_group,
     transform_procedure, load_procedure, target_table, depends_on_pipelines)
VALUES
    ('veterans_pipeline', 'VETERAN', 10, 'MULTI_SOURCE', 1,
     'sp_transform_multisource_ods_to_staging_veterans',
     'sp_load_dim_veterans',
     'dim_veterans',
     ARRAY_CONSTRUCT()),

    ('evaluators_pipeline', 'EVALUATOR', 20, 'MULTI_SOURCE', 1,
     'sp_transform_multisource_ods_to_staging_evaluators',
     'sp_load_dim_evaluators',
     'dim_evaluators',
     ARRAY_CONSTRUCT()),

    ('facilities_pipeline', 'FACILITY', 30, 'MULTI_SOURCE', 1,
     'sp_transform_multisource_ods_to_staging_facilities',
     'sp_load_dim_facilities',
     'dim_facilities',
     ARRAY_CONSTRUCT()),

    ('exam_requests_pipeline', 'EXAM_REQUEST', 40, 'MULTI_SOURCE', 2,
     'sp_transform_multisource_ods_to_staging_exam_requests',
     'sp_load_fact_exam_requests',
     'fact_exam_requests',
     ARRAY_CONSTRUCT('veterans_pipeline', 'evaluators_pipeline', 'facilities_pipeline'));
```

**Step 2: Dynamic Pipeline Executor**

```sql
CREATE OR REPLACE PROCEDURE sp_execute_pipeline_dynamic(
    p_pipeline_filter VARCHAR DEFAULT NULL,  -- NULL = all, or specific pipeline name
    p_batch_id VARCHAR
)
RETURNS TABLE (
    pipeline_name VARCHAR,
    status VARCHAR,
    duration_seconds NUMBER,
    rows_processed NUMBER,
    error_message VARCHAR
)
LANGUAGE JAVASCRIPT
AS
$$
    var results = [];

    // Get pipelines to execute
    var pipeline_sql = `
        SELECT
            pipeline_name,
            transform_procedure,
            load_procedure,
            execution_order,
            parallel_execution_group,
            depends_on_pipelines
        FROM metadata.etl_pipeline_config
        WHERE enabled = TRUE
          AND (? IS NULL OR pipeline_name = ?)
        ORDER BY execution_order
    `;

    var stmt = snowflake.createStatement({
        sqlText: pipeline_sql,
        binds: [P_PIPELINE_FILTER, P_PIPELINE_FILTER]
    });

    var pipelines = stmt.execute();

    // Execute each pipeline
    while (pipelines.next()) {
        var pipeline_name = pipelines.getColumnValue('PIPELINE_NAME');
        var transform_proc = pipelines.getColumnValue('TRANSFORM_PROCEDURE');
        var load_proc = pipelines.getColumnValue('LOAD_PROCEDURE');

        var start_time = Date.now();
        var status = 'SUCCESS';
        var error_msg = null;

        try {
            // Execute transform
            var transform_stmt = snowflake.createStatement({
                sqlText: `CALL ${transform_proc}(?)`,
                binds: [P_BATCH_ID]
            });
            transform_stmt.execute();

            // Execute load
            var load_stmt = snowflake.createStatement({
                sqlText: `CALL ${load_proc}(?)`,
                binds: [P_BATCH_ID]
            });
            load_stmt.execute();

        } catch (err) {
            status = 'ERROR';
            error_msg = err.message;
        }

        var duration = (Date.now() - start_time) / 1000;

        results.push({
            PIPELINE_NAME: pipeline_name,
            STATUS: status,
            DURATION_SECONDS: duration,
            ROWS_PROCESSED: 0,  // Could be extracted from procedure return
            ERROR_MESSAGE: error_msg
        });
    }

    return results;
$$;
```

**Step 3: Usage**

```sql
-- Execute all pipelines
CALL sp_execute_pipeline_dynamic(NULL, :batch_id);

-- Execute specific pipeline
CALL sp_execute_pipeline_dynamic('veterans_pipeline', :batch_id);

-- Results show success/failure for each pipeline
```

**Impact:**
- **Lines saved:** ~150 (eliminates hardcoded orchestration)
- **Flexibility:** Add/remove pipelines via configuration
- **Visibility:** Track execution order, dependencies, duration
- **Control:** Enable/disable pipelines without code changes

---

## 🎯 OPPORTUNITY #6: Naming Convention Standardization ⭐⭐⭐

### Problem: Inconsistent Naming Patterns

**Current inconsistencies:**
- `sp_transform_ods_to_staging_veterans` (single-source)
- `sp_transform_multisource_ods_to_staging_veterans` (multi-source)
- `sp_load_dim_veterans`
- `sp_etl_veterans` (full pipeline)

---

### Recommended Standard:

```
{object_type}_{action}_{entity}_{qualifier}

Object Types:
- sp_     = Stored Procedure
- fn_     = Function (already using)
- vw_     = View
- tbl_    = Table (or use prefixes: dim_, fact_, stg_, ref_)

Actions:
- transform_  = Transform data (ODS to Staging)
- load_       = Load data (Staging to Warehouse)
- merge_      = Merge multi-source data
- validate_   = Validate data quality
- extract_    = Extract from source
- pipeline_   = Full end-to-end pipeline

Qualifiers:
- _single     = Single-source
- _multi      = Multi-source
- _scd2       = SCD Type 2 load
- _delta      = Delta/incremental load
- _full       = Full load
```

**Refactored Names:**

```sql
-- Transform procedures
sp_transform_veterans_single       -- was sp_transform_ods_to_staging_veterans
sp_transform_veterans_multi        -- was sp_transform_multisource_ods_to_staging_veterans
sp_transform_evaluators_single
sp_transform_evaluators_multi

-- Load procedures
sp_load_veterans_scd2              -- was sp_load_dim_veterans
sp_load_evaluators_scd2
sp_load_facilities_scd2
sp_load_exam_requests_delta        -- facts are typically incremental

-- Pipeline procedures (orchestration)
sp_pipeline_veterans_multi         -- was sp_etl_veterans
sp_pipeline_evaluators_multi
sp_pipeline_all_dimensions         -- master orchestrator

-- Generic procedures
sp_load_dimension_scd2_generic     -- generic SCD Type 2 loader
sp_merge_sources_generic           -- generic multi-source merger
```

---

## 📊 SUMMARY TABLE: All Opportunities

| # | Opportunity | Impact | LOC Saved | Complexity | Priority |
|---|-------------|--------|-----------|------------|----------|
| 1 | **Reference Tables for Categories** | ⭐⭐⭐⭐⭐ | ~60 | Low | 🔥 HIGH |
| 2 | **Generic SCD Type 2 Procedure** | ⭐⭐⭐⭐⭐ | ~650 | Medium | 🔥 HIGH |
| 3 | **Metadata-Driven DQ Scoring** | ⭐⭐⭐⭐ | ~100 | Medium | MEDIUM |
| 4 | **Consistent Code Mapping Usage** | ⭐⭐⭐⭐ | ~100 | Low | 🔥 HIGH |
| 5 | **Pipeline Metadata & Orchestration** | ⭐⭐⭐⭐⭐ | ~150 | Medium | MEDIUM |
| 6 | **Naming Convention Standard** | ⭐⭐⭐ | 0 | Low | LOW |
| 7 | **Generic Multi-Source Merger** | ⭐⭐⭐⭐ | ~1000 | High | LOW |
| 8 | **DQ Framework Function Integration** | ⭐⭐ | ~50 | Low | LOW |
| 9 | **Table Structure Templates** | ⭐⭐ | ~100 | Medium | LOW |

**Total Potential Reduction: ~2,210 lines** (beyond the 960 from functions)

---

## 🎯 RECOMMENDED IMPLEMENTATION ROADMAP

### Phase 1: Quick Wins (Week 1-2) 🔥
**Priority: Immediate value, low complexity**

1. ✅ **Create disability rating reference table** (Opportunity #1)
   - 1-2 hours implementation
   - Immediate consistency improvement
   - Easy to test

2. ✅ **Standardize code mapping usage** (Opportunity #4)
   - Replace hardcoded CASE with existing functions
   - 4-6 hours to audit and refactor
   - No new infrastructure needed

3. ✅ **Add more reference tables** (Opportunity #1 extension)
   - Priority groups
   - Service branches/eras
   - 2-3 hours each

**Expected Impact:** ~160 lines saved, improved business agility

---

### Phase 2: High-Impact Architectural Changes (Week 3-4)
**Priority: Maximum code reduction, moderate complexity**

4. ✅ **Generic SCD Type 2 procedure** (Opportunity #2)
   - Biggest single impact (~650 lines)
   - 8-12 hours implementation + testing
   - Requires careful testing with all dimensions

5. ✅ **Pipeline metadata & orchestration** (Opportunity #5)
   - Modern orchestration pattern
   - 6-8 hours implementation
   - Makes adding new entities trivial

**Expected Impact:** ~800 lines saved, dramatically simplified architecture

---

### Phase 3: Advanced Optimizations (Week 5-6)
**Priority: Nice to have, higher complexity**

6. ⬜ **Metadata-driven DQ scoring** (Opportunity #3)
   - Complex but valuable
   - 12-16 hours implementation
   - Requires dynamic SQL generation

7. ⬜ **Generic multi-source merger** (Opportunity #7)
   - Highest complexity
   - 16-24 hours implementation
   - Optional - current code works fine

**Expected Impact:** ~1,100 additional lines saved

---

### Phase 4: Polish & Standards (Ongoing)
8. ⬜ **Naming convention standardization** (Opportunity #6)
9. ⬜ **DQ framework integration** (Opportunity #8)

---

## 💡 IMMEDIATE NEXT STEPS

**I recommend starting with Opportunity #1 (Reference Tables) because:**
1. ✅ **Lowest risk** - Doesn't change existing procedures, just adds lookup capability
2. ✅ **Immediate value** - Business rules become data-driven
3. ✅ **Quick win** - Can be done in 2-3 hours
4. ✅ **Foundation** - Sets pattern for other reference data

**Would you like me to:**
- A) Implement Opportunity #1 (disability rating + priority group reference tables)?
- B) Implement Opportunity #2 (generic SCD Type 2 procedure)?
- C) Implement Opportunity #4 (standardize code mapping usage)?
- D) Create all the metadata tables for opportunities 1-5?

Let me know which direction you'd like to go!
