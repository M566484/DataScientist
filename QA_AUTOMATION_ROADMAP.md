# VES Data Warehouse - QA Automation Roadmap

**Version:** 1.0
**Date:** 2025-11-17
**Purpose:** Automated testing strategy for multi-source data integration with minimal manual effort

---

## Executive Summary

### Challenge
Lean team with 8-week delivery timeline needs comprehensive QA without dedicated QA engineers.

### Solution
**Automated Testing Framework** using native Snowflake capabilities:
- SQL-based test scripts (no external tools needed)
- Automated test execution via Snowflake TASKS
- Self-documenting test results tables
- Continuous validation during ETL runs
- Pre-deployment smoke tests

### Benefits
- ✅ **90% reduction** in manual testing effort
- ✅ **Zero external tools** - all tests in Snowflake SQL
- ✅ **Built-in during development** - not a separate phase
- ✅ **Regression protection** - tests run automatically
- ✅ **Early issue detection** - tests run with every ETL

---

## QA Automation Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    TEST FRAMEWORK LAYERS                      │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  Layer 1: UNIT TESTS (Component Level)                       │
│  ├─ Table structure validation                               │
│  ├─ Data type constraints                                    │
│  ├─ Referential integrity                                    │
│  └─ Business rule validation                                 │
│                                                               │
│  Layer 2: INTEGRATION TESTS (Data Flow)                      │
│  ├─ ODS → Staging transformation                             │
│  ├─ Entity matching accuracy                                 │
│  ├─ Code mapping validation                                  │
│  └─ Staging → Warehouse loading                              │
│                                                               │
│  Layer 3: DATA QUALITY TESTS (Reconciliation)                │
│  ├─ Record count validation                                  │
│  ├─ OMS vs VEMS comparison                                   │
│  ├─ Duplicate detection                                      │
│  └─ Data completeness checks                                 │
│                                                               │
│  Layer 4: END-TO-END TESTS (Full Pipeline)                   │
│  ├─ Complete ETL execution                                   │
│  ├─ Data lineage verification                                │
│  ├─ Performance benchmarks                                   │
│  └─ Regression testing                                       │
│                                                               │
│  Layer 5: SMOKE TESTS (Pre-Deployment)                       │
│  ├─ Critical path validation                                 │
│  ├─ Environment readiness                                    │
│  └─ Deployment verification                                  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │  TEST RESULTS TABLES    │
              │  - test_executions      │
              │  - test_results         │
              │  - test_assertions      │
              └─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │   AUTOMATED REPORTING   │
              │  - Daily test summary   │
              │  - Failure alerts       │
              │  - Trend analysis       │
              └─────────────────────────┘
```

---

## Phased Implementation Roadmap

### Phase 1: Sprint 1 - Foundation (Week 1-2)
**Effort:** 8 hours (built into development)

**Deliverables:**
1. Test framework tables created
2. Basic unit tests for ODS tables
3. Data type validation tests
4. Test execution procedure framework

**What Gets Tested:**
- All ODS tables exist with correct structure
- Source_system discriminator working
- Batch control logging functional
- Error logging captures failures

**Automation Level:** 60% (manual execution, automated assertions)

---

### Phase 2: Sprint 2 - Integration Testing (Week 3-4)
**Effort:** 12 hours (built into development)

**Deliverables:**
1. Entity matching validation tests
2. Multi-source transformation tests
3. Data reconciliation tests
4. Staging layer validation suite

**What Gets Tested:**
- Veteran SSN matching accuracy (>95% target)
- Evaluator NPI matching accuracy (>95% target)
- Code mapping correctness (100% known codes)
- Conflict detection logging
- SCD Type 2 logic correctness

**Automation Level:** 80% (automated execution, automated validation)

---

### Phase 3: Sprint 3 - End-to-End Testing (Week 5-6)
**Effort:** 10 hours (built into development)

**Deliverables:**
1. Full pipeline integration tests
2. Fact table validation suite
3. Performance benchmarking tests
4. Regression test suite
5. Automated daily test execution

**What Gets Tested:**
- Complete ETL runs successfully
- Data volumes match expectations
- All surrogate keys resolve
- No orphaned fact records
- Pipeline completes within SLA (<2 hours)

**Automation Level:** 95% (fully automated)

---

### Phase 4: Sprint 4 - Continuous Testing (Week 7-8)
**Effort:** 6 hours (ongoing)

**Deliverables:**
1. Automated smoke tests for deployment
2. Production monitoring tests
3. Data quality trending reports
4. Automated regression suite

**What Gets Tested:**
- Pre-deployment validation passes
- Production data quality maintained
- No regressions introduced
- Performance remains acceptable

**Automation Level:** 100% (fully automated with alerting)

---

## Test Categories & Priorities

### Critical (Must Automate - Sprint 1-2)
| Test Type | Coverage | Automated | Priority |
|-----------|----------|-----------|----------|
| **Schema Validation** | All tables exist with correct structure | ✅ Yes | P0 |
| **Data Type Validation** | All columns have correct data types | ✅ Yes | P0 |
| **Primary Key Validation** | No duplicates in PK columns | ✅ Yes | P0 |
| **Referential Integrity** | All FKs reference valid PKs | ✅ Yes | P0 |
| **Entity Matching** | >95% match rate for veterans/evaluators | ✅ Yes | P0 |
| **Code Mapping** | 100% of known codes map correctly | ✅ Yes | P0 |

### High (Should Automate - Sprint 2-3)
| Test Type | Coverage | Automated | Priority |
|-----------|----------|-----------|----------|
| **Record Count Validation** | ODS → Staging → Warehouse counts match | ✅ Yes | P1 |
| **Null Checks** | Required fields are not null | ✅ Yes | P1 |
| **Data Completeness** | Expected fields populated | ✅ Yes | P1 |
| **Duplicate Detection** | No unintended duplicates | ✅ Yes | P1 |
| **SCD Type 2 Logic** | Historical tracking works correctly | ✅ Yes | P1 |
| **Conflict Logging** | OMS/VEMS conflicts captured | ✅ Yes | P1 |

### Medium (Nice to Automate - Sprint 3-4)
| Test Type | Coverage | Automated | Priority |
|-----------|----------|-----------|----------|
| **Business Rule Validation** | Domain constraints enforced | ✅ Yes | P2 |
| **Performance Testing** | ETL completes within SLA | ✅ Yes | P2 |
| **Data Distribution** | Values within expected ranges | ✅ Yes | P2 |
| **Historical Comparison** | Trends align with expectations | ⚠️ Partial | P2 |

### Low (Manual Acceptable - Sprint 4)
| Test Type | Coverage | Automated | Priority |
|-----------|----------|-----------|----------|
| **UAT Scenarios** | Business user acceptance | ❌ Manual | P3 |
| **Exploratory Testing** | Ad-hoc validation | ❌ Manual | P3 |
| **Edge Case Analysis** | Unusual data scenarios | ⚠️ Partial | P3 |

---

## Test Automation Framework

### Core Components

#### 1. Test Metadata Tables
```sql
-- Stores test definitions
CREATE TABLE qa_test_definitions (
    test_id INTEGER AUTOINCREMENT PRIMARY KEY,
    test_name VARCHAR(200) NOT NULL,
    test_category VARCHAR(50), -- UNIT, INTEGRATION, E2E, SMOKE
    test_layer VARCHAR(50),    -- ODS, STAGING, WAREHOUSE, PIPELINE
    test_sql TEXT NOT NULL,     -- The actual test query
    expected_result VARCHAR(50), -- PASS, FAIL, or specific value
    severity VARCHAR(20),       -- CRITICAL, HIGH, MEDIUM, LOW
    active_flag BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stores test execution results
CREATE TABLE qa_test_executions (
    execution_id INTEGER AUTOINCREMENT PRIMARY KEY,
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    execution_type VARCHAR(50), -- SCHEDULED, MANUAL, PRE_DEPLOYMENT
    batch_id VARCHAR(50),
    total_tests INTEGER,
    passed_tests INTEGER,
    failed_tests INTEGER,
    skipped_tests INTEGER,
    execution_duration_seconds INTEGER,
    overall_status VARCHAR(20) -- PASS, FAIL
);

-- Stores individual test results
CREATE TABLE qa_test_results (
    result_id INTEGER AUTOINCREMENT PRIMARY KEY,
    execution_id INTEGER,
    test_id INTEGER,
    test_name VARCHAR(200),
    actual_result VARCHAR(1000),
    expected_result VARCHAR(1000),
    test_status VARCHAR(20),    -- PASS, FAIL, SKIP, ERROR
    error_message TEXT,
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (execution_id) REFERENCES qa_test_executions(execution_id),
    FOREIGN KEY (test_id) REFERENCES qa_test_definitions(test_id)
);

-- Stores test assertions for detailed validation
CREATE TABLE qa_test_assertions (
    assertion_id INTEGER AUTOINCREMENT PRIMARY KEY,
    result_id INTEGER,
    assertion_name VARCHAR(200),
    assertion_value VARCHAR(1000),
    assertion_status VARCHAR(20),
    assertion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (result_id) REFERENCES qa_test_results(result_id)
);
```

#### 2. Test Execution Framework
```sql
-- Master test execution procedure
CREATE OR REPLACE PROCEDURE sp_execute_qa_tests(
    p_test_category VARCHAR DEFAULT 'ALL',
    p_batch_id VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_execution_id INTEGER;
    v_test_count INTEGER;
    v_passed INTEGER DEFAULT 0;
    v_failed INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_NTZ;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();

    -- Create execution record
    INSERT INTO qa_test_executions (execution_type, batch_id, overall_status)
    VALUES ('MANUAL', :p_batch_id, 'RUNNING')
    RETURNING execution_id INTO v_execution_id;

    -- Execute all active tests
    -- (Individual test execution logic here)

    -- Update execution summary
    UPDATE qa_test_executions
    SET total_tests = v_test_count,
        passed_tests = v_passed,
        failed_tests = v_failed,
        execution_duration_seconds = DATEDIFF(second, v_start_time, CURRENT_TIMESTAMP()),
        overall_status = CASE WHEN v_failed = 0 THEN 'PASS' ELSE 'FAIL' END
    WHERE execution_id = v_execution_id;

    RETURN 'Test execution completed. Execution ID: ' || v_execution_id ||
           ' | Passed: ' || v_passed || ' | Failed: ' || v_failed;
END;
$$;
```

#### 3. Test Result Reporting
```sql
-- Daily test summary view
CREATE OR REPLACE VIEW vw_qa_daily_test_summary AS
SELECT
    DATE(execution_timestamp) as test_date,
    execution_type,
    COUNT(DISTINCT execution_id) as total_executions,
    SUM(total_tests) as total_tests_run,
    SUM(passed_tests) as total_passed,
    SUM(failed_tests) as total_failed,
    ROUND(SUM(passed_tests) * 100.0 / NULLIF(SUM(total_tests), 0), 2) as pass_rate_pct,
    AVG(execution_duration_seconds) as avg_duration_seconds
FROM qa_test_executions
WHERE execution_timestamp >= CURRENT_DATE - 30
GROUP BY DATE(execution_timestamp), execution_type
ORDER BY test_date DESC;

-- Failed test details view
CREATE OR REPLACE VIEW vw_qa_failed_tests AS
SELECT
    r.execution_timestamp,
    r.test_name,
    d.test_category,
    d.severity,
    r.expected_result,
    r.actual_result,
    r.error_message,
    e.batch_id
FROM qa_test_results r
JOIN qa_test_definitions d ON r.test_id = d.test_id
JOIN qa_test_executions e ON r.execution_id = e.execution_id
WHERE r.test_status = 'FAIL'
  AND r.execution_timestamp >= CURRENT_DATE - 7
ORDER BY r.execution_timestamp DESC, d.severity;

-- Test trend analysis view
CREATE OR REPLACE VIEW vw_qa_test_trends AS
SELECT
    d.test_name,
    d.test_category,
    COUNT(*) as execution_count,
    SUM(CASE WHEN r.test_status = 'PASS' THEN 1 ELSE 0 END) as pass_count,
    SUM(CASE WHEN r.test_status = 'FAIL' THEN 1 ELSE 0 END) as fail_count,
    ROUND(SUM(CASE WHEN r.test_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stability_pct,
    MAX(r.execution_timestamp) as last_run
FROM qa_test_results r
JOIN qa_test_definitions d ON r.test_id = d.test_id
WHERE r.execution_timestamp >= CURRENT_DATE - 30
GROUP BY d.test_name, d.test_category
HAVING fail_count > 0  -- Show only tests that have failed at least once
ORDER BY stability_pct ASC, fail_count DESC;
```

---

## Test Library by Sprint

### Sprint 1 Tests: Foundation & ODS Layer

#### Test Set 1.1: Schema Validation
```sql
-- Test: All ODS tables exist
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ODS_TABLES_EXIST',
    'UNIT',
    'ODS',
    $$
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'ODS_RAW'
      AND table_name IN (
          'ods_veterans_source',
          'ods_evaluators_source',
          'ods_facilities_source',
          'ods_exam_requests_source',
          'ods_evaluations_source',
          'ods_appointments_source',
          'ods_qa_events_source',
          'ods_claims_source',
          'ods_batch_control',
          'ods_error_log'
      )
    $$,
    '10',  -- Expected: all 10 tables
    'CRITICAL'
);

-- Test: ODS tables have source_system column
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ODS_SOURCE_SYSTEM_COLUMN_EXISTS',
    'UNIT',
    'ODS',
    $$
    SELECT COUNT(DISTINCT table_name)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'ODS_RAW'
      AND column_name = 'source_system'
      AND table_name LIKE 'ods_%_source'
    $$,
    '8',  -- Expected: 8 source tables
    'CRITICAL'
);
```

#### Test Set 1.2: Data Type Validation
```sql
-- Test: Batch control batch_id is VARCHAR(50)
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'BATCH_CONTROL_BATCH_ID_TYPE',
    'UNIT',
    'ODS',
    $$
    SELECT data_type || '(' || character_maximum_length || ')'
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'ODS_RAW'
      AND table_name = 'ods_batch_control'
      AND column_name = 'batch_id'
    $$,
    'VARCHAR(50)',
    'HIGH'
);
```

#### Test Set 1.3: Referential Integrity
```sql
-- Test: All ODS records reference valid batch_id
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ODS_VETERANS_VALID_BATCH_ID',
    'INTEGRATION',
    'ODS',
    $$
    SELECT COUNT(*)
    FROM ODS_RAW.ods_veterans_source v
    WHERE v.batch_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM ODS_RAW.ods_batch_control b
          WHERE b.batch_id = v.batch_id
      )
    $$,
    '0',  -- Expected: no orphaned records
    'HIGH'
);
```

---

### Sprint 2 Tests: Multi-Source Integration

#### Test Set 2.1: Entity Matching Accuracy
```sql
-- Test: Veteran matching achieves >95% confidence
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'VETERAN_MATCH_CONFIDENCE_95PCT',
    'INTEGRATION',
    'REFERENCE',
    $$
    SELECT
        CASE
            WHEN AVG(match_confidence) >= 95 THEN 'PASS'
            ELSE 'FAIL: ' || ROUND(AVG(match_confidence), 2) || '% average'
        END
    FROM REFERENCE.ref_entity_crosswalk_veteran
    $$,
    'PASS',
    'CRITICAL'
);

-- Test: No duplicate master veteran IDs
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'NO_DUPLICATE_MASTER_VETERAN_IDS',
    'UNIT',
    'REFERENCE',
    $$
    SELECT COUNT(*)
    FROM (
        SELECT master_veteran_id, COUNT(*) as cnt
        FROM REFERENCE.ref_entity_crosswalk_veteran
        GROUP BY master_veteran_id
        HAVING COUNT(*) > 1
    )
    $$,
    '0',
    'CRITICAL'
);

-- Test: All veterans in staging have crosswalk entry
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'STAGING_VETERANS_HAVE_CROSSWALK',
    'INTEGRATION',
    'STAGING',
    $$
    SELECT COUNT(*)
    FROM STAGING.stg_veterans s
    WHERE NOT EXISTS (
        SELECT 1
        FROM REFERENCE.ref_entity_crosswalk_veteran x
        WHERE x.master_veteran_id = s.veteran_id
    )
    $$,
    '0',
    'HIGH'
);
```

#### Test Set 2.2: Code Mapping Validation
```sql
-- Test: All specialty codes have mappings
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ALL_SPECIALTY_CODES_MAPPED',
    'UNIT',
    'REFERENCE',
    $$
    WITH source_codes AS (
        SELECT DISTINCT source_system, specialty_code
        FROM (
            SELECT 'OMS' as source_system, specialty_code
            FROM ODS_RAW.ods_evaluations_source
            WHERE source_system = 'OMS' AND specialty_code IS NOT NULL
            UNION
            SELECT 'VEMS' as source_system, specialty_name as specialty_code
            FROM ODS_RAW.ods_evaluations_source
            WHERE source_system = 'VEMS' AND specialty_name IS NOT NULL
        )
    )
    SELECT COUNT(*)
    FROM source_codes s
    WHERE NOT EXISTS (
        SELECT 1
        FROM REFERENCE.ref_code_mapping_specialty m
        WHERE m.source_system = s.source_system
          AND m.source_code = s.specialty_code
          AND m.active_flag = TRUE
    )
    $$,
    '0',  -- All codes should map
    'HIGH'
);

-- Test: Code mapping UDFs return values
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'SPECIALTY_CODE_UDF_WORKS',
    'INTEGRATION',
    'REFERENCE',
    $$
    SELECT
        CASE
            WHEN fn_map_specialty_code('OMS', 'PSYCH') IS NOT NULL
                 AND fn_map_specialty_code('VEMS', 'PSYCHIATRY') IS NOT NULL
            THEN 'PASS'
            ELSE 'FAIL'
        END
    $$,
    'PASS',
    'CRITICAL'
);
```

#### Test Set 2.3: Transformation Validation
```sql
-- Test: Record count matches from ODS to Staging
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ODS_TO_STAGING_VETERAN_COUNT_MATCH',
    'INTEGRATION',
    'STAGING',
    $$
    WITH ods_count AS (
        SELECT COUNT(DISTINCT veteran_ssn) as cnt
        FROM ODS_RAW.ods_veterans_source
        WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED')
    ),
    staging_count AS (
        SELECT COUNT(*) as cnt
        FROM STAGING.stg_veterans
        WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED')
    )
    SELECT
        CASE
            WHEN o.cnt = s.cnt THEN 'PASS'
            ELSE 'FAIL: ODS=' || o.cnt || ', Staging=' || s.cnt
        END
    FROM ods_count o, staging_count s
    $$,
    'PASS',
    'CRITICAL'
);

-- Test: All staging veterans have DQ scores
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'STAGING_VETERANS_HAVE_DQ_SCORES',
    'UNIT',
    'STAGING',
    $$
    SELECT COUNT(*)
    FROM STAGING.stg_veterans
    WHERE dq_score IS NULL OR dq_score < 0 OR dq_score > 100
    $$,
    '0',
    'MEDIUM'
);

-- Test: Conflict logging is working
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'CONFLICT_LOGGING_FUNCTIONAL',
    'INTEGRATION',
    'REFERENCE',
    $$
    SELECT
        CASE
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL: No conflicts logged (may be OK if data matches)'
        END
    FROM REFERENCE.ref_reconciliation_log
    WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED')
    $$,
    'PASS',
    'MEDIUM'
);
```

---

### Sprint 3 Tests: Facts & End-to-End

#### Test Set 3.1: Fact Table Validation
```sql
-- Test: All fact evaluations have valid veteran surrogate keys
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'FACT_EVALUATIONS_VALID_VETERAN_SK',
    'INTEGRATION',
    'WAREHOUSE',
    $$
    SELECT COUNT(*)
    FROM WAREHOUSE.fact_evaluations_completed f
    WHERE NOT EXISTS (
        SELECT 1
        FROM WAREHOUSE.dim_veterans d
        WHERE d.veteran_sk = f.veteran_sk
    )
    $$,
    '0',  -- No orphaned records
    'CRITICAL'
);

-- Test: No negative or zero surrogate keys in facts
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'FACT_EVALUATIONS_POSITIVE_SURROGATE_KEYS',
    'UNIT',
    'WAREHOUSE',
    $$
    SELECT COUNT(*)
    FROM WAREHOUSE.fact_evaluations_completed
    WHERE veteran_sk <= 0
       OR evaluator_sk <= 0
       OR facility_sk <= 0
    $$,
    '0',
    'HIGH'
);

-- Test: Fact record count matches staging
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'STAGING_TO_FACT_EVALUATION_COUNT_MATCH',
    'INTEGRATION',
    'WAREHOUSE',
    $$
    WITH staging_count AS (
        SELECT COUNT(*) as cnt
        FROM STAGING.stg_fact_evaluations
        WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED')
    ),
    fact_count AS (
        SELECT COUNT(*) as cnt
        FROM WAREHOUSE.fact_evaluations_completed f
        JOIN ODS_RAW.ods_batch_control b
          ON f.evaluation_date >= b.batch_start_timestamp::DATE
        WHERE b.batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED')
    )
    SELECT
        CASE
            WHEN ABS(s.cnt - f.cnt) / NULLIF(s.cnt, 0) <= 0.05 THEN 'PASS'  -- 5% tolerance
            ELSE 'FAIL: Staging=' || s.cnt || ', Fact=' || f.cnt
        END
    FROM staging_count s, fact_count f
    $$,
    'PASS',
    'HIGH'
);
```

#### Test Set 3.2: SCD Type 2 Validation
```sql
-- Test: Only one current record per veteran
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ONE_CURRENT_RECORD_PER_VETERAN',
    'UNIT',
    'WAREHOUSE',
    $$
    SELECT COUNT(*)
    FROM (
        SELECT veteran_id, COUNT(*) as cnt
        FROM WAREHOUSE.dim_veterans
        WHERE is_current = TRUE
        GROUP BY veteran_id
        HAVING COUNT(*) > 1
    )
    $$,
    '0',
    'CRITICAL'
);

-- Test: Historical records are properly end-dated
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'HISTORICAL_RECORDS_END_DATED',
    'UNIT',
    'WAREHOUSE',
    $$
    SELECT COUNT(*)
    FROM WAREHOUSE.dim_veterans
    WHERE is_current = FALSE
      AND expiration_date IS NULL
    $$,
    '0',
    'HIGH'
);

-- Test: Effective dates don't overlap for same veteran
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'NO_OVERLAPPING_EFFECTIVE_DATES',
    'UNIT',
    'WAREHOUSE',
    $$
    SELECT COUNT(*)
    FROM WAREHOUSE.dim_veterans d1
    JOIN WAREHOUSE.dim_veterans d2
      ON d1.veteran_id = d2.veteran_id
      AND d1.veteran_sk < d2.veteran_sk
    WHERE d1.effective_date <= d2.effective_date
      AND d1.expiration_date > d2.effective_date
    $$,
    '0',
    'HIGH'
);
```

#### Test Set 3.3: End-to-End Pipeline Tests
```sql
-- Test: Master ETL completes successfully
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'MASTER_ETL_COMPLETES_SUCCESSFULLY',
    'E2E',
    'PIPELINE',
    $$
    SELECT batch_status
    FROM ODS_RAW.ods_batch_control
    WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control)
    $$,
    'COMPLETED',
    'CRITICAL'
);

-- Test: ETL completes within SLA (2 hours)
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ETL_COMPLETES_WITHIN_SLA',
    'E2E',
    'PIPELINE',
    $$
    SELECT
        CASE
            WHEN DATEDIFF(minute, batch_start_timestamp, batch_end_timestamp) <= 120 THEN 'PASS'
            ELSE 'FAIL: ' || DATEDIFF(minute, batch_start_timestamp, batch_end_timestamp) || ' minutes'
        END
    FROM ODS_RAW.ods_batch_control
    WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control)
      AND batch_status = 'COMPLETED'
    $$,
    'PASS',
    'HIGH'
);

-- Test: No errors logged during ETL
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'NO_ETL_ERRORS_LOGGED',
    'E2E',
    'PIPELINE',
    $$
    SELECT COUNT(*)
    FROM ODS_RAW.ods_error_log
    WHERE batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED')
      AND error_type IN ('CRITICAL', 'FATAL')
    $$,
    '0',
    'HIGH'
);
```

---

### Sprint 4 Tests: Smoke Tests & Regression

#### Test Set 4.1: Pre-Deployment Smoke Tests
```sql
-- Test: All critical procedures exist
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'ALL_CRITICAL_PROCEDURES_EXIST',
    'SMOKE',
    'WAREHOUSE',
    $$
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.PROCEDURES
    WHERE procedure_schema = 'WAREHOUSE'
      AND procedure_name IN (
          'sp_etl_master_pipeline_multisource',
          'sp_build_crosswalk_veterans',
          'sp_transform_multisource_ods_to_staging_veterans',
          'sp_load_dim_veterans'
      )
    $$,
    '4',
    'CRITICAL'
);

-- Test: All mart views are queryable
INSERT INTO qa_test_definitions (test_name, test_category, test_layer, test_sql, expected_result, severity)
VALUES (
    'MART_VIEWS_QUERYABLE',
    'SMOKE',
    'MARTS',
    $$
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM MARTS_CLINICAL.vw_evaluator_performance WHERE 1=0) = 0
                 AND (SELECT COUNT(*) FROM MARTS_CLINICAL.vw_facility_performance_dashboard WHERE 1=0) = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END
    $$,
    'PASS',
    'HIGH'
);
```

---

## Automated Test Execution Schedule

### During Development (Sprint 1-3)
```sql
-- Run unit tests after each DDL change
-- Developers execute manually: CALL sp_execute_qa_tests('UNIT', NULL);
```

### During ETL Development (Sprint 2-3)
```sql
-- Run integration tests after each ETL procedure change
-- CALL sp_execute_qa_tests('INTEGRATION', 'TEST_BATCH_001');
```

### After Sprint 3 (MVP Complete)
```sql
-- Create automated daily test execution
CREATE OR REPLACE TASK task_daily_qa_tests
    WAREHOUSE = ETL_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- 6 AM daily
AS
    CALL sp_execute_qa_tests('ALL', NULL);

-- Create automated post-ETL test execution
CREATE OR REPLACE TASK task_post_etl_qa_tests
    WAREHOUSE = ETL_WH
    AFTER task_etl_master_pipeline  -- Runs after ETL completes
AS
    CALL sp_execute_qa_tests('E2E', NULL);

-- Enable tasks
ALTER TASK task_daily_qa_tests RESUME;
ALTER TASK task_post_etl_qa_tests RESUME;
```

### Pre-Deployment (Before Each Release)
```sql
-- Run full smoke test suite
CALL sp_execute_qa_tests('SMOKE', NULL);

-- Verify no critical failures
SELECT * FROM vw_qa_failed_tests
WHERE severity = 'CRITICAL'
  AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 hour';
```

---

## Test Data Management Strategy

### Approach 1: Use Production-Like Sample Data (Recommended)
```sql
-- Create test data schema
CREATE SCHEMA IF NOT EXISTS TEST_DATA;

-- Load sample OMS data
CREATE TABLE TEST_DATA.sample_oms_veterans AS
SELECT TOP 1000 *
FROM ODS_RAW.ods_veterans_source
WHERE source_system = 'OMS'
  AND batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED');

-- Load sample VEMS data
CREATE TABLE TEST_DATA.sample_vems_veterans AS
SELECT TOP 1000 *
FROM ODS_RAW.ods_veterans_source
WHERE source_system = 'VEMS'
  AND batch_id = (SELECT MAX(batch_id) FROM ODS_RAW.ods_batch_control WHERE batch_status = 'COMPLETED');

-- Create synthetic conflicts for testing
UPDATE TEST_DATA.sample_vems_veterans
SET disability_rating = disability_rating + 10
WHERE veteran_ssn IN (
    SELECT veteran_ssn
    FROM TEST_DATA.sample_oms_veterans
    LIMIT 50
);
```

### Approach 2: Generate Synthetic Test Data
```sql
-- Generate synthetic veterans for testing
CREATE OR REPLACE PROCEDURE sp_generate_test_veterans(
    p_count INTEGER,
    p_source_system VARCHAR
)
AS
$$
BEGIN
    INSERT INTO TEST_DATA.test_veterans_source (
        source_system,
        source_record_id,
        veteran_ssn,
        first_name,
        last_name,
        disability_rating,
        batch_id
    )
    SELECT
        :p_source_system,
        :p_source_system || '_TEST_' || SEQ4(),
        LPAD(SEQ4()::VARCHAR, 9, '0'),  -- Synthetic SSN
        'TEST_FN_' || SEQ4(),
        'TEST_LN_' || SEQ4(),
        UNIFORM(0, 100, RANDOM()),      -- Random disability rating
        'TEST_BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD')
    FROM TABLE(GENERATOR(ROWCOUNT => :p_count));
END;
$$;

-- Generate 100 OMS and 100 VEMS test veterans
CALL sp_generate_test_veterans(100, 'OMS');
CALL sp_generate_test_veterans(100, 'VEMS');
```

---

## CI/CD Integration

### Pre-Commit Checks (Developer Workflow)
```bash
# Run unit tests before committing
snowsql -c dev -q "CALL sp_execute_qa_tests('UNIT', NULL);"

# Check for failures
snowsql -c dev -q "
SELECT COUNT(*) as failures
FROM vw_qa_failed_tests
WHERE severity IN ('CRITICAL', 'HIGH')
  AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '5 minutes';"

# Exit with error if failures found
if [ $failures -gt 0 ]; then
    echo "CRITICAL or HIGH severity test failures found. Please fix before committing."
    exit 1
fi
```

### Deployment Pipeline
```yaml
# Example GitHub Actions / GitLab CI workflow
stages:
  - deploy_to_test
  - run_smoke_tests
  - deploy_to_prod

deploy_test:
  script:
    - snowsql -c test -f snowflake/schema/02_master_deployment.sql

smoke_tests:
  script:
    - snowsql -c test -q "CALL sp_execute_qa_tests('SMOKE', NULL);"
    - |
      failures=$(snowsql -c test -q "
        SELECT COUNT(*)
        FROM vw_qa_failed_tests
        WHERE severity = 'CRITICAL'
          AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '10 minutes';" -o output_format=plain)
      if [ "$failures" -gt 0 ]; then
        echo "Smoke tests failed. Blocking production deployment."
        exit 1
      fi

deploy_prod:
  when: manual
  only:
    - main
  script:
    - snowsql -c prod -f snowflake/schema/02_master_deployment.sql
```

---

## Monitoring & Alerting

### Daily Test Report Email
```sql
-- Create procedure to generate daily report
CREATE OR REPLACE PROCEDURE sp_send_daily_test_report()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_report VARCHAR;
BEGIN
    -- Generate report (would integrate with email service)
    SELECT LISTAGG(
        'Date: ' || test_date ||
        ' | Executions: ' || total_executions ||
        ' | Pass Rate: ' || pass_rate_pct || '%' ||
        ' | Failed: ' || total_failed, '\n'
    )
    INTO v_report
    FROM vw_qa_daily_test_summary
    WHERE test_date >= CURRENT_DATE - 7
    ORDER BY test_date DESC;

    -- In production, this would send email via external function
    -- For now, just log the report
    RETURN 'Daily Test Report Generated: ' || v_report;
END;
$$;

-- Schedule daily report
CREATE OR REPLACE TASK task_daily_test_report
    WAREHOUSE = ETL_WH
    SCHEDULE = 'USING CRON 0 7 * * * America/New_York'  -- 7 AM daily
AS
    CALL sp_send_daily_test_report();

ALTER TASK task_daily_test_report RESUME;
```

### Failure Alerting
```sql
-- Create alert for critical test failures
CREATE OR REPLACE PROCEDURE sp_check_critical_test_failures()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_failure_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_failure_count
    FROM vw_qa_failed_tests
    WHERE severity = 'CRITICAL'
      AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 hour';

    IF (v_failure_count > 0) THEN
        -- In production, this would send alert via external function
        RETURN 'ALERT: ' || v_failure_count || ' critical test failures detected!';
    END IF;

    RETURN 'No critical failures';
END;
$$;

-- Schedule failure check every hour
CREATE OR REPLACE TASK task_hourly_failure_check
    WAREHOUSE = ETL_WH
    SCHEDULE = 'USING CRON 0 * * * * America/New_York'  -- Every hour
AS
    CALL sp_check_critical_test_failures();

ALTER TASK task_hourly_failure_check RESUME;
```

---

## Success Metrics

### Key QA Metrics to Track

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Test Coverage** | >90% of critical paths | Count of test definitions |
| **Test Pass Rate** | >95% daily | vw_qa_daily_test_summary |
| **Test Execution Time** | <10 minutes for full suite | execution_duration_seconds |
| **Critical Failures** | 0 before deployment | vw_qa_failed_tests severity=CRITICAL |
| **Test Stability** | >98% (tests don't flake) | vw_qa_test_trends stability_pct |
| **Defect Escape Rate** | <2% to production | Manual tracking |

### Weekly QA Health Report
```sql
-- Generate weekly QA health report
SELECT
    'Test Coverage' as metric,
    COUNT(DISTINCT test_id)::VARCHAR || ' tests defined' as value
FROM qa_test_definitions
WHERE active_flag = TRUE

UNION ALL

SELECT
    'Pass Rate (Last 7 Days)',
    ROUND(AVG(pass_rate_pct), 2)::VARCHAR || '%'
FROM vw_qa_daily_test_summary
WHERE test_date >= CURRENT_DATE - 7

UNION ALL

SELECT
    'Critical Failures (Last 7 Days)',
    COUNT(*)::VARCHAR
FROM vw_qa_failed_tests
WHERE severity = 'CRITICAL'

UNION ALL

SELECT
    'Avg Execution Time',
    ROUND(AVG(execution_duration_seconds), 0)::VARCHAR || ' seconds'
FROM qa_test_executions
WHERE execution_timestamp >= CURRENT_DATE - 7;
```

---

## Team Responsibilities

### Data Engineers (Sprint 1-3)
- Write unit tests when creating tables/procedures
- Run integration tests before checking in code
- Fix failing tests within same sprint
- Target: 2-3 tests per procedure

### QA/Data Analyst (Sprint 2-4)
- Define data quality assertions
- Create test data scenarios
- Validate test coverage
- Review test results daily
- Target: 5-10 hours/week

### DevOps Engineer (Sprint 3-4)
- Setup automated test execution tasks
- Configure alerting
- Integrate tests into CI/CD
- Target: 8 hours total

---

## Risk Mitigation

### Risk 1: Tests Take Too Long to Run
**Mitigation:**
- Run only unit tests during development (<2 minutes)
- Run full suite nightly and pre-deployment
- Partition tests by layer (ODS, Staging, Warehouse)

### Risk 2: Tests Fail Due to Data Variability
**Mitigation:**
- Use percentage-based assertions instead of exact counts
- Use dedicated test data that's controlled
- Document expected variability ranges

### Risk 3: False Positives
**Mitigation:**
- Review failed tests before alerting
- Implement test stability tracking
- Disable flaky tests and fix root cause

---

## Next Steps

### Week 1-2 (Sprint 1)
- [ ] Create test framework tables
- [ ] Implement test execution procedure
- [ ] Write first 10 unit tests for ODS layer
- [ ] Run tests manually after each DDL change

### Week 3-4 (Sprint 2)
- [ ] Add integration tests for entity matching
- [ ] Add code mapping validation tests
- [ ] Implement test result reporting views
- [ ] Run tests daily manually

### Week 5-6 (Sprint 3)
- [ ] Add end-to-end pipeline tests
- [ ] Add fact table validation tests
- [ ] Setup automated test execution tasks
- [ ] Configure failure alerting

### Week 7-8 (Sprint 4)
- [ ] Add smoke tests for deployment
- [ ] Create regression test suite
- [ ] Implement CI/CD integration
- [ ] Document test procedures for team

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Owner:** Data Engineering Team
**Review Frequency:** Weekly during sprints, monthly post-MVP
