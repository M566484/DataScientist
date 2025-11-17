# VES Naming Convention Alignment Report
## Veteran Evaluation Services - Dimensional Model

**Date**: 2024-11-16
**Status**: Analysis Complete - Awaiting Approval for Migration
**Impact Level**: 🔴 **HIGH** - Requires significant refactoring

---

## Executive Summary

The current dimensional model has several deviations from the official **VES Snowflake Database Naming Conventions**. While the model is functionally sound and includes comprehensive business logic, column comments, and optimizations, it does not align with the established VES standards for:

1. **Schema organization** (should use WAREHOUSE instead of separate DIM/FACT schemas)
2. **Object name casing** (should use lowercase snake_case, not UPPERCASE)
3. **Surrogate key patterns** (should use `*_sk` suffix, not `*_KEY`)
4. **Fact table naming** (should use `fact_` prefix with event names)
5. **Source system identification** (missing staging layer pattern)

**Recommendation**: Refactor the model to align with VES standards before production deployment to ensure consistency across the enterprise data warehouse and avoid technical debt.

---

## Detailed Analysis

### 1. Schema Organization ❌

**Current State:**
```
VETERAN_EVALUATION_DW (Database)
├── DIM (Schema) - Dimension tables
├── FACT (Schema) - Fact tables
├── STG (Schema) - Staging area
└── UTIL (Schema) - Utility objects
```

**Required per VES Standards:**
```
VESDW_PRD (Database)
├── REFERENCE (Schema) - EDW metadata, manually maintained
├── STAGING (Schema) - 1:1 views with light transformations
├── WAREHOUSE (Schema) - Star schema (dimensions + facts together)
├── MARTS_CLINICAL (Schema) - Clinical operations marts
├── MARTS_OPERATIONS (Schema) - Scheduling & capacity marts
├── MARTS_FINANCE (Schema) - Revenue & claims marts
├── MARTS_QUALITY (Schema) - HEDIS & compliance marts
├── MARTS_PROVIDER (Schema) - Examiner performance marts
└── MARTS_EXECUTIVE (Schema) - Leadership KPIs
```

**Deviation**:
- Database name: `VETERAN_EVALUATION_DW` vs `VESDW_PRD`
- Using separate `DIM` and `FACT` schemas instead of unified `WAREHOUSE`
- Staging schema named `STG` instead of `STAGING`
- Missing `REFERENCE` schema for metadata/lookups
- Missing department-specific `MARTS_*` schemas
- Missing source system identification in staging layer

**Impact**: Medium - Affects discoverability and alignment with enterprise architecture

---

### 2. Table Name Casing ❌

**Current State:**
```sql
-- Dimensions (all UPPERCASE)
DIM_DATE
DIM_VETERAN
DIM_EVALUATOR
DIM_FACILITY
DIM_EVALUATION_TYPE
DIM_MEDICAL_CONDITION
DIM_CLAIM
DIM_APPOINTMENT

-- Facts (all UPPERCASE)
FACT_EVALUATION
FACT_CLAIM_STATUS
FACT_APPOINTMENT
FACT_DAILY_SNAPSHOT
```

**Required per VES Standards:**
```sql
-- All table names in lowercase snake_case
dim_dates
dim_veterans
dim_evaluators
dim_facilities
dim_evaluation_types
dim_medical_conditions
dim_claims
dim_appointments

fact_evaluations_completed
fact_claim_status_changes
fact_appointments_scheduled
fact_daily_facility_snapshot
```

**VES Convention**:
> "Use **lowercase** with **underscores** (snake_case) for table and column names... Writing in lowercase improves **readability and typing comfort**... The uppercase/lowercase distinction for databases/schemas vs tables/columns creates **visual hierarchy** in fully-qualified names: `VESDW_PRD.STAGING.stg_vems_core__exams`"

**Impact**: High - Affects all SQL queries and BI tool metadata

---

### 3. Column Name Casing ❌

**Current State:**
```sql
-- All column names in UPPERCASE
VETERAN_KEY
VETERAN_ID
FIRST_NAME
LAST_NAME
SERVICE_BRANCH
DISABILITY_RATING
CREATED_TIMESTAMP
```

**Required per VES Standards:**
```sql
-- All column names in lowercase snake_case
veteran_sk
veteran_id
first_name
last_name
service_branch
disability_rating
created_timestamp
```

**VES Convention**:
> "Use **lowercase** with **underscores** (snake_case) for table and column names... Snowflake stores all unquoted identifiers as **UPPERCASE** internally... Write for human readability; let Snowflake handle the internal uppercase conversion automatically."

**Impact**: High - Affects all SQL queries, BI tools, and data catalog

---

### 4. Surrogate Key Pattern ❌

**Current State:**
```sql
-- Using *_KEY suffix
VETERAN_KEY INTEGER AUTOINCREMENT PRIMARY KEY
EVALUATOR_KEY INTEGER AUTOINCREMENT PRIMARY KEY
FACILITY_KEY INTEGER AUTOINCREMENT PRIMARY KEY
EVALUATION_TYPE_KEY INTEGER AUTOINCREMENT PRIMARY KEY
```

**Required per VES Standards:**
```sql
-- Using *_sk suffix (surrogate key)
veteran_sk INTEGER AUTOINCREMENT PRIMARY KEY
evaluator_sk INTEGER AUTOINCREMENT PRIMARY KEY
facility_sk INTEGER AUTOINCREMENT PRIMARY KEY
evaluation_type_sk INTEGER AUTOINCREMENT PRIMARY KEY
```

**VES Convention**:
> "Pattern: `[table_name_singular]_sk`... The surrogate key (SK) serves as the primary key for dimensions and can be used as foreign keys in fact tables."

**Impact**: High - Affects all joins and foreign key references

---

### 5. Fact Table Naming ❌

**Current State:**
```sql
FACT_EVALUATION          -- Generic noun
FACT_CLAIM_STATUS        -- Status noun
FACT_APPOINTMENT         -- Generic noun
FACT_DAILY_SNAPSHOT      -- Snapshot noun
```

**Required per VES Standards:**
```sql
fact_evaluations_completed        -- Business event (past tense)
fact_claim_status_changes         -- Business event
fact_appointments_scheduled       -- Business event (past tense)
fact_daily_facility_snapshot      -- Specific snapshot
```

**VES Convention**:
> "Use past tense verbs or nouns that describe completed actions... Represent the 'action' or 'granularity' of business processes."

**Examples from VES docs**:
- `fact_exams_completed`
- `fact_appointments_scheduled`
- `fact_claims_submitted`
- `fact_exam_requests_received`

**Impact**: Medium - Affects clarity and alignment with business processes

---

### 6. Staging Layer Pattern ❌

**Current State:**
- No staging tables defined
- Missing source system identification pattern

**Required per VES Standards:**
```sql
-- Individual source views pattern
stg_vems_core__veterans
stg_vems_core__evaluations
stg_vems_core__appointments
stg_vems_core__facilities
stg_vems_core__claims

-- Pattern: stg_[source]_[domain]__[table_name]
-- Where:
--   source = source system (vems, oms, etc.)
--   domain = functional domain (core, pnm, etc.)
--   table_name = entity name
```

**VES Convention**:
> "Pattern: `stg_[source]_[domain]__[table_name]`... Double underscore (`__`) separates source/domain prefix from table name... Materialized as **views** only"

**Impact**: Medium - Staging layer not yet implemented, but critical for ETL architecture

---

### 7. Date Dimension Naming ⚠️

**Current State:**
```sql
DIM_DATE  (singular)
```

**Required per VES Standards:**
```sql
dim_dates  (plural)
```

**VES Convention**:
> "Use **plural** for table names representing collections of entities... Example: `dim_dates` (calendar dimension for time-series analysis)"

**Impact**: Low - Semantic preference, but impacts consistency

---

## Compliance Matrix

| Convention | Current | Required | Status | Impact |
|------------|---------|----------|--------|--------|
| Database name | `VETERAN_EVALUATION_DW` | `VESDW_PRD` | ❌ | Medium |
| Schema organization | `DIM`, `FACT`, `STG`, `UTIL` | `WAREHOUSE`, `STAGING`, `MARTS_*`, `REFERENCE` | ❌ | High |
| Table name case | UPPERCASE | lowercase snake_case | ❌ | High |
| Column name case | UPPERCASE | lowercase snake_case | ❌ | High |
| Surrogate keys | `*_KEY` | `*_sk` | ❌ | High |
| Fact table naming | `FACT_[NOUN]` | `fact_[event]` | ❌ | Medium |
| Dimension plural | Mixed | Plural | ⚠️ | Low |
| Staging pattern | Not implemented | `stg_[source]_[domain]__[table]` | ❌ | High |
| Column comments | ✅ Comprehensive | ✅ Required | ✅ | N/A |
| Clustering keys | ✅ Implemented | ✅ Recommended | ✅ | N/A |
| SCD Type 2 | ✅ Implemented | ✅ Recommended | ✅ | N/A |

**Overall Compliance**: 🔴 **25%** (2 of 8 major conventions met)

---

## Migration Impact Assessment

### Files Requiring Changes

#### SQL DDL Files (All)
- ✏️ `snowflake/schema/00_setup_database.sql` - Database and schema names
- ✏️ `snowflake/schema/01_populate_date_dimension.sql` - Table and column references
- ✏️ `snowflake/schema/02_master_deployment.sql` - All references
- ✏️ All dimension files (`snowflake/dimensions/*.sql`) - Table names, column names, keys
- ✏️ All fact files (`snowflake/facts/*.sql`) - Table names, column names, keys

#### Documentation Files
- ✏️ `README.md` - All examples and code snippets
- ✏️ `DIMENSIONAL_MODEL_DOCUMENTATION.md` - All SQL examples, table references
- ✏️ `ER_DIAGRAM.md` - All table and column references

**Total Files**: 18 SQL files + 3 documentation files = **21 files**

### Breaking Changes

#### SQL Queries
All existing SQL queries will break due to:
- Schema name changes (`DIM.DIM_VETERAN` → `WAREHOUSE.dim_veterans`)
- Table name changes (case sensitive if quoted)
- Column name changes (case sensitive if quoted)

#### BI Tools
- Tableau/Power BI/Qlik connections will need updates
- Data source configurations require republishing
- Published reports/dashboards require metadata refresh

#### ETL Pipelines
- Stored procedures referencing tables need updates
- Mulesoft integration points need schema/table updates
- Snowflake tasks and streams need reconfiguration

---

## Migration Strategy Options

### Option 1: Complete Refactoring (Recommended)
**Approach**: Refactor all objects to align with VES standards before initial deployment

**Pros**:
- Clean alignment with enterprise standards
- No technical debt
- Easier long-term maintenance
- Consistent with other VES data marts

**Cons**:
- Requires rework of all 18 SQL files
- All documentation must be updated
- Initial deployment delayed

**Timeline**: 2-3 days

**Risk**: Low (no production systems impacted yet)

---

### Option 2: Phased Migration
**Approach**: Deploy current model, then migrate incrementally

**Phases**:
1. **Phase 1**: Create aliases/synonyms for current objects
2. **Phase 2**: Deploy new objects with correct naming
3. **Phase 3**: Migrate consuming systems
4. **Phase 4**: Deprecate old objects

**Pros**:
- Allows immediate deployment
- Gradual transition reduces disruption

**Cons**:
- Maintains dual object sets temporarily
- Increased complexity during transition
- Risk of confusion between old/new naming
- Technical debt until completion

**Timeline**: 4-6 weeks

**Risk**: Medium (maintaining parallel structures)

---

### Option 3: Defer to Next Iteration (Not Recommended)
**Approach**: Deploy as-is, address in future release

**Pros**:
- Fastest path to deployment

**Cons**:
- Creates immediate technical debt
- Harder to refactor after production use
- Inconsistent with VES enterprise architecture
- May require approval exceptions
- Confuses future developers/analysts

**Timeline**: No immediate work, but costly later

**Risk**: High (growing technical debt, rework costs)

---

## Recommended Migration Plan

### Phase 1: Preparation (4 hours)
1. ✅ Create this alignment report (COMPLETE)
2. 📋 Get stakeholder approval for refactoring approach
3. 📋 Create git feature branch: `feature/ves-naming-alignment`
4. 📋 Document object name mappings (old → new)

### Phase 2: Schema & Database Updates (2 hours)
1. Update `00_setup_database.sql`:
   - Rename database: `VETERAN_EVALUATION_DW` → `VESDW_PRD` (if applicable)
   - Create `WAREHOUSE` schema (merge DIM + FACT)
   - Rename `STG` → `STAGING`
   - Add `REFERENCE` schema
   - Add `MARTS_CLINICAL`, `MARTS_OPERATIONS`, `MARTS_EXECUTIVE` schemas

### Phase 3: Dimension Table Refactoring (6 hours)
1. For each dimension table:
   - Convert table name to lowercase (e.g., `DIM_DATE` → `dim_dates`)
   - Convert all column names to lowercase
   - Change `*_KEY` → `*_sk` for all surrogate keys
   - Update all foreign key references
   - Update column comments (already lowercase in comments)
   - Move from `DIM` schema to `WAREHOUSE` schema

### Phase 4: Fact Table Refactoring (6 hours)
1. For each fact table:
   - Rename with `fact_` prefix and event name:
     - `FACT_EVALUATION` → `fact_evaluations_completed`
     - `FACT_CLAIM_STATUS` → `fact_claim_status_changes`
     - `FACT_APPOINTMENT` → `fact_appointments_scheduled`
     - `FACT_DAILY_SNAPSHOT` → `fact_daily_facility_snapshot`
   - Convert all column names to lowercase
   - Change `*_KEY` → `*_sk` for all keys
   - Update all foreign key references
   - Update clustering key definitions
   - Move from `FACT` schema to `WAREHOUSE` schema

### Phase 5: Staging Layer (4 hours)
1. Create staging views following VES pattern:
   - `stg_vems_core__veterans`
   - `stg_vems_core__evaluations`
   - etc.
2. Add source_system tracking columns
3. Document staging layer in deployment guide

### Phase 6: Documentation Updates (4 hours)
1. Update `README.md` with new naming
2. Update `DIMENSIONAL_MODEL_DOCUMENTATION.md`
3. Update `ER_DIAGRAM.md`
4. Update all SQL examples
5. Create migration guide for consumers

### Phase 7: Testing & Validation (2 hours)
1. Validate all DDL scripts execute without errors
2. Test column comments are preserved
3. Verify clustering keys are maintained
4. Check foreign key metadata is correct
5. Validate date dimension population

### Phase 8: Deployment (1 hour)
1. Create pull request with all changes
2. Code review with data team
3. Merge to main branch
4. Deploy to Snowflake development environment
5. Validate deployment

**Total Estimated Effort**: 29 hours (~4 days)

---

## Object Name Mapping

### Dimension Tables

| Current Name | New Name | Schema Change | Notes |
|--------------|----------|---------------|-------|
| `DIM.DIM_DATE` | `WAREHOUSE.dim_dates` | DIM → WAREHOUSE | Pluralized |
| `DIM.DIM_VETERAN` | `WAREHOUSE.dim_veterans` | DIM → WAREHOUSE | Pluralized |
| `DIM.DIM_EVALUATOR` | `WAREHOUSE.dim_evaluators` | DIM → WAREHOUSE | Pluralized |
| `DIM.DIM_FACILITY` | `WAREHOUSE.dim_facilities` | DIM → WAREHOUSE | Pluralized, renamed |
| `DIM.DIM_EVALUATION_TYPE` | `WAREHOUSE.dim_evaluation_types` | DIM → WAREHOUSE | Pluralized |
| `DIM.DIM_MEDICAL_CONDITION` | `WAREHOUSE.dim_medical_conditions` | DIM → WAREHOUSE | Pluralized |
| `DIM.DIM_CLAIM` | `WAREHOUSE.dim_claims` | DIM → WAREHOUSE | Pluralized |
| `DIM.DIM_APPOINTMENT` | `WAREHOUSE.dim_appointments` | DIM → WAREHOUSE | Pluralized |

### Fact Tables

| Current Name | New Name | Schema Change | Notes |
|--------------|----------|---------------|-------|
| `FACT.FACT_EVALUATION` | `WAREHOUSE.fact_evaluations_completed` | FACT → WAREHOUSE | Event-based naming |
| `FACT.FACT_CLAIM_STATUS` | `WAREHOUSE.fact_claim_status_changes` | FACT → WAREHOUSE | Event-based naming |
| `FACT.FACT_APPOINTMENT` | `WAREHOUSE.fact_appointments_scheduled` | FACT → WAREHOUSE | Event-based naming |
| `FACT.FACT_DAILY_SNAPSHOT` | `WAREHOUSE.fact_daily_facility_snapshot` | FACT → WAREHOUSE | Clarified granularity |

### Key Column Examples

| Current Pattern | New Pattern | Example Transformation |
|-----------------|-------------|------------------------|
| `VETERAN_KEY` | `veteran_sk` | `DIM_VETERAN.VETERAN_KEY` → `dim_veterans.veteran_sk` |
| `EVALUATOR_KEY` | `evaluator_sk` | `DIM_EVALUATOR.EVALUATOR_KEY` → `dim_evaluators.evaluator_sk` |
| `FACILITY_KEY` | `facility_sk` | `DIM_FACILITY.FACILITY_KEY` → `dim_facilities.facility_sk` |
| `DATE_KEY` | `date_sk` | `DIM_DATE.DATE_KEY` → `dim_dates.date_sk` |
| `VETERAN_ID` | `veteran_id` | (No change - already correct pattern) |
| `FIRST_NAME` | `first_name` | (Case change only) |
| `IS_CURRENT` | `is_current` | (Case change only) |

---

## Sample Before/After Comparison

### Before (Current):
```sql
-- Query example with current naming
SELECT
    v.VETERAN_ID,
    v.FULL_NAME,
    v.SERVICE_BRANCH,
    e.EVALUATION_DURATION_MINUTES,
    e.REPORT_COMPLETENESS_SCORE,
    d.FULL_DATE
FROM VETERAN_EVALUATION_DW.FACT.FACT_EVALUATION e
JOIN VETERAN_EVALUATION_DW.DIM.DIM_VETERAN v
    ON e.VETERAN_KEY = v.VETERAN_KEY
JOIN VETERAN_EVALUATION_DW.DIM.DIM_DATE d
    ON e.EVALUATION_DATE_KEY = d.DATE_KEY
WHERE v.IS_CURRENT = TRUE
  AND d.FISCAL_YEAR = 2024;
```

### After (VES Aligned):
```sql
-- Query example with VES naming conventions
SELECT
    v.veteran_id,
    v.full_name,
    v.service_branch,
    e.evaluation_duration_minutes,
    e.report_completeness_score,
    d.full_date
FROM VESDW_PRD.WAREHOUSE.fact_evaluations_completed e
JOIN VESDW_PRD.WAREHOUSE.dim_veterans v
    ON e.veteran_sk = v.veteran_sk
JOIN VESDW_PRD.WAREHOUSE.dim_dates d
    ON e.evaluation_date_sk = d.date_sk
WHERE v.is_current = TRUE
  AND d.fiscal_year = 2024;
```

**Key Differences**:
- Database: `VETERAN_EVALUATION_DW` → `VESDW_PRD`
- Schemas: `DIM`/`FACT` → `WAREHOUSE`
- Tables: UPPERCASE → lowercase (e.g., `DIM_VETERAN` → `dim_veterans`)
- Columns: UPPERCASE → lowercase (e.g., `VETERAN_KEY` → `veteran_sk`)
- Keys: `*_KEY` → `*_sk` (e.g., `VETERAN_KEY` → `veteran_sk`)

---

## Recommendations

### Immediate Actions (Required)

1. **🔴 Decision Point**: Choose migration strategy (Option 1 recommended)
2. **🔴 Stakeholder Approval**: Get approval from data architecture team
3. **🔴 Timeline Commitment**: Allocate 4 days for refactoring before deployment

### Best Practices Going Forward

1. **Enforce Standards**: Use SQL linters (SQLFluff) configured for VES conventions
2. **Code Reviews**: Require naming convention checks in pull request reviews
3. **Templates**: Create object templates that follow VES standards
4. **Documentation**: Keep this alignment report for reference during development
5. **Training**: Share VES naming conventions with all team members

### Risk Mitigation

1. **Automated Testing**: Create data quality tests that validate object existence
2. **Deployment Scripts**: Use variables for schema/table names to ease future changes
3. **Version Control**: Use git tags to mark pre/post-migration states
4. **Rollback Plan**: Document rollback procedures in case of deployment issues

---

## Conclusion

While the current dimensional model is **technically sound** with excellent data modeling practices (SCD Type 2, comprehensive comments, clustering keys), it **does not align** with the VES Snowflake naming conventions that govern the enterprise data warehouse.

**The recommended path forward is Option 1: Complete Refactoring** before initial deployment. This ensures:
- ✅ Clean alignment with VES enterprise architecture
- ✅ No technical debt from day one
- ✅ Easier collaboration with other VES data teams
- ✅ Compliance with established standards
- ✅ Reduced long-term maintenance costs

**Next Step**: Get stakeholder approval to proceed with the refactoring effort.

---

**Document Version**: 1.0
**Author**: Data Engineering Team
**Review Required**: Data Architecture Team, VES Technical Leadership
**Status**: **AWAITING APPROVAL**
