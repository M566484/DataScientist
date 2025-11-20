# Function Naming Standardization Migration Guide

## Overview

This guide provides step-by-step instructions for migrating from the old function naming convention to the standardized `fn_` prefix convention.

**Functions Being Renamed:**
- `get_dw_database()` → `fn_get_dw_database()`
- `get_ods_database()` → `fn_get_ods_database()`
- `get_dw_environment()` → `fn_get_dw_environment()`

**Files Affected:** 20+ SQL files across the snowflake directory

**Migration Strategy:** Phased approach with backward compatibility

---

## Why This Change?

### Consistency
All user-defined functions now follow the same naming pattern:
- ✅ `fn_normalize_string_upper()`
- ✅ `fn_clean_phone_number()`
- ✅ `fn_calculate_age()`
- ✅ `fn_get_dw_database()` ← **NEW**

### Clarity
The `fn_` prefix immediately identifies user-defined functions vs Snowflake built-ins:
- `fn_get_dw_database()` - Clearly a UDF
- `CURRENT_DATABASE()` - Clearly a built-in

### Standards
Aligns with the naming convention established in the refactoring project (25+ functions).

---

## Migration Timeline

| Phase | Duration | Activities | Risk |
|-------|----------|------------|------|
| **Phase 1** | 30 minutes | Create new functions with fn_ prefix | 🟢 Low |
| **Phase 2** | 1-2 hours | Update references in new files | 🟢 Low |
| **Phase 3** | 2-3 hours | Update references in existing files | 🟡 Medium |
| **Phase 4** | 2-4 weeks | Validation period | 🟢 Low |
| **Phase 5** | 30 minutes | Remove old functions | 🟢 Low |

**Total Active Time:** 3-5 hours
**Total Calendar Time:** 2-4 weeks (for validation)

---

## Phase 1: Create New Functions (30 minutes)

### Step 1.1: Deploy New Functions

```sql
-- Run this script
@snowflake/functions/01_rename_environment_functions.sql

-- Or execute the CREATE OR REPLACE FUNCTION statements for:
-- - fn_get_dw_database()
-- - fn_get_ods_database()
-- - fn_get_dw_environment()
```

### Step 1.2: Verify Functions Work

```sql
-- Test new functions
SELECT
    fn_get_dw_database() AS dw_db,
    fn_get_ods_database() AS ods_db,
    fn_get_dw_environment() AS env;

-- Compare with old functions (should match)
SELECT
    get_dw_database() AS old_dw,
    fn_get_dw_database() AS new_dw,
    CASE WHEN old_dw = new_dw THEN '✅' ELSE '❌' END AS dw_match,

    get_ods_database() AS old_ods,
    fn_get_ods_database() AS new_ods,
    CASE WHEN old_ods = new_ods THEN '✅' ELSE '❌' END AS ods_match;
```

### Step 1.3: Create Backward Compatibility Wrappers

The script already creates these - old functions now call new ones:

```sql
-- Old functions still work (call new functions internally)
CREATE OR REPLACE FUNCTION get_dw_database()
AS $$ SELECT fn_get_dw_database() $$;

-- This ensures existing code continues working during migration
```

**Result:** ✅ New functions active, old functions still work via wrappers

---

## Phase 2: Update New Files (1-2 hours)

Files created during the refactoring project should use new names from the start.

### Files to Update:

1. ✅ **snowflake/functions/00_common_data_functions.sql**
   - No references (✅ already clean)

2. ✅ **snowflake/reference/02_ref_disability_rating_categories.sql**
   ```sql
   -- FIND: get_dw_database()
   -- REPLACE WITH: fn_get_dw_database()
   ```

3. ✅ **snowflake/reference/03_ref_priority_groups.sql**
   ```sql
   -- FIND: get_dw_database()
   -- REPLACE WITH: fn_get_dw_database()
   ```

4. ✅ **snowflake/metadata/01_create_metadata_tables.sql**
   ```sql
   -- FIND: get_dw_database()
   -- REPLACE WITH: fn_get_dw_database()
   ```

5. ✅ **snowflake/etl/00_generic_scd_procedures.sql**
   ```sql
   -- FIND: get_dw_database()
   -- REPLACE WITH: fn_get_dw_database()
   ```

### Automated Find/Replace

For each file, use your editor's find/replace:
- **Find:** `get_dw_database()`
- **Replace:** `fn_get_dw_database()`

Then:
- **Find:** `get_ods_database()`
- **Replace:** `fn_get_ods_database()`

Then:
- **Find:** `get_dw_environment()`
- **Replace:** `fn_get_dw_environment()`

**Result:** ✅ All new refactoring files use standardized naming

---

## Phase 3: Update Existing Files (2-3 hours)

### Priority 1: ETL Procedures (High Impact)

**Files:**
- `snowflake/etl/01_etl_procedures_dimensions.sql`
- `snowflake/etl/02_etl_procedures_facts.sql`
- `snowflake/etl/03_etl_procedures_multi_source.sql`

**Pattern:**
```sql
-- FIND:
SET v_dw_database VARCHAR DEFAULT (SELECT get_dw_database());
SET v_ods_database VARCHAR DEFAULT (SELECT get_ods_database());

-- REPLACE WITH:
SET v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
SET v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());
```

### Priority 2: Dimension Tables

**Files:**
- `snowflake/dimensions/01_dim_date.sql` through `09_dim_exam_request_types.sql`

**Pattern:**
```sql
-- FIND:
SET dw_database = (SELECT get_dw_database());

-- REPLACE WITH:
SET dw_database = (SELECT fn_get_dw_database());
```

### Priority 3: Fact Tables

**Files:**
- `snowflake/facts/*.sql` (all fact table definitions)

**Same pattern as dimensions**

### Priority 4: Schema Setup

**Files:**
- `snowflake/schema/00_setup_database.sql`
- `snowflake/schema/01_populate_date_dimension.sql`
- `snowflake/schema/02_master_deployment.sql`

**Pattern:**
```sql
-- FIND:
SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);

-- REPLACE WITH:
SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
```

---

## Complete File List with Update Instructions

### Files Requiring Updates:

| # | File | References | Priority | Time |
|---|------|------------|----------|------|
| 1 | `etl/00_generic_scd_procedures.sql` | ~10 | HIGH | 5 min |
| 2 | `etl/01_etl_procedures_dimensions.sql` | ~15 | HIGH | 10 min |
| 3 | `etl/02_etl_procedures_facts.sql` | ~12 | HIGH | 10 min |
| 4 | `etl/03_etl_procedures_multi_source.sql` | ~20 | HIGH | 15 min |
| 5 | `metadata/01_create_metadata_tables.sql` | ~5 | HIGH | 5 min |
| 6 | `reference/02_ref_disability_rating_categories.sql` | ~5 | HIGH | 5 min |
| 7 | `reference/03_ref_priority_groups.sql` | ~5 | HIGH | 5 min |
| 8 | `dimensions/01_dim_date.sql` | ~3 | MEDIUM | 3 min |
| 9 | `dimensions/02_dim_veteran.sql` | ~3 | MEDIUM | 3 min |
| 10 | `dimensions/03_dim_evaluator.sql` | ~3 | MEDIUM | 3 min |
| 11 | `dimensions/04_dim_facility.sql` | ~3 | MEDIUM | 3 min |
| 12 | `dimensions/05_dim_evaluation_type.sql` | ~3 | MEDIUM | 3 min |
| 13 | `dimensions/06_dim_medical_condition.sql` | ~3 | MEDIUM | 3 min |
| 14 | `dimensions/07_dim_claim.sql` | ~3 | MEDIUM | 3 min |
| 15 | `dimensions/08_dim_appointment.sql` | ~3 | MEDIUM | 3 min |
| 16 | `dimensions/09_dim_exam_request_types.sql` | ~3 | MEDIUM | 3 min |
| 17 | `facts/01_fact_evaluation.sql` | ~3 | MEDIUM | 3 min |
| 18 | `facts/02_fact_claim_status.sql` | ~3 | MEDIUM | 3 min |
| 19 | `facts/03_fact_appointment.sql` | ~3 | MEDIUM | 3 min |
| 20 | `schema/00_setup_database.sql` | ~2 | MEDIUM | 3 min |

**Total:** ~2-3 hours for all files

---

## Testing Strategy

### After Each File Update:

```sql
-- 1. Syntax check (run the CREATE statement)
-- If no errors, syntax is correct

-- 2. Functional test
SELECT fn_get_dw_database();
-- Should return valid database name

-- 3. Compare old vs new (before removing wrappers)
SELECT
    get_dw_database() AS old,
    fn_get_dw_database() AS new,
    CASE WHEN old = new THEN '✅ MATCH' ELSE '❌ ERROR' END AS status;
```

### After All Updates:

```sql
-- Run comprehensive test
SELECT
    'Dimensions' AS object_type,
    COUNT(*) AS object_count
FROM information_schema.tables
WHERE table_schema = 'WAREHOUSE'
  AND table_type = 'BASE TABLE'
  AND table_name LIKE 'dim_%'
UNION ALL
SELECT
    'Facts',
    COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'WAREHOUSE'
  AND table_type = 'BASE TABLE'
  AND table_name LIKE 'fact_%'
UNION ALL
SELECT
    'Functions (fn_)',
    COUNT(*)
FROM information_schema.functions
WHERE function_schema = 'WAREHOUSE'
  AND function_name LIKE 'fn_%'
UNION ALL
SELECT
    'Functions (old style)',
    COUNT(*)
FROM information_schema.functions
WHERE function_schema = 'WAREHOUSE'
  AND function_name IN ('get_dw_database', 'get_ods_database', 'get_dw_environment');

-- Expected:
-- - Dimensions: 9+
-- - Facts: 9+
-- - Functions (fn_): 28+ (25 from refactoring + 3 environment)
-- - Functions (old style): 3 (wrappers, to be removed in Phase 5)
```

---

## Phase 4: Validation Period (2-4 weeks)

### Week 1-2: Monitor Production

- [ ] Run all ETL pipelines with updated code
- [ ] Monitor for errors or failures
- [ ] Check execution logs
- [ ] Validate data integrity

### Week 3-4: Extended Testing

- [ ] Run end-to-end tests
- [ ] Test all data marts
- [ ] Validate reports and dashboards
- [ ] User acceptance testing

### Success Criteria:

✅ All ETL pipelines run successfully
✅ No errors related to function calls
✅ Data integrity validated
✅ No performance degradation
✅ Stakeholder approval obtained

---

## Phase 5: Remove Old Functions (30 minutes)

**ONLY AFTER 2-4 WEEKS OF SUCCESSFUL PRODUCTION USE**

```sql
-- Verify no references to old function names remain
-- Search all code for:
-- - get_dw_database()
-- - get_ods_database()
-- - get_dw_environment()

-- If any found, update them first!

-- Then drop old functions:
DROP FUNCTION IF EXISTS get_dw_database();
DROP FUNCTION IF EXISTS get_ods_database();
DROP FUNCTION IF EXISTS get_dw_environment();

-- Verify only fn_ versions remain
SHOW FUNCTIONS LIKE '%database%';
SHOW FUNCTIONS LIKE '%environment%';

-- Expected results:
-- ✅ fn_get_dw_database
-- ✅ fn_get_ods_database
-- ✅ fn_get_dw_environment
-- ❌ get_dw_database (should not exist)
-- ❌ get_ods_database (should not exist)
-- ❌ get_dw_environment (should not exist)
```

---

## Rollback Procedure

If issues are encountered during migration:

### Quick Rollback:

```sql
-- Restore old function definitions (if wrappers removed too early)
CREATE OR REPLACE FUNCTION get_dw_database()
RETURNS VARCHAR
AS $$ SELECT fn_get_dw_database() $$;

CREATE OR REPLACE FUNCTION get_ods_database()
RETURNS VARCHAR
AS $$ SELECT fn_get_ods_database() $$;

CREATE OR REPLACE FUNCTION get_dw_environment()
RETURNS VARCHAR
AS $$ SELECT fn_get_dw_environment() $$;

-- Old code will work again via wrappers
```

### Full Rollback:

1. Restore old function definitions from backup
2. Revert file changes using version control
3. Investigate issues before re-attempting

---

## Automated Migration Script

For bulk updates, you can create a Python/PowerShell script:

```python
import os
import re

def update_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # Replace function calls
    content = content.replace('get_dw_database()', 'fn_get_dw_database()')
    content = content.replace('get_ods_database()', 'fn_get_ods_database()')
    content = content.replace('get_dw_environment()', 'fn_get_dw_environment()')

    with open(filepath, 'w') as f:
        f.write(content)

    print(f"Updated: {filepath}")

# Update all SQL files in snowflake directory
for root, dirs, files in os.walk('snowflake'):
    for file in files:
        if file.endswith('.sql'):
            filepath = os.path.join(root, file)
            update_file(filepath)

print("✅ All files updated!")
```

**Note:** Review changes carefully before committing!

---

## Best Practices

### During Migration:

1. ✅ **Update files incrementally** - Don't try to do all at once
2. ✅ **Test after each update** - Catch errors early
3. ✅ **Use version control** - Easy rollback if needed
4. ✅ **Keep wrappers active** - Provides safety net
5. ✅ **Document changes** - Track what's been updated

### After Migration:

1. ✅ **Monitor for 2-4 weeks** - Ensure stability
2. ✅ **Get stakeholder approval** - Before removing old functions
3. ✅ **Update documentation** - Reflect new naming
4. ✅ **Train team** - On new naming convention
5. ✅ **Celebrate** - Consistent naming achieved! 🎉

---

## FAQ

**Q: Can I update all files at once?**
A: Yes, but incremental is safer. Update and test high-priority files first.

**Q: What if I miss a reference?**
A: The backward-compatibility wrappers will keep it working. Fix during validation period.

**Q: How long should I keep the old functions?**
A: At least 2-4 weeks of successful production use. Longer is safer.

**Q: Will this affect performance?**
A: No. The wrapper functions have negligible overhead, and they'll be removed eventually.

**Q: What if someone creates new code with old function names?**
A: The wrappers will make it work, but code reviews should catch and fix it.

---

## Success Checklist

### Phase 1:
- [ ] New fn_ functions created
- [ ] Functions tested and validated
- [ ] Backward compatibility wrappers active

### Phase 2:
- [ ] All new refactoring files updated
- [ ] Changes tested
- [ ] Changes committed to version control

### Phase 3:
- [ ] High priority ETL files updated
- [ ] Medium priority dimension/fact files updated
- [ ] Schema setup files updated
- [ ] All changes tested
- [ ] All changes committed

### Phase 4:
- [ ] Production monitoring complete (2-4 weeks)
- [ ] No errors or issues found
- [ ] Stakeholder approval obtained

### Phase 5:
- [ ] All references verified updated
- [ ] Old functions dropped
- [ ] Final validation complete
- [ ] Documentation updated

---

## Timeline Summary

**Week 1:** Phases 1-3 (Create functions, update files) - 3-5 hours
**Weeks 2-5:** Phase 4 (Validation period) - Monitoring only
**Week 6:** Phase 5 (Remove old functions) - 30 minutes

**Total:** ✅ Complete migration in 6 weeks with validated production use

---

## Conclusion

This migration standardizes all function naming to use the `fn_` prefix, improving:
- ✅ **Consistency** (all UDFs follow same pattern)
- ✅ **Clarity** (UDFs clearly distinguished from built-ins)
- ✅ **Standards** (aligns with refactoring project)

**Status:** Ready to begin Phase 1!

