-- =====================================================================================================================
-- FIX: Function Naming Inconsistencies
-- =====================================================================================================================
-- Purpose: Update all references from old function names (get_*) to new standardized names (fn_get_*)
-- Priority: CRITICAL (Priority 1)
-- Impact: Resolves naming inconsistency across entire codebase
--
-- Background:
-- - The codebase has standardized on fn_ prefix for all UDFs
-- - Many files still use deprecated get_dw_database(), get_ods_database(), get_dw_environment()
-- - Backward-compatibility wrappers exist but should be removed after migration
--
-- This script:
-- 1. Verifies new functions exist
-- 2. Tests that they work correctly
-- 3. Documents all files that need manual updates
-- 4. Provides verification queries
--
-- Author: Code Review Remediation
-- Date: 2025-11-22
-- =====================================================================================================================

-- =====================================================================================================================
-- STEP 1: VERIFY NEW FUNCTIONS EXIST
-- =====================================================================================================================

SELECT 'Verifying new functions exist...' AS step;

-- Check if new functions are created
SHOW FUNCTIONS LIKE '%fn_get_%';

-- Expected output: fn_get_dw_database, fn_get_ods_database, fn_get_dw_environment

-- =====================================================================================================================
-- STEP 2: TEST NEW FUNCTIONS
-- =====================================================================================================================

SELECT 'Testing new functions...' AS step;

-- Test fn_get_dw_database
SELECT fn_get_dw_database() AS dw_database;
-- Expected: Your data warehouse database name (e.g., 'VES_DW', 'VES_DW_DEV')

-- Test fn_get_ods_database
SELECT fn_get_ods_database() AS ods_database;
-- Expected: Your ODS database name (e.g., 'VES_ODS', 'VES_ODS_DEV')

-- Test fn_get_dw_environment
SELECT fn_get_dw_environment() AS environment;
-- Expected: 'DEV', 'TEST', or 'PROD'

-- =====================================================================================================================
-- STEP 3: COMPARE OLD VS NEW (Should be identical)
-- =====================================================================================================================

SELECT 'Comparing old vs new functions...' AS step;

SELECT
    get_dw_database() AS old_dw_function,
    fn_get_dw_database() AS new_dw_function,
    CASE
        WHEN get_dw_database() = fn_get_dw_database() THEN 'MATCH ✅'
        ELSE 'MISMATCH ❌'
    END AS dw_validation,

    get_ods_database() AS old_ods_function,
    fn_get_ods_database() AS new_ods_function,
    CASE
        WHEN get_ods_database() = fn_get_ods_database() THEN 'MATCH ✅'
        ELSE 'MISMATCH ❌'
    END AS ods_validation,

    get_dw_environment() AS old_env_function,
    fn_get_dw_environment() AS new_env_function,
    CASE
        WHEN get_dw_environment() = fn_get_dw_environment() THEN 'MATCH ✅'
        ELSE 'MISMATCH ❌'
    END AS env_validation;

-- All validations should show "MATCH ✅"
-- If any show "MISMATCH ❌", STOP and investigate before proceeding

-- =====================================================================================================================
-- STEP 4: MANUAL FILE UPDATES REQUIRED
-- =====================================================================================================================

SELECT 'FILES REQUIRING MANUAL UPDATES:' AS step;

/*
The following files need manual find-and-replace:

FILE: snowflake/schema/00_setup_database.sql
LINE: 9
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/schema/01_populate_date_dimension.sql
LINE: 8
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/schema/02_master_deployment.sql
LINES: 82, 90, 98, 144, 148, 156, 188, 189, 191, 193, 196, 201, 202
FIND:    get_dw_database()
REPLACE: fn_get_dw_database()

FILE: snowflake/staging/01_create_staging_tables.sql
LINES: 8, 485
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());
AND
FIND:    FROM IDENTIFIER(get_dw_database() || '.INFORMATION_SCHEMA.TABLES')
REPLACE: FROM IDENTIFIER(fn_get_dw_database() || '.INFORMATION_SCHEMA.TABLES')

FILE: snowflake/quality/00_advanced_data_quality_framework.sql
LINE: 27
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/testing/01_create_qa_framework.sql
LINE: 8
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/reference/01_create_reference_tables.sql
LINE: 8
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/etl/01_etl_procedures_dimensions.sql
LINES: 8, 24, 25
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());
AND
FIND:    v_dw_database VARCHAR DEFAULT (SELECT get_dw_database());
REPLACE: v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
AND
FIND:    v_ods_database VARCHAR DEFAULT (SELECT get_ods_database());
REPLACE: v_ods_database VARCHAR DEFAULT (SELECT fn_get_ods_database());

FILE: snowflake/dimensions/01_dim_date.sql
LINES: 8
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/dimensions/02_dim_veteran.sql
LINES: 8
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());

FILE: snowflake/dimensions/10_dim_specialty.sql
LINES: 8, 123
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());
AND
FIND:    FROM IDENTIFIER(get_dw_database() || '.REFERENCE.ref_code_mapping_specialty')
REPLACE: FROM IDENTIFIER(fn_get_dw_database() || '.REFERENCE.ref_code_mapping_specialty')

FILE: snowflake/facts/01_fact_evaluation.sql
LINES: 8, 96, 97, 98, 99, 100, 101, 102, 103
FIND:    SET dw_database = (SELECT get_dw_database());
REPLACE: SET dw_database = (SELECT fn_get_dw_database());
AND
FIND:    FOREIGN KEY (veteran_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_veterans')(veteran_sk),
REPLACE: FOREIGN KEY (veteran_sk) REFERENCES IDENTIFIER(fn_get_dw_database() || '.WAREHOUSE.dim_veterans')(veteran_sk),
... (repeat for all FOREIGN KEY constraints)

*/

-- =====================================================================================================================
-- STEP 5: VERIFICATION AFTER MANUAL UPDATES
-- =====================================================================================================================

SELECT 'After updating all files, run this verification:' AS step;

/*
-- Search for any remaining references to old functions
-- (Run this in your IDE/editor to search across all .sql files)

SEARCH PATTERN: "get_dw_database\(\)"
EXPECTED RESULTS: Should only appear in:
  - snowflake/functions/01_rename_environment_functions.sql (backward-compatibility wrapper)

SEARCH PATTERN: "get_ods_database\(\)"
EXPECTED RESULTS: Should only appear in:
  - snowflake/functions/01_rename_environment_functions.sql (backward-compatibility wrapper)

SEARCH PATTERN: "get_dw_environment\(\)"
EXPECTED RESULTS: Should only appear in:
  - snowflake/functions/01_rename_environment_functions.sql (backward-compatibility wrapper)
*/

-- =====================================================================================================================
-- STEP 6: REMOVE BACKWARD-COMPATIBILITY WRAPPERS (After 2-4 weeks of successful operation)
-- =====================================================================================================================

SELECT 'After 2-4 weeks of successful operation, remove deprecated functions:' AS step;

/*
-- ONLY RUN THIS AFTER VERIFYING ALL FILES HAVE BEEN UPDATED
-- WAIT 2-4 WEEKS TO ENSURE NO HIDDEN DEPENDENCIES

DROP FUNCTION IF EXISTS get_dw_database();
DROP FUNCTION IF EXISTS get_ods_database();
DROP FUNCTION IF EXISTS get_dw_environment();

-- Verify only new functions remain
SHOW FUNCTIONS LIKE '%get_%database%';
-- Expected: Only fn_get_dw_database and fn_get_ods_database

SHOW FUNCTIONS LIKE '%get_%environment%';
-- Expected: Only fn_get_dw_environment
*/

-- =====================================================================================================================
-- STEP 7: ADD REGRESSION PREVENTION
-- =====================================================================================================================

SELECT 'Add linting rule to prevent future regressions:' AS step;

/*
-- Add to your CI/CD pipeline or pre-commit hooks:

#!/bin/bash
# Check for deprecated function usage

if grep -r "get_dw_database()" --include="*.sql" --exclude-dir="fixes" .; then
    echo "❌ ERROR: Found deprecated get_dw_database() usage"
    echo "Use fn_get_dw_database() instead"
    exit 1
fi

if grep -r "get_ods_database()" --include="*.sql" --exclude-dir="fixes" .; then
    echo "❌ ERROR: Found deprecated get_ods_database() usage"
    echo "Use fn_get_ods_database() instead"
    exit 1
fi

if grep -r "get_dw_environment()" --include="*.sql" --exclude-dir="fixes" .; then
    echo "❌ ERROR: Found deprecated get_dw_environment() usage"
    echo "Use fn_get_dw_environment() instead"
    exit 1
fi

echo "✅ All function naming checks passed"
*/

-- =====================================================================================================================
-- SUMMARY & NEXT STEPS
-- =====================================================================================================================

SELECT '====================================' AS summary;
SELECT 'FUNCTION NAMING FIX - SUMMARY' AS summary;
SELECT '====================================' AS summary;

SELECT 'Total Files to Update: 12+' AS statistic
UNION ALL
SELECT 'Find Pattern: get_dw_database(), get_ods_database(), get_dw_environment()' AS statistic
UNION ALL
SELECT 'Replace Pattern: fn_get_dw_database(), fn_get_ods_database(), fn_get_dw_environment()' AS statistic
UNION ALL
SELECT 'Priority: CRITICAL' AS statistic
UNION ALL
SELECT 'Estimated Time: 2-3 hours for manual updates + testing' AS statistic;

SELECT '====================================' AS next_steps;
SELECT 'NEXT STEPS' AS next_steps;
SELECT '====================================' AS next_steps;

SELECT '1. Review the list of files above' AS step
UNION ALL
SELECT '2. Use find-and-replace in your IDE to update all instances' AS step
UNION ALL
SELECT '3. Test each file individually in DEV environment' AS step
UNION ALL
SELECT '4. Run full ETL pipeline to verify no regressions' AS step
UNION ALL
SELECT '5. Deploy to TEST environment' AS step
UNION ALL
SELECT '6. After 2-4 weeks, remove backward-compatibility wrappers' AS step
UNION ALL
SELECT '7. Add linting rules to CI/CD pipeline' AS step;

-- =====================================================================================================================
-- END OF SCRIPT
-- =====================================================================================================================
