-- =====================================================================================================================
-- FUNCTION NAMING STANDARDIZATION: Environment Functions
-- =====================================================================================================================
-- Purpose: Rename environment configuration functions to follow fn_ naming convention
-- Impact: Updates get_dw_database, get_ods_database, get_dw_environment to use fn_ prefix
--
-- IMPORTANT: This script must be run BEFORE updating references in other files
--
-- Functions to rename:
--   get_dw_database()      -> fn_get_dw_database()
--   get_ods_database()     -> fn_get_ods_database()
--   get_dw_environment()   -> fn_get_dw_environment()
--
-- Deployment Strategy:
--   Step 1: Create new fn_ prefixed functions (keeping old ones)
--   Step 2: Test new functions work correctly
--   Step 3: Update all references to use new function names (separate migration)
--   Step 4: Drop old functions after validation period
-- =====================================================================================================================

-- =====================================================================================================================
-- STEP 1: Create New Functions with fn_ Prefix
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_get_dw_database
-- Purpose: Returns the data warehouse database name for the current environment
-- Replaces: get_dw_database()
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_dw_database()
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns data warehouse database name from environment_config. Follows fn_ naming convention for all UDFs.'
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'DW_DATABASE'
      AND UPPER(environment_name) = UPPER(CURRENT_DATABASE())
    LIMIT 1
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_get_ods_database
-- Purpose: Returns the ODS (Operational Data Store) database name for the current environment
-- Replaces: get_ods_database()
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_ods_database()
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns ODS database name from environment_config. Follows fn_ naming convention for all UDFs.'
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'ODS_DATABASE'
      AND UPPER(environment_name) = UPPER(CURRENT_DATABASE())
    LIMIT 1
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Function: fn_get_dw_environment
-- Purpose: Returns the current environment name (DEV, TEST, PROD)
-- Replaces: get_dw_environment()
-- ---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_dw_environment()
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns current environment name (DEV, TEST, PROD) from environment_config. Follows fn_ naming convention for all UDFs.'
AS
$$
    SELECT UPPER(environment_name)
    FROM environment_config
    WHERE UPPER(environment_name) = UPPER(CURRENT_DATABASE())
    LIMIT 1
$$;

-- =====================================================================================================================
-- STEP 2: Test New Functions
-- =====================================================================================================================

-- Test fn_get_dw_database
SELECT fn_get_dw_database() AS dw_database;
-- Expected: Your data warehouse database name (e.g., 'VES_DW', 'VES_DW_DEV', etc.)

-- Test fn_get_ods_database
SELECT fn_get_ods_database() AS ods_database;
-- Expected: Your ODS database name (e.g., 'VES_ODS', 'VES_ODS_DEV', etc.)

-- Test fn_get_dw_environment
SELECT fn_get_dw_environment() AS environment;
-- Expected: 'DEV', 'TEST', or 'PROD'

-- Compare old vs new (both should return same values)
SELECT
    get_dw_database() AS old_dw_function,
    fn_get_dw_database() AS new_dw_function,
    CASE WHEN old_dw_function = new_dw_function THEN '✅ MATCH' ELSE '❌ MISMATCH' END AS dw_validation,

    get_ods_database() AS old_ods_function,
    fn_get_ods_database() AS new_ods_function,
    CASE WHEN old_ods_function = new_ods_function THEN '✅ MATCH' ELSE '❌ MISMATCH' END AS ods_validation,

    get_dw_environment() AS old_env_function,
    fn_get_dw_environment() AS new_env_function,
    CASE WHEN old_env_function = new_env_function THEN '✅ MATCH' ELSE '❌ MISMATCH' END AS env_validation;

-- All validations should show ✅ MATCH

-- =====================================================================================================================
-- STEP 3: Create Backward-Compatibility Wrappers (Temporary)
-- =====================================================================================================================
-- These allow old code to continue working while migration is in progress
-- Drop these after all references are updated

CREATE OR REPLACE FUNCTION get_dw_database()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'DEPRECATED: Use fn_get_dw_database() instead. Temporary wrapper for backward compatibility during migration.'
AS
$$
    SELECT fn_get_dw_database()
$$;

CREATE OR REPLACE FUNCTION get_ods_database()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'DEPRECATED: Use fn_get_ods_database() instead. Temporary wrapper for backward compatibility during migration.'
AS
$$
    SELECT fn_get_ods_database()
$$;

CREATE OR REPLACE FUNCTION get_dw_environment()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'DEPRECATED: Use fn_get_dw_environment() instead. Temporary wrapper for backward compatibility during migration.'
AS
$$
    SELECT fn_get_dw_environment()
$$;

-- =====================================================================================================================
-- STEP 4: Verify All Functions Work
-- =====================================================================================================================

SHOW FUNCTIONS LIKE '%get_%database%';
SHOW FUNCTIONS LIKE '%get_%environment%';

-- Should see both old and new versions

SELECT
    'fn_get_dw_database' AS function_name,
    fn_get_dw_database() AS result
UNION ALL
SELECT 'fn_get_ods_database', fn_get_ods_database()
UNION ALL
SELECT 'fn_get_dw_environment', fn_get_dw_environment()
UNION ALL
SELECT 'get_dw_database (old)', get_dw_database()
UNION ALL
SELECT 'get_ods_database (old)', get_ods_database()
UNION ALL
SELECT 'get_dw_environment (old)', get_dw_environment();

-- All should return valid values

-- =====================================================================================================================
-- STEP 5: Clean Up Old Functions (After Migration Complete)
-- =====================================================================================================================
-- DO NOT RUN UNTIL ALL REFERENCES ARE UPDATED
-- Wait 2-4 weeks after full deployment to ensure no issues

/*
-- Drop old functions (uncomment after migration complete)
DROP FUNCTION IF EXISTS get_dw_database();
DROP FUNCTION IF EXISTS get_ods_database();
DROP FUNCTION IF EXISTS get_dw_environment();

-- Verify only fn_ versions remain
SHOW FUNCTIONS LIKE '%get_%database%';
-- Expected: Only fn_get_dw_database and fn_get_ods_database

SHOW FUNCTIONS LIKE '%get_%environment%';
-- Expected: Only fn_get_dw_environment
*/

-- =====================================================================================================================
-- SUMMARY
-- =====================================================================================================================
-- ✅ Step 1: New fn_ prefixed functions created
-- ✅ Step 2: Functions tested and validated
-- ✅ Step 3: Backward-compatibility wrappers created
-- ⬜ Step 4: Update all references (see FUNCTION_NAMING_MIGRATION_GUIDE.md)
-- ⬜ Step 5: Drop old functions after validation period
-- =====================================================================================================================

-- =====================================================================================================================
-- MAINTENANCE LOG
-- =====================================================================================================================
-- Date       | Author              | Change
-- -----------|---------------------|------------------------------------------------------------------------
-- 2025-11-20 | Data Engineering    | Initial creation - Rename environment functions to fn_ prefix
--            |                     | Created fn_get_dw_database, fn_get_ods_database, fn_get_dw_environment
--            |                     | Added backward-compatibility wrappers for graceful migration
-- =====================================================================================================================
