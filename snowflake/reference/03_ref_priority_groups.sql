-- =====================================================================================================================
-- REFERENCE TABLE: VA Priority Groups
-- =====================================================================================================================
-- Purpose: Data-driven VA Priority Group definitions and eligibility rules
-- Replaces: Validation logic scattered across ETL procedures
-- Benefits:
--   - Centralized eligibility rules
--   - Self-documenting priority group criteria
--   - Easy to add new groups or update rules
--   - Supports eligibility determination and reporting
--
-- Background: VA Priority Groups determine enrollment priority and copay requirements
--   Group 1 (highest priority) to Group 8 (lowest priority)
--   Based on: service-connected disability rating, income level, special eligibility
--
-- Usage:
--   SELECT fn_validate_priority_group(5);  -- Returns 5
--   SELECT * FROM reference.ref_priority_groups WHERE priority_group = 3;
-- =====================================================================================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA REFERENCE;

-- =====================================================================================================================
-- TABLE: VA Priority Groups
-- =====================================================================================================================

CREATE OR REPLACE TABLE ref_priority_groups (
    priority_group INTEGER PRIMARY KEY,
    group_name VARCHAR(100) NOT NULL,
    description TEXT,
    eligibility_criteria TEXT,

    -- Key characteristics
    service_connected_required BOOLEAN DEFAULT FALSE,
    min_disability_rating INTEGER,
    max_disability_rating INTEGER,

    -- Financial
    copay_required BOOLEAN DEFAULT TRUE,
    copay_category VARCHAR(50),
    income_limits_apply BOOLEAN DEFAULT TRUE,

    -- Special categories
    special_eligibility_flags ARRAY,  -- e.g., ['POW', 'PURPLE_HEART', 'MOH']

    -- Benefits
    enrollment_priority INTEGER,  -- 1 = highest
    pharmacy_copay_tier VARCHAR(20),
    inpatient_copay_tier VARCHAR(20),

    -- Administrative
    sort_order INTEGER,
    active_flag BOOLEAN DEFAULT TRUE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    end_date DATE,
    notes TEXT,
    reference_url VARCHAR(500),

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'VA Priority Groups for healthcare enrollment. Determines enrollment priority and copay requirements. Based on 38 CFR §17.36.';

-- =====================================================================================================================
-- POPULATE: VA Priority Groups (Current as of 2025)
-- =====================================================================================================================

INSERT INTO ref_priority_groups
    (priority_group, group_name, description, eligibility_criteria,
     service_connected_required, min_disability_rating, max_disability_rating,
     copay_required, income_limits_apply, special_eligibility_flags,
     enrollment_priority, pharmacy_copay_tier, inpatient_copay_tier, sort_order)
VALUES
    (1, 'Priority Group 1',
     'Veterans with service-connected disabilities rated 50% or more',
     'SC disability rating >= 50% OR Unemployable due to SC disability',
     TRUE, 50, 100,
     FALSE, FALSE, ARRAY_CONSTRUCT('UNEMPLOYABLE'),
     1, 'EXEMPT', 'EXEMPT', 1),

    (2, 'Priority Group 2',
     'Veterans with service-connected disabilities rated 30% or 40%',
     'SC disability rating = 30% OR SC disability rating = 40%',
     TRUE, 30, 40,
     FALSE, FALSE, NULL,
     2, 'EXEMPT', 'EXEMPT', 2),

    (3, 'Priority Group 3',
     'Former POWs, Purple Heart recipients, Medal of Honor recipients, and other special groups',
     'Former POW OR Purple Heart OR Medal of Honor OR SC disability 10-20% OR Discharged for disability from service OR Catastrophically disabled',
     FALSE, 0, 20,
     FALSE, FALSE, ARRAY_CONSTRUCT('POW', 'PURPLE_HEART', 'MOH', 'CATASTROPHIC_DISABILITY'),
     3, 'EXEMPT', 'EXEMPT', 3),

    (4, 'Priority Group 4',
     'Veterans receiving aid and attendance or housebound benefits',
     'Receiving VA aid and attendance OR Receiving VA housebound benefits OR Determined catastrophically disabled',
     FALSE, 0, 100,
     FALSE, FALSE, ARRAY_CONSTRUCT('AID_ATTENDANCE', 'HOUSEBOUND'),
     4, 'EXEMPT', 'EXEMPT', 4),

    (5, 'Priority Group 5',
     'Non-service-connected veterans and service-connected 0% veterans with income below VA limit',
     'Non-SC veterans OR SC 0% rated, AND income below VA national income threshold OR receiving VA pension',
     FALSE, 0, 0,
     TRUE, TRUE, ARRAY_CONSTRUCT('LOW_INCOME', 'VA_PENSION'),
     5, 'TIER_1', 'TIER_1', 5),

    (6, 'Priority Group 6',
     'Veterans exposed to toxic substances or radiation, Gulf War veterans, and combat veterans',
     'Compensable 0% SC disability OR Exposed to Agent Orange/radiation OR Gulf War veteran (<5 years) OR Combat veteran (<5 years) OR Camp Lejeune exposure',
     FALSE, 0, 0,
     TRUE, TRUE, ARRAY_CONSTRUCT('AGENT_ORANGE', 'RADIATION', 'GULF_WAR', 'COMBAT_VETERAN', 'CAMP_LEJEUNE'),
     6, 'TIER_2', 'TIER_2', 6),

    (7, 'Priority Group 7',
     'Veterans with income above VA national threshold and below geographic threshold',
     'Non-SC veterans OR SC 0% non-compensable, AND income above national threshold but below geographic threshold',
     FALSE, 0, 0,
     TRUE, TRUE, ARRAY_CONSTRUCT('MODERATE_INCOME'),
     7, 'TIER_3', 'TIER_3', 7),

    (8, 'Priority Group 8',
     'Veterans with income above both VA and geographic thresholds',
     'Non-SC veterans OR SC 0% non-compensable, AND income above both national and geographic thresholds OR Net worth above VA limit',
     FALSE, 0, 0,
     TRUE, TRUE, ARRAY_CONSTRUCT('HIGH_INCOME'),
     8, 'TIER_4', 'TIER_4', 8);

-- =====================================================================================================================
-- FUNCTION: Get Priority Group Details
-- =====================================================================================================================

CREATE OR REPLACE FUNCTION fn_get_priority_group_details(group_number INTEGER)
RETURNS OBJECT
LANGUAGE SQL
COMMENT = 'Returns complete priority group details including eligibility, copays, and benefits'
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'priority_group', priority_group,
        'group_name', group_name,
        'description', description,
        'eligibility_criteria', eligibility_criteria,
        'copay_required', copay_required,
        'income_limits_apply', income_limits_apply,
        'pharmacy_copay_tier', pharmacy_copay_tier,
        'inpatient_copay_tier', inpatient_copay_tier
    )
    FROM reference.ref_priority_groups
    WHERE priority_group = group_number
      AND active_flag = TRUE
    LIMIT 1
$$;

-- =====================================================================================================================
-- FUNCTION: Determine Priority Group (Business Logic Helper)
-- =====================================================================================================================

CREATE OR REPLACE FUNCTION fn_determine_priority_group(
    disability_rating INTEGER,
    service_connected_flag BOOLEAN,
    income_level VARCHAR,  -- 'LOW', 'MODERATE', 'HIGH'
    special_flags ARRAY    -- e.g., ['POW', 'PURPLE_HEART']
)
RETURNS INTEGER
LANGUAGE SQL
COMMENT = 'Determines appropriate VA priority group based on veteran characteristics (SQL version). Returns priority group number (1-8).'
AS
$$
    CASE
        -- Priority Group 1: SC >= 50%
        WHEN service_connected_flag AND disability_rating >= 50 THEN 1

        -- Priority Group 2: SC 30-40%
        WHEN service_connected_flag AND disability_rating >= 30 AND disability_rating <= 40 THEN 2

        -- Priority Group 3: Special eligibility flags
        WHEN special_flags IS NOT NULL AND (
            ARRAY_CONTAINS('POW'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('PURPLE_HEART'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('MOH'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('CATASTROPHIC_DISABILITY'::VARIANT, special_flags)
        ) THEN 3

        -- Priority Group 3: SC 10-20%
        WHEN service_connected_flag AND disability_rating >= 10 AND disability_rating <= 20 THEN 3

        -- Priority Group 4: Aid and attendance
        WHEN special_flags IS NOT NULL AND (
            ARRAY_CONTAINS('AID_ATTENDANCE'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('HOUSEBOUND'::VARIANT, special_flags)
        ) THEN 4

        -- Priority Group 5: Non-SC with low income
        WHEN NOT service_connected_flag AND income_level = 'LOW' THEN 5

        -- Priority Group 6: Special exposures
        WHEN special_flags IS NOT NULL AND (
            ARRAY_CONTAINS('AGENT_ORANGE'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('RADIATION'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('GULF_WAR'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('COMBAT_VETERAN'::VARIANT, special_flags) OR
            ARRAY_CONTAINS('CAMP_LEJEUNE'::VARIANT, special_flags)
        ) THEN 6

        -- Priority Group 7: Moderate income
        WHEN income_level = 'MODERATE' THEN 7

        -- Priority Group 8: Default (high income)
        ELSE 8
    END
$$;

-- =====================================================================================================================
-- TESTING QUERIES
-- =====================================================================================================================

/*
-- Test the reference table
SELECT * FROM ref_priority_groups ORDER BY priority_group;

-- Test priority group details
SELECT
    priority_group,
    fn_get_priority_group_details(priority_group) AS details
FROM ref_priority_groups;

-- Test priority group determination logic
SELECT
    fn_determine_priority_group(70, TRUE, 'LOW', NULL) AS pg_sc_70,  -- Should return 1
    fn_determine_priority_group(35, TRUE, 'HIGH', NULL) AS pg_sc_35,  -- Should return 2
    fn_determine_priority_group(0, FALSE, 'LOW', ARRAY_CONSTRUCT('POW')) AS pg_pow,  -- Should return 3
    fn_determine_priority_group(0, FALSE, 'LOW', NULL) AS pg_low_income,  -- Should return 5
    fn_determine_priority_group(0, FALSE, 'HIGH', NULL) AS pg_high_income;  -- Should return 8

-- Validate priority groups in veteran dimension
SELECT
    priority_group,
    COUNT(*) AS veteran_count,
    fn_get_priority_group_details(priority_group):group_name AS group_name,
    fn_get_priority_group_details(priority_group):copay_required AS copay_required
FROM warehouse.dim_veterans
WHERE is_current = TRUE
  AND priority_group IS NOT NULL
GROUP BY priority_group
ORDER BY priority_group;
*/

-- =====================================================================================================================
-- EXAMPLE USAGE IN ETL PROCEDURES
-- =====================================================================================================================

/*
-- BEFORE: Simple validation only
CASE WHEN priority_group BETWEEN 1 AND 8 THEN priority_group ELSE NULL END AS priority_group

-- AFTER: Validation with enrichment
fn_validate_priority_group(priority_group) AS priority_group,
fn_get_priority_group_details(priority_group) AS priority_group_details

-- ENHANCED: Determine priority group if missing
COALESCE(
    fn_validate_priority_group(priority_group),
    fn_determine_priority_group(
        disability_rating,
        service_connected_flag,
        CASE
            WHEN annual_income < 35000 THEN 'LOW'
            WHEN annual_income < 70000 THEN 'MODERATE'
            ELSE 'HIGH'
        END,
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN combat_veteran_flag THEN 'COMBAT_VETERAN' END,
            CASE WHEN pow_flag THEN 'POW' END,
            CASE WHEN purple_heart_flag THEN 'PURPLE_HEART' END
        )
    )
) AS priority_group
*/

-- =====================================================================================================================
-- MAINTENANCE LOG
-- =====================================================================================================================
-- Date       | Author              | Change
-- -----------|---------------------|------------------------------------------------------------------------
-- 2025-11-20 | Data Engineering    | Initial creation - VA priority groups reference table
--            |                     | Based on 38 CFR §17.36 enrollment eligibility rules
--            |                     | Includes priority group determination business logic
-- =====================================================================================================================
