-- =====================================================================================================================
-- REFERENCE TABLE: VA Disability Rating Categories
-- =====================================================================================================================
-- Purpose: Data-driven categorization of VA disability ratings instead of hardcoded CASE statements
-- Replaces: 8-line CASE statement repeated in 4+ files
-- Benefits:
--   - Business rules changeable via UPDATE, no code deployment
--   - Historical tracking of category definition changes
--   - Additional context (benefit tiers, compensation ranges)
--   - Consistent categories across all ETL and reports
--
-- Usage:
--   SELECT fn_categorize_disability_rating(85);  -- Returns '70-90%'
--   SELECT * FROM reference.ref_disability_rating_categories WHERE 80 BETWEEN min_rating AND max_rating;
-- =====================================================================================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA REFERENCE;

-- =====================================================================================================================
-- TABLE: Disability Rating Categories
-- =====================================================================================================================

CREATE OR REPLACE TABLE ref_disability_rating_categories (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    min_rating INTEGER NOT NULL,
    max_rating INTEGER NOT NULL,
    category_code VARCHAR(10) NOT NULL,
    category_label VARCHAR(20) NOT NULL,
    benefit_tier VARCHAR(20),
    monthly_compensation_min DECIMAL(10,2),
    monthly_compensation_max DECIMAL(10,2),
    monthly_compensation_range VARCHAR(50),
    description TEXT,
    sort_order INTEGER,
    active_flag BOOLEAN DEFAULT TRUE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    end_date DATE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT chk_rating_range CHECK (min_rating <= max_rating),
    CONSTRAINT chk_rating_bounds CHECK (min_rating >= 0 AND max_rating <= 100),
    CONSTRAINT uq_rating_range UNIQUE (min_rating, max_rating)
)
COMMENT = 'VA disability rating categories for compensation and benefits determination. Based on VA Schedule for Rating Disabilities (38 CFR Part 4).';

-- =====================================================================================================================
-- POPULATE: Current VA Disability Rating Categories
-- =====================================================================================================================

INSERT INTO ref_disability_rating_categories
    (min_rating, max_rating, category_code, category_label, benefit_tier,
     monthly_compensation_min, monthly_compensation_max, monthly_compensation_range,
     description, sort_order)
VALUES
    (0, 0, '0PCT', '0%', 'NONE',
     0.00, 0.00, '$0',
     'No service-connected disability or condition not yet rated', 1),

    (10, 30, '10-30PCT', '10-30%', 'LOW',
     171.23, 524.31, '$171-$524',
     'Low-level service-connected disabilities. May qualify for healthcare benefits.', 2),

    (40, 60, '40-60PCT', '40-60%', 'MODERATE',
     755.28, 1437.06, '$755-$1,437',
     'Moderate service-connected disabilities. Eligible for additional dependent compensation.', 3),

    (70, 90, '70-90PCT', '70-90%', 'HIGH',
     1729.05, 2241.39, '$1,729-$2,241',
     'High-level service-connected disabilities. Significant impact on daily functioning.', 4),

    (100, 100, '100PCT', '100%', 'FULL',
     3737.85, 3737.85, '$3,737+',
     'Total service-connected disability. May qualify for TDIU, SMC, or other special compensation.', 5);

-- =====================================================================================================================
-- FUNCTION: Get Disability Rating Category (Simple)
-- =====================================================================================================================
-- This function is already in snowflake/functions/00_common_data_functions.sql
-- But we provide an enhanced version here that uses the reference table

CREATE OR REPLACE FUNCTION fn_categorize_disability_rating_enhanced(rating NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Returns disability rating category label from reference table. Returns NULL for invalid ratings or if no category matches.'
AS
$$
    SELECT category_label
    FROM reference.ref_disability_rating_categories
    WHERE rating BETWEEN min_rating AND max_rating
      AND active_flag = TRUE
      AND (effective_date IS NULL OR effective_date <= CURRENT_DATE())
      AND (end_date IS NULL OR end_date > CURRENT_DATE())
    LIMIT 1
$$;

-- =====================================================================================================================
-- FUNCTION: Get Full Disability Rating Details
-- =====================================================================================================================

CREATE OR REPLACE FUNCTION fn_get_disability_rating_details(rating NUMBER)
RETURNS OBJECT
LANGUAGE SQL
COMMENT = 'Returns complete disability rating details including category, tier, and compensation range. Returns NULL for invalid ratings.'
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'category_code', category_code,
        'category_label', category_label,
        'benefit_tier', benefit_tier,
        'monthly_compensation_range', monthly_compensation_range,
        'monthly_compensation_min', monthly_compensation_min,
        'monthly_compensation_max', monthly_compensation_max,
        'description', description
    )
    FROM reference.ref_disability_rating_categories
    WHERE rating BETWEEN min_rating AND max_rating
      AND active_flag = TRUE
      AND (effective_date IS NULL OR effective_date <= CURRENT_DATE())
      AND (end_date IS NULL OR end_date > CURRENT_DATE())
    LIMIT 1
$$;

-- =====================================================================================================================
-- TESTING QUERIES
-- =====================================================================================================================

/*
-- Test the reference table
SELECT * FROM ref_disability_rating_categories ORDER BY sort_order;

-- Test lookup for various ratings
SELECT
    0 AS rating,
    fn_categorize_disability_rating_enhanced(0) AS category,
    fn_get_disability_rating_details(0) AS details
UNION ALL
SELECT 10, fn_categorize_disability_rating_enhanced(10), fn_get_disability_rating_details(10)
UNION ALL
SELECT 50, fn_categorize_disability_rating_enhanced(50), fn_get_disability_rating_details(50)
UNION ALL
SELECT 85, fn_categorize_disability_rating_enhanced(85), fn_get_disability_rating_details(85)
UNION ALL
SELECT 100, fn_categorize_disability_rating_enhanced(100), fn_get_disability_rating_details(100)
UNION ALL
SELECT 150, fn_categorize_disability_rating_enhanced(150), fn_get_disability_rating_details(150);  -- Should return NULL

-- Compare old vs new approach
SELECT
    disability_rating,
    -- OLD: Hardcoded CASE statement (8 lines)
    CASE
        WHEN disability_rating = 0 THEN '0%'
        WHEN disability_rating BETWEEN 10 AND 30 THEN '10-30%'
        WHEN disability_rating BETWEEN 40 AND 60 THEN '40-60%'
        WHEN disability_rating BETWEEN 70 AND 90 THEN '70-90%'
        WHEN disability_rating = 100 THEN '100%'
        ELSE NULL
    END AS old_method,
    -- NEW: Function call (1 line)
    fn_categorize_disability_rating_enhanced(disability_rating) AS new_method,
    -- Should be identical
    CASE WHEN old_method = new_method THEN '✓ MATCH' ELSE '✗ MISMATCH' END AS validation
FROM warehouse.dim_veterans
WHERE is_current = TRUE
LIMIT 100;
*/

-- =====================================================================================================================
-- EXAMPLE: Changing Business Rules (No Code Deployment Required!)
-- =====================================================================================================================

/*
-- Example 1: Split the 70-90% category into two separate categories
-- This can be done via simple UPDATE/INSERT without touching ETL code!

-- First, deactivate the old 70-90% category
UPDATE ref_disability_rating_categories
SET active_flag = FALSE, end_date = CURRENT_DATE()
WHERE category_code = '70-90PCT';

-- Insert two new categories
INSERT INTO ref_disability_rating_categories
    (min_rating, max_rating, category_code, category_label, benefit_tier,
     monthly_compensation_range, description, sort_order)
VALUES
    (70, 80, '70-80PCT', '70-80%', 'HIGH', '$1,729-$2,019', 'High-level disabilities', 4),
    (90, 90, '90PCT', '90%', 'VERY_HIGH', '$2,241', 'Very high-level disabilities', 5);

-- All ETL procedures now automatically use the new categories!
*/

-- =====================================================================================================================
-- MAINTENANCE LOG
-- =====================================================================================================================
-- Date       | Author              | Change
-- -----------|---------------------|------------------------------------------------------------------------
-- 2025-11-20 | Data Engineering    | Initial creation - VA disability rating categories reference table
--            |                     | Replaces hardcoded CASE statements in 4+ files
--            |                     | Based on 2025 VA compensation rates
-- =====================================================================================================================
