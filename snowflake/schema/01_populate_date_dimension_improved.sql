-- =====================================================
-- Populate dim_dates - Date Dimension Population (IMPROVED)
-- =====================================================
-- Purpose: Populate date dimension using configuration values
-- Standards: VES Snowflake Naming Conventions v1.0
--
-- IMPROVEMENTS:
-- - Uses fn_get_config() instead of hardcoded dates
-- - Configurable date ranges
-- - Better error handling
-- - Validates configuration exists
--
-- Author: Phase 2 Improvements
-- Date: 2025-11-22
-- =====================================================

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

-- Create improved procedure to populate the date dimension
CREATE OR REPLACE PROCEDURE populate_dim_dates_from_config()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Populate date dimension using values from system_configuration table. No hardcoded dates.'
AS
$$
DECLARE
    v_start_date DATE;
    v_end_date DATE;
    v_rows_inserted INTEGER;
    v_error_message VARCHAR;
BEGIN
    -- Get date range from configuration
    BEGIN
        v_start_date := TRY_TO_DATE(fn_get_config('date_dimension', 'start_date'));
        v_end_date := TRY_TO_DATE(fn_get_config('date_dimension', 'end_date'));
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: Failed to read date dimension configuration: ' || SQLERRM;
    END;

    -- Validate configuration values
    IF (v_start_date IS NULL) THEN
        RETURN 'ERROR: date_dimension.start_date not configured in system_configuration';
    END IF;

    IF (v_end_date IS NULL) THEN
        RETURN 'ERROR: date_dimension.end_date not configured in system_configuration';
    END IF;

    IF (v_start_date >= v_end_date) THEN
        RETURN 'ERROR: start_date must be before end_date. Got start=' || v_start_date || ', end=' || v_end_date;
    END IF;

    -- Call original populate procedure with config values
    BEGIN
        CALL populate_dim_dates(v_start_date, v_end_date);

        SELECT SQLROWCOUNT INTO v_rows_inserted;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'ERROR: Failed to populate date dimension: ' || SQLERRM;
            RETURN v_error_message;
    END;

    RETURN 'SUCCESS: Populated date dimension with ' || v_rows_inserted ||
           ' rows from ' || v_start_date || ' to ' || v_end_date;
END;
$$;

-- Create original procedure (with parameters) for flexibility
CREATE OR REPLACE PROCEDURE populate_dim_dates(
    start_date DATE,
    end_date DATE
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Populate date dimension with specified date range. Use populate_dim_dates_from_config() for config-driven approach.'
AS
$$
DECLARE
    rows_inserted INTEGER;
BEGIN
    -- Validate parameters
    IF (start_date IS NULL OR end_date IS NULL) THEN
        RETURN 'ERROR: start_date and end_date parameters are required';
    END IF;

    IF (start_date >= end_date) THEN
        RETURN 'ERROR: start_date must be before end_date';
    END IF;

    -- Insert dates into dim_dates
    INSERT INTO dim_dates (
        date_sk,
        full_date,
        year_number,
        year_name,
        quarter_number,
        quarter_name,
        year_quarter,
        month_number,
        month_name,
        month_abbr,
        year_month,
        week_of_year,
        week_of_month,
        day_of_month,
        day_of_year,
        day_of_week,
        day_name,
        day_abbr,
        is_weekend,
        is_weekday,
        fiscal_year,
        fiscal_quarter,
        fiscal_month
    )
    WITH date_series AS (
        SELECT
            DATEADD(DAY, SEQ4(), :start_date) AS full_date
        FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(DAY, :start_date, :end_date) + 1))
    )
    SELECT
        TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_sk,
        full_date,

        -- Year attributes
        YEAR(full_date) AS year_number,
        TO_CHAR(full_date, 'YYYY') AS year_name,

        -- Quarter attributes
        QUARTER(full_date) AS quarter_number,
        'Q' || QUARTER(full_date) AS quarter_name,
        TO_CHAR(full_date, 'YYYY') || '-Q' || QUARTER(full_date) AS year_quarter,

        -- Month attributes
        MONTH(full_date) AS month_number,
        TO_CHAR(full_date, 'MMMM') AS month_name,
        TO_CHAR(full_date, 'MON') AS month_abbr,
        TO_CHAR(full_date, 'YYYY-MM') AS year_month,

        -- Week attributes
        WEEKOFYEAR(full_date) AS week_of_year,
        CEIL(DAY(full_date) / 7.0) AS week_of_month,

        -- Day attributes
        DAY(full_date) AS day_of_month,
        DAYOFYEAR(full_date) AS day_of_year,
        DAYOFWEEK(full_date) AS day_of_week,
        DAYNAME(full_date) AS day_name,
        TO_CHAR(full_date, 'DY') AS day_abbr,

        -- Weekend/Weekday flags
        CASE WHEN DAYOFWEEK(full_date) IN (5, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(full_date) IN (5, 6) THEN FALSE ELSE TRUE END AS is_weekday,

        -- VA Fiscal Year (starts October 1)
        CASE
            WHEN MONTH(full_date) >= 10 THEN YEAR(full_date) + 1
            ELSE YEAR(full_date)
        END AS fiscal_year,

        -- Fiscal Quarter
        CASE
            WHEN MONTH(full_date) IN (10, 11, 12) THEN 1
            WHEN MONTH(full_date) IN (1, 2, 3) THEN 2
            WHEN MONTH(full_date) IN (4, 5, 6) THEN 3
            ELSE 4
        END AS fiscal_quarter,

        -- Fiscal Month (1-12, starting from October)
        CASE
            WHEN MONTH(full_date) >= 10 THEN MONTH(full_date) - 9
            ELSE MONTH(full_date) + 3
        END AS fiscal_month

    FROM date_series;

    rows_inserted := SQLROWCOUNT;

    RETURN 'Successfully inserted ' || rows_inserted || ' rows into dim_dates';
END;
$$;

-- =====================================================
-- Populate Federal Holidays
-- =====================================================

CREATE OR REPLACE PROCEDURE populate_federal_holidays()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Populate federal holidays in dim_dates table'
AS
$$
DECLARE
    rows_updated INTEGER DEFAULT 0;
BEGIN
    -- Fixed date holidays
    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'New Year''s Day'
    WHERE month_number = 1 AND day_of_month = 1;

    rows_updated := rows_updated + SQLROWCOUNT;

    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'Independence Day'
    WHERE month_number = 7 AND day_of_month = 4;

    rows_updated := rows_updated + SQLROWCOUNT;

    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'Veterans Day'
    WHERE month_number = 11 AND day_of_month = 11;

    rows_updated := rows_updated + SQLROWCOUNT;

    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'Christmas Day'
    WHERE month_number = 12 AND day_of_month = 25;

    rows_updated := rows_updated + SQLROWCOUNT;

    -- Memorial Day (Last Monday in May)
    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'Memorial Day'
    WHERE month_number = 5
      AND day_name = 'Monday'
      AND day_of_month >= 25;

    rows_updated := rows_updated + SQLROWCOUNT;

    -- Labor Day (First Monday in September)
    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'Labor Day'
    WHERE month_number = 9
      AND day_name = 'Monday'
      AND day_of_month <= 7;

    rows_updated := rows_updated + SQLROWCOUNT;

    -- Thanksgiving (Fourth Thursday in November)
    UPDATE dim_dates
    SET is_holiday = TRUE,
        holiday_name = 'Thanksgiving'
    WHERE month_number = 11
      AND day_name = 'Thursday'
      AND day_of_month BETWEEN 22 AND 28;

    rows_updated := rows_updated + SQLROWCOUNT;

    RETURN 'Successfully marked ' || rows_updated || ' holiday dates';
END;
$$;

-- =====================================================
-- Execute the improved procedures
-- =====================================================

-- Populate date dimension using configuration
CALL populate_dim_dates_from_config();

-- Populate holidays
CALL populate_federal_holidays();

-- =====================================================
-- Verification
-- =====================================================

SELECT 'Date dimension populated successfully' AS status;

-- Show summary
SELECT
    MIN(full_date) AS start_date,
    MAX(full_date) AS end_date,
    COUNT(*) AS total_dates,
    SUM(CASE WHEN is_holiday THEN 1 ELSE 0 END) AS holiday_count,
    SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_count
FROM dim_dates;

-- Show holidays
SELECT
    full_date,
    day_name,
    holiday_name,
    fiscal_year
FROM dim_dates
WHERE is_holiday = TRUE
ORDER BY full_date
LIMIT 20;
