-- =====================================================
-- Populate dim_dates - Date Dimension Population
-- =====================================================
-- Purpose: Populate date dimension with 10 years of data
-- Range: 2020-01-01 to 2029-12-31
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

-- Create a procedure to populate the date dimension
CREATE OR REPLACE PROCEDURE populate_dim_dates(
    start_date DATE,
    end_date DATE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INTEGER;
BEGIN
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
        TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
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

-- Execute the procedure to populate 10 years of data
CALL populate_dim_dates('2020-01-01', '2029-12-31');

-- Add federal holidays (US federal holidays)
-- Note: This is a simplified version. In production, you'd want a more comprehensive holiday calendar

UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'New Year''s Day'
WHERE month_number = 1 AND day_of_month = 1;

UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'Independence Day'
WHERE month_number = 7 AND day_of_month = 4;

UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'Veterans Day'
WHERE month_number = 11 AND day_of_month = 11;

UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'Christmas Day'
WHERE month_number = 12 AND day_of_month = 25;

-- Memorial Day (Last Monday in May)
UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'Memorial Day'
WHERE month_number = 5
  AND day_name = 'Monday'
  AND day_of_month >= 25;

-- Labor Day (First Monday in September)
UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'Labor Day'
WHERE month_number = 9
  AND day_name = 'Monday'
  AND day_of_month <= 7;

-- Thanksgiving (Fourth Thursday in November)
UPDATE dim_dates
SET is_holiday = TRUE,
    holiday_name = 'Thanksgiving'
WHERE month_number = 11
  AND day_name = 'Thursday'
  AND day_of_month BETWEEN 22 AND 28;

SELECT 'Date dimension populated successfully' AS status;
