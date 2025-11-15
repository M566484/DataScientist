-- =====================================================
-- Populate DIM_DATE - Date Dimension Population
-- =====================================================
-- Purpose: Populate date dimension with 10 years of data
-- Range: 2020-01-01 to 2029-12-31

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

-- Create a procedure to populate the date dimension
CREATE OR REPLACE PROCEDURE POPULATE_DIM_DATE(
    START_DATE DATE,
    END_DATE DATE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INTEGER;
BEGIN
    -- Insert dates into DIM_DATE
    INSERT INTO DIM_DATE (
        DATE_KEY,
        FULL_DATE,
        YEAR_NUMBER,
        YEAR_NAME,
        QUARTER_NUMBER,
        QUARTER_NAME,
        YEAR_QUARTER,
        MONTH_NUMBER,
        MONTH_NAME,
        MONTH_ABBR,
        YEAR_MONTH,
        WEEK_OF_YEAR,
        WEEK_OF_MONTH,
        DAY_OF_MONTH,
        DAY_OF_YEAR,
        DAY_OF_WEEK,
        DAY_NAME,
        DAY_ABBR,
        IS_WEEKEND,
        IS_WEEKDAY,
        FISCAL_YEAR,
        FISCAL_QUARTER,
        FISCAL_MONTH
    )
    WITH DATE_SERIES AS (
        SELECT
            DATEADD(DAY, SEQ4(), :START_DATE) AS FULL_DATE
        FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(DAY, :START_DATE, :END_DATE) + 1))
    )
    SELECT
        TO_NUMBER(TO_CHAR(FULL_DATE, 'YYYYMMDD')) AS DATE_KEY,
        FULL_DATE,

        -- Year attributes
        YEAR(FULL_DATE) AS YEAR_NUMBER,
        TO_CHAR(FULL_DATE, 'YYYY') AS YEAR_NAME,

        -- Quarter attributes
        QUARTER(FULL_DATE) AS QUARTER_NUMBER,
        'Q' || QUARTER(FULL_DATE) AS QUARTER_NAME,
        TO_CHAR(FULL_DATE, 'YYYY') || '-Q' || QUARTER(FULL_DATE) AS YEAR_QUARTER,

        -- Month attributes
        MONTH(FULL_DATE) AS MONTH_NUMBER,
        TO_CHAR(FULL_DATE, 'MMMM') AS MONTH_NAME,
        TO_CHAR(FULL_DATE, 'MON') AS MONTH_ABBR,
        TO_CHAR(FULL_DATE, 'YYYY-MM') AS YEAR_MONTH,

        -- Week attributes
        WEEKOFYEAR(FULL_DATE) AS WEEK_OF_YEAR,
        CEIL(DAY(FULL_DATE) / 7.0) AS WEEK_OF_MONTH,

        -- Day attributes
        DAY(FULL_DATE) AS DAY_OF_MONTH,
        DAYOFYEAR(FULL_DATE) AS DAY_OF_YEAR,
        DAYOFWEEK(FULL_DATE) AS DAY_OF_WEEK,
        TO_CHAR(FULL_DATE, 'DDDD') AS DAY_NAME,
        TO_CHAR(FULL_DATE, 'DY') AS DAY_ABBR,

        -- Weekend/Weekday flags
        CASE WHEN DAYOFWEEK(FULL_DATE) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
        CASE WHEN DAYOFWEEK(FULL_DATE) IN (0, 6) THEN FALSE ELSE TRUE END AS IS_WEEKDAY,

        -- VA Fiscal Year (starts October 1)
        CASE
            WHEN MONTH(FULL_DATE) >= 10 THEN YEAR(FULL_DATE) + 1
            ELSE YEAR(FULL_DATE)
        END AS FISCAL_YEAR,

        -- Fiscal Quarter
        CASE
            WHEN MONTH(FULL_DATE) IN (10, 11, 12) THEN 1
            WHEN MONTH(FULL_DATE) IN (1, 2, 3) THEN 2
            WHEN MONTH(FULL_DATE) IN (4, 5, 6) THEN 3
            ELSE 4
        END AS FISCAL_QUARTER,

        -- Fiscal Month (1-12, starting from October)
        CASE
            WHEN MONTH(FULL_DATE) >= 10 THEN MONTH(FULL_DATE) - 9
            ELSE MONTH(FULL_DATE) + 3
        END AS FISCAL_MONTH

    FROM DATE_SERIES;

    rows_inserted := SQLROWCOUNT;

    RETURN 'Successfully inserted ' || rows_inserted || ' rows into DIM_DATE';
END;
$$;

-- Execute the procedure to populate 10 years of data
CALL POPULATE_DIM_DATE('2020-01-01', '2029-12-31');

-- Add federal holidays (US federal holidays)
-- Note: This is a simplified version. In production, you'd want a more comprehensive holiday calendar

UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'New Year''s Day'
WHERE MONTH_NUMBER = 1 AND DAY_OF_MONTH = 1;

UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'Independence Day'
WHERE MONTH_NUMBER = 7 AND DAY_OF_MONTH = 4;

UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'Veterans Day'
WHERE MONTH_NUMBER = 11 AND DAY_OF_MONTH = 11;

UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'Christmas Day'
WHERE MONTH_NUMBER = 12 AND DAY_OF_MONTH = 25;

-- Memorial Day (Last Monday in May)
UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'Memorial Day'
WHERE MONTH_NUMBER = 5
  AND DAY_NAME = 'Monday'
  AND DAY_OF_MONTH >= 25;

-- Labor Day (First Monday in September)
UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'Labor Day'
WHERE MONTH_NUMBER = 9
  AND DAY_NAME = 'Monday'
  AND DAY_OF_MONTH <= 7;

-- Thanksgiving (Fourth Thursday in November)
UPDATE DIM_DATE
SET IS_HOLIDAY = TRUE,
    HOLIDAY_NAME = 'Thanksgiving'
WHERE MONTH_NUMBER = 11
  AND DAY_NAME = 'Thursday'
  AND DAY_OF_MONTH BETWEEN 22 AND 28;

SELECT 'Date dimension populated successfully' AS STATUS;
