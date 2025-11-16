-- =====================================================
-- DIM_DATE - Date Dimension
-- =====================================================
-- Purpose: Standard date dimension for time-based analysis
-- Type: Conformed Dimension

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY INTEGER PRIMARY KEY,
    FULL_DATE DATE NOT NULL UNIQUE,

    -- Year attributes
    YEAR_NUMBER INTEGER NOT NULL,
    YEAR_NAME VARCHAR(4) NOT NULL,

    -- Quarter attributes
    QUARTER_NUMBER INTEGER NOT NULL,
    QUARTER_NAME VARCHAR(6) NOT NULL,
    YEAR_QUARTER VARCHAR(7) NOT NULL,

    -- Month attributes
    MONTH_NUMBER INTEGER NOT NULL,
    MONTH_NAME VARCHAR(20) NOT NULL,
    MONTH_ABBR VARCHAR(3) NOT NULL,
    YEAR_MONTH VARCHAR(7) NOT NULL,

    -- Week attributes
    WEEK_OF_YEAR INTEGER NOT NULL,
    WEEK_OF_MONTH INTEGER NOT NULL,

    -- Day attributes
    DAY_OF_MONTH INTEGER NOT NULL,
    DAY_OF_YEAR INTEGER NOT NULL,
    DAY_OF_WEEK INTEGER NOT NULL,
    DAY_NAME VARCHAR(20) NOT NULL,
    DAY_ABBR VARCHAR(3) NOT NULL,

    -- Business day indicators
    IS_WEEKEND BOOLEAN NOT NULL,
    IS_WEEKDAY BOOLEAN NOT NULL,
    IS_HOLIDAY BOOLEAN DEFAULT FALSE,
    HOLIDAY_NAME VARCHAR(100),

    -- Fiscal period (VA fiscal year starts Oct 1)
    FISCAL_YEAR INTEGER NOT NULL,
    FISCAL_QUARTER INTEGER NOT NULL,
    FISCAL_MONTH INTEGER NOT NULL,

    -- Metadata
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Date dimension table for time-based analysis. VA fiscal year starts October 1.';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_DATE.DATE_KEY IS 'Primary key in YYYYMMDD format (e.g., 20240115)';
COMMENT ON COLUMN DIM_DATE.FULL_DATE IS 'Actual calendar date';
COMMENT ON COLUMN DIM_DATE.YEAR_NUMBER IS 'Four-digit year (e.g., 2024)';
COMMENT ON COLUMN DIM_DATE.YEAR_NAME IS 'Year as string (e.g., "2024")';
COMMENT ON COLUMN DIM_DATE.QUARTER_NUMBER IS 'Calendar quarter number (1-4)';
COMMENT ON COLUMN DIM_DATE.QUARTER_NAME IS 'Quarter name (e.g., "Q1", "Q2")';
COMMENT ON COLUMN DIM_DATE.YEAR_QUARTER IS 'Year and quarter combined (e.g., "2024-Q1")';
COMMENT ON COLUMN DIM_DATE.MONTH_NUMBER IS 'Month number (1-12)';
COMMENT ON COLUMN DIM_DATE.MONTH_NAME IS 'Full month name (e.g., "January")';
COMMENT ON COLUMN DIM_DATE.MONTH_ABBR IS 'Three-letter month abbreviation (e.g., "JAN")';
COMMENT ON COLUMN DIM_DATE.YEAR_MONTH IS 'Year and month combined (e.g., "2024-01")';
COMMENT ON COLUMN DIM_DATE.WEEK_OF_YEAR IS 'Week number within the year (1-53)';
COMMENT ON COLUMN DIM_DATE.WEEK_OF_MONTH IS 'Week number within the month (1-5)';
COMMENT ON COLUMN DIM_DATE.DAY_OF_MONTH IS 'Day number within the month (1-31)';
COMMENT ON COLUMN DIM_DATE.DAY_OF_YEAR IS 'Day number within the year (1-366)';
COMMENT ON COLUMN DIM_DATE.DAY_OF_WEEK IS 'Day of week number (0=Sunday, 6=Saturday)';
COMMENT ON COLUMN DIM_DATE.DAY_NAME IS 'Full day name (e.g., "Monday")';
COMMENT ON COLUMN DIM_DATE.DAY_ABBR IS 'Three-letter day abbreviation (e.g., "MON")';
COMMENT ON COLUMN DIM_DATE.IS_WEEKEND IS 'TRUE if Saturday or Sunday';
COMMENT ON COLUMN DIM_DATE.IS_WEEKDAY IS 'TRUE if Monday through Friday';
COMMENT ON COLUMN DIM_DATE.IS_HOLIDAY IS 'TRUE if federal holiday';
COMMENT ON COLUMN DIM_DATE.HOLIDAY_NAME IS 'Name of holiday if applicable (e.g., "Veterans Day")';
COMMENT ON COLUMN DIM_DATE.FISCAL_YEAR IS 'VA fiscal year (starts Oct 1, e.g., Oct 2023 = FY 2024)';
COMMENT ON COLUMN DIM_DATE.FISCAL_QUARTER IS 'VA fiscal quarter (1-4, Q1=Oct-Dec)';
COMMENT ON COLUMN DIM_DATE.FISCAL_MONTH IS 'VA fiscal month (1-12, 1=October)';
COMMENT ON COLUMN DIM_DATE.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_DATE.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
