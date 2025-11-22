-- =====================================================
-- dim_dates - Date Dimension
-- =====================================================
-- Purpose: Standard date dimension for time-based analysis
-- Type: Conformed Dimension
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_dates (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,

    -- Year attributes
    year_number INTEGER NOT NULL,
    year_name VARCHAR(4) NOT NULL,

    -- Quarter attributes
    quarter_number INTEGER NOT NULL,
    quarter_name VARCHAR(6) NOT NULL,
    year_quarter VARCHAR(7) NOT NULL,

    -- Month attributes
    month_number INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    month_abbr VARCHAR(3) NOT NULL,
    year_month VARCHAR(7) NOT NULL,

    -- Week attributes
    week_of_year INTEGER NOT NULL,
    week_of_month INTEGER NOT NULL,

    -- Day attributes
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_abbr VARCHAR(3) NOT NULL,

    -- Business day indicators
    is_weekend BOOLEAN NOT NULL,
    is_weekday BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),

    -- Fiscal period (VA fiscal year starts Oct 1)
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL,
    fiscal_month INTEGER NOT NULL,

    -- Metadata
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Date dimension table for time-based analysis. VA fiscal year starts October 1.';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_dates.date_sk IS 'Primary key in YYYYMMDD format (e.g., 20240115)';
COMMENT ON COLUMN dim_dates.full_date IS 'Actual calendar date';
COMMENT ON COLUMN dim_dates.year_number IS 'Four-digit year (e.g., 2024)';
COMMENT ON COLUMN dim_dates.year_name IS 'Year as string (e.g., "2024")';
COMMENT ON COLUMN dim_dates.quarter_number IS 'Calendar quarter number (1-4)';
COMMENT ON COLUMN dim_dates.quarter_name IS 'Quarter name (e.g., "Q1", "Q2")';
COMMENT ON COLUMN dim_dates.year_quarter IS 'Year and quarter combined (e.g., "2024-Q1")';
COMMENT ON COLUMN dim_dates.month_number IS 'Month number (1-12)';
COMMENT ON COLUMN dim_dates.month_name IS 'Full month name (e.g., "January")';
COMMENT ON COLUMN dim_dates.month_abbr IS 'Three-letter month abbreviation (e.g., "JAN")';
COMMENT ON COLUMN dim_dates.year_month IS 'Year and month combined (e.g., "2024-01")';
COMMENT ON COLUMN dim_dates.week_of_year IS 'Week number within the year (1-53)';
COMMENT ON COLUMN dim_dates.week_of_month IS 'Week number within the month (1-5)';
COMMENT ON COLUMN dim_dates.day_of_month IS 'Day number within the month (1-31)';
COMMENT ON COLUMN dim_dates.day_of_year IS 'Day number within the year (1-366)';
COMMENT ON COLUMN dim_dates.day_of_week IS 'Day of week number (0=Sunday, 6=Saturday)';
COMMENT ON COLUMN dim_dates.day_name IS 'Full day name (e.g., "Monday")';
COMMENT ON COLUMN dim_dates.day_abbr IS 'Three-letter day abbreviation (e.g., "MON")';
COMMENT ON COLUMN dim_dates.is_weekend IS 'TRUE if Saturday or Sunday';
COMMENT ON COLUMN dim_dates.is_weekday IS 'TRUE if Monday through Friday';
COMMENT ON COLUMN dim_dates.is_holiday IS 'TRUE if federal holiday';
COMMENT ON COLUMN dim_dates.holiday_name IS 'Name of holiday if applicable (e.g., "Veterans Day")';
COMMENT ON COLUMN dim_dates.fiscal_year IS 'VA fiscal year (starts Oct 1, e.g., Oct 2023 = FY 2024)';
COMMENT ON COLUMN dim_dates.fiscal_quarter IS 'VA fiscal quarter (1-4, Q1=Oct-Dec)';
COMMENT ON COLUMN dim_dates.fiscal_month IS 'VA fiscal month (1-12, 1=October)';
COMMENT ON COLUMN dim_dates.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_dates.updated_timestamp IS 'Timestamp when record was last updated';
