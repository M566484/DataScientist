-- =====================================================
-- dim_facilities - VA Facility Dimension
-- =====================================================
-- Purpose: VA medical centers and evaluation facilities
-- SCD Type: Type 2
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE dim_facilities (
    facility_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    facility_id VARCHAR(50) NOT NULL,  -- Business key (Station Number)

    -- Facility Information
    facility_name VARCHAR(255) NOT NULL,
    facility_type VARCHAR(50),  -- VAMC, CBOC, Vet Center, Contract Facility
    station_number VARCHAR(10),  -- VA Station Number

    -- Location Information
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR(100),
    region VARCHAR(50),  -- VISN (Veterans Integrated Service Network)
    visn_number INTEGER,

    -- Contact Information
    phone VARCHAR(20),
    fax VARCHAR(20),
    email VARCHAR(255),
    website VARCHAR(255),

    -- Facility Characteristics
    bed_count INTEGER,
    specialty_services VARIANT,  -- JSON array of services offered
    trauma_level VARCHAR(20),
    teaching_facility_flag BOOLEAN DEFAULT FALSE,

    -- Accreditation
    accredited_flag BOOLEAN DEFAULT FALSE,
    accreditation_body VARCHAR(100),
    accreditation_date DATE,
    accreditation_expiration DATE,

    -- Status
    active_flag BOOLEAN DEFAULT TRUE,
    opened_date DATE,
    closed_date DATE,

    -- SCD Type 2 attributes
    effective_start_date TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for VA facilities and medical centers';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_facilities.facility_sk IS 'Surrogate primary key for the facility dimension';
COMMENT ON COLUMN dim_facilities.facility_id IS 'Business key - Unique facility identifier (Station Number)';
COMMENT ON COLUMN dim_facilities.facility_name IS 'Official facility name';
COMMENT ON COLUMN dim_facilities.facility_type IS 'Facility type (VAMC, CBOC, Vet Center, Contract Facility)';
COMMENT ON COLUMN dim_facilities.station_number IS 'VA Station Number (official facility identifier)';
COMMENT ON COLUMN dim_facilities.address_line1 IS 'Street address line 1';
COMMENT ON COLUMN dim_facilities.address_line2 IS 'Street address line 2';
COMMENT ON COLUMN dim_facilities.city IS 'City';
COMMENT ON COLUMN dim_facilities.state IS 'Two-letter state code';
COMMENT ON COLUMN dim_facilities.zip_code IS 'ZIP code';
COMMENT ON COLUMN dim_facilities.county IS 'County name';
COMMENT ON COLUMN dim_facilities.region IS 'VISN region name';
COMMENT ON COLUMN dim_facilities.visn_number IS 'Veterans Integrated Service Network number (1-23)';
COMMENT ON COLUMN dim_facilities.phone IS 'Main phone number';
COMMENT ON COLUMN dim_facilities.fax IS 'Fax number';
COMMENT ON COLUMN dim_facilities.email IS 'General email address';
COMMENT ON COLUMN dim_facilities.website IS 'Facility website URL';
COMMENT ON COLUMN dim_facilities.bed_count IS 'Number of inpatient beds';
COMMENT ON COLUMN dim_facilities.specialty_services IS 'JSON array of specialty services offered';
COMMENT ON COLUMN dim_facilities.trauma_level IS 'Trauma center level designation';
COMMENT ON COLUMN dim_facilities.teaching_facility_flag IS 'TRUE if academic teaching facility';
COMMENT ON COLUMN dim_facilities.accredited_flag IS 'TRUE if currently accredited';
COMMENT ON COLUMN dim_facilities.accreditation_body IS 'Accrediting organization name (e.g., Joint Commission)';
COMMENT ON COLUMN dim_facilities.accreditation_date IS 'Date of current accreditation';
COMMENT ON COLUMN dim_facilities.accreditation_expiration IS 'Accreditation expiration date';
COMMENT ON COLUMN dim_facilities.active_flag IS 'TRUE if facility is currently operational';
COMMENT ON COLUMN dim_facilities.opened_date IS 'Date facility opened';
COMMENT ON COLUMN dim_facilities.closed_date IS 'Date facility closed (if applicable)';
COMMENT ON COLUMN dim_facilities.effective_start_date IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN dim_facilities.effective_end_date IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN dim_facilities.is_current IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN dim_facilities.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_facilities.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_facilities.updated_timestamp IS 'Timestamp when record was last updated';
