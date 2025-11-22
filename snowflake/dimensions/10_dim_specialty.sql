-- =====================================================
-- dim_specialties - Medical Specialty Dimension
-- =====================================================
-- Purpose: Medical specialties for evaluators and exam requests
-- SCD Type: Type 1
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_specialties (
    specialty_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    specialty_id VARCHAR(50) NOT NULL UNIQUE,  -- Business key (standard_code from reference)

    -- Specialty Information
    specialty_name VARCHAR(255) NOT NULL,
    specialty_description TEXT,
    specialty_category VARCHAR(100),  -- MENTAL_HEALTH, MUSCULOSKELETAL, NEUROLOGICAL, CARDIOVASCULAR, GENERAL

    -- Specialty Characteristics
    board_certification_required BOOLEAN DEFAULT TRUE,
    subspecialty_flag BOOLEAN DEFAULT FALSE,
    parent_specialty_id VARCHAR(50),  -- For subspecialties

    -- Demand & Capacity
    typical_exam_duration_minutes INTEGER,
    demand_level VARCHAR(20),  -- HIGH, MEDIUM, LOW
    shortage_indicator BOOLEAN DEFAULT FALSE,

    -- Regulatory & Accreditation
    board_name VARCHAR(255),  -- American Board of Psychiatry and Neurology, etc.
    license_requirements TEXT,
    certification_codes ARRAY,  -- Array of board certification codes

    -- Status
    active_flag BOOLEAN DEFAULT TRUE,
    deprecated_date DATE,
    replacement_specialty_id VARCHAR(50),

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for medical specialties';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_specialties.specialty_sk IS 'Surrogate primary key for the specialty dimension';
COMMENT ON COLUMN dim_specialties.specialty_id IS 'Business key - Unique specialty identifier (standard code)';
COMMENT ON COLUMN dim_specialties.specialty_name IS 'Name of the medical specialty';
COMMENT ON COLUMN dim_specialties.specialty_description IS 'Detailed description of the specialty';
COMMENT ON COLUMN dim_specialties.specialty_category IS 'Specialty category (MENTAL_HEALTH, MUSCULOSKELETAL, NEUROLOGICAL, CARDIOVASCULAR, GENERAL)';
COMMENT ON COLUMN dim_specialties.board_certification_required IS 'TRUE if board certification is required';
COMMENT ON COLUMN dim_specialties.subspecialty_flag IS 'TRUE if this is a subspecialty';
COMMENT ON COLUMN dim_specialties.parent_specialty_id IS 'ID of parent specialty for subspecialties';
COMMENT ON COLUMN dim_specialties.typical_exam_duration_minutes IS 'Typical duration for exams in this specialty';
COMMENT ON COLUMN dim_specialties.demand_level IS 'Demand level (HIGH, MEDIUM, LOW)';
COMMENT ON COLUMN dim_specialties.shortage_indicator IS 'TRUE if there is a shortage of providers in this specialty';
COMMENT ON COLUMN dim_specialties.board_name IS 'Name of the certifying board';
COMMENT ON COLUMN dim_specialties.license_requirements IS 'License requirements for this specialty';
COMMENT ON COLUMN dim_specialties.certification_codes IS 'Array of board certification codes';
COMMENT ON COLUMN dim_specialties.active_flag IS 'TRUE if this specialty is currently active';
COMMENT ON COLUMN dim_specialties.deprecated_date IS 'Date this specialty was deprecated';
COMMENT ON COLUMN dim_specialties.replacement_specialty_id IS 'ID of replacement specialty if deprecated';
COMMENT ON COLUMN dim_specialties.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_specialties.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_specialties.updated_timestamp IS 'Timestamp when record was last updated';

-- =====================================================
-- Populate Initial Specialty Data
-- =====================================================

-- Insert specialties based on reference data
INSERT INTO dim_specialties (
    specialty_id,
    specialty_name,
    specialty_category,
    board_certification_required,
    subspecialty_flag,
    typical_exam_duration_minutes,
    demand_level,
    shortage_indicator,
    board_name,
    active_flag,
    source_system
)
SELECT DISTINCT
    standard_code AS specialty_id,
    standard_value AS specialty_name,
    category AS specialty_category,
    TRUE AS board_certification_required,
    FALSE AS subspecialty_flag,
    CASE
        WHEN category = 'MENTAL_HEALTH' THEN 60
        WHEN category = 'MUSCULOSKELETAL' THEN 45
        WHEN category = 'NEUROLOGICAL' THEN 60
        WHEN category = 'CARDIOVASCULAR' THEN 45
        WHEN category = 'GENERAL' THEN 30
        ELSE 45
    END AS typical_exam_duration_minutes,
    CASE
        WHEN standard_code = 'PSYCHIATRY' THEN 'HIGH'
        WHEN standard_code = 'NEUROLOGY' THEN 'HIGH'
        WHEN standard_code = 'ORTHOPEDICS' THEN 'MEDIUM'
        WHEN standard_code = 'CARDIOLOGY' THEN 'MEDIUM'
        ELSE 'LOW'
    END AS demand_level,
    CASE
        WHEN standard_code IN ('PSYCHIATRY', 'NEUROLOGY') THEN TRUE
        ELSE FALSE
    END AS shortage_indicator,
    CASE
        WHEN standard_code = 'PSYCHIATRY' THEN 'American Board of Psychiatry and Neurology'
        WHEN standard_code = 'NEUROLOGY' THEN 'American Board of Psychiatry and Neurology'
        WHEN standard_code = 'ORTHOPEDICS' THEN 'American Board of Orthopaedic Surgery'
        WHEN standard_code = 'CARDIOLOGY' THEN 'American Board of Internal Medicine'
        WHEN standard_code = 'GENERAL_MEDICINE' THEN 'American Board of Internal Medicine'
        ELSE 'Various Medical Boards'
    END AS board_name,
    TRUE AS active_flag,
    'REFERENCE' AS source_system
FROM IDENTIFIER(fn_get_dw_database() || '.REFERENCE.ref_code_mapping_specialty')
WHERE active_flag = TRUE;

-- =====================================================
-- Add Additional Common VA Specialties
-- =====================================================

INSERT INTO dim_specialties (
    specialty_id,
    specialty_name,
    specialty_category,
    board_certification_required,
    subspecialty_flag,
    typical_exam_duration_minutes,
    demand_level,
    shortage_indicator,
    board_name,
    active_flag,
    source_system
) VALUES
-- Additional Mental Health specialties
('PSYCHOLOGY', 'PSYCHOLOGY', 'MENTAL_HEALTH', TRUE, FALSE, 60, 'HIGH', TRUE,
 'American Board of Professional Psychology', TRUE, 'MANUAL'),
('PTSD_SPECIALIST', 'PTSD SPECIALIST', 'MENTAL_HEALTH', TRUE, TRUE, 90, 'HIGH', TRUE,
 'American Board of Psychiatry and Neurology', TRUE, 'MANUAL'),

-- Additional specialties
('AUDIOLOGY', 'AUDIOLOGY', 'SENSORY', TRUE, FALSE, 45, 'MEDIUM', FALSE,
 'American Board of Audiology', TRUE, 'MANUAL'),
('OPHTHALMOLOGY', 'OPHTHALMOLOGY', 'SENSORY', TRUE, FALSE, 45, 'MEDIUM', FALSE,
 'American Board of Ophthalmology', TRUE, 'MANUAL'),
('DERMATOLOGY', 'DERMATOLOGY', 'DERMATOLOGICAL', TRUE, FALSE, 30, 'LOW', FALSE,
 'American Board of Dermatology', TRUE, 'MANUAL'),
('PULMONOLOGY', 'PULMONOLOGY', 'RESPIRATORY', TRUE, FALSE, 45, 'MEDIUM', FALSE,
 'American Board of Internal Medicine', TRUE, 'MANUAL'),
('GASTROENTEROLOGY', 'GASTROENTEROLOGY', 'GASTROINTESTINAL', TRUE, FALSE, 45, 'MEDIUM', FALSE,
 'American Board of Internal Medicine', TRUE, 'MANUAL'),
('ENDOCRINOLOGY', 'ENDOCRINOLOGY', 'ENDOCRINE', TRUE, FALSE, 45, 'MEDIUM', FALSE,
 'American Board of Internal Medicine', TRUE, 'MANUAL'),
('RHEUMATOLOGY', 'RHEUMATOLOGY', 'MUSCULOSKELETAL', TRUE, TRUE, 45, 'MEDIUM', FALSE,
 'American Board of Internal Medicine', TRUE, 'MANUAL'),
('PAIN_MANAGEMENT', 'PAIN MANAGEMENT', 'PAIN', TRUE, FALSE, 45, 'HIGH', FALSE,
 'American Board of Anesthesiology', TRUE, 'MANUAL');

-- =====================================================
-- Verification Queries
-- =====================================================

-- Show all specialties
SELECT
    specialty_id,
    specialty_name,
    specialty_category,
    demand_level,
    shortage_indicator,
    active_flag
FROM dim_specialties
WHERE active_flag = TRUE
ORDER BY specialty_category, specialty_name;

-- Count by category
SELECT
    specialty_category,
    COUNT(*) AS specialty_count,
    COUNT(CASE WHEN shortage_indicator = TRUE THEN 1 END) AS shortage_count
FROM dim_specialties
WHERE active_flag = TRUE
GROUP BY specialty_category
ORDER BY specialty_count DESC;

SELECT 'Specialty dimension populated successfully' AS status,
       COUNT(*) AS total_specialties
FROM dim_specialties
WHERE active_flag = TRUE;
