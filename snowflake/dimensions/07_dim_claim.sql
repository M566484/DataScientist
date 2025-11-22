-- =====================================================
-- dim_claims - Claim Dimension
-- =====================================================
-- Purpose: VA disability claims information
-- SCD Type: Type 2
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_claims (
    claim_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    claim_id VARCHAR(50) NOT NULL,  -- Business key (Claim Number)

    -- Claim Information
    claim_number VARCHAR(50) NOT NULL,
    claim_type VARCHAR(50),  -- Original, Supplemental, Secondary, Increase, etc.
    claim_status VARCHAR(50),  -- Pending, In Progress, Decided, Remand, etc.
    claim_filed_date DATE,
    claim_received_date DATE,

    -- Processing Information
    regional_office VARCHAR(100),
    assigned_rating_specialist VARCHAR(100),
    priority_flag BOOLEAN DEFAULT FALSE,
    priority_reason VARCHAR(255),  -- FDC, Homeless, Terminal Illness, etc.

    -- Decision Information
    decision_date DATE,
    decision_type VARCHAR(50),  -- Granted, Denied, Deferred, etc.
    appeal_filed_flag BOOLEAN DEFAULT FALSE,
    appeal_date DATE,

    -- Processing Metrics
    days_pending INTEGER,
    target_completion_date DATE,

    -- Contentions
    number_of_contentions INTEGER DEFAULT 0,
    primary_contention VARCHAR(255),

    -- Evidence
    evidence_requested_flag BOOLEAN DEFAULT FALSE,
    evidence_received_flag BOOLEAN DEFAULT FALSE,
    exam_requested_flag BOOLEAN DEFAULT FALSE,
    exam_completed_flag BOOLEAN DEFAULT FALSE,

    -- SCD Type 2 attributes
    effective_start_date TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    effective_end_date TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for VA disability claims';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_claims.claim_sk IS 'Surrogate primary key for the claim dimension';
COMMENT ON COLUMN dim_claims.claim_id IS 'Business key - Unique claim identifier';
COMMENT ON COLUMN dim_claims.claim_number IS 'Claim number displayed to users';
COMMENT ON COLUMN dim_claims.claim_type IS 'Claim type (Original, Supplemental, Secondary, Increase, etc.)';
COMMENT ON COLUMN dim_claims.claim_status IS 'Current claim status (Pending, In Progress, Decided, Remand, etc.)';
COMMENT ON COLUMN dim_claims.claim_filed_date IS 'Date claim was filed by veteran';
COMMENT ON COLUMN dim_claims.claim_received_date IS 'Date claim was received by VA';
COMMENT ON COLUMN dim_claims.regional_office IS 'Regional office processing the claim';
COMMENT ON COLUMN dim_claims.assigned_rating_specialist IS 'Name of assigned rating specialist';
COMMENT ON COLUMN dim_claims.priority_flag IS 'TRUE if priority processing applies';
COMMENT ON COLUMN dim_claims.priority_reason IS 'Reason for priority (FDC, Homeless, Terminal Illness, etc.)';
COMMENT ON COLUMN dim_claims.decision_date IS 'Date decision was made';
COMMENT ON COLUMN dim_claims.decision_type IS 'Decision type (Granted, Denied, Deferred, etc.)';
COMMENT ON COLUMN dim_claims.appeal_filed_flag IS 'TRUE if appeal has been filed';
COMMENT ON COLUMN dim_claims.appeal_date IS 'Date appeal was filed';
COMMENT ON COLUMN dim_claims.days_pending IS 'Number of days claim has been pending';
COMMENT ON COLUMN dim_claims.target_completion_date IS 'Target date for completion';
COMMENT ON COLUMN dim_claims.number_of_contentions IS 'Number of contentions in the claim';
COMMENT ON COLUMN dim_claims.primary_contention IS 'Primary contention being claimed';
COMMENT ON COLUMN dim_claims.evidence_requested_flag IS 'TRUE if additional evidence was requested';
COMMENT ON COLUMN dim_claims.evidence_received_flag IS 'TRUE if requested evidence has been received';
COMMENT ON COLUMN dim_claims.exam_requested_flag IS 'TRUE if medical exam was requested';
COMMENT ON COLUMN dim_claims.exam_completed_flag IS 'TRUE if medical exam has been completed';
COMMENT ON COLUMN dim_claims.effective_start_date IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN dim_claims.effective_end_date IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN dim_claims.is_current IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN dim_claims.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_claims.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_claims.updated_timestamp IS 'Timestamp when record was last updated';
