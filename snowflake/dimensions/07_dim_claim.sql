-- =====================================================
-- DIM_CLAIM - Claim Dimension
-- =====================================================
-- Purpose: VA disability claims information
-- SCD Type: Type 2

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_CLAIM (
    CLAIM_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
    CLAIM_ID VARCHAR(50) NOT NULL,  -- Business key (Claim Number)

    -- Claim Information
    CLAIM_NUMBER VARCHAR(50) NOT NULL,
    CLAIM_TYPE VARCHAR(50),  -- Original, Supplemental, Secondary, Increase, etc.
    CLAIM_STATUS VARCHAR(50),  -- Pending, In Progress, Decided, Remand, etc.
    CLAIM_FILED_DATE DATE,
    CLAIM_RECEIVED_DATE DATE,

    -- Processing Information
    REGIONAL_OFFICE VARCHAR(100),
    ASSIGNED_RATING_SPECIALIST VARCHAR(100),
    PRIORITY_FLAG BOOLEAN DEFAULT FALSE,
    PRIORITY_REASON VARCHAR(255),  -- FDC, Homeless, Terminal Illness, etc.

    -- Decision Information
    DECISION_DATE DATE,
    DECISION_TYPE VARCHAR(50),  -- Granted, Denied, Deferred, etc.
    APPEAL_FILED_FLAG BOOLEAN DEFAULT FALSE,
    APPEAL_DATE DATE,

    -- Processing Metrics
    DAYS_PENDING INTEGER,
    TARGET_COMPLETION_DATE DATE,

    -- Contentions
    NUMBER_OF_CONTENTIONS INTEGER DEFAULT 0,
    PRIMARY_CONTENTION VARCHAR(255),

    -- Evidence
    EVIDENCE_REQUESTED_FLAG BOOLEAN DEFAULT FALSE,
    EVIDENCE_RECEIVED_FLAG BOOLEAN DEFAULT FALSE,
    EXAM_REQUESTED_FLAG BOOLEAN DEFAULT FALSE,
    EXAM_COMPLETED_FLAG BOOLEAN DEFAULT FALSE,

    -- SCD Type 2 attributes
    EFFECTIVE_START_DATE TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END_DATE TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    IS_CURRENT BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    SOURCE_SYSTEM VARCHAR(50),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 2 SCD dimension for VA disability claims';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_CLAIM.CLAIM_KEY IS 'Surrogate primary key for the claim dimension';
COMMENT ON COLUMN DIM_CLAIM.CLAIM_ID IS 'Business key - Unique claim identifier';
COMMENT ON COLUMN DIM_CLAIM.CLAIM_NUMBER IS 'Claim number displayed to users';
COMMENT ON COLUMN DIM_CLAIM.CLAIM_TYPE IS 'Claim type (Original, Supplemental, Secondary, Increase, etc.)';
COMMENT ON COLUMN DIM_CLAIM.CLAIM_STATUS IS 'Current claim status (Pending, In Progress, Decided, Remand, etc.)';
COMMENT ON COLUMN DIM_CLAIM.CLAIM_FILED_DATE IS 'Date claim was filed by veteran';
COMMENT ON COLUMN DIM_CLAIM.CLAIM_RECEIVED_DATE IS 'Date claim was received by VA';
COMMENT ON COLUMN DIM_CLAIM.REGIONAL_OFFICE IS 'Regional office processing the claim';
COMMENT ON COLUMN DIM_CLAIM.ASSIGNED_RATING_SPECIALIST IS 'Name of assigned rating specialist';
COMMENT ON COLUMN DIM_CLAIM.PRIORITY_FLAG IS 'TRUE if priority processing applies';
COMMENT ON COLUMN DIM_CLAIM.PRIORITY_REASON IS 'Reason for priority (FDC, Homeless, Terminal Illness, etc.)';
COMMENT ON COLUMN DIM_CLAIM.DECISION_DATE IS 'Date decision was made';
COMMENT ON COLUMN DIM_CLAIM.DECISION_TYPE IS 'Decision type (Granted, Denied, Deferred, etc.)';
COMMENT ON COLUMN DIM_CLAIM.APPEAL_FILED_FLAG IS 'TRUE if appeal has been filed';
COMMENT ON COLUMN DIM_CLAIM.APPEAL_DATE IS 'Date appeal was filed';
COMMENT ON COLUMN DIM_CLAIM.DAYS_PENDING IS 'Number of days claim has been pending';
COMMENT ON COLUMN DIM_CLAIM.TARGET_COMPLETION_DATE IS 'Target date for completion';
COMMENT ON COLUMN DIM_CLAIM.NUMBER_OF_CONTENTIONS IS 'Number of contentions in the claim';
COMMENT ON COLUMN DIM_CLAIM.PRIMARY_CONTENTION IS 'Primary contention being claimed';
COMMENT ON COLUMN DIM_CLAIM.EVIDENCE_REQUESTED_FLAG IS 'TRUE if additional evidence was requested';
COMMENT ON COLUMN DIM_CLAIM.EVIDENCE_RECEIVED_FLAG IS 'TRUE if requested evidence has been received';
COMMENT ON COLUMN DIM_CLAIM.EXAM_REQUESTED_FLAG IS 'TRUE if medical exam was requested';
COMMENT ON COLUMN DIM_CLAIM.EXAM_COMPLETED_FLAG IS 'TRUE if medical exam has been completed';
COMMENT ON COLUMN DIM_CLAIM.EFFECTIVE_START_DATE IS 'Start date when this version of the record became effective';
COMMENT ON COLUMN DIM_CLAIM.EFFECTIVE_END_DATE IS 'End date when this version of the record became obsolete (9999-12-31 if current)';
COMMENT ON COLUMN DIM_CLAIM.IS_CURRENT IS 'TRUE if this is the current active version of the record';
COMMENT ON COLUMN DIM_CLAIM.SOURCE_SYSTEM IS 'Source system that provided this data';
COMMENT ON COLUMN DIM_CLAIM.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_CLAIM.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
