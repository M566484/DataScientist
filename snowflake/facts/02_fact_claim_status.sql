-- =====================================================
-- fact_claim_status_changes - Claim Status Fact Table
-- =====================================================
-- Purpose: Track claim status changes over time
-- Grain: One row per claim status change
-- Type: Accumulating Snapshot Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT fn_get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE fact_claim_status_changes (
    claim_status_fact_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    veteran_sk INTEGER NOT NULL,
    claim_sk INTEGER NOT NULL,
    facility_sk INTEGER,

    -- Date Foreign Keys (Milestone Dates)
    claim_filed_date_sk INTEGER,
    claim_received_date_sk INTEGER,
    initial_review_date_sk INTEGER,
    evidence_request_date_sk INTEGER,
    evidence_received_date_sk INTEGER,
    exam_scheduled_date_sk INTEGER,
    exam_completed_date_sk INTEGER,
    rating_decision_date_sk INTEGER,
    notification_sent_date_sk INTEGER,

    -- Degenerate Dimensions
    claim_id VARCHAR(50) NOT NULL,
    status_change_id VARCHAR(50) NOT NULL UNIQUE,

    -- Status Information
    previous_status VARCHAR(50),
    current_status VARCHAR(50) NOT NULL,
    status_change_reason VARCHAR(255),

    -- Processing Metrics
    days_in_previous_status INTEGER,
    total_days_pending INTEGER,
    days_to_complete INTEGER,

    -- Milestone Flags
    evidence_requested BOOLEAN DEFAULT FALSE,
    evidence_received BOOLEAN DEFAULT FALSE,
    exam_requested BOOLEAN DEFAULT FALSE,
    exam_completed BOOLEAN DEFAULT FALSE,
    decision_made BOOLEAN DEFAULT FALSE,
    notification_sent BOOLEAN DEFAULT FALSE,

    -- Processing Efficiency Metrics
    days_claim_to_initial_review INTEGER,
    days_review_to_evidence_request INTEGER,
    days_evidence_request_to_receipt INTEGER,
    days_evidence_to_exam_schedule INTEGER,
    days_exam_schedule_to_complete INTEGER,
    days_exam_to_decision INTEGER,
    days_decision_to_notification INTEGER,

    -- Claim Characteristics
    number_of_contentions INTEGER,
    number_of_exams_required INTEGER,
    number_of_exams_completed INTEGER,
    fully_developed_claim_flag BOOLEAN DEFAULT FALSE,

    -- Decision Metrics
    rating_percentage_granted INTEGER,
    service_connected_granted INTEGER,  -- Count of conditions granted
    service_connected_denied INTEGER,   -- Count of conditions denied
    deferred_conditions INTEGER,        -- Count of conditions deferred

    -- Quality Metrics
    sufficient_evidence_flag BOOLEAN,
    remand_flag BOOLEAN DEFAULT FALSE,
    remand_reason VARCHAR(255),
    additional_development_needed BOOLEAN DEFAULT FALSE,

    -- Administrative
    assigned_specialist VARCHAR(100),
    regional_office_code VARCHAR(10),
    priority_processing_flag BOOLEAN DEFAULT FALSE,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Foreign Key Constraints
    FOREIGN KEY (veteran_sk) REFERENCES IDENTIFIER(fn_get_dw_database() || '.WAREHOUSE.dim_veterans')(veteran_sk),
    FOREIGN KEY (claim_sk) REFERENCES IDENTIFIER(fn_get_dw_database() || '.WAREHOUSE.dim_claims')(claim_sk),
    FOREIGN KEY (facility_sk) REFERENCES IDENTIFIER(fn_get_dw_database() || '.WAREHOUSE.dim_facilities')(facility_sk)
)
COMMENT = 'Accumulating snapshot fact table for claim status and processing milestones'
CLUSTER BY (claim_sk, rating_decision_date_sk);

-- Column comments for data dictionary
COMMENT ON COLUMN fact_claim_status_changes.claim_status_fact_sk IS 'Surrogate primary key for the claim status fact';
COMMENT ON COLUMN fact_claim_status_changes.veteran_sk IS 'Foreign key to dim_veterans dimension';
COMMENT ON COLUMN fact_claim_status_changes.claim_sk IS 'Foreign key to dim_claims dimension';
COMMENT ON COLUMN fact_claim_status_changes.facility_sk IS 'Foreign key to dim_facilities dimension';
COMMENT ON COLUMN fact_claim_status_changes.claim_filed_date_sk IS 'Foreign key to dim_dates - claim filed milestone';
COMMENT ON COLUMN fact_claim_status_changes.claim_received_date_sk IS 'Foreign key to dim_dates - claim received milestone';
COMMENT ON COLUMN fact_claim_status_changes.initial_review_date_sk IS 'Foreign key to dim_dates - initial review milestone';
COMMENT ON COLUMN fact_claim_status_changes.evidence_request_date_sk IS 'Foreign key to dim_dates - evidence requested milestone';
COMMENT ON COLUMN fact_claim_status_changes.evidence_received_date_sk IS 'Foreign key to dim_dates - evidence received milestone';
COMMENT ON COLUMN fact_claim_status_changes.exam_scheduled_date_sk IS 'Foreign key to dim_dates - exam scheduled milestone';
COMMENT ON COLUMN fact_claim_status_changes.exam_completed_date_sk IS 'Foreign key to dim_dates - exam completed milestone';
COMMENT ON COLUMN fact_claim_status_changes.rating_decision_date_sk IS 'Foreign key to dim_dates - rating decision made milestone';
COMMENT ON COLUMN fact_claim_status_changes.notification_sent_date_sk IS 'Foreign key to dim_dates - decision notification sent milestone';
COMMENT ON COLUMN fact_claim_status_changes.claim_id IS 'Unique claim identifier (degenerate dimension)';
COMMENT ON COLUMN fact_claim_status_changes.status_change_id IS 'Unique status change event identifier';
COMMENT ON COLUMN fact_claim_status_changes.previous_status IS 'Previous claim status before this change';
COMMENT ON COLUMN fact_claim_status_changes.current_status IS 'Current claim status';
COMMENT ON COLUMN fact_claim_status_changes.status_change_reason IS 'Reason for status change';
COMMENT ON COLUMN fact_claim_status_changes.days_in_previous_status IS 'Number of days claim was in previous status';
COMMENT ON COLUMN fact_claim_status_changes.total_days_pending IS 'Total days claim has been pending';
COMMENT ON COLUMN fact_claim_status_changes.days_to_complete IS 'Total days from filing to completion';
COMMENT ON COLUMN fact_claim_status_changes.evidence_requested IS 'TRUE if evidence was requested from veteran';
COMMENT ON COLUMN fact_claim_status_changes.evidence_received IS 'TRUE if requested evidence was received';
COMMENT ON COLUMN fact_claim_status_changes.exam_requested IS 'TRUE if medical exam was requested';
COMMENT ON COLUMN fact_claim_status_changes.exam_completed IS 'TRUE if medical exam was completed';
COMMENT ON COLUMN fact_claim_status_changes.decision_made IS 'TRUE if rating decision has been made';
COMMENT ON COLUMN fact_claim_status_changes.notification_sent IS 'TRUE if decision notification was sent to veteran';
COMMENT ON COLUMN fact_claim_status_changes.days_claim_to_initial_review IS 'Days from claim filed to initial review';
COMMENT ON COLUMN fact_claim_status_changes.days_review_to_evidence_request IS 'Days from initial review to evidence request';
COMMENT ON COLUMN fact_claim_status_changes.days_evidence_request_to_receipt IS 'Days from evidence request to receipt';
COMMENT ON COLUMN fact_claim_status_changes.days_evidence_to_exam_schedule IS 'Days from evidence receipt to exam scheduled';
COMMENT ON COLUMN fact_claim_status_changes.days_exam_schedule_to_complete IS 'Days from exam scheduled to exam completed';
COMMENT ON COLUMN fact_claim_status_changes.days_exam_to_decision IS 'Days from exam completed to rating decision';
COMMENT ON COLUMN fact_claim_status_changes.days_decision_to_notification IS 'Days from decision to notification sent';
COMMENT ON COLUMN fact_claim_status_changes.number_of_contentions IS 'Number of contentions in the claim';
COMMENT ON COLUMN fact_claim_status_changes.number_of_exams_required IS 'Number of medical exams required';
COMMENT ON COLUMN fact_claim_status_changes.number_of_exams_completed IS 'Number of medical exams completed';
COMMENT ON COLUMN fact_claim_status_changes.fully_developed_claim_flag IS 'TRUE if Fully Developed Claim (FDC) program';
COMMENT ON COLUMN fact_claim_status_changes.rating_percentage_granted IS 'Total disability rating percentage granted';
COMMENT ON COLUMN fact_claim_status_changes.service_connected_granted IS 'Count of conditions granted service connection';
COMMENT ON COLUMN fact_claim_status_changes.service_connected_denied IS 'Count of conditions denied service connection';
COMMENT ON COLUMN fact_claim_status_changes.deferred_conditions IS 'Count of conditions deferred for additional development';
COMMENT ON COLUMN fact_claim_status_changes.sufficient_evidence_flag IS 'TRUE if sufficient evidence exists for decision';
COMMENT ON COLUMN fact_claim_status_changes.remand_flag IS 'TRUE if claim was remanded (sent back) for additional work';
COMMENT ON COLUMN fact_claim_status_changes.remand_reason IS 'Reason claim was remanded';
COMMENT ON COLUMN fact_claim_status_changes.additional_development_needed IS 'TRUE if additional development is needed';
COMMENT ON COLUMN fact_claim_status_changes.assigned_specialist IS 'Name of assigned rating specialist';
COMMENT ON COLUMN fact_claim_status_changes.regional_office_code IS 'Regional office code processing the claim';
COMMENT ON COLUMN fact_claim_status_changes.priority_processing_flag IS 'TRUE if claim has priority processing';
COMMENT ON COLUMN fact_claim_status_changes.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN fact_claim_status_changes.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN fact_claim_status_changes.updated_timestamp IS 'Timestamp when record was last updated';
