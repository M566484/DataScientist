-- =====================================================
-- fct_daily_facility_snapshot - Daily Metrics Snapshot
-- =====================================================
-- Purpose: Daily snapshot of key performance indicators
-- Grain: One row per facility per date
-- Type: Periodic Snapshot Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE fct_daily_facility_snapshot (
    daily_snapshot_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    facility_sk INTEGER NOT NULL,
    snapshot_date_sk INTEGER NOT NULL,

    -- Degenerate Dimension
    snapshot_id VARCHAR(50) NOT NULL,

    -- Evaluation Metrics
    evaluations_scheduled_count INTEGER DEFAULT 0,
    evaluations_completed_count INTEGER DEFAULT 0,
    evaluations_no_show_count INTEGER DEFAULT 0,
    evaluations_cancelled_count INTEGER DEFAULT 0,
    evaluation_completion_rate DECIMAL(5,2),  -- Percentage

    -- Appointment Metrics
    appointments_scheduled_count INTEGER DEFAULT 0,
    appointments_available_slots INTEGER DEFAULT 0,
    appointments_utilization_rate DECIMAL(5,2),  -- Percentage
    average_wait_time_days DECIMAL(8,2),

    -- Claim Metrics
    claims_received_count INTEGER DEFAULT 0,
    claims_pending_count INTEGER DEFAULT 0,
    claims_completed_count INTEGER DEFAULT 0,
    average_claim_age_days DECIMAL(8,2),
    claims_over_125_days_count INTEGER DEFAULT 0,  -- VA target metric

    -- Backlog Metrics
    evaluation_backlog_count INTEGER DEFAULT 0,
    exam_request_backlog_count INTEGER DEFAULT 0,
    dbq_pending_submission_count INTEGER DEFAULT 0,

    -- Quality Metrics
    sufficient_exam_rate DECIMAL(5,2),  -- Percentage
    timely_report_submission_rate DECIMAL(5,2),  -- Percentage
    average_report_completeness_score DECIMAL(5,2),

    -- Evaluator Metrics
    active_evaluators_count INTEGER DEFAULT 0,
    evaluator_utilization_rate DECIMAL(5,2),
    average_evaluations_per_evaluator DECIMAL(5,2),

    -- Veteran Metrics
    unique_veterans_served_count INTEGER DEFAULT 0,
    new_veterans_count INTEGER DEFAULT 0,
    returning_veterans_count INTEGER DEFAULT 0,

    -- Telehealth Metrics
    telehealth_appointments_count INTEGER DEFAULT 0,
    telehealth_completion_rate DECIMAL(5,2),
    telehealth_technical_issues_count INTEGER DEFAULT 0,

    -- Financial Metrics
    total_evaluation_costs DECIMAL(12,2) DEFAULT 0,
    total_contractor_payments DECIMAL(12,2) DEFAULT 0,
    total_travel_reimbursements DECIMAL(12,2) DEFAULT 0,
    average_cost_per_evaluation DECIMAL(10,2),

    -- Wait Time Performance
    appointments_within_20_days_count INTEGER DEFAULT 0,
    appointments_21_30_days_count INTEGER DEFAULT 0,
    appointments_over_30_days_count INTEGER DEFAULT 0,
    wait_time_compliance_rate DECIMAL(5,2),  -- Percentage meeting VA goals

    -- Satisfaction Metrics
    surveys_sent_count INTEGER DEFAULT 0,
    surveys_completed_count INTEGER DEFAULT 0,
    average_satisfaction_score DECIMAL(3,2),
    net_promoter_score DECIMAL(5,2),

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Unique constraint on facility and date
    UNIQUE (facility_sk, snapshot_date_sk),

    -- Foreign Key Constraints
    FOREIGN KEY (facility_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities(facility_sk),
    FOREIGN KEY (snapshot_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk)
)
COMMENT = 'Periodic snapshot fact table with daily performance metrics by facility'
CLUSTER BY (snapshot_date_sk, facility_sk);

-- Column comments for data dictionary
COMMENT ON COLUMN fct_daily_facility_snapshot.daily_snapshot_sk IS 'Surrogate primary key for the daily snapshot fact';
COMMENT ON COLUMN fct_daily_facility_snapshot.facility_sk IS 'Foreign key to dim_facilities dimension';
COMMENT ON COLUMN fct_daily_facility_snapshot.snapshot_date_sk IS 'Foreign key to dim_dates - snapshot date';
COMMENT ON COLUMN fct_daily_facility_snapshot.snapshot_id IS 'Unique snapshot identifier (degenerate dimension)';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluations_scheduled_count IS 'Number of evaluations scheduled for this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluations_completed_count IS 'Number of evaluations completed on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluations_no_show_count IS 'Number of no-shows on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluations_cancelled_count IS 'Number of cancelled evaluations on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluation_completion_rate IS 'Percentage of scheduled evaluations completed';
COMMENT ON COLUMN fct_daily_facility_snapshot.appointments_scheduled_count IS 'Number of appointments scheduled for this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.appointments_available_slots IS 'Number of available appointment slots';
COMMENT ON COLUMN fct_daily_facility_snapshot.appointments_utilization_rate IS 'Percentage of appointment slots utilized';
COMMENT ON COLUMN fct_daily_facility_snapshot.average_wait_time_days IS 'Average wait time from request to appointment in days';
COMMENT ON COLUMN fct_daily_facility_snapshot.claims_received_count IS 'Number of new claims received on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.claims_pending_count IS 'Number of claims pending as of this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.claims_completed_count IS 'Number of claims completed on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.average_claim_age_days IS 'Average age of pending claims in days';
COMMENT ON COLUMN fct_daily_facility_snapshot.claims_over_125_days_count IS 'Number of claims pending over 125 days (VA target metric)';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluation_backlog_count IS 'Number of evaluations in backlog as of this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.exam_request_backlog_count IS 'Number of exam requests awaiting scheduling';
COMMENT ON COLUMN fct_daily_facility_snapshot.dbq_pending_submission_count IS 'Number of DBQ forms pending submission';
COMMENT ON COLUMN fct_daily_facility_snapshot.sufficient_exam_rate IS 'Percentage of exams marked as sufficient for rating';
COMMENT ON COLUMN fct_daily_facility_snapshot.timely_report_submission_rate IS 'Percentage of reports submitted within target timeframe';
COMMENT ON COLUMN fct_daily_facility_snapshot.average_report_completeness_score IS 'Average completeness score for reports (0-100)';
COMMENT ON COLUMN fct_daily_facility_snapshot.active_evaluators_count IS 'Number of active evaluators as of this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.evaluator_utilization_rate IS 'Percentage of evaluator capacity utilized';
COMMENT ON COLUMN fct_daily_facility_snapshot.average_evaluations_per_evaluator IS 'Average number of evaluations per evaluator';
COMMENT ON COLUMN fct_daily_facility_snapshot.unique_veterans_served_count IS 'Number of unique veterans served on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.new_veterans_count IS 'Number of new veterans (first visit)';
COMMENT ON COLUMN fct_daily_facility_snapshot.returning_veterans_count IS 'Number of returning veterans';
COMMENT ON COLUMN fct_daily_facility_snapshot.telehealth_appointments_count IS 'Number of telehealth appointments on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.telehealth_completion_rate IS 'Percentage of telehealth appointments completed successfully';
COMMENT ON COLUMN fct_daily_facility_snapshot.telehealth_technical_issues_count IS 'Number of telehealth appointments with technical issues';
COMMENT ON COLUMN fct_daily_facility_snapshot.total_evaluation_costs IS 'Total cost of evaluations on this day';
COMMENT ON COLUMN fct_daily_facility_snapshot.total_contractor_payments IS 'Total payments to contract evaluators';
COMMENT ON COLUMN fct_daily_facility_snapshot.total_travel_reimbursements IS 'Total travel reimbursements paid to veterans';
COMMENT ON COLUMN fct_daily_facility_snapshot.average_cost_per_evaluation IS 'Average cost per evaluation';
COMMENT ON COLUMN fct_daily_facility_snapshot.appointments_within_20_days_count IS 'Number of appointments scheduled within 20 days';
COMMENT ON COLUMN fct_daily_facility_snapshot.appointments_21_30_days_count IS 'Number of appointments scheduled in 21-30 days';
COMMENT ON COLUMN fct_daily_facility_snapshot.appointments_over_30_days_count IS 'Number of appointments scheduled over 30 days out';
COMMENT ON COLUMN fct_daily_facility_snapshot.wait_time_compliance_rate IS 'Percentage meeting VA wait time goals';
COMMENT ON COLUMN fct_daily_facility_snapshot.surveys_sent_count IS 'Number of satisfaction surveys sent';
COMMENT ON COLUMN fct_daily_facility_snapshot.surveys_completed_count IS 'Number of satisfaction surveys completed';
COMMENT ON COLUMN fct_daily_facility_snapshot.average_satisfaction_score IS 'Average satisfaction score (1-5 scale)';
COMMENT ON COLUMN fct_daily_facility_snapshot.net_promoter_score IS 'Net Promoter Score (percentage promoters minus detractors)';
COMMENT ON COLUMN fct_daily_facility_snapshot.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN fct_daily_facility_snapshot.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN fct_daily_facility_snapshot.updated_timestamp IS 'Timestamp when record was last updated';
