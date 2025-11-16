-- =====================================================
-- fct_appointments_scheduled - Appointment Fact Table
-- =====================================================
-- Purpose: Track appointment scheduling and attendance
-- Grain: One row per appointment
-- Type: Transaction Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE fct_appointments_scheduled (
    appointment_fact_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    veteran_sk INTEGER NOT NULL,
    evaluator_sk INTEGER,
    facility_sk INTEGER NOT NULL,
    evaluation_type_sk INTEGER,
    appointment_sk INTEGER NOT NULL,
    claim_sk INTEGER,

    -- Date Foreign Keys
    requested_date_sk INTEGER NOT NULL,
    scheduled_date_sk INTEGER,
    appointment_date_sk INTEGER,
    completed_date_sk INTEGER,
    cancelled_date_sk INTEGER,

    -- Degenerate Dimensions
    appointment_id VARCHAR(50) NOT NULL UNIQUE,
    confirmation_number VARCHAR(50),

    -- Scheduling Metrics
    days_from_request_to_schedule INTEGER,
    days_from_schedule_to_appointment INTEGER,
    total_wait_days INTEGER,

    -- Appointment Details
    scheduled_start_time TIME,
    scheduled_end_time TIME,
    actual_start_time TIME,
    actual_end_time TIME,
    scheduled_duration_minutes INTEGER,
    actual_duration_minutes INTEGER,
    duration_variance_minutes INTEGER,

    -- Attendance Status
    scheduled_flag BOOLEAN DEFAULT TRUE,
    confirmed_flag BOOLEAN DEFAULT FALSE,
    attended_flag BOOLEAN DEFAULT FALSE,
    completed_flag BOOLEAN DEFAULT FALSE,
    no_show_flag BOOLEAN DEFAULT FALSE,
    cancelled_flag BOOLEAN DEFAULT FALSE,
    late_arrival_flag BOOLEAN DEFAULT FALSE,
    minutes_late INTEGER DEFAULT 0,

    -- Cancellation Details
    cancelled_by VARCHAR(50),  -- Veteran, Provider, System
    cancellation_reason_code VARCHAR(20),
    cancellation_reason_description VARCHAR(255),
    advance_cancellation_flag BOOLEAN,  -- Cancelled with >24 hours notice
    cancellation_hours_notice INTEGER,

    -- Rescheduling
    rescheduled_flag BOOLEAN DEFAULT FALSE,
    reschedule_count INTEGER DEFAULT 0,
    reschedule_reason VARCHAR(255),
    new_appointment_id VARCHAR(50),

    -- Appointment Type Details
    appointment_method VARCHAR(50),  -- In-Person, Telehealth, Phone
    visit_type VARCHAR(50),
    urgency_level VARCHAR(20),  -- Routine, Priority, Urgent
    new_patient_flag BOOLEAN DEFAULT FALSE,

    -- Reminders and Confirmations
    reminder_1_sent_flag BOOLEAN DEFAULT FALSE,
    reminder_1_sent_date DATE,
    reminder_2_sent_flag BOOLEAN DEFAULT FALSE,
    reminder_2_sent_date DATE,
    confirmation_received_flag BOOLEAN DEFAULT FALSE,
    confirmation_received_date DATE,
    confirmation_method VARCHAR(50),  -- Email, Phone, Text, Portal

    -- Wait Time Performance
    meets_va_wait_time_goal BOOLEAN,  -- VA goal: within 20 days for urgent, 28 days for routine
    wait_time_category VARCHAR(50),  -- Excellent, Good, Needs Improvement, Poor
    third_party_care_flag BOOLEAN DEFAULT FALSE,  -- Community Care appointment

    -- Telehealth Specific
    telehealth_flag BOOLEAN DEFAULT FALSE,
    telehealth_platform VARCHAR(50),
    technical_issues_flag BOOLEAN DEFAULT FALSE,
    technical_issue_description VARCHAR(255),
    connectivity_quality_score INTEGER,  -- 1-5 scale

    -- Travel
    travel_distance_miles DECIMAL(8,2),
    travel_time_minutes INTEGER,
    travel_reimbursement_eligible BOOLEAN DEFAULT FALSE,
    travel_reimbursement_amount DECIMAL(10,2),

    -- Veteran Satisfaction
    satisfaction_survey_sent_flag BOOLEAN DEFAULT FALSE,
    satisfaction_score INTEGER,  -- 1-5 scale
    would_recommend_flag BOOLEAN,

    -- Quality and Compliance
    same_day_scheduling_flag BOOLEAN DEFAULT FALSE,
    secure_messaging_used_flag BOOLEAN DEFAULT FALSE,
    interpreter_required_flag BOOLEAN DEFAULT FALSE,
    interpreter_provided_flag BOOLEAN,
    accommodation_required_flag BOOLEAN DEFAULT FALSE,
    accommodation_provided_flag BOOLEAN,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Foreign Key Constraints
    FOREIGN KEY (veteran_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans(veteran_sk),
    FOREIGN KEY (evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (facility_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities(facility_sk),
    FOREIGN KEY (evaluation_type_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluation_types(evaluation_type_sk),
    FOREIGN KEY (appointment_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_appointments(appointment_sk),
    FOREIGN KEY (claim_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_claims(claim_sk)
)
COMMENT = 'Transaction fact table for appointment scheduling and attendance'
CLUSTER BY (appointment_date_sk, facility_sk);

-- Column comments for data dictionary
COMMENT ON COLUMN fct_appointments_scheduled.appointment_fact_sk IS 'Surrogate primary key for the appointment fact';
COMMENT ON COLUMN fct_appointments_scheduled.veteran_sk IS 'Foreign key to dim_veterans dimension';
COMMENT ON COLUMN fct_appointments_scheduled.evaluator_sk IS 'Foreign key to dim_evaluators dimension';
COMMENT ON COLUMN fct_appointments_scheduled.facility_sk IS 'Foreign key to dim_facilities dimension';
COMMENT ON COLUMN fct_appointments_scheduled.evaluation_type_sk IS 'Foreign key to dim_evaluation_types dimension';
COMMENT ON COLUMN fct_appointments_scheduled.appointment_sk IS 'Foreign key to dim_appointments dimension';
COMMENT ON COLUMN fct_appointments_scheduled.claim_sk IS 'Foreign key to dim_claims dimension';
COMMENT ON COLUMN fct_appointments_scheduled.requested_date_sk IS 'Foreign key to dim_dates - date appointment was requested';
COMMENT ON COLUMN fct_appointments_scheduled.scheduled_date_sk IS 'Foreign key to dim_dates - date appointment was scheduled';
COMMENT ON COLUMN fct_appointments_scheduled.appointment_date_sk IS 'Foreign key to dim_dates - actual appointment date';
COMMENT ON COLUMN fct_appointments_scheduled.completed_date_sk IS 'Foreign key to dim_dates - date appointment was completed';
COMMENT ON COLUMN fct_appointments_scheduled.cancelled_date_sk IS 'Foreign key to dim_dates - date appointment was cancelled';
COMMENT ON COLUMN fct_appointments_scheduled.appointment_id IS 'Unique appointment identifier (degenerate dimension)';
COMMENT ON COLUMN fct_appointments_scheduled.confirmation_number IS 'Appointment confirmation number';
COMMENT ON COLUMN fct_appointments_scheduled.days_from_request_to_schedule IS 'Days between request and scheduling';
COMMENT ON COLUMN fct_appointments_scheduled.days_from_schedule_to_appointment IS 'Days between scheduling and appointment date';
COMMENT ON COLUMN fct_appointments_scheduled.total_wait_days IS 'Total wait days from request to appointment';
COMMENT ON COLUMN fct_appointments_scheduled.scheduled_start_time IS 'Scheduled start time';
COMMENT ON COLUMN fct_appointments_scheduled.scheduled_end_time IS 'Scheduled end time';
COMMENT ON COLUMN fct_appointments_scheduled.actual_start_time IS 'Actual start time';
COMMENT ON COLUMN fct_appointments_scheduled.actual_end_time IS 'Actual end time';
COMMENT ON COLUMN fct_appointments_scheduled.scheduled_duration_minutes IS 'Scheduled duration in minutes';
COMMENT ON COLUMN fct_appointments_scheduled.actual_duration_minutes IS 'Actual duration in minutes';
COMMENT ON COLUMN fct_appointments_scheduled.duration_variance_minutes IS 'Difference between actual and scheduled duration';
COMMENT ON COLUMN fct_appointments_scheduled.scheduled_flag IS 'TRUE if appointment was successfully scheduled';
COMMENT ON COLUMN fct_appointments_scheduled.confirmed_flag IS 'TRUE if veteran confirmed the appointment';
COMMENT ON COLUMN fct_appointments_scheduled.attended_flag IS 'TRUE if veteran attended the appointment';
COMMENT ON COLUMN fct_appointments_scheduled.completed_flag IS 'TRUE if appointment was completed successfully';
COMMENT ON COLUMN fct_appointments_scheduled.no_show_flag IS 'TRUE if veteran did not show up';
COMMENT ON COLUMN fct_appointments_scheduled.cancelled_flag IS 'TRUE if appointment was cancelled';
COMMENT ON COLUMN fct_appointments_scheduled.late_arrival_flag IS 'TRUE if veteran arrived late';
COMMENT ON COLUMN fct_appointments_scheduled.minutes_late IS 'Number of minutes late (0 if on time)';
COMMENT ON COLUMN fct_appointments_scheduled.cancelled_by IS 'Who cancelled (Veteran, Provider, System)';
COMMENT ON COLUMN fct_appointments_scheduled.cancellation_reason_code IS 'Cancellation reason code';
COMMENT ON COLUMN fct_appointments_scheduled.cancellation_reason_description IS 'Cancellation reason description';
COMMENT ON COLUMN fct_appointments_scheduled.advance_cancellation_flag IS 'TRUE if cancelled with >24 hours notice';
COMMENT ON COLUMN fct_appointments_scheduled.cancellation_hours_notice IS 'Hours of advance notice for cancellation';
COMMENT ON COLUMN fct_appointments_scheduled.rescheduled_flag IS 'TRUE if appointment was rescheduled';
COMMENT ON COLUMN fct_appointments_scheduled.reschedule_count IS 'Number of times this appointment has been rescheduled';
COMMENT ON COLUMN fct_appointments_scheduled.reschedule_reason IS 'Reason for rescheduling';
COMMENT ON COLUMN fct_appointments_scheduled.new_appointment_id IS 'ID of rescheduled appointment if applicable';
COMMENT ON COLUMN fct_appointments_scheduled.appointment_method IS 'Method (In-Person, Telehealth, Phone)';
COMMENT ON COLUMN fct_appointments_scheduled.visit_type IS 'Visit type (C&P Exam, Follow-up, etc.)';
COMMENT ON COLUMN fct_appointments_scheduled.urgency_level IS 'Urgency level (Routine, Priority, Urgent)';
COMMENT ON COLUMN fct_appointments_scheduled.new_patient_flag IS 'TRUE if this is first appointment with this provider';
COMMENT ON COLUMN fct_appointments_scheduled.reminder_1_sent_flag IS 'TRUE if first reminder was sent';
COMMENT ON COLUMN fct_appointments_scheduled.reminder_1_sent_date IS 'Date first reminder was sent';
COMMENT ON COLUMN fct_appointments_scheduled.reminder_2_sent_flag IS 'TRUE if second reminder was sent';
COMMENT ON COLUMN fct_appointments_scheduled.reminder_2_sent_date IS 'Date second reminder was sent';
COMMENT ON COLUMN fct_appointments_scheduled.confirmation_received_flag IS 'TRUE if veteran confirmed appointment';
COMMENT ON COLUMN fct_appointments_scheduled.confirmation_received_date IS 'Date confirmation was received';
COMMENT ON COLUMN fct_appointments_scheduled.confirmation_method IS 'Confirmation method (Email, Phone, Text, Portal)';
COMMENT ON COLUMN fct_appointments_scheduled.meets_va_wait_time_goal IS 'TRUE if meets VA wait time goals (20-28 days)';
COMMENT ON COLUMN fct_appointments_scheduled.wait_time_category IS 'Wait time category (Excellent, Good, Needs Improvement, Poor)';
COMMENT ON COLUMN fct_appointments_scheduled.third_party_care_flag IS 'TRUE if community care appointment (non-VA)';
COMMENT ON COLUMN fct_appointments_scheduled.telehealth_flag IS 'TRUE if telehealth appointment';
COMMENT ON COLUMN fct_appointments_scheduled.telehealth_platform IS 'Telehealth platform used (Zoom, Teams, etc.)';
COMMENT ON COLUMN fct_appointments_scheduled.technical_issues_flag IS 'TRUE if technical issues occurred';
COMMENT ON COLUMN fct_appointments_scheduled.technical_issue_description IS 'Description of technical issues';
COMMENT ON COLUMN fct_appointments_scheduled.connectivity_quality_score IS 'Connection quality score (1-5 scale, 5=excellent)';
COMMENT ON COLUMN fct_appointments_scheduled.travel_distance_miles IS 'Distance veteran traveled to appointment in miles';
COMMENT ON COLUMN fct_appointments_scheduled.travel_time_minutes IS 'Travel time in minutes';
COMMENT ON COLUMN fct_appointments_scheduled.travel_reimbursement_eligible IS 'TRUE if eligible for travel reimbursement';
COMMENT ON COLUMN fct_appointments_scheduled.travel_reimbursement_amount IS 'Travel reimbursement amount paid';
COMMENT ON COLUMN fct_appointments_scheduled.satisfaction_survey_sent_flag IS 'TRUE if satisfaction survey was sent';
COMMENT ON COLUMN fct_appointments_scheduled.satisfaction_score IS 'Satisfaction score (1-5 scale, 5=very satisfied)';
COMMENT ON COLUMN fct_appointments_scheduled.would_recommend_flag IS 'TRUE if veteran would recommend (Net Promoter Score)';
COMMENT ON COLUMN fct_appointments_scheduled.same_day_scheduling_flag IS 'TRUE if scheduled same day as request';
COMMENT ON COLUMN fct_appointments_scheduled.secure_messaging_used_flag IS 'TRUE if secure messaging was used for scheduling';
COMMENT ON COLUMN fct_appointments_scheduled.interpreter_required_flag IS 'TRUE if interpreter services required';
COMMENT ON COLUMN fct_appointments_scheduled.interpreter_provided_flag IS 'TRUE if interpreter services were provided';
COMMENT ON COLUMN fct_appointments_scheduled.accommodation_required_flag IS 'TRUE if disability accommodations required';
COMMENT ON COLUMN fct_appointments_scheduled.accommodation_provided_flag IS 'TRUE if accommodations were provided';
COMMENT ON COLUMN fct_appointments_scheduled.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN fct_appointments_scheduled.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN fct_appointments_scheduled.updated_timestamp IS 'Timestamp when record was last updated';
