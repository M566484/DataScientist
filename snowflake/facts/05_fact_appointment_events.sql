-- =====================================================
-- fact_appointment_events - Appointment Lifecycle Events
-- =====================================================
-- Purpose: Track complete history of appointment lifecycle events
-- Grain: One row per appointment event (scheduled, confirmed, cancelled, rescheduled, completed)
-- Type: Transaction Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0
-- Complements: fact_appointments_scheduled (accumulating snapshot)

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE fact_appointment_events (
    appointment_event_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    veteran_sk INTEGER NOT NULL,
    evaluator_sk INTEGER,
    facility_sk INTEGER NOT NULL,
    evaluation_type_sk INTEGER,
    appointment_sk INTEGER,
    claim_sk INTEGER,

    -- Date Foreign Keys
    event_date_sk INTEGER NOT NULL,  -- Date this event occurred

    -- Degenerate Dimensions
    appointment_id VARCHAR(50) NOT NULL,  -- NOT UNIQUE - multiple events per appointment
    event_id VARCHAR(50) NOT NULL UNIQUE,  -- Unique event identifier

    -- Event Details
    event_type VARCHAR(50) NOT NULL,  -- SCHEDULED, CONFIRMED, RESCHEDULED, CANCELLED, NO_SHOW, COMPLETED, LATE_ARRIVAL
    event_status VARCHAR(50) NOT NULL,  -- SUCCESS, FAILED, PENDING
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    event_sequence_number INTEGER,  -- 1, 2, 3... for this appointment

    -- Rescheduling Tracking
    is_reschedule_event BOOLEAN DEFAULT FALSE,
    previous_appointment_id VARCHAR(50),  -- Link to previous appointment if rescheduled
    new_appointment_id VARCHAR(50),  -- Link to new appointment if this was rescheduled
    reschedule_reason_code VARCHAR(20),
    reschedule_reason_description VARCHAR(255),

    -- Cancellation Tracking
    is_cancellation_event BOOLEAN DEFAULT FALSE,
    cancelled_by VARCHAR(50),  -- Veteran, Provider, System, Admin
    cancellation_reason_code VARCHAR(20),
    cancellation_reason_description VARCHAR(255),
    cancellation_category VARCHAR(50),  -- Medical, Scheduling Conflict, Transportation, etc.
    advance_notice_hours INTEGER,  -- Hours notice before appointment
    cancellation_fee_applicable BOOLEAN DEFAULT FALSE,

    -- Scheduling Details (for SCHEDULED events)
    scheduled_appointment_date DATE,
    scheduled_start_time TIME,
    scheduled_duration_minutes INTEGER,
    appointment_method VARCHAR(50),  -- In-Person, Telehealth, Phone
    urgency_level VARCHAR(20),

    -- Completion Details (for COMPLETED events)
    actual_start_time TIME,
    actual_end_time TIME,
    actual_duration_minutes INTEGER,
    duration_variance_minutes INTEGER,

    -- Attendance Details (for NO_SHOW or LATE_ARRIVAL events)
    minutes_late INTEGER,
    no_show_reason_code VARCHAR(20),
    no_show_contacted_flag BOOLEAN,

    -- User/Actor Information
    event_initiated_by VARCHAR(50),  -- Veteran, Staff, System
    event_source_system VARCHAR(50),  -- VEMS, OMS, Portal, Call Center
    event_user_id VARCHAR(50),
    event_location VARCHAR(100),  -- Where event was initiated

    -- Communication Tracking
    notification_sent_flag BOOLEAN DEFAULT FALSE,
    notification_method VARCHAR(50),  -- Email, SMS, Phone, Portal
    notification_sent_timestamp TIMESTAMP_NTZ,
    veteran_acknowledged_flag BOOLEAN DEFAULT FALSE,

    -- Business Context
    wait_time_days_at_event INTEGER,  -- Total wait time when this event occurred
    reschedule_count_at_event INTEGER,  -- How many times rescheduled before this event
    is_same_day_event BOOLEAN DEFAULT FALSE,
    is_walk_in BOOLEAN DEFAULT FALSE,

    -- Measures for Analysis
    event_processing_time_seconds INTEGER,  -- How long event took to process
    system_availability_flag BOOLEAN DEFAULT TRUE,
    error_flag BOOLEAN DEFAULT FALSE,
    error_code VARCHAR(20),
    error_description VARCHAR(255),

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100),

    -- Foreign Key Constraints
    FOREIGN KEY (veteran_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_veterans')(veteran_sk),
    FOREIGN KEY (evaluator_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_evaluators')(evaluator_sk),
    FOREIGN KEY (facility_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_facilities')(facility_sk),
    FOREIGN KEY (evaluation_type_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_evaluation_types')(evaluation_type_sk),
    FOREIGN KEY (appointment_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_appointments')(appointment_sk),
    FOREIGN KEY (claim_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_claims')(claim_sk),
    FOREIGN KEY (event_date_sk) REFERENCES IDENTIFIER(get_dw_database() || '.WAREHOUSE.dim_dates')(date_sk)
)
COMMENT = 'Transaction fact table capturing complete lifecycle history of appointment events - one row per event'
CLUSTER BY (event_date_sk, appointment_id);

-- Column comments for data dictionary
COMMENT ON COLUMN fact_appointment_events.appointment_event_sk IS 'Surrogate primary key for the appointment event fact';
COMMENT ON COLUMN fact_appointment_events.appointment_id IS 'Appointment identifier - can have multiple events for same appointment';
COMMENT ON COLUMN fact_appointment_events.event_id IS 'Unique event identifier';
COMMENT ON COLUMN fact_appointment_events.event_type IS 'Type of event: SCHEDULED, CONFIRMED, RESCHEDULED, CANCELLED, NO_SHOW, COMPLETED, LATE_ARRIVAL';
COMMENT ON COLUMN fact_appointment_events.event_status IS 'Status of event: SUCCESS, FAILED, PENDING';
COMMENT ON COLUMN fact_appointment_events.event_timestamp IS 'Exact timestamp when event occurred';
COMMENT ON COLUMN fact_appointment_events.event_sequence_number IS 'Sequential number for this appointment (1=first event, 2=second event, etc.)';
COMMENT ON COLUMN fact_appointment_events.previous_appointment_id IS 'Links to previous appointment ID if this is a rescheduled appointment';
COMMENT ON COLUMN fact_appointment_events.new_appointment_id IS 'Links to new appointment ID if this appointment was rescheduled';
COMMENT ON COLUMN fact_appointment_events.reschedule_reason_code IS 'Coded reason for rescheduling';
COMMENT ON COLUMN fact_appointment_events.cancelled_by IS 'Actor who cancelled: Veteran, Provider, System, Admin';
COMMENT ON COLUMN fact_appointment_events.cancellation_reason_code IS 'Coded cancellation reason';
COMMENT ON COLUMN fact_appointment_events.advance_notice_hours IS 'Hours of advance notice before scheduled appointment';
COMMENT ON COLUMN fact_appointment_events.scheduled_appointment_date IS 'Date appointment was scheduled for (at time of this event)';
COMMENT ON COLUMN fact_appointment_events.actual_duration_minutes IS 'Actual duration for completed appointments';
COMMENT ON COLUMN fact_appointment_events.minutes_late IS 'Minutes late for late arrival events';
COMMENT ON COLUMN fact_appointment_events.event_initiated_by IS 'Who/what initiated this event: Veteran, Staff, System';
COMMENT ON COLUMN fact_appointment_events.event_source_system IS 'System where event originated: VEMS, OMS, Portal, Call Center';
COMMENT ON COLUMN fact_appointment_events.notification_sent_flag IS 'TRUE if veteran was notified of this event';
COMMENT ON COLUMN fact_appointment_events.notification_method IS 'How veteran was notified: Email, SMS, Phone, Portal';
COMMENT ON COLUMN fact_appointment_events.wait_time_days_at_event IS 'Total wait time when this event occurred';
COMMENT ON COLUMN fact_appointment_events.reschedule_count_at_event IS 'Number of prior reschedules when this event occurred';
COMMENT ON COLUMN fact_appointment_events.is_same_day_event IS 'TRUE if event occurred same day as scheduled appointment';
COMMENT ON COLUMN fact_appointment_events.event_processing_time_seconds IS 'Time taken to process this event in the system';
COMMENT ON COLUMN fact_appointment_events.error_flag IS 'TRUE if event encountered errors';
COMMENT ON COLUMN fact_appointment_events.created_timestamp IS 'Timestamp when event record was created in data warehouse';
