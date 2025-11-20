-- =====================================================
-- dim_appointments - Appointment Dimension
-- =====================================================
-- Purpose: Appointment scheduling details
-- SCD Type: Type 1 dimension for appointment details
-- Standards: VES Snowflake Naming Conventions v1.0

SET dw_database = (SELECT get_dw_database());
USE DATABASE IDENTIFIER($dw_database);
USE SCHEMA WAREHOUSE;

CREATE OR REPLACE TABLE dim_appointments (
    appointment_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    appointment_id VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Appointment Type
    appointment_type VARCHAR(50),  -- In-Person, Telehealth, Phone
    visit_type VARCHAR(50),  -- C&P Exam, Follow-up, Consultation

    -- Scheduling Information
    scheduled_duration_minutes INTEGER,
    buffer_time_minutes INTEGER DEFAULT 0,

    -- Status
    appointment_status VARCHAR(50),  -- Scheduled, Confirmed, Completed, Cancelled, No-Show
    cancellation_reason VARCHAR(255),
    rescheduled_flag BOOLEAN DEFAULT FALSE,
    rescheduled_count INTEGER DEFAULT 0,

    -- Reminders
    reminder_sent_flag BOOLEAN DEFAULT FALSE,
    reminder_sent_date DATE,
    confirmation_received_flag BOOLEAN DEFAULT FALSE,

    -- Wait Time
    days_until_appointment INTEGER,
    scheduling_tier VARCHAR(20),  -- Within 20 days, 21-30 days, Over 30 days

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for appointment details';

-- Column comments for data dictionary
COMMENT ON COLUMN dim_appointments.appointment_sk IS 'Surrogate primary key for the appointment dimension';
COMMENT ON COLUMN dim_appointments.appointment_id IS 'Business key - Unique appointment identifier';
COMMENT ON COLUMN dim_appointments.appointment_type IS 'Appointment type (In-Person, Telehealth, Phone)';
COMMENT ON COLUMN dim_appointments.visit_type IS 'Visit type (C&P Exam, Follow-up, Consultation)';
COMMENT ON COLUMN dim_appointments.scheduled_duration_minutes IS 'Scheduled appointment duration in minutes';
COMMENT ON COLUMN dim_appointments.buffer_time_minutes IS 'Buffer time between appointments in minutes';
COMMENT ON COLUMN dim_appointments.appointment_status IS 'Appointment status (Scheduled, Confirmed, Completed, Cancelled, No-Show)';
COMMENT ON COLUMN dim_appointments.cancellation_reason IS 'Reason for cancellation if applicable';
COMMENT ON COLUMN dim_appointments.rescheduled_flag IS 'TRUE if appointment has been rescheduled';
COMMENT ON COLUMN dim_appointments.rescheduled_count IS 'Number of times this appointment has been rescheduled';
COMMENT ON COLUMN dim_appointments.reminder_sent_flag IS 'TRUE if appointment reminder was sent';
COMMENT ON COLUMN dim_appointments.reminder_sent_date IS 'Date reminder was sent';
COMMENT ON COLUMN dim_appointments.confirmation_received_flag IS 'TRUE if veteran confirmed appointment';
COMMENT ON COLUMN dim_appointments.days_until_appointment IS 'Number of days from scheduling to appointment';
COMMENT ON COLUMN dim_appointments.scheduling_tier IS 'Wait time tier (Within 20 days, 21-30 days, Over 30 days)';
COMMENT ON COLUMN dim_appointments.source_system IS 'Source system that provided this data';
COMMENT ON COLUMN dim_appointments.created_timestamp IS 'Timestamp when record was created';
COMMENT ON COLUMN dim_appointments.updated_timestamp IS 'Timestamp when record was last updated';
