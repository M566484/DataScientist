-- =====================================================
-- DIM_APPOINTMENT - Appointment Dimension
-- =====================================================
-- Purpose: Appointment scheduling details
-- SCD Type: Type 1 dimension for appointment details

USE SCHEMA VETERAN_EVALUATION_DW.DIM;

CREATE OR REPLACE TABLE DIM_APPOINTMENT (
    APPOINTMENT_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
    APPOINTMENT_ID VARCHAR(50) NOT NULL UNIQUE,  -- Business key

    -- Appointment Type
    APPOINTMENT_TYPE VARCHAR(50),  -- In-Person, Telehealth, Phone
    VISIT_TYPE VARCHAR(50),  -- C&P Exam, Follow-up, Consultation

    -- Scheduling Information
    SCHEDULED_DURATION_MINUTES INTEGER,
    BUFFER_TIME_MINUTES INTEGER DEFAULT 0,

    -- Status
    APPOINTMENT_STATUS VARCHAR(50),  -- Scheduled, Confirmed, Completed, Cancelled, No-Show
    CANCELLATION_REASON VARCHAR(255),
    RESCHEDULED_FLAG BOOLEAN DEFAULT FALSE,
    RESCHEDULED_COUNT INTEGER DEFAULT 0,

    -- Reminders
    REMINDER_SENT_FLAG BOOLEAN DEFAULT FALSE,
    REMINDER_SENT_DATE DATE,
    CONFIRMATION_RECEIVED_FLAG BOOLEAN DEFAULT FALSE,

    -- Wait Time
    DAYS_UNTIL_APPOINTMENT INTEGER,
    SCHEDULING_TIER VARCHAR(20),  -- Within 20 days, 21-30 days, Over 30 days

    -- Metadata
    SOURCE_SYSTEM VARCHAR(50),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Type 1 dimension for appointment details';

-- Column comments for data dictionary
COMMENT ON COLUMN DIM_APPOINTMENT.APPOINTMENT_KEY IS 'Surrogate primary key for the appointment dimension';
COMMENT ON COLUMN DIM_APPOINTMENT.APPOINTMENT_ID IS 'Business key - Unique appointment identifier';
COMMENT ON COLUMN DIM_APPOINTMENT.APPOINTMENT_TYPE IS 'Appointment type (In-Person, Telehealth, Phone)';
COMMENT ON COLUMN DIM_APPOINTMENT.VISIT_TYPE IS 'Visit type (C&P Exam, Follow-up, Consultation)';
COMMENT ON COLUMN DIM_APPOINTMENT.SCHEDULED_DURATION_MINUTES IS 'Scheduled appointment duration in minutes';
COMMENT ON COLUMN DIM_APPOINTMENT.BUFFER_TIME_MINUTES IS 'Buffer time between appointments in minutes';
COMMENT ON COLUMN DIM_APPOINTMENT.APPOINTMENT_STATUS IS 'Appointment status (Scheduled, Confirmed, Completed, Cancelled, No-Show)';
COMMENT ON COLUMN DIM_APPOINTMENT.CANCELLATION_REASON IS 'Reason for cancellation if applicable';
COMMENT ON COLUMN DIM_APPOINTMENT.RESCHEDULED_FLAG IS 'TRUE if appointment has been rescheduled';
COMMENT ON COLUMN DIM_APPOINTMENT.RESCHEDULED_COUNT IS 'Number of times this appointment has been rescheduled';
COMMENT ON COLUMN DIM_APPOINTMENT.REMINDER_SENT_FLAG IS 'TRUE if appointment reminder was sent';
COMMENT ON COLUMN DIM_APPOINTMENT.REMINDER_SENT_DATE IS 'Date reminder was sent';
COMMENT ON COLUMN DIM_APPOINTMENT.CONFIRMATION_RECEIVED_FLAG IS 'TRUE if veteran confirmed appointment';
COMMENT ON COLUMN DIM_APPOINTMENT.DAYS_UNTIL_APPOINTMENT IS 'Number of days from scheduling to appointment';
COMMENT ON COLUMN DIM_APPOINTMENT.SCHEDULING_TIER IS 'Wait time tier (Within 20 days, 21-30 days, Over 30 days)';
COMMENT ON COLUMN DIM_APPOINTMENT.SOURCE_SYSTEM IS 'Source system that provided this data';
COMMENT ON COLUMN DIM_APPOINTMENT.CREATED_TIMESTAMP IS 'Timestamp when record was created';
COMMENT ON COLUMN DIM_APPOINTMENT.UPDATED_TIMESTAMP IS 'Timestamp when record was last updated';
