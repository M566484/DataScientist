-- =====================================================
-- fct_examiner_assignments - Examiner Assignment Events
-- =====================================================
-- Purpose: Track examiner work assignments and capacity management
-- Grain: One row per assignment event (assigned, accepted, rejected, transferred, completed)
-- Type: Transaction Fact Table
-- Standards: VES Snowflake Naming Conventions v1.0

USE SCHEMA VETERAN_EVALUATION_DW.WAREHOUSE;

CREATE OR REPLACE TABLE fct_examiner_assignments (
    assignment_event_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    -- Foreign Keys to Dimensions
    evaluator_sk INTEGER NOT NULL,  -- The examiner being assigned
    veteran_sk INTEGER NOT NULL,
    facility_sk INTEGER,
    evaluation_type_sk INTEGER,
    medical_condition_sk INTEGER,
    exam_request_type_sk INTEGER,

    -- Assigner Information
    assigned_by_evaluator_sk INTEGER,  -- Who made the assignment (supervisor, system)
    transferred_to_evaluator_sk INTEGER,  -- If transferred, who received it
    transferred_from_evaluator_sk INTEGER,  -- If received via transfer, who sent it

    -- Date Foreign Keys
    assignment_event_date_sk INTEGER NOT NULL,
    exam_request_date_sk INTEGER,  -- When request originally received
    scheduled_exam_date_sk INTEGER,  -- When exam is scheduled for

    -- Degenerate Dimensions
    assignment_event_id VARCHAR(50) NOT NULL UNIQUE,
    assignment_id VARCHAR(50) NOT NULL,  -- Can have multiple events per assignment
    exam_request_id VARCHAR(50),
    evaluation_id VARCHAR(50),  -- Links to eventual evaluation
    work_queue_id VARCHAR(50),

    -- Event Details
    event_type VARCHAR(50) NOT NULL,  -- ASSIGNED, ACCEPTED, REJECTED, TRANSFERRED_OUT, TRANSFERRED_IN, REASSIGNED, CANCELLED, COMPLETED, RETURNED
    event_status VARCHAR(50) NOT NULL,  -- PENDING, COMPLETED, CANCELLED
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    event_sequence_number INTEGER,  -- Sequence for this assignment

    -- Assignment Method
    assignment_method VARCHAR(50),  -- AUTO_ASSIGNMENT, MANUAL_ASSIGNMENT, PREFERRED_EXAMINER, SPECIALIST_REQUIRED, EMERGENCY_ASSIGNMENT
    assignment_algorithm_version VARCHAR(20),  -- Version of auto-assignment algorithm
    assignment_rule_applied VARCHAR(255),  -- Business rule that triggered assignment
    assignment_priority_score DECIMAL(5,2),  -- Algorithm priority score
    auto_assignment_eligible BOOLEAN DEFAULT TRUE,
    manual_override_flag BOOLEAN DEFAULT FALSE,
    manual_override_reason VARCHAR(255),

    -- Examiner Capacity at Assignment Time
    examiner_current_workload INTEGER,  -- Active cases at time of assignment
    examiner_available_capacity INTEGER,  -- Available slots
    examiner_utilization_percentage DECIMAL(5,2),  -- Workload/capacity ratio
    examiner_avg_cases_per_week DECIMAL(5,2),
    examiner_pending_assignments INTEGER,
    examiner_overdue_cases INTEGER,

    -- Examiner Performance Context
    examiner_acceptance_rate DECIMAL(5,2),  -- Historical acceptance rate
    examiner_completion_rate DECIMAL(5,2),  -- On-time completion rate
    examiner_quality_score DECIMAL(5,2),  -- Recent quality score
    examiner_avg_turnaround_days DECIMAL(5,2),
    examiner_specialty_match_score DECIMAL(5,2),  -- How well specialty matches need

    -- Acceptance/Rejection Details
    is_acceptance BOOLEAN DEFAULT FALSE,
    is_rejection BOOLEAN DEFAULT FALSE,
    acceptance_timestamp TIMESTAMP_NTZ,
    rejection_timestamp TIMESTAMP_NTZ,
    time_to_response_hours DECIMAL(10,2),  -- Time to accept/reject
    rejection_reason_code VARCHAR(20),
    rejection_reason_description VARCHAR(255),
    rejection_category VARCHAR(50),  -- SCHEDULING_CONFLICT, CAPACITY, SPECIALTY_MISMATCH, GEOGRAPHY, PERSONAL

    -- Transfer Details
    is_transfer_out BOOLEAN DEFAULT FALSE,
    is_transfer_in BOOLEAN DEFAULT FALSE,
    transfer_reason_code VARCHAR(20),
    transfer_reason_description VARCHAR(255),
    transfer_initiated_by VARCHAR(50),  -- EXAMINER, SUPERVISOR, SYSTEM, QA_TEAM
    transfer_approved_flag BOOLEAN,
    transfer_approved_by VARCHAR(100),
    transfer_timestamp TIMESTAMP_NTZ,

    -- Geographic and Location
    examiner_zip_code VARCHAR(10),
    examiner_city VARCHAR(100),
    examiner_state VARCHAR(2),
    veteran_zip_code VARCHAR(10),
    veteran_city VARCHAR(100),
    veteran_state VARCHAR(2),
    travel_distance_miles DECIMAL(8,2),
    location_preference_match BOOLEAN,  -- Examiner preferred location matches
    regional_assignment BOOLEAN DEFAULT TRUE,

    -- Scheduling Context
    requested_exam_date DATE,
    examiner_availability_next_30_days INTEGER,  -- Open slots in next 30 days
    examiner_next_available_date DATE,
    scheduling_conflict_flag BOOLEAN DEFAULT FALSE,
    scheduling_conflict_details VARCHAR(255),

    -- Complexity and Requirements
    case_complexity_level VARCHAR(20),  -- ROUTINE, MODERATE, COMPLEX, HIGH_COMPLEXITY
    specialty_required VARCHAR(100),
    specialty_match_flag BOOLEAN,
    examiner_certified_for_specialty BOOLEAN,
    experience_level_required VARCHAR(20),  -- JUNIOR, SENIOR, EXPERT
    examiner_experience_match BOOLEAN,
    estimated_exam_duration_minutes INTEGER,
    requires_special_equipment BOOLEAN DEFAULT FALSE,
    special_requirements TEXT,

    -- SLA and Timeliness
    exam_request_sla_days INTEGER,
    days_since_request_received INTEGER,
    days_until_sla_breach INTEGER,
    sla_risk_level VARCHAR(20),  -- LOW, MEDIUM, HIGH, CRITICAL
    at_risk_flag BOOLEAN DEFAULT FALSE,
    expedited_assignment BOOLEAN DEFAULT FALSE,
    expedite_reason VARCHAR(255),

    -- Priority and Urgency
    assignment_priority VARCHAR(20),  -- ROUTINE, PRIORITY, URGENT, EXPEDITE
    priority_score INTEGER,  -- Numeric priority (higher = more urgent)
    priority_reason VARCHAR(255),
    veteran_priority_flag BOOLEAN DEFAULT FALSE,  -- Homeless, terminally ill, etc.
    business_priority_flag BOOLEAN DEFAULT FALSE,  -- Contract requirement, etc.

    -- Queue Management
    source_queue VARCHAR(100),  -- Which queue assignment came from
    queue_position INTEGER,  -- Position in queue before assignment
    time_in_queue_hours DECIMAL(10,2),
    queue_priority_adjustment INTEGER,  -- Manual priority changes
    round_robin_sequence INTEGER,  -- For round-robin assignment

    -- Workload Balancing
    workload_balanced_flag BOOLEAN DEFAULT FALSE,
    workload_balance_score DECIMAL(5,2),  -- How well this balances load
    facility_capacity_percentage DECIMAL(5,2),
    facility_backlog_count INTEGER,
    alternative_examiners_available INTEGER,
    load_balancing_rule_applied VARCHAR(100),

    -- Assignment Outcome
    assignment_successful_flag BOOLEAN,
    assignment_completion_timestamp TIMESTAMP_NTZ,
    exam_scheduled_flag BOOLEAN DEFAULT FALSE,
    exam_scheduled_timestamp TIMESTAMP_NTZ,
    exam_completed_flag BOOLEAN DEFAULT FALSE,
    exam_completed_timestamp TIMESTAMP_NTZ,
    days_to_scheduling INTEGER,
    days_to_completion INTEGER,

    -- Reassignment Tracking
    is_reassignment BOOLEAN DEFAULT FALSE,
    reassignment_number INTEGER DEFAULT 0,  -- How many times reassigned
    original_assignment_id VARCHAR(50),  -- First assignment in chain
    previous_assignment_id VARCHAR(50),  -- Immediate previous
    reassignment_reason VARCHAR(255),
    max_reassignments_flag BOOLEAN DEFAULT FALSE,  -- Hit reassignment limit

    -- Performance Impact
    assignment_delay_flag BOOLEAN DEFAULT FALSE,  -- Assignment delayed vs optimal
    delay_reason VARCHAR(255),
    optimal_assignment_flag BOOLEAN,  -- Was this the optimal choice
    suboptimal_reason VARCHAR(255),
    assignment_efficiency_score DECIMAL(5,2),

    -- Financial Context
    estimated_payment_amount DECIMAL(10,2),
    payment_tier VARCHAR(20),  -- Standard, complex, specialist rates
    mileage_reimbursement_estimated DECIMAL(10,2),
    total_estimated_cost DECIMAL(10,2),

    -- Communication Tracking
    examiner_notified_flag BOOLEAN DEFAULT FALSE,
    notification_timestamp TIMESTAMP_NTZ,
    notification_method VARCHAR(50),  -- EMAIL, SMS, PORTAL, PHONE
    notification_delivered_flag BOOLEAN,
    examiner_viewed_assignment BOOLEAN DEFAULT FALSE,
    assignment_viewed_timestamp TIMESTAMP_NTZ,
    reminder_sent_count INTEGER DEFAULT 0,

    -- System and Process
    assignment_system VARCHAR(50),  -- System that made assignment
    assignment_batch_id VARCHAR(50),  -- For batch assignments
    bulk_assignment_flag BOOLEAN DEFAULT FALSE,
    exception_flag BOOLEAN DEFAULT FALSE,
    exception_code VARCHAR(20),
    exception_description VARCHAR(255),
    manual_intervention_required BOOLEAN DEFAULT FALSE,
    intervention_reason VARCHAR(255),

    -- Compliance and Audit
    assignment_authorized BOOLEAN DEFAULT TRUE,
    authorization_verified_flag BOOLEAN,
    compliance_check_passed BOOLEAN DEFAULT TRUE,
    compliance_issues TEXT,
    audit_flag BOOLEAN DEFAULT FALSE,
    audit_reason VARCHAR(255),

    -- Historical Context
    examiner_previous_assignment_count INTEGER,  -- Total historical assignments
    examiner_assignments_last_30_days INTEGER,
    veteran_previous_exams_count INTEGER,
    veteran_previous_examiner_sk INTEGER,  -- Previous examiner if re-exam
    repeat_examiner_flag BOOLEAN DEFAULT FALSE,  -- Same examiner as before
    examiner_familiar_with_veteran BOOLEAN DEFAULT FALSE,

    -- Optimization Metrics
    assignment_score DECIMAL(5,2),  -- Overall assignment quality score
    geographic_optimization_score DECIMAL(5,2),
    capacity_optimization_score DECIMAL(5,2),
    specialty_optimization_score DECIMAL(5,2),
    timeliness_optimization_score DECIMAL(5,2),

    -- Notes and Comments
    assignment_notes TEXT,
    examiner_notes TEXT,
    supervisor_notes TEXT,

    -- Metadata
    source_system VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100),

    -- Foreign Key Constraints
    FOREIGN KEY (evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (veteran_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans(veteran_sk),
    FOREIGN KEY (facility_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities(facility_sk),
    FOREIGN KEY (evaluation_type_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluation_types(evaluation_type_sk),
    FOREIGN KEY (medical_condition_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_medical_conditions(medical_condition_sk),
    FOREIGN KEY (exam_request_type_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_exam_request_types(exam_request_type_sk),
    FOREIGN KEY (assigned_by_evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (transferred_to_evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (transferred_from_evaluator_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (veteran_previous_examiner_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators(evaluator_sk),
    FOREIGN KEY (assignment_event_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk),
    FOREIGN KEY (exam_request_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk),
    FOREIGN KEY (scheduled_exam_date_sk) REFERENCES VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates(date_sk)
)
COMMENT = 'Transaction fact table tracking examiner assignment events - supports workload optimization and capacity management'
CLUSTER BY (assignment_event_date_sk, evaluator_sk, event_type);

-- Column comments for data dictionary
COMMENT ON COLUMN fct_examiner_assignments.assignment_event_sk IS 'Surrogate primary key for the assignment event';
COMMENT ON COLUMN fct_examiner_assignments.assignment_event_id IS 'Unique identifier for this specific assignment event';
COMMENT ON COLUMN fct_examiner_assignments.assignment_id IS 'Assignment identifier - can have multiple events per assignment';
COMMENT ON COLUMN fct_examiner_assignments.exam_request_id IS 'Links to the exam request being assigned';
COMMENT ON COLUMN fct_examiner_assignments.evaluation_id IS 'Links to eventual evaluation record if completed';
COMMENT ON COLUMN fct_examiner_assignments.evaluator_sk IS 'Foreign key to examiner being assigned work';
COMMENT ON COLUMN fct_examiner_assignments.assigned_by_evaluator_sk IS 'Foreign key to person/system that made assignment';
COMMENT ON COLUMN fct_examiner_assignments.transferred_to_evaluator_sk IS 'If transferred, foreign key to receiving examiner';
COMMENT ON COLUMN fct_examiner_assignments.transferred_from_evaluator_sk IS 'If received via transfer, foreign key to sending examiner';
COMMENT ON COLUMN fct_examiner_assignments.event_type IS 'Type of assignment event: ASSIGNED, ACCEPTED, REJECTED, TRANSFERRED_OUT, TRANSFERRED_IN, REASSIGNED, CANCELLED, COMPLETED, RETURNED';
COMMENT ON COLUMN fct_examiner_assignments.event_status IS 'Status of event: PENDING, COMPLETED, CANCELLED';
COMMENT ON COLUMN fct_examiner_assignments.event_timestamp IS 'Timestamp when assignment event occurred';
COMMENT ON COLUMN fct_examiner_assignments.event_sequence_number IS 'Sequential number for events on this assignment';
COMMENT ON COLUMN fct_examiner_assignments.assignment_method IS 'How assignment was made: AUTO_ASSIGNMENT, MANUAL_ASSIGNMENT, PREFERRED_EXAMINER, SPECIALIST_REQUIRED, EMERGENCY_ASSIGNMENT';
COMMENT ON COLUMN fct_examiner_assignments.assignment_algorithm_version IS 'Version of auto-assignment algorithm used';
COMMENT ON COLUMN fct_examiner_assignments.assignment_rule_applied IS 'Business rule that triggered this assignment';
COMMENT ON COLUMN fct_examiner_assignments.assignment_priority_score IS 'Algorithm priority score for this assignment';
COMMENT ON COLUMN fct_examiner_assignments.examiner_current_workload IS 'Number of active cases examiner had at time of assignment';
COMMENT ON COLUMN fct_examiner_assignments.examiner_available_capacity IS 'Number of available slots examiner had';
COMMENT ON COLUMN fct_examiner_assignments.examiner_utilization_percentage IS 'Percentage of capacity utilized at time of assignment';
COMMENT ON COLUMN fct_examiner_assignments.examiner_acceptance_rate IS 'Historical acceptance rate for this examiner';
COMMENT ON COLUMN fct_examiner_assignments.examiner_completion_rate IS 'On-time completion rate for this examiner';
COMMENT ON COLUMN fct_examiner_assignments.examiner_quality_score IS 'Recent quality score for this examiner';
COMMENT ON COLUMN fct_examiner_assignments.examiner_specialty_match_score IS 'How well examiner specialty matches case requirements';
COMMENT ON COLUMN fct_examiner_assignments.is_acceptance IS 'TRUE if this event is an acceptance';
COMMENT ON COLUMN fct_examiner_assignments.is_rejection IS 'TRUE if this event is a rejection';
COMMENT ON COLUMN fct_examiner_assignments.time_to_response_hours IS 'Hours between assignment and examiner response (accept/reject)';
COMMENT ON COLUMN fct_examiner_assignments.rejection_reason_code IS 'Coded reason for rejection';
COMMENT ON COLUMN fct_examiner_assignments.rejection_category IS 'Category of rejection: SCHEDULING_CONFLICT, CAPACITY, SPECIALTY_MISMATCH, GEOGRAPHY, PERSONAL';
COMMENT ON COLUMN fct_examiner_assignments.is_transfer_out IS 'TRUE if this event is transferring work to another examiner';
COMMENT ON COLUMN fct_examiner_assignments.is_transfer_in IS 'TRUE if this event is receiving work from another examiner';
COMMENT ON COLUMN fct_examiner_assignments.transfer_reason_code IS 'Coded reason for transfer';
COMMENT ON COLUMN fct_examiner_assignments.transfer_initiated_by IS 'Who initiated transfer: EXAMINER, SUPERVISOR, SYSTEM, QA_TEAM';
COMMENT ON COLUMN fct_examiner_assignments.travel_distance_miles IS 'Distance from examiner to exam location';
COMMENT ON COLUMN fct_examiner_assignments.location_preference_match IS 'TRUE if assignment matches examiner location preferences';
COMMENT ON COLUMN fct_examiner_assignments.examiner_next_available_date IS 'Next available date examiner has open slots';
COMMENT ON COLUMN fct_examiner_assignments.scheduling_conflict_flag IS 'TRUE if examiner has scheduling conflicts';
COMMENT ON COLUMN fct_examiner_assignments.case_complexity_level IS 'Complexity level: ROUTINE, MODERATE, COMPLEX, HIGH_COMPLEXITY';
COMMENT ON COLUMN fct_examiner_assignments.specialty_required IS 'Medical specialty required for this case';
COMMENT ON COLUMN fct_examiner_assignments.specialty_match_flag IS 'TRUE if examiner specialty matches requirement';
COMMENT ON COLUMN fct_examiner_assignments.examiner_certified_for_specialty IS 'TRUE if examiner is certified for required specialty';
COMMENT ON COLUMN fct_examiner_assignments.exam_request_sla_days IS 'SLA days allowed for this exam request';
COMMENT ON COLUMN fct_examiner_assignments.days_since_request_received IS 'Days elapsed since exam request was received';
COMMENT ON COLUMN fct_examiner_assignments.days_until_sla_breach IS 'Days remaining until SLA breach';
COMMENT ON COLUMN fct_examiner_assignments.sla_risk_level IS 'Risk of SLA breach: LOW, MEDIUM, HIGH, CRITICAL';
COMMENT ON COLUMN fct_examiner_assignments.at_risk_flag IS 'TRUE if at risk of missing SLA';
COMMENT ON COLUMN fct_examiner_assignments.assignment_priority IS 'Priority level: ROUTINE, PRIORITY, URGENT, EXPEDITE';
COMMENT ON COLUMN fct_examiner_assignments.priority_score IS 'Numeric priority score (higher = more urgent)';
COMMENT ON COLUMN fct_examiner_assignments.source_queue IS 'Which work queue this assignment came from';
COMMENT ON COLUMN fct_examiner_assignments.queue_position IS 'Position in queue before assignment';
COMMENT ON COLUMN fct_examiner_assignments.time_in_queue_hours IS 'Hours spent in queue before assignment';
COMMENT ON COLUMN fct_examiner_assignments.workload_balanced_flag IS 'TRUE if assignment was made for workload balancing';
COMMENT ON COLUMN fct_examiner_assignments.workload_balance_score IS 'Score indicating how well this balances workload';
COMMENT ON COLUMN fct_examiner_assignments.facility_capacity_percentage IS 'Facility capacity utilization at time of assignment';
COMMENT ON COLUMN fct_examiner_assignments.alternative_examiners_available IS 'Number of alternative examiners who could take this case';
COMMENT ON COLUMN fct_examiner_assignments.assignment_successful_flag IS 'TRUE if assignment ultimately resulted in completed exam';
COMMENT ON COLUMN fct_examiner_assignments.exam_scheduled_flag IS 'TRUE if exam was scheduled following this assignment';
COMMENT ON COLUMN fct_examiner_assignments.exam_completed_flag IS 'TRUE if exam was completed following this assignment';
COMMENT ON COLUMN fct_examiner_assignments.days_to_scheduling IS 'Days from assignment to exam scheduled';
COMMENT ON COLUMN fct_examiner_assignments.days_to_completion IS 'Days from assignment to exam completed';
COMMENT ON COLUMN fct_examiner_assignments.is_reassignment IS 'TRUE if this is a reassignment (not initial assignment)';
COMMENT ON COLUMN fct_examiner_assignments.reassignment_number IS 'Number of times this case has been reassigned';
COMMENT ON COLUMN fct_examiner_assignments.original_assignment_id IS 'Assignment ID of the first assignment in reassignment chain';
COMMENT ON COLUMN fct_examiner_assignments.previous_assignment_id IS 'Assignment ID of immediate previous assignment';
COMMENT ON COLUMN fct_examiner_assignments.assignment_delay_flag IS 'TRUE if assignment was delayed vs optimal timing';
COMMENT ON COLUMN fct_examiner_assignments.optimal_assignment_flag IS 'TRUE if this was the optimal assignment choice';
COMMENT ON COLUMN fct_examiner_assignments.assignment_efficiency_score IS 'Score indicating efficiency of this assignment';
COMMENT ON COLUMN fct_examiner_assignments.estimated_payment_amount IS 'Estimated payment to examiner for this exam';
COMMENT ON COLUMN fct_examiner_assignments.examiner_notified_flag IS 'TRUE if examiner was notified of assignment';
COMMENT ON COLUMN fct_examiner_assignments.notification_method IS 'How examiner was notified: EMAIL, SMS, PORTAL, PHONE';
COMMENT ON COLUMN fct_examiner_assignments.examiner_viewed_assignment IS 'TRUE if examiner viewed assignment in system';
COMMENT ON COLUMN fct_examiner_assignments.assignment_batch_id IS 'Batch ID if this was part of bulk assignment';
COMMENT ON COLUMN fct_examiner_assignments.bulk_assignment_flag IS 'TRUE if part of bulk assignment process';
COMMENT ON COLUMN fct_examiner_assignments.manual_intervention_required IS 'TRUE if assignment required manual intervention';
COMMENT ON COLUMN fct_examiner_assignments.examiner_previous_assignment_count IS 'Total historical assignments for this examiner';
COMMENT ON COLUMN fct_examiner_assignments.examiner_assignments_last_30_days IS 'Assignments examiner received in last 30 days';
COMMENT ON COLUMN fct_examiner_assignments.repeat_examiner_flag IS 'TRUE if same examiner as previous exam for this veteran';
COMMENT ON COLUMN fct_examiner_assignments.assignment_score IS 'Overall assignment quality score';
COMMENT ON COLUMN fct_examiner_assignments.geographic_optimization_score IS 'Score for geographic optimization';
COMMENT ON COLUMN fct_examiner_assignments.capacity_optimization_score IS 'Score for capacity optimization';
COMMENT ON COLUMN fct_examiner_assignments.specialty_optimization_score IS 'Score for specialty matching optimization';
COMMENT ON COLUMN fct_examiner_assignments.timeliness_optimization_score IS 'Score for timeliness optimization';
COMMENT ON COLUMN fct_examiner_assignments.created_timestamp IS 'Timestamp when assignment event record was created in data warehouse';
