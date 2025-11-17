-- =====================================================
-- VES Data Pipeline - ETL Procedures for Facts
-- =====================================================
-- Purpose: Transform and load fact tables
-- Pattern: ODS → Staging → Warehouse (with surrogate key lookups)
-- Standards: VES Snowflake Naming Conventions v1.0

USE DATABASE VETERAN_EVALUATION_DW;
USE SCHEMA WAREHOUSE;

-- =====================================================
-- ETL Procedure: Transform ODS to Staging - Exam Requests
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_ods_to_staging_exam_requests(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_fact_exam_requests;

    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_fact_exam_requests (
        exam_request_id,
        va_request_number,
        veteran_id,
        assigned_evaluator_id,
        facility_id,
        exam_request_type_id,
        request_received_date,
        eligibility_confirmed_date,
        examiner_assigned_date,
        appointment_scheduled_date,
        exam_completed_date,
        request_closed_date,
        request_priority,
        requested_conditions,
        requested_conditions_count,
        requires_specialist_flag,
        required_specialty,
        complex_case_flag,
        eligibility_status,
        assignment_status,
        assignment_method,
        scheduled_flag,
        completed_flag,
        exam_location,
        sla_days_allowed,
        days_to_assignment,
        days_to_scheduling,
        total_cycle_time_days,
        sla_met_flag,
        sla_variance_days,
        request_status,
        source_system,
        batch_id
    )
    SELECT
        -- Degenerate Dimensions
        exam_request_id,
        va_request_number,

        -- Dimension Business Keys (for SK lookup)
        COALESCE(veteran_va_id, 'SSN-' || veteran_ssn) AS veteran_id,
        assigned_evaluator_npi AS assigned_evaluator_id,
        facility_code AS facility_id,
        request_type AS exam_request_type_id,

        -- Dates
        request_received_date,
        eligibility_confirmed_date,
        assignment_date AS examiner_assigned_date,
        appointment_scheduled_date,
        exam_completed_date,
        request_closed_date,

        -- Request Details
        UPPER(TRIM(request_priority)) AS request_priority,
        UPPER(TRIM(requested_conditions)) AS requested_conditions,
        ARRAY_SIZE(SPLIT(requested_conditions, ',')) AS requested_conditions_count,
        COALESCE(requires_specialist_flag, FALSE) AS requires_specialist_flag,
        UPPER(TRIM(required_specialty)) AS required_specialty,
        CASE WHEN ARRAY_SIZE(SPLIT(requested_conditions, ',')) > 3 THEN TRUE ELSE FALSE END AS complex_case_flag,

        -- Eligibility
        UPPER(TRIM(eligibility_status)) AS eligibility_status,

        -- Assignment
        CASE
            WHEN assigned_evaluator_npi IS NOT NULL THEN 'ASSIGNED'
            ELSE 'PENDING'
        END AS assignment_status,
        UPPER(TRIM(assignment_method)) AS assignment_method,

        -- Scheduling
        CASE WHEN appointment_scheduled_date IS NOT NULL THEN TRUE ELSE FALSE END AS scheduled_flag,

        -- Completion
        CASE WHEN exam_completed_date IS NOT NULL THEN TRUE ELSE FALSE END AS completed_flag,
        UPPER(TRIM(exam_location)) AS exam_location,

        -- SLA Metrics (derived)
        sla_days_allowed,
        DATEDIFF(day, request_received_date, assignment_date) AS days_to_assignment,
        DATEDIFF(day, assignment_date, appointment_scheduled_date) AS days_to_scheduling,
        DATEDIFF(day, request_received_date, COALESCE(request_closed_date, CURRENT_DATE())) AS total_cycle_time_days,
        CASE
            WHEN request_closed_date IS NOT NULL
            THEN DATEDIFF(day, request_received_date, request_closed_date) <= sla_days_allowed
            ELSE NULL
        END AS sla_met_flag,
        CASE
            WHEN request_closed_date IS NOT NULL
            THEN DATEDIFF(day, request_received_date, request_closed_date) - sla_days_allowed
            ELSE NULL
        END AS sla_variance_days,

        -- Status
        UPPER(TRIM(request_status)) AS request_status,

        -- Metadata
        source_system,
        :p_batch_id AS batch_id

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_exam_requests_source
    WHERE batch_id = :p_batch_id
      AND extraction_timestamp = (
          SELECT MAX(extraction_timestamp)
          FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_exam_requests_source
          WHERE batch_id = :p_batch_id
      );

    RETURN 'Transformed ' || SQLROWCOUNT || ' exam request records to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Load Fact - Exam Requests
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_load_fact_exam_requests(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_merged INTEGER;
BEGIN
    -- Merge staging into fact table (accumulating snapshot pattern)
    MERGE INTO VETERAN_EVALUATION_DW.WAREHOUSE.fact_exam_requests tgt
    USING (
        SELECT
            stg.*,
            -- Resolve surrogate keys
            vet.veteran_sk,
            eval.evaluator_sk,
            fac.facility_sk,
            req_type.exam_request_type_sk,
            -- Date surrogate keys
            d_req.date_sk AS request_received_date_sk,
            d_elig.date_sk AS eligibility_confirmed_date_sk,
            d_assign.date_sk AS examiner_assigned_date_sk,
            d_sched.date_sk AS appointment_scheduled_date_sk,
            d_comp.date_sk AS exam_completed_date_sk,
            d_close.date_sk AS request_closed_date_sk
        FROM VETERAN_EVALUATION_DW.STAGING.stg_fact_exam_requests stg
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans vet
            ON stg.veteran_id = vet.veteran_id
            AND vet.is_current = TRUE
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators eval
            ON stg.assigned_evaluator_id = eval.evaluator_id
            AND eval.is_current = TRUE
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities fac
            ON stg.facility_id = fac.facility_id
            AND fac.is_current = TRUE
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_exam_request_types req_type
            ON stg.exam_request_type_id = req_type.exam_request_type_id
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_req
            ON stg.request_received_date = d_req.full_date
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_elig
            ON stg.eligibility_confirmed_date = d_elig.full_date
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_assign
            ON stg.examiner_assigned_date = d_assign.full_date
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_sched
            ON stg.appointment_scheduled_date = d_sched.full_date
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_comp
            ON stg.exam_completed_date = d_comp.full_date
        LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_close
            ON stg.request_closed_date = d_close.full_date
        WHERE stg.batch_id = :p_batch_id
    ) src
    ON tgt.exam_request_id = src.exam_request_id
    WHEN MATCHED THEN
        -- Update accumulating snapshot with new milestone dates
        UPDATE SET
            veteran_sk = src.veteran_sk,
            assigned_evaluator_sk = src.evaluator_sk,
            facility_sk = src.facility_sk,
            exam_request_type_sk = src.exam_request_type_sk,
            eligibility_confirmed_date_sk = COALESCE(src.eligibility_confirmed_date_sk, tgt.eligibility_confirmed_date_sk),
            examiner_assigned_date_sk = COALESCE(src.examiner_assigned_date_sk, tgt.examiner_assigned_date_sk),
            appointment_scheduled_date_sk = COALESCE(src.appointment_scheduled_date_sk, tgt.appointment_scheduled_date_sk),
            exam_completed_date_sk = COALESCE(src.exam_completed_date_sk, tgt.exam_completed_date_sk),
            request_closed_date_sk = COALESCE(src.request_closed_date_sk, tgt.request_closed_date_sk),
            eligibility_status = src.eligibility_status,
            assignment_status = src.assignment_status,
            scheduled_flag = src.scheduled_flag,
            completed_flag = src.completed_flag,
            days_to_assignment = src.days_to_assignment,
            days_to_scheduling = src.days_to_scheduling,
            total_cycle_time_days = src.total_cycle_time_days,
            sla_met_flag = src.sla_met_flag,
            sla_variance_days = src.sla_variance_days,
            request_status = src.request_status,
            updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        -- Insert new exam request
        INSERT (
            exam_request_id,
            va_request_number,
            veteran_sk,
            assigned_evaluator_sk,
            facility_sk,
            exam_request_type_sk,
            request_received_date_sk,
            eligibility_confirmed_date_sk,
            examiner_assigned_date_sk,
            appointment_scheduled_date_sk,
            exam_completed_date_sk,
            request_closed_date_sk,
            request_priority,
            requested_conditions,
            requested_conditions_count,
            requires_specialist_flag,
            required_specialty,
            complex_case_flag,
            eligibility_status,
            assignment_status,
            assignment_method,
            scheduled_flag,
            completed_flag,
            exam_location,
            sla_days_allowed,
            days_to_assignment,
            days_to_scheduling,
            total_cycle_time_days,
            sla_met_flag,
            sla_variance_days,
            request_status,
            source_system,
            created_timestamp,
            updated_timestamp
        ) VALUES (
            src.exam_request_id,
            src.va_request_number,
            src.veteran_sk,
            src.evaluator_sk,
            src.facility_sk,
            src.exam_request_type_sk,
            src.request_received_date_sk,
            src.eligibility_confirmed_date_sk,
            src.examiner_assigned_date_sk,
            src.appointment_scheduled_date_sk,
            src.exam_completed_date_sk,
            src.request_closed_date_sk,
            src.request_priority,
            src.requested_conditions,
            src.requested_conditions_count,
            src.requires_specialist_flag,
            src.required_specialty,
            src.complex_case_flag,
            src.eligibility_status,
            src.assignment_status,
            src.assignment_method,
            src.scheduled_flag,
            src.completed_flag,
            src.exam_location,
            src.sla_days_allowed,
            src.days_to_assignment,
            src.days_to_scheduling,
            src.total_cycle_time_days,
            src.sla_met_flag,
            src.sla_variance_days,
            src.request_status,
            src.source_system,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        );

    v_rows_merged := SQLROWCOUNT;

    RETURN 'Merged ' || v_rows_merged || ' exam request records';
END;
$$;

-- =====================================================
-- ETL Procedure: Full Pipeline - Exam Requests
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_etl_exam_requests(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Transform ODS to Staging
    CALL sp_transform_ods_to_staging_exam_requests(:p_batch_id);

    -- Load Staging to Warehouse
    CALL sp_load_fact_exam_requests(:p_batch_id);

    RETURN 'Exam requests ETL pipeline completed successfully';
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO VETERAN_EVALUATION_DW.ODS_RAW.ods_error_log (
            batch_id,
            source_table,
            error_type,
            error_message
        ) VALUES (
            :p_batch_id,
            'fact_exam_requests',
            'ETL_ERROR',
            SQLERRM
        );
        RETURN 'Error in exam requests ETL: ' || SQLERRM;
END;
$$;

-- =====================================================
-- ETL Procedure: Transform ODS to Staging - Evaluations
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_transform_ods_to_staging_evaluations(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE VETERAN_EVALUATION_DW.STAGING.stg_fact_evaluations;

    INSERT INTO VETERAN_EVALUATION_DW.STAGING.stg_fact_evaluations (
        evaluation_id,
        exam_request_id,
        dbq_form_id,
        veteran_id,
        evaluator_id,
        facility_id,
        evaluation_type_id,
        medical_condition_code,
        evaluation_date,
        scheduled_date,
        evaluation_duration_minutes,
        evaluation_location_type,
        telehealth_flag,
        evaluation_completed_flag,
        dbq_submitted_flag,
        nexus_opinion_provided,
        nexus_opinion,
        current_severity,
        functional_impact_score,
        recommended_rating_percentage,
        service_connected_opinion,
        report_completeness_score,
        sufficient_exam_flag,
        qa_reviewed_flag,
        qa_review_date,
        report_delivered_to_va_date,
        va_delivery_confirmed,
        evaluation_cost_amount,
        contractor_payment_amount,
        source_system,
        batch_id
    )
    SELECT
        -- Degenerate Dimensions
        evaluation_id,
        exam_request_id,
        dbq_form_id,

        -- Dimension Business Keys
        COALESCE(veteran_va_id, 'SSN-' || veteran_ssn) AS veteran_id,
        evaluator_npi AS evaluator_id,
        facility_code AS facility_id,
        UPPER(TRIM(evaluation_type)) AS evaluation_type_id,
        medical_condition_code,

        -- Dates
        evaluation_date,
        evaluation_date AS scheduled_date,  -- TODO: Get from appointments

        -- Evaluation Metrics
        evaluation_duration_minutes,
        UPPER(TRIM(evaluation_location_type)) AS evaluation_location_type,
        COALESCE(telehealth_flag, FALSE) AS telehealth_flag,

        -- Completion
        CASE WHEN evaluation_date IS NOT NULL THEN TRUE ELSE FALSE END AS evaluation_completed_flag,
        CASE WHEN dbq_form_id IS NOT NULL THEN TRUE ELSE FALSE END AS dbq_submitted_flag,

        -- Assessment
        CASE WHEN nexus_opinion IS NOT NULL THEN TRUE ELSE FALSE END AS nexus_opinion_provided,
        UPPER(TRIM(nexus_opinion)) AS nexus_opinion,
        UPPER(TRIM(current_severity)) AS current_severity,
        functional_impact_score,
        recommended_rating_percentage,
        UPPER(TRIM(service_connected_opinion)) AS service_connected_opinion,

        -- Quality
        report_completeness_score,
        COALESCE(sufficient_exam_flag, TRUE) AS sufficient_exam_flag,

        -- QA
        COALESCE(qa_reviewed_flag, FALSE) AS qa_reviewed_flag,
        qa_review_date,

        -- Delivery
        report_delivered_to_va_date,
        COALESCE(va_delivery_confirmed, FALSE) AS va_delivery_confirmed,

        -- Financial
        evaluation_cost_amount,
        contractor_payment_amount,

        -- Metadata
        source_system,
        :p_batch_id AS batch_id

    FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluations_source
    WHERE batch_id = :p_batch_id
      AND extraction_timestamp = (
          SELECT MAX(extraction_timestamp)
          FROM VETERAN_EVALUATION_DW.ODS_RAW.ods_evaluations_source
          WHERE batch_id = :p_batch_id
      );

    RETURN 'Transformed ' || SQLROWCOUNT || ' evaluation records to staging';
END;
$$;

-- =====================================================
-- ETL Procedure: Load Fact - Evaluations
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_load_fact_evaluations(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER;
BEGIN
    -- Insert new evaluations (transaction fact - insert only)
    INSERT INTO VETERAN_EVALUATION_DW.WAREHOUSE.fact_evaluations_completed (
        evaluation_id,
        exam_request_id,
        dbq_form_id,
        veteran_sk,
        evaluator_sk,
        facility_sk,
        evaluation_type_sk,
        medical_condition_sk,
        evaluation_date_sk,
        scheduled_date_sk,
        evaluation_duration_minutes,
        evaluation_completed_flag,
        dbq_submitted_flag,
        nexus_opinion_provided,
        nexus_opinion,
        current_severity,
        functional_impact_score,
        recommended_rating_percentage,
        service_connected_opinion,
        report_completeness_score,
        sufficient_exam_flag,
        qa_reviewed_flag,
        qa_review_date,
        report_delivered_to_va_date,
        va_delivery_confirmed,
        telehealth_flag,
        evaluation_cost_amount,
        contractor_payment_amount,
        source_system,
        created_timestamp,
        updated_timestamp
    )
    SELECT
        stg.evaluation_id,
        stg.exam_request_id,
        stg.dbq_form_id,
        vet.veteran_sk,
        eval.evaluator_sk,
        fac.facility_sk,
        eval_type.evaluation_type_sk,
        med_cond.medical_condition_sk,
        d_eval.date_sk AS evaluation_date_sk,
        d_sched.date_sk AS scheduled_date_sk,
        stg.evaluation_duration_minutes,
        stg.evaluation_completed_flag,
        stg.dbq_submitted_flag,
        stg.nexus_opinion_provided,
        stg.nexus_opinion,
        stg.current_severity,
        stg.functional_impact_score,
        stg.recommended_rating_percentage,
        stg.service_connected_opinion,
        stg.report_completeness_score,
        stg.sufficient_exam_flag,
        stg.qa_reviewed_flag,
        stg.qa_review_date,
        stg.report_delivered_to_va_date,
        stg.va_delivery_confirmed,
        stg.telehealth_flag,
        stg.evaluation_cost_amount,
        stg.contractor_payment_amount,
        stg.source_system,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM VETERAN_EVALUATION_DW.STAGING.stg_fact_evaluations stg
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans vet
        ON stg.veteran_id = vet.veteran_id AND vet.is_current = TRUE
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluators eval
        ON stg.evaluator_id = eval.evaluator_id AND eval.is_current = TRUE
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_facilities fac
        ON stg.facility_id = fac.facility_id AND fac.is_current = TRUE
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_evaluation_types eval_type
        ON stg.evaluation_type_id = eval_type.evaluation_type_code
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_medical_conditions med_cond
        ON stg.medical_condition_code = med_cond.condition_code
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_eval
        ON stg.evaluation_date = d_eval.full_date
    LEFT JOIN VETERAN_EVALUATION_DW.WAREHOUSE.dim_dates d_sched
        ON stg.scheduled_date = d_sched.full_date
    WHERE stg.batch_id = :p_batch_id
      AND NOT EXISTS (
          -- Prevent duplicates
          SELECT 1
          FROM VETERAN_EVALUATION_DW.WAREHOUSE.fact_evaluations_completed fact
          WHERE fact.evaluation_id = stg.evaluation_id
      );

    v_rows_inserted := SQLROWCOUNT;

    RETURN 'Inserted ' || v_rows_inserted || ' evaluation records';
END;
$$;

-- =====================================================
-- Data Quality Check Procedure
-- =====================================================

CREATE OR REPLACE PROCEDURE sp_validate_fact_data_quality(
    p_batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_orphan_count INTEGER;
    v_null_sk_count INTEGER;
BEGIN
    -- Check for orphan veteran records (no SK resolved)
    SELECT COUNT(*)
    INTO v_orphan_count
    FROM VETERAN_EVALUATION_DW.STAGING.stg_fact_exam_requests stg
    WHERE stg.batch_id = :p_batch_id
      AND NOT EXISTS (
          SELECT 1
          FROM VETERAN_EVALUATION_DW.WAREHOUSE.dim_veterans dim
          WHERE dim.veteran_id = stg.veteran_id
            AND dim.is_current = TRUE
      );

    IF (v_orphan_count > 0) THEN
        INSERT INTO VETERAN_EVALUATION_DW.ODS_RAW.ods_error_log (
            batch_id,
            source_table,
            error_type,
            error_message
        ) VALUES (
            :p_batch_id,
            'stg_fact_exam_requests',
            'DATA_QUALITY_WARNING',
            'Found ' || v_orphan_count || ' exam requests with no matching veteran'
        );
    END IF;

    RETURN 'Data quality validation complete. Orphan records: ' || v_orphan_count;
END;
$$;

-- =====================================================
-- Usage Examples
-- =====================================================

-- Execute fact ETL pipelines
-- CALL sp_etl_exam_requests('BATCH_20250117_120000');
-- CALL sp_etl_evaluations('BATCH_20250117_120000');

-- Validate data quality
-- CALL sp_validate_fact_data_quality('BATCH_20250117_120000');
