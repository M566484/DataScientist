-- =====================================================================================
-- SYSTEM CONFIGURATION FRAMEWORK
-- =====================================================================================
-- Purpose: Central configuration management for all environment-specific settings
-- Eliminates hardcoding and enables easy environment promotion (DEV → QA → PROD)
--
-- Benefits:
-- 1. Single source of truth for all configurations
-- 2. No code changes needed for different environments
-- 3. Easy to audit and version control configuration changes
-- 4. Supports feature flags and A/B testing
--
-- Author: Data Team
-- Date: 2025-11-21
-- =====================================================================================

USE DATABASE IDENTIFIER(fn_get_dw_database());
USE SCHEMA metadata;

-- =====================================================================================
-- CONFIGURATION TABLE
-- =====================================================================================

CREATE TABLE IF NOT EXISTS system_configuration (
    config_category VARCHAR(50) NOT NULL,
    config_key VARCHAR(100) NOT NULL,
    config_value VARCHAR(500),
    config_value_type VARCHAR(20) DEFAULT 'STRING', -- STRING, NUMBER, BOOLEAN, JSON
    description VARCHAR(1000),
    is_active BOOLEAN DEFAULT TRUE,
    is_sensitive BOOLEAN DEFAULT FALSE, -- For passwords, API keys (should be masked in queries)
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by VARCHAR(100) DEFAULT CURRENT_USER(),
    PRIMARY KEY (config_category, config_key)
);

-- =====================================================================================
-- CONFIGURATION FUNCTIONS - Type-safe getters
-- =====================================================================================

-- Get string configuration value
CREATE OR REPLACE FUNCTION fn_get_config(p_category VARCHAR, p_key VARCHAR)
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM IDENTIFIER(fn_get_dw_database() || '.metadata.system_configuration')
    WHERE config_category = p_category
      AND config_key = p_key
      AND is_active = TRUE
    LIMIT 1
$$;

-- Get numeric configuration value
CREATE OR REPLACE FUNCTION fn_get_config_number(p_category VARCHAR, p_key VARCHAR)
RETURNS NUMBER
AS
$$
    SELECT TRY_TO_NUMBER(config_value)
    FROM IDENTIFIER(fn_get_dw_database() || '.metadata.system_configuration')
    WHERE config_category = p_category
      AND config_key = p_key
      AND is_active = TRUE
    LIMIT 1
$$;

-- Get boolean configuration value
CREATE OR REPLACE FUNCTION fn_get_config_boolean(p_category VARCHAR, p_key VARCHAR)
RETURNS BOOLEAN
AS
$$
    SELECT TRY_TO_BOOLEAN(config_value)
    FROM IDENTIFIER(fn_get_dw_database() || '.metadata.system_configuration')
    WHERE config_category = p_category
      AND config_key = p_key
      AND is_active = TRUE
    LIMIT 1
$$;

-- =====================================================================================
-- DEFAULT CONFIGURATION VALUES
-- =====================================================================================

-- Database Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('database', 'dw_database_name', 'VESDW_PRD', 'STRING', 'Data warehouse database name', TRUE, FALSE),
    ('database', 'ods_database_name', 'VESODS_PRDDATA_PRD', 'STRING', 'ODS database name', TRUE, FALSE),
    ('database', 'retention_days_ods', '30', 'NUMBER', 'ODS data retention in days', TRUE, FALSE),
    ('database', 'retention_days_staging', '7', 'NUMBER', 'Staging data retention in days', TRUE, FALSE),
    ('database', 'retention_days_warehouse', '2555', 'NUMBER', 'Warehouse retention (7 years)', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    config_value_type = src.config_value_type,
    description = src.description,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Pipeline Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('pipeline', 'default_batch_size', '10000', 'NUMBER', 'Default batch size for processing', TRUE, FALSE),
    ('pipeline', 'max_retry_attempts', '3', 'NUMBER', 'Maximum retry attempts for failed tasks', TRUE, FALSE),
    ('pipeline', 'retry_delay_seconds', '300', 'NUMBER', 'Delay between retry attempts (5 minutes)', TRUE, FALSE),
    ('pipeline', 'enable_parallel_processing', 'TRUE', 'BOOLEAN', 'Enable parallel dimension/fact loading', TRUE, FALSE),
    ('pipeline', 'pipeline_timeout_hours', '4', 'NUMBER', 'Maximum pipeline execution time before timeout', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Data Quality Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('quality', 'min_dq_score_critical', '95', 'NUMBER', 'Minimum DQ score for critical data (blocks pipeline)', TRUE, FALSE),
    ('quality', 'min_dq_score_important', '80', 'NUMBER', 'Minimum DQ score for important data (warns)', TRUE, FALSE),
    ('quality', 'min_dq_score_advisory', '70', 'NUMBER', 'Minimum DQ score for advisory (logs only)', TRUE, FALSE),
    ('quality', 'enable_anomaly_detection', 'TRUE', 'BOOLEAN', 'Enable statistical anomaly detection', TRUE, FALSE),
    ('quality', 'anomaly_z_score_threshold', '3.0', 'NUMBER', 'Z-score threshold for anomaly detection', TRUE, FALSE),
    ('quality', 'max_null_percentage', '5', 'NUMBER', 'Maximum acceptable null percentage for critical fields', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- SLA Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('sla', 'pipeline_max_duration_hours', '2', 'NUMBER', 'Maximum acceptable pipeline duration', TRUE, FALSE),
    ('sla', 'data_freshness_max_hours', '4', 'NUMBER', 'Maximum data age before considered stale', TRUE, FALSE),
    ('sla', 'query_timeout_seconds', '300', 'NUMBER', 'Query timeout threshold (5 minutes)', TRUE, FALSE),
    ('sla', 'min_success_rate_percentage', '95', 'NUMBER', 'Minimum pipeline success rate', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Monitoring & Alerting Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('alerting', 'enable_email_alerts', 'TRUE', 'BOOLEAN', 'Enable email notifications', TRUE, FALSE),
    ('alerting', 'enable_slack_alerts', 'FALSE', 'BOOLEAN', 'Enable Slack notifications', TRUE, FALSE),
    ('alerting', 'critical_alert_recipients', 'data-team@company.com', 'STRING', 'Email recipients for critical alerts', TRUE, FALSE),
    ('alerting', 'warning_alert_recipients', 'data-team@company.com', 'STRING', 'Email recipients for warnings', TRUE, FALSE),
    ('alerting', 'alert_cooldown_minutes', '30', 'NUMBER', 'Minimum time between duplicate alerts', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Performance & Cost Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('performance', 'enable_query_result_cache', 'TRUE', 'BOOLEAN', 'Enable query result caching', TRUE, FALSE),
    ('performance', 'warehouse_auto_suspend_seconds', '300', 'NUMBER', 'Auto-suspend idle time (5 minutes)', TRUE, FALSE),
    ('performance', 'enable_clustering', 'TRUE', 'BOOLEAN', 'Enable automatic clustering for large tables', TRUE, FALSE),
    ('cost', 'daily_credit_limit', '100', 'NUMBER', 'Daily Snowflake credit limit', TRUE, FALSE),
    ('cost', 'credit_warning_threshold_pct', '75', 'NUMBER', 'Credit usage warning threshold', TRUE, FALSE),
    ('cost', 'credit_suspend_threshold_pct', '90', 'NUMBER', 'Credit usage suspension threshold', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- Multi-Source Integration Configuration
MERGE INTO system_configuration tgt
USING (
    SELECT * FROM VALUES
    ('integration', 'primary_source_system', 'VEMS', 'STRING', 'Primary source system for veterans', TRUE, FALSE),
    ('integration', 'enable_oms_integration', 'TRUE', 'BOOLEAN', 'Enable OMS data integration', TRUE, FALSE),
    ('integration', 'enable_vems_integration', 'TRUE', 'BOOLEAN', 'Enable VEMS data integration', TRUE, FALSE),
    ('integration', 'match_confidence_threshold', '85', 'NUMBER', 'Minimum confidence for entity matching', TRUE, FALSE),
    ('integration', 'conflict_resolution_strategy', 'PREFER_VEMS', 'STRING', 'Strategy for resolving conflicts', TRUE, FALSE)
) AS src (config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive)
ON tgt.config_category = src.config_category AND tgt.config_key = src.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = src.config_value,
    updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    config_category, config_key, config_value, config_value_type, description, is_active, is_sensitive
) VALUES (
    src.config_category, src.config_key, src.config_value, src.config_value_type, src.description, src.is_active, src.is_sensitive
);

-- =====================================================================================
-- CONFIGURATION AUDIT TABLE (Track all changes)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS system_configuration_audit (
    audit_id INTEGER AUTOINCREMENT PRIMARY KEY,
    config_category VARCHAR(50),
    config_key VARCHAR(100),
    old_value VARCHAR(500),
    new_value VARCHAR(500),
    changed_by VARCHAR(100),
    changed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    change_reason VARCHAR(1000)
);

-- =====================================================================================
-- CONFIGURATION CHANGE PROCEDURE (Use this to update configs with audit trail)
-- =====================================================================================

CREATE OR REPLACE PROCEDURE sp_update_configuration(
    p_category VARCHAR,
    p_key VARCHAR,
    p_new_value VARCHAR,
    p_reason VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_old_value VARCHAR;
    v_dw_database VARCHAR DEFAULT (SELECT fn_get_dw_database());
BEGIN
    -- Get old value
    SELECT config_value INTO v_old_value
    FROM IDENTIFIER(:v_dw_database || '.metadata.system_configuration')
    WHERE config_category = :p_category AND config_key = :p_key;

    -- Update configuration
    UPDATE IDENTIFIER(:v_dw_database || '.metadata.system_configuration')
    SET config_value = :p_new_value,
        updated_timestamp = CURRENT_TIMESTAMP(),
        updated_by = CURRENT_USER()
    WHERE config_category = :p_category AND config_key = :p_key;

    -- Log to audit table
    INSERT INTO IDENTIFIER(:v_dw_database || '.metadata.system_configuration_audit')
        (config_category, config_key, old_value, new_value, changed_by, change_reason)
    VALUES
        (:p_category, :p_key, :v_old_value, :p_new_value, CURRENT_USER(), :p_reason);

    RETURN 'Configuration updated: ' || :p_category || '.' || :p_key || ' = ' || :p_new_value;
END;
$$;

-- =====================================================================================
-- CONFIGURATION VIEWS - Easy querying
-- =====================================================================================

-- View all active configurations
CREATE OR REPLACE VIEW vw_active_configurations AS
SELECT
    config_category,
    config_key,
    CASE
        WHEN is_sensitive = TRUE THEN '***MASKED***'
        ELSE config_value
    END AS config_value,
    config_value_type,
    description,
    updated_timestamp,
    updated_by
FROM system_configuration
WHERE is_active = TRUE
ORDER BY config_category, config_key;

-- View configuration by category
CREATE OR REPLACE VIEW vw_config_by_category AS
SELECT
    config_category,
    COUNT(*) AS total_configs,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_configs,
    MAX(updated_timestamp) AS last_updated
FROM system_configuration
GROUP BY config_category
ORDER BY config_category;

-- View recent configuration changes
CREATE OR REPLACE VIEW vw_config_change_history AS
SELECT
    config_category,
    config_key,
    old_value,
    new_value,
    changed_by,
    changed_timestamp,
    change_reason
FROM system_configuration_audit
ORDER BY changed_timestamp DESC
LIMIT 100;

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
-- Example 1: Get configuration value
SELECT fn_get_config('pipeline', 'default_batch_size'); -- Returns '10000'

-- Example 2: Get numeric configuration
SELECT fn_get_config_number('quality', 'min_dq_score_critical'); -- Returns 95

-- Example 3: Get boolean configuration
SELECT fn_get_config_boolean('pipeline', 'enable_parallel_processing'); -- Returns TRUE

-- Example 4: Update configuration with audit trail
CALL sp_update_configuration(
    'pipeline',
    'default_batch_size',
    '20000',
    'Increased batch size for performance improvement'
);

-- Example 5: View all configurations
SELECT * FROM vw_active_configurations;

-- Example 6: View configuration change history
SELECT * FROM vw_config_change_history;

-- Example 7: Use in stored procedures
CREATE OR REPLACE PROCEDURE sp_example_procedure()
AS
$$
DECLARE
    v_batch_size NUMBER DEFAULT (SELECT fn_get_config_number('pipeline', 'default_batch_size'));
    v_enable_parallel BOOLEAN DEFAULT (SELECT fn_get_config_boolean('pipeline', 'enable_parallel_processing'));
BEGIN
    -- Use configuration values
    IF (v_enable_parallel = TRUE) THEN
        -- Parallel processing logic
    END IF;
END;
$$;
*/

-- =====================================================================================
-- VALIDATION QUERY
-- =====================================================================================

SELECT
    'System Configuration Framework Deployed' AS status,
    COUNT(*) AS total_configurations,
    COUNT(DISTINCT config_category) AS total_categories
FROM system_configuration;
