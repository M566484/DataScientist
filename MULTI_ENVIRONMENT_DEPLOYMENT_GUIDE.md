# Multi-Environment Deployment Guide
## Managing DEV → TST → PRD Environments in Snowflake

**Eliminating Hardcoded Database Names for Seamless Environment Promotion**

Author: Mark Chappell
Last Updated: 2024-11-18
Difficulty: Intermediate
Estimated Setup Time: 1-2 hours

---

## Table of Contents

- [Overview](#overview)
- [Environment Architecture](#environment-architecture)
- [Strategy 1: Environment Configuration Table](#strategy-1-environment-configuration-table)
- [Strategy 2: Session Variables](#strategy-2-session-variables)
- [Strategy 3: Parameterized Stored Procedures](#strategy-3-parameterized-stored-procedures)
- [Strategy 4: Deployment Scripts with Templates](#strategy-4-deployment-scripts-with-templates)
- [Recommended Approach](#recommended-approach)
- [Implementation Guide](#implementation-guide)
- [Deployment Process](#deployment-process)
- [Testing Strategy](#testing-strategy)
- [Best Practices](#best-practices)

---

## Overview

### The Problem

Hardcoded database names in SQL scripts create deployment challenges:

```sql
-- ❌ PROBLEM: Hardcoded database name
USE DATABASE VESDW_PRD;
INSERT INTO VESDW_PRD.staging.stg_veteran_merged ...
```

**Issues:**
- Must manually edit scripts for each environment (DEV → TST → PRD)
- Risk of deploying DEV code to PRD with wrong database references
- No single source of truth for environment configuration
- Difficult to test deployment scripts before production

### The Solution

**Dynamic database references** that adapt to the environment:

```sql
-- ✅ SOLUTION: Environment-aware reference
USE DATABASE IDENTIFIER($environment_database);
INSERT INTO IDENTIFIER($environment_database || '.staging.stg_veteran_merged') ...
```

### Environment Strategy

```
Development         Test              Production
─────────────────  ─────────────────  ─────────────────
VESDW_DEV          VESDW_TST          VESDW_PRD
VESODS_DEV         VESODS_TST         VESODS_PRD

↓ Deploy & Test    ↓ UAT Testing      ↓ Production
```

---

## Environment Architecture

### Database Naming Convention

| Environment | DW Database | ODS Database | Purpose |
|-------------|-------------|--------------|---------|
| **DEV** | `VESDW_DEV` | `VESODS_DEV` | Development & unit testing |
| **TST** | `VESDW_TST` | `VESODS_TST` | Integration & UAT testing |
| **PRD** | `VESDW_PRD` | `VESODS_PRD` | Production |

### Schema Structure (Consistent Across Environments)

```
{ENV}_DATABASE
├── staging          # Same in all environments
├── warehouse        # Same in all environments
├── marts            # Same in all environments
└── metadata         # Same in all environments
```

**Key Point:** Schema names, table names, and column names are **identical** across environments. Only the database prefix changes.

---

## Strategy 1: Environment Configuration Table

### Overview

Create a metadata table that stores environment-specific configuration. All procedures read from this table.

### Implementation

#### Step 1: Create Configuration Table

```sql
-- Run in each environment (DEV, TST, PRD)
USE ROLE DATA_ENGINEER_ROLE;

-- Determine current environment database
USE DATABASE VESDW_DEV;  -- Or VESDW_TST, VESDW_PRD
USE SCHEMA metadata;

-- Create environment configuration table
CREATE TABLE IF NOT EXISTS environment_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(500),
    description VARCHAR(1000),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert environment-specific configuration
-- This is the ONLY place environment name is hardcoded!
INSERT INTO environment_config (config_key, config_value, description)
VALUES
    ('ENVIRONMENT_NAME', 'DEV', 'Current environment: DEV, TST, or PRD'),
    ('DW_DATABASE', 'VESDW_DEV', 'Data warehouse database name'),
    ('ODS_DATABASE', 'VESODS_DEV', 'Operational data store database name'),
    ('ENVIRONMENT_COLOR', 'GREEN', 'UI color indicator for safety'),
    ('DEPLOYMENT_DATE', CURRENT_TIMESTAMP()::VARCHAR, 'When this environment was last deployed'),
    ('DEPLOYMENT_VERSION', '2.0.0', 'Current version deployed');

-- For TST environment, change values:
-- UPDATE environment_config SET config_value = 'TST' WHERE config_key = 'ENVIRONMENT_NAME';
-- UPDATE environment_config SET config_value = 'VESDW_TST' WHERE config_key = 'DW_DATABASE';
-- UPDATE environment_config SET config_value = 'VESODS_TST' WHERE config_key = 'ODS_DATABASE';

-- For PRD environment, change values:
-- UPDATE environment_config SET config_value = 'PRD' WHERE config_key = 'ENVIRONMENT_NAME';
-- UPDATE environment_config SET config_value = 'VESDW_PRD' WHERE config_key = 'DW_DATABASE';
-- UPDATE environment_config SET config_value = 'VESODS_PRD' WHERE config_key = 'ODS_DATABASE';
```

#### Step 2: Create Helper Functions

```sql
-- Function to get current environment
CREATE OR REPLACE FUNCTION get_environment()
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'ENVIRONMENT_NAME'
$$;

-- Function to get DW database name
CREATE OR REPLACE FUNCTION fn_get_dw_database()
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'DW_DATABASE'
$$;

-- Function to get ODS database name
CREATE OR REPLACE FUNCTION fn_get_ods_database()
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'ODS_DATABASE'
$$;

-- Test the functions
SELECT get_environment() AS environment;
SELECT fn_get_dw_database() AS dw_database;
SELECT fn_get_ods_database() AS ods_database;
```

#### Step 3: Update Stored Procedures to Use Config

```sql
-- Example: Environment-aware merge procedure
CREATE OR REPLACE PROCEDURE sp_merge_veteran_to_staging()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR;
    v_ods_database VARCHAR;
    v_merge_sql VARCHAR;
    v_rows_merged INT;
BEGIN
    -- Get environment-specific database names
    SELECT fn_get_dw_database() INTO :v_dw_database;
    SELECT fn_get_ods_database() INTO :v_ods_database;

    -- Build dynamic SQL with environment databases
    v_merge_sql := '
    MERGE INTO ' || :v_dw_database || '.staging.stg_veteran_merged AS target
    USING (
        SELECT
            COALESCE(v.veteran_id, o.veteran_id) AS veteran_id,
            v.veteran_ssn,
            COALESCE(v.first_name, o.first_name) AS first_name,
            -- ... rest of merge logic ...
        FROM ' || :v_ods_database || '.VEMS_CORE.ods_vems_veteran v
        FULL OUTER JOIN ' || :v_ods_database || '.OMS.ods_oms_veteran o
            ON v.veteran_id = o.veteran_id
    ) AS source
    ON target.veteran_id = source.veteran_id
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...
    ';

    -- Execute dynamic SQL
    EXECUTE IMMEDIATE :v_merge_sql;

    RETURN 'Merge complete in environment: ' || get_environment();
END;
$$;
```

---

## Strategy 2: Session Variables

### Overview

Use Snowflake session variables to set the current environment. Great for interactive development.

### Implementation

```sql
-- Set session variables at the start of your session
SET (ENVIRONMENT_NAME, DW_DATABASE, ODS_DATABASE) = ('DEV', 'VESDW_DEV', 'VESODS_DEV');

-- For TST:
-- SET (ENVIRONMENT_NAME, DW_DATABASE, ODS_DATABASE) = ('TST', 'VESDW_TST', 'VESODS_TST');

-- For PRD:
-- SET (ENVIRONMENT_NAME, DW_DATABASE, ODS_DATABASE) = ('PRD', 'VESDW_PRD', 'VESODS_PRD');

-- Use variables in queries
USE DATABASE IDENTIFIER($DW_DATABASE);

-- Use in dynamic SQL
SELECT * FROM IDENTIFIER($DW_DATABASE || '.warehouse.dim_veteran');

-- Use in stored procedures
CREATE OR REPLACE PROCEDURE sp_example()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_db VARCHAR DEFAULT $DW_DATABASE;
BEGIN
    USE DATABASE IDENTIFIER(:v_db);
    -- ... rest of procedure
END;
$$;
```

### Session Variable Helper Script

```sql
-- File: set_environment.sql
-- Purpose: Set session variables for current environment
-- Usage: Source this file at start of session

-- Prompt user for environment (in practice, you'd pass as parameter)
-- For DEV:
SET ENVIRONMENT_NAME = 'DEV';
SET DW_DATABASE = 'VESDW_DEV';
SET ODS_DATABASE = 'VESODS_DEV';

-- Verify
SELECT
    $ENVIRONMENT_NAME AS environment,
    $DW_DATABASE AS dw_database,
    $ODS_DATABASE AS ods_database;

-- Set default database
USE DATABASE IDENTIFIER($DW_DATABASE);
USE SCHEMA metadata;

-- Display warning banner
SELECT
    '═══════════════════════════════════════════════════' AS banner,
    '  ENVIRONMENT: ' || $ENVIRONMENT_NAME AS message,
    '  DATABASE: ' || $DW_DATABASE AS database_info,
    '═══════════════════════════════════════════════════' AS banner2;
```

---

## Strategy 3: Parameterized Stored Procedures

### Overview

Pass environment as a parameter to stored procedures. Most explicit and testable.

### Implementation

```sql
-- Create procedure that accepts environment parameter
CREATE OR REPLACE PROCEDURE sp_merge_veteran_to_staging_v2(
    p_environment VARCHAR DEFAULT 'DEV'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_database VARCHAR;
    v_ods_database VARCHAR;
    v_merge_sql VARCHAR;
BEGIN
    -- Determine database names based on environment parameter
    v_dw_database := 'VESDW_' || :p_environment;
    v_ods_database := 'VESODS_' || :p_environment;

    -- Validate environment
    IF (:p_environment NOT IN ('DEV', 'TST', 'PRD')) THEN
        RETURN 'ERROR: Invalid environment. Must be DEV, TST, or PRD.';
    END IF;

    -- Build dynamic SQL
    v_merge_sql := '
    MERGE INTO ' || :v_dw_database || '.staging.stg_veteran_merged AS target
    USING (
        SELECT * FROM ' || :v_ods_database || '.VEMS_CORE.ods_vems_veteran
    ) AS source
    ON target.veteran_id = source.veteran_id
    -- ... rest of merge logic
    ';

    EXECUTE IMMEDIATE :v_merge_sql;

    RETURN 'Merge complete for environment: ' || :p_environment;
END;
$$;

-- Call with explicit environment
CALL sp_merge_veteran_to_staging_v2('DEV');   -- DEV environment
CALL sp_merge_veteran_to_staging_v2('TST');   -- TST environment
CALL sp_merge_veteran_to_staging_v2('PRD');   -- PRD environment
```

---

## Strategy 4: Deployment Scripts with Templates

### Overview

Use SQL templates with placeholders that get replaced during deployment.

### Implementation

#### Step 1: Create Template Files

**File: `dim_veteran_etl.template.sql`**
```sql
-- Template file with placeholders
-- Placeholders: {{DW_DATABASE}}, {{ODS_DATABASE}}, {{ENVIRONMENT}}

USE DATABASE {{DW_DATABASE}};
USE SCHEMA warehouse;

CREATE OR REPLACE PROCEDURE sp_load_dim_veteran()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INT DEFAULT 0;
BEGIN
    -- Environment: {{ENVIRONMENT}}
    -- Using database: {{DW_DATABASE}}

    INSERT INTO {{DW_DATABASE}}.warehouse.dim_veteran (
        veteran_natural_key,
        first_name,
        last_name,
        -- ... columns
    )
    SELECT
        veteran_id,
        first_name,
        last_name
        -- ... columns
    FROM {{DW_DATABASE}}.staging.stg_veteran_merged;

    RETURN 'Load complete for {{ENVIRONMENT}}';
END;
$$;
```

#### Step 2: Create Deployment Script (Bash)

**File: `deploy.sh`**
```bash
#!/bin/bash

# Deployment script for multi-environment
# Usage: ./deploy.sh DEV|TST|PRD

ENVIRONMENT=$1

if [ -z "$ENVIRONMENT" ]; then
    echo "Usage: ./deploy.sh {DEV|TST|PRD}"
    exit 1
fi

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(DEV|TST|PRD)$ ]]; then
    echo "Error: Environment must be DEV, TST, or PRD"
    exit 1
fi

# Set database names based on environment
DW_DATABASE="VESDW_${ENVIRONMENT}"
ODS_DATABASE="VESODS_${ENVIRONMENT}"

echo "═══════════════════════════════════════════════════"
echo "  Deploying to: $ENVIRONMENT"
echo "  DW Database: $DW_DATABASE"
echo "  ODS Database: $ODS_DATABASE"
echo "═══════════════════════════════════════════════════"

# Process all template files
for template_file in *.template.sql; do
    output_file="${template_file%.template.sql}.${ENVIRONMENT}.sql"

    echo "Processing: $template_file -> $output_file"

    # Replace placeholders
    sed -e "s/{{DW_DATABASE}}/${DW_DATABASE}/g" \
        -e "s/{{ODS_DATABASE}}/${ODS_DATABASE}/g" \
        -e "s/{{ENVIRONMENT}}/${ENVIRONMENT}/g" \
        "$template_file" > "$output_file"

    # Execute with Snowflake
    echo "Executing: $output_file"
    snowsql -f "$output_file"

    # Cleanup generated file (optional)
    # rm "$output_file"
done

echo "═══════════════════════════════════════════════════"
echo "  Deployment to $ENVIRONMENT complete!"
echo "═══════════════════════════════════════════════════"
```

#### Step 3: Create Python Deployment Script (Alternative)

**File: `deploy.py`**
```python
#!/usr/bin/env python3
"""
Multi-environment deployment script for VES Data Warehouse
Usage: python deploy.py --env DEV|TST|PRD --files dim_veteran_etl.template.sql
"""

import argparse
import os
import subprocess
from pathlib import Path

def replace_placeholders(template_content, environment):
    """Replace placeholders in template with environment-specific values"""
    replacements = {
        '{{ENVIRONMENT}}': environment,
        '{{DW_DATABASE}}': f'VESDW_{environment}',
        '{{ODS_DATABASE}}': f'VESODS_{environment}',
    }

    result = template_content
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)

    return result

def deploy_template(template_file, environment):
    """Process and deploy a single template file"""
    print(f"\n📄 Processing: {template_file}")

    # Read template
    with open(template_file, 'r') as f:
        template_content = f.read()

    # Replace placeholders
    sql_content = replace_placeholders(template_content, environment)

    # Generate output filename
    output_file = template_file.replace('.template.sql', f'.{environment}.sql')

    # Write processed SQL
    with open(output_file, 'w') as f:
        f.write(sql_content)

    print(f"✅ Generated: {output_file}")

    # Execute with SnowSQL
    print(f"🚀 Executing in {environment}...")
    result = subprocess.run(['snowsql', '-f', output_file],
                          capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✅ Deployment successful")
    else:
        print(f"❌ Deployment failed: {result.stderr}")
        return False

    # Optional: cleanup
    # os.remove(output_file)

    return True

def main():
    parser = argparse.ArgumentParser(description='Deploy VES DW to environment')
    parser.add_argument('--env', required=True, choices=['DEV', 'TST', 'PRD'],
                       help='Target environment')
    parser.add_argument('--files', nargs='+', required=True,
                       help='Template files to deploy')

    args = parser.parse_args()

    print("═" * 60)
    print(f"  VES Data Warehouse Deployment")
    print(f"  Environment: {args.env}")
    print(f"  Database: VESDW_{args.env}")
    print("═" * 60)

    # Deploy each template file
    success_count = 0
    for template_file in args.files:
        if deploy_template(template_file, args.env):
            success_count += 1

    print("\n" + "═" * 60)
    print(f"  Deployment Summary:")
    print(f"  Successful: {success_count}/{len(args.files)}")
    print("═" * 60)

if __name__ == '__main__':
    main()
```

---

## Recommended Approach

### Best Practice: Hybrid Strategy

Combine multiple strategies for maximum flexibility:

1. **Configuration Table** - For production stability
2. **Session Variables** - For interactive development
3. **Deployment Scripts** - For automated deployments

### Implementation Checklist

- [ ] Create `environment_config` table in each environment (DEV, TST, PRD)
- [ ] Create helper functions (`get_environment()`, `fn_get_dw_database()`, `fn_get_ods_database()`, etc.)
- [ ] Update all stored procedures to use helper functions
- [ ] Create deployment script templates
- [ ] Test deployment script in DEV
- [ ] Document deployment process
- [ ] Train team on new approach

---

## Implementation Guide

### Phase 1: Setup Configuration (1 hour)

#### Step 1: Create Config in Each Environment

```sql
-- Execute in VESDW_DEV
USE DATABASE VESDW_DEV;
USE SCHEMA metadata;
-- Run the environment_config table creation from Strategy 1
-- Set values: ENVIRONMENT_NAME='DEV', DW_DATABASE='VESDW_DEV', etc.

-- Execute in VESDW_TST
USE DATABASE VESDW_TST;
USE SCHEMA metadata;
-- Run the environment_config table creation
-- Set values: ENVIRONMENT_NAME='TST', DW_DATABASE='VESDW_TST', etc.

-- Execute in VESDW_PRD
USE DATABASE VESDW_PRD;
USE SCHEMA metadata;
-- Run the environment_config table creation
-- Set values: ENVIRONMENT_NAME='PRD', DW_DATABASE='VESDW_PRD', etc.
```

#### Step 2: Verify Configuration

```sql
-- Run in each environment to verify
SELECT
    config_key,
    config_value,
    description
FROM environment_config
ORDER BY config_key;

-- Should show different values in each environment
-- DEV: DW_DATABASE = 'VESDW_DEV'
-- TST: DW_DATABASE = 'VESDW_TST'
-- PRD: DW_DATABASE = 'VESDW_PRD'
```

### Phase 2: Convert Existing Procedures (2-3 hours)

#### Before (Hardcoded):
```sql
CREATE OR REPLACE PROCEDURE sp_merge_veteran_to_staging()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO VESDW_PRD.staging.stg_veteran_merged AS target
    USING (
        SELECT * FROM VESODS_PRD.VEMS_CORE.ods_vems_veteran
    ) AS source
    -- ... rest of logic
END;
$$;
```

#### After (Environment-Aware):
```sql
CREATE OR REPLACE PROCEDURE sp_merge_veteran_to_staging()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_dw_db VARCHAR;
    v_ods_db VARCHAR;
    v_sql VARCHAR;
BEGIN
    -- Get environment-specific databases
    SELECT config_value INTO :v_dw_db
    FROM environment_config WHERE config_key = 'DW_DATABASE';

    SELECT config_value INTO :v_ods_db
    FROM environment_config WHERE config_key = 'ODS_DATABASE';

    -- Build dynamic SQL
    v_sql := '
    MERGE INTO ' || :v_dw_db || '.staging.stg_veteran_merged AS target
    USING (
        SELECT * FROM ' || :v_ods_db || '.VEMS_CORE.ods_vems_veteran
    ) AS source
    ON target.veteran_id = source.veteran_id
    -- ... rest of logic
    ';

    EXECUTE IMMEDIATE :v_sql;

    RETURN 'Merge complete in ' || get_environment();
END;
$$;
```

### Phase 3: Create Deployment Automation (1 hour)

Create deployment scripts (choose one):
- Bash script (Linux/Mac)
- PowerShell script (Windows)
- Python script (cross-platform)

See Strategy 4 examples above.

---

## Deployment Process

### DEV → TST → PRD Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ STEP 1: Develop & Test in DEV                              │
│                                                              │
│  1. Develop code in VESDW_DEV                               │
│  2. Test with sample data                                   │
│  3. Run unit tests                                          │
│  4. Commit to Git (feature branch)                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STEP 2: Deploy to TST                                      │
│                                                              │
│  1. Merge feature branch to main                           │
│  2. Run deployment script:                                  │
│     ./deploy.sh TST                                         │
│  3. Execute integration tests                               │
│  4. UAT testing by business users                          │
│  5. Get sign-off                                            │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STEP 3: Deploy to PRD                                      │
│                                                              │
│  1. Create release tag in Git                              │
│  2. Schedule maintenance window                             │
│  3. Run deployment script:                                  │
│     ./deploy.sh PRD                                         │
│  4. Run smoke tests                                         │
│  5. Monitor for 24 hours                                    │
│  6. Update documentation                                    │
└─────────────────────────────────────────────────────────────┘
```

### Deployment Checklist

**Pre-Deployment**
- [ ] Code reviewed and approved
- [ ] All tests passing in DEV
- [ ] UAT completed in TST (for TST→PRD)
- [ ] Change request approved
- [ ] Backup verification complete
- [ ] Rollback plan documented

**During Deployment**
- [ ] Execute deployment script
- [ ] Monitor for errors
- [ ] Validate row counts
- [ ] Run smoke tests
- [ ] Check data quality scores

**Post-Deployment**
- [ ] All tasks running successfully
- [ ] No errors in task history
- [ ] Data freshness acceptable
- [ ] Send notification to stakeholders
- [ ] Update deployment log

---

## Testing Strategy

### Unit Tests (DEV)

```sql
-- Test: Verify environment configuration
SELECT
    CASE
        WHEN get_environment() = 'DEV' THEN 'PASS'
        ELSE 'FAIL: Expected DEV, got ' || get_environment()
    END AS test_result,
    'Environment check' AS test_name;

-- Test: Verify database names
SELECT
    CASE
        WHEN fn_get_dw_database() = 'VESDW_DEV' THEN 'PASS'
        ELSE 'FAIL: Expected VESDW_DEV, got ' || fn_get_dw_database()
    END AS test_result,
    'DW database check' AS test_name;
```

### Integration Tests (TST)

```sql
-- Test: End-to-end pipeline
CALL sp_merge_veteran_to_staging();
CALL sp_load_dim_veteran();

-- Verify results
SELECT COUNT(*) AS record_count
FROM IDENTIFIER(fn_get_dw_database() || '.warehouse.dim_veteran')
WHERE is_current = TRUE;
```

### Smoke Tests (PRD)

```sql
-- Quick validation after production deployment
-- Run in VESDW_PRD

-- 1. Verify environment
SELECT get_environment();  -- Should return 'PRD'

-- 2. Check row counts
SELECT
    'dim_veteran' AS table_name,
    COUNT(*) AS row_count
FROM warehouse.dim_veteran
WHERE is_current = TRUE;

-- 3. Check task status
SHOW TASKS;

-- 4. Check recent loads
SELECT * FROM metadata.vw_pipeline_health_dashboard
WHERE execution_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP());
```

---

## Best Practices

### ✅ DO

1. **Use Configuration Tables**
   - Single source of truth per environment
   - Easy to audit and update
   - Procedures automatically use correct environment

2. **Validate Environment**
   - Check environment name before destructive operations
   - Add safety checks in procedures
   - Use color coding in monitoring dashboards

3. **Automate Deployments**
   - Use scripts, not manual execution
   - Test deployment scripts in DEV first
   - Log all deployment activities

4. **Version Control Everything**
   - All SQL code in Git
   - Tag releases for production
   - Document changes in commit messages

5. **Test Thoroughly**
   - Unit tests in DEV
   - Integration tests in TST
   - Smoke tests in PRD
   - Never skip TST environment

### ❌ DON'T

1. **Don't Hardcode Database Names**
   - Use dynamic references
   - Use configuration tables or variables
   - Make code environment-agnostic

2. **Don't Deploy Directly to PRD**
   - Always go through TST first
   - Get sign-off before PRD deployment
   - Have rollback plan ready

3. **Don't Modify Production Data Manually**
   - All changes through scripts
   - All scripts version controlled
   - All changes auditable

4. **Don't Use Same Role for All Environments**
   - DEV_ROLE for DEV environment
   - TST_ROLE for TST environment
   - PRD_ROLE for PRD environment
   - Prevents accidental cross-environment access

5. **Don't Skip Documentation**
   - Document deployment process
   - Keep runbooks updated
   - Document rollback procedures

---

## Example: Complete Environment-Aware Procedure

```sql
-- Complete example showing all best practices
CREATE OR REPLACE PROCEDURE sp_load_dim_veteran_env_aware()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_environment VARCHAR;
    v_dw_database VARCHAR;
    v_rows_inserted INT DEFAULT 0;
    v_rows_updated INT DEFAULT 0;
    v_sql VARCHAR;
    v_result VARCHAR;
BEGIN
    -- Step 1: Get environment configuration
    SELECT config_value INTO :v_environment
    FROM environment_config WHERE config_key = 'ENVIRONMENT_NAME';

    SELECT config_value INTO :v_dw_database
    FROM environment_config WHERE config_key = 'DW_DATABASE';

    -- Step 2: Safety check for production
    IF (:v_environment = 'PRD') THEN
        -- Additional validation for production
        IF (CURRENT_TIME() NOT BETWEEN '22:00:00' AND '06:00:00') THEN
            RETURN 'ERROR: Production loads only allowed during maintenance window (10 PM - 6 AM)';
        END IF;
    END IF;

    -- Step 3: Log start of process
    INSERT INTO IDENTIFIER(:v_dw_database || '.metadata.etl_log') (
        procedure_name,
        environment,
        start_timestamp,
        status
    )
    VALUES (
        'sp_load_dim_veteran_env_aware',
        :v_environment,
        CURRENT_TIMESTAMP(),
        'RUNNING'
    );

    -- Step 4: Build and execute dynamic SQL
    v_sql := '
        INSERT INTO ' || :v_dw_database || '.warehouse.dim_veteran (
            veteran_natural_key,
            first_name,
            last_name,
            is_current,
            valid_from,
            valid_to
        )
        SELECT
            veteran_id,
            first_name,
            last_name,
            TRUE,
            CURRENT_TIMESTAMP(),
            NULL
        FROM ' || :v_dw_database || '.staging.stg_veteran_merged
    ';

    EXECUTE IMMEDIATE :v_sql;
    LET v_rows_inserted := SQLROWCOUNT;

    -- Step 5: Log completion
    UPDATE IDENTIFIER(:v_dw_database || '.metadata.etl_log')
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = 'SUCCESS',
        rows_processed = :v_rows_inserted
    WHERE procedure_name = 'sp_load_dim_veteran_env_aware'
      AND status = 'RUNNING';

    -- Step 6: Return result
    v_result := 'SUCCESS: Loaded ' || :v_rows_inserted || ' records in ' || :v_environment;
    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        -- Log error
        UPDATE IDENTIFIER(:v_dw_database || '.metadata.etl_log')
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = 'FAILED',
            error_message = SQLERRM
        WHERE procedure_name = 'sp_load_dim_veteran_env_aware'
          AND status = 'RUNNING';

        RETURN 'ERROR in ' || :v_environment || ': ' || SQLERRM;
END;
$$;

-- Test in each environment
CALL sp_load_dim_veteran_env_aware();
```

---

## Summary

### Key Takeaways

1. **Never hardcode database names** - Use configuration or variables
2. **Test in DEV, validate in TST, deploy to PRD** - No shortcuts
3. **Automate deployments** - Scripts reduce errors
4. **Configuration table** - Best for production stability
5. **Version control** - Track all changes

### Next Steps

1. Implement `environment_config` table in all environments
2. Create helper functions for database references
3. Convert existing procedures to use dynamic SQL
4. Create deployment scripts
5. Test full deployment cycle (DEV → TST → PRD)
6. Document your specific deployment process
7. Train team on new workflow

---

## Document Information

**Author:** Mark Chappell
**Version:** 1.0
**Last Updated:** 2024-11-18
**Related Documents:**
- [DIM_VETERAN_LOADING_GUIDE.md](DIM_VETERAN_LOADING_GUIDE.md) - Updated with environment-aware approach
- [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Includes deployment procedures

---

**You're now ready for professional, multi-environment deployments! 🚀**
