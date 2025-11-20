# Populating dim_veteran from Multiple Source Systems - Step-by-Step Guide

**A Beginner's Guide to Loading Veteran Dimensions from OMS and VEMS in Snowflake**

Author: Mark Chappell
Last Updated: 2024-11-18
Difficulty: Intermediate
Estimated Time: 2-3 hours for initial setup

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Multi-Environment Strategy](#multi-environment-strategy)
- [Architecture Overview](#architecture-overview)
- [Step 1: Understand Your Source Systems](#step-1-understand-your-source-systems)
- [Step 2: Set Up ODS Layer Tables](#step-2-set-up-ods-layer-tables)
- [Step 2a: Set Up Environment Configuration](#step-2a-set-up-environment-configuration)
- [Step 3: Create Streams for Change Data Capture](#step-3-create-streams-for-change-data-capture)
- [Step 4: Create Staging Layer with Merge Logic](#step-4-create-staging-layer-with-merge-logic)
- [Step 5: Create the dim_veteran Dimension Table](#step-5-create-the-dim_veteran-dimension-table)
- [Step 6: Create ETL Stored Procedure](#step-6-create-etl-stored-procedure)
- [Step 7: Set Up Orchestration with Tasks](#step-7-set-up-orchestration-with-tasks)
- [Step 8: Initial Load and Testing](#step-8-initial-load-and-testing)
- [Step 9: Monitor and Validate](#step-9-monitor-and-validate)
- [Troubleshooting Common Issues](#troubleshooting-common-issues)
- [Best Practices](#best-practices)

---

## Overview

### What You'll Learn

In this guide, you'll learn how to:
1. Extract veteran data from two source systems (OMS and VEMS)
2. Use Snowflake Streams to capture changes automatically
3. Merge data from multiple sources in a staging layer
4. Load the merged data into a dimension table with SCD Type 2 tracking
5. Automate the entire process with Snowflake Tasks

### What You'll Build

```
OMS (SQL Server)         VEMS (Salesforce)
     ↓                          ↓
     └──────→ [Mulesoft ETL] ←─┘
                   ↓
[ODS Layer: ods_oms_veteran] [ODS Layer: ods_vems_veteran]
     ↓                          ↓
[Stream: stream_oms_veteran] [Stream: stream_vems_veteran]
     ↓                          ↓
     └─────────→ [Staging: stg_veteran_merged] ←─────────┘
                         ↓
                 [dim_veteran with SCD Type 2]
                         ↓
                 [Automated with Tasks]
```

### Why This Approach?

- **Streams** = Automatic change detection (no need to track "last updated" dates)
- **Staging** = Clean data merge before loading to dimension
- **SCD Type 2** = Keep full history of changes to veteran records
- **Tasks** = Fully automated, no manual intervention needed

---

## Prerequisites

### Required Access

- [ ] Snowflake account with DATA_ENGINEER_ROLE (or ACCOUNTADMIN for initial setup)
- [ ] Access to create databases, schemas, tables, streams, and tasks
- [ ] Virtual warehouse for ETL work (e.g., `ETL_WH`)
- [ ] Ownership of databases: VESODS_PRDDATA_PRD, VESDW_PRD

**Role Strategy:**
- **ACCOUNTADMIN**: Used only for initial database creation
- **DATA_ENGINEER_ROLE**: Used for all development work (tables, procedures, tasks, streams)
- This follows the principle of least privilege for better security

### Required Knowledge

- Basic SQL (SELECT, INSERT, UPDATE)
- Basic understanding of data warehousing concepts
- No prior Snowflake experience required!

### Databases You'll Use

```sql
-- ODS database (source system replica)
VESODS_PRDDATA_PRD
  └── VEMS_CORE schema (VEMS data)
  └── OMS schema (OMS data)

-- Data warehouse database
VESDW_PRD (or VESDW_DEV, VESDW_TST depending on environment)
  └── staging schema (merged data)
  └── warehouse schema (dimensional model)
  └── metadata schema (monitoring)
```

---

## Multi-Environment Strategy

### Environment Overview

This guide supports deployment across three environments:

| Environment | DW Database | ODS Database | Purpose |
|-------------|-------------|--------------|---------|
| **DEV** | `VESDW_DEV` | `VESODS_DEV` | Development & unit testing |
| **TST** | `VESDW_TST` | `VESODS_TST` | Integration & UAT testing |
| **PRD** | `VESDW_PRD` | `VESODS_PRD` | Production |

### Key Principle: Environment-Agnostic Code

**❌ PROBLEM: Hardcoded database names**
```sql
-- This only works in PRD!
USE DATABASE VESDW_PRD;
SELECT * FROM VESDW_PRD.warehouse.dim_veteran;
```

**✅ SOLUTION: Environment-aware code**
```sql
-- This works in ANY environment!
USE DATABASE IDENTIFIER(get_dw_database());
SELECT * FROM IDENTIFIER(get_dw_database() || '.warehouse.dim_veteran');
```

### How This Guide Handles Environments

This guide uses **two approaches** for environment management:

1. **Configuration Table Approach** (Recommended for Production)
   - Create `environment_config` table in each environment
   - Store database names in configuration
   - Procedures automatically use correct environment
   - See [Step 2a](#step-2a-set-up-environment-configuration)

2. **Simplified Examples** (For Learning)
   - Examples show `VESDW_DEV` for clarity
   - In production, replace with dynamic references
   - See comments in code for production approach

### Quick Start by Environment

**If you're working in DEV:**
- Replace `VESDW_PRD` with `VESDW_DEV` in examples
- Replace `VESODS_PRD` with `VESODS_DEV` in examples

**If you're working in TST:**
- Replace `VESDW_PRD` with `VESDW_TST` in examples
- Replace `VESODS_PRD` with `VESODS_TST` in examples

**For Production-Ready Code:**
- Follow [Step 2a](#step-2a-set-up-environment-configuration) to set up environment configuration
- Use the "Production Version" code snippets provided throughout

### Complete Multi-Environment Guide

For comprehensive coverage of multi-environment deployments, including:
- Automated deployment scripts
- Environment promotion (DEV → TST → PRD)
- Testing strategies
- Deployment checklists

See: **[MULTI_ENVIRONMENT_DEPLOYMENT_GUIDE.md](MULTI_ENVIRONMENT_DEPLOYMENT_GUIDE.md)**

---

## Architecture Overview

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 1: SOURCE SYSTEMS                                              │
│                                                                      │
│  OMS (SQL Server)                  VEMS Core (Salesforce)           │
│  - Operations Management System    - Veterans Exam Management System│
│  - Has: veteran_id, name,         - Has: veteran_ssn, demographics, │
│    contact info                     service history                 │
│  - Data moved via Mulesoft        - Data moved via Mulesoft         │
└─────────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 2: ODS LAYER (Operational Data Store)                         │
│                                                                      │
│  VESODS_PRDDATA_PRD.OMS.ods_oms_veteran                            │
│  VESODS_PRDDATA_PRD.VEMS_CORE.ods_vems_veteran                     │
│                                                                      │
│  Purpose: Replicate source data as-is, minimal transformation       │
│  Load: Scheduled extraction (e.g., hourly via external ETL tool)   │
└─────────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 3: STREAMS (Change Data Capture)                              │
│                                                                      │
│  stream_oms_veteran  → Tracks changes in ods_oms_veteran           │
│  stream_vems_veteran → Tracks changes in ods_vems_veteran          │
│                                                                      │
│  Purpose: Automatically capture INSERTs, UPDATEs, DELETEs          │
│  Benefit: Only process changed records (incremental loading)        │
└─────────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 4: STAGING LAYER (Merge & Cleanse)                            │
│                                                                      │
│  VESDW_PRD.staging.stg_veteran_merged                               │
│                                                                      │
│  Purpose: Merge OMS and VEMS data, resolve conflicts, cleanse      │
│  Logic: VEMS is "source of truth" for demographics,                │
│         OMS provides contact info if missing in VEMS                │
└─────────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 5: DIMENSION TABLE (SCD Type 2)                               │
│                                                                      │
│  VESDW_PRD.warehouse.dim_veteran                                    │
│                                                                      │
│  Purpose: Final dimensional model with historical tracking          │
│  Features: SCD Type 2 (is_current, valid_from, valid_to)           │
└─────────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 6: ORCHESTRATION (Automated Tasks)                            │
│                                                                      │
│  task_hourly_dim_veteran_load                                       │
│                                                                      │
│  Purpose: Run ETL automatically every hour                          │
│  Trigger: CRON schedule + stream has data                           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Understand Your Source Systems

### Source System 1: OMS (Operations Management System)

**Location:** SQL Server database
**What it contains:** Basic veteran information from appointment scheduling (includes Core and PNM data)
**Data Transfer:** Mulesoft ETL process moves data to Snowflake ODS

**Key fields:**
- `veteran_id` - Unique identifier (business key)
- `first_name`, `last_name` - Name
- `phone`, `email` - Contact information
- `address`, `city`, `state`, `zip` - Address
- `last_updated_date` - When record was last modified

**Example data:**
```sql
veteran_id | first_name | last_name | phone          | email
-----------|------------|-----------|----------------|------------------
V123456    | John       | Smith     | 555-123-4567   | john@email.com
V789012    | Jane       | Doe       | 555-987-6543   | jane@email.com
```

### Source System 2: VEMS (Veterans Exam Management System)

**Location:** SQL Server database
**What it contains:** Comprehensive veteran demographics and service history

**Key fields:**
- `veteran_ssn` - Social Security Number (unique identifier)
- `veteran_id` - Also has veteran_id for cross-reference
- `first_name`, `last_name`, `date_of_birth`
- `gender`, `ethnicity`, `race`
- `branch_of_service`, `service_start_date`, `service_end_date`
- `disability_rating_percentage`, `priority_group`
- `last_updated_timestamp`

**Example data:**
```sql
veteran_ssn | veteran_id | first_name | disability_rating | priority_group
------------|------------|------------|-------------------|---------------
123-45-6789 | V123456    | John       | 70                | 2
987-65-4321 | V789012    | Jane       | 50                | 3
```

### Why Two Sources?

- **VEMS** is the "source of truth" for veteran demographics and service history (authoritative)
- **OMS** has more up-to-date contact information (veterans update when scheduling appointments)
- **Strategy:** Use VEMS as primary, fill gaps with OMS data

---

## Step 2: Set Up ODS Layer Tables

### What is the ODS Layer?

The **Operational Data Store (ODS)** is a staging area that replicates source system data "as-is" into Snowflake. Think of it as a landing zone before any transformation.

**Why ODS?**
- Isolates source systems (they don't need Snowflake access)
- Data is extracted by external ETL tools (Fivetran, Airbyte, custom scripts)
- Provides a stable snapshot for Snowflake processing

### Step 2.1: Create ODS Database and Schemas

```sql
-- Connect to Snowflake (use ACCOUNTADMIN for database creation)
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ETL_WH;

-- Create ODS database if it doesn't exist
CREATE DATABASE IF NOT EXISTS VESODS_PRDDATA_PRD
    COMMENT = 'Operational Data Store - Replica of source systems';

-- Grant ownership to DATA_ENGINEER_ROLE
GRANT OWNERSHIP ON DATABASE VESODS_PRDDATA_PRD TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON DATABASE VESODS_PRDDATA_PRD TO ROLE DATA_ENGINEER_ROLE;

-- Switch to DATA_ENGINEER_ROLE for all subsequent operations
USE ROLE DATA_ENGINEER_ROLE;

-- Create schemas for each source system
CREATE SCHEMA IF NOT EXISTS VESODS_PRDDATA_PRD.OMS
    COMMENT = 'OMS (Operations Management System) data';

CREATE SCHEMA IF NOT EXISTS VESODS_PRDDATA_PRD.VEMS_CORE
    COMMENT = 'VEMS Core system data';

-- Verify creation
SHOW DATABASES LIKE 'VESODS_PRDDATA_PRD';
SHOW SCHEMAS IN DATABASE VESODS_PRDDATA_PRD;
```

**What you should see:**
```
Database: VESODS_PRDDATA_PRD
Schemas:
  - OMS
  - VEMS_CORE
  - INFORMATION_SCHEMA
  - PUBLIC
```

### Step 2.2: Create ODS Table for OMS

```sql
-- Switch to ODS database
USE DATABASE VESODS_PRDDATA_PRD;
USE SCHEMA OMS;

-- Create ODS table for OMS veteran data
CREATE OR REPLACE TABLE ods_oms_veteran (
    -- Business keys
    veteran_id VARCHAR(50) NOT NULL,

    -- Name fields
    first_name VARCHAR(50),
    middle_name VARCHAR(50),
    last_name VARCHAR(100),

    -- Contact information
    phone_primary VARCHAR(20),
    email VARCHAR(200),

    -- Address
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),

    -- Metadata
    source_system VARCHAR(50) DEFAULT 'OMS',
    last_updated_date TIMESTAMP_NTZ,

    -- ODS audit fields
    ods_inserted_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ods_updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'ODS replica of OMS veteran data - loaded via external ETL';

-- Create index on business key for faster lookups
-- Note: Snowflake doesn't have traditional indexes, but we can cluster
ALTER TABLE ods_oms_veteran CLUSTER BY (veteran_id);
```

**Explanation:**
- `veteran_id` - Business key from source system
- `ods_inserted_timestamp` - When record first appeared in ODS
- `ods_updated_timestamp` - When record was last updated in ODS
- `CLUSTER BY` - Snowflake's way of organizing data for faster queries

### Step 2.3: Create ODS Table for VEMS

```sql
-- Switch to VEMS schema
USE SCHEMA VEMS_CORE;

-- Create ODS table for VEMS veteran data
CREATE OR REPLACE TABLE ods_vems_veteran (
    -- Business keys
    veteran_ssn VARCHAR(11),  -- Encrypted in production!
    veteran_id VARCHAR(50) NOT NULL,

    -- Name fields
    first_name VARCHAR(50),
    middle_name VARCHAR(50),
    last_name VARCHAR(100),
    suffix VARCHAR(10),

    -- Demographics
    date_of_birth DATE,
    gender VARCHAR(20),
    ethnicity VARCHAR(50),
    race VARCHAR(50),
    marital_status VARCHAR(20),

    -- Military service
    branch_of_service VARCHAR(50),
    service_start_date DATE,
    service_end_date DATE,
    discharge_status VARCHAR(50),
    combat_veteran_flag BOOLEAN,

    -- VA benefits
    service_connected_disability_flag BOOLEAN,
    disability_rating_percentage NUMBER(3,0),
    priority_group NUMBER(1,0),
    enrollment_date DATE,

    -- Address (may be less current than OMS)
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR(100),

    -- Contact
    phone_primary VARCHAR(20),
    phone_secondary VARCHAR(20),
    email VARCHAR(200),
    preferred_contact_method VARCHAR(20),

    -- Status flags
    active_duty_status VARCHAR(20),
    homeless_flag BOOLEAN,
    rural_flag BOOLEAN,

    -- Metadata
    source_system VARCHAR(50) DEFAULT 'VEMS_CORE',
    last_updated_timestamp TIMESTAMP_NTZ,

    -- ODS audit fields
    ods_inserted_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ods_updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'ODS replica of VEMS veteran data - loaded via external ETL';

-- Cluster by both keys for flexible queries
ALTER TABLE ods_vems_veteran CLUSTER BY (veteran_id, veteran_ssn);
```

### Step 2.4: Load Initial Data into ODS

**Note:** In production, this is typically done by an external ETL tool (Fivetran, Airbyte, or custom scripts). For this example, we'll insert sample data manually.

```sql
-- Insert sample OMS data
USE SCHEMA OMS;

INSERT INTO ods_oms_veteran (
    veteran_id, first_name, middle_name, last_name,
    phone_primary, email,
    address_line1, city, state, zip_code,
    last_updated_date
)
VALUES
    ('V123456', 'John', 'M', 'Smith', '555-123-4567', 'john.smith@email.com',
     '123 Main St', 'Buffalo', 'NY', '14201', '2024-01-15 10:30:00'),
    ('V789012', 'Jane', 'A', 'Doe', '555-987-6543', 'jane.doe@email.com',
     '456 Oak Ave', 'Albany', 'NY', '12203', '2024-01-16 14:20:00'),
    ('V345678', 'Robert', 'L', 'Johnson', '555-222-3333', 'robert.j@email.com',
     '789 Pine Rd', 'Syracuse', 'NY', '13202', '2024-01-17 09:15:00');

-- Verify
SELECT COUNT(*) AS oms_count FROM ods_oms_veteran;
SELECT * FROM ods_oms_veteran LIMIT 5;
```

```sql
-- Insert sample VEMS data
USE SCHEMA VEMS_CORE;

INSERT INTO ods_vems_veteran (
    veteran_ssn, veteran_id, first_name, middle_name, last_name,
    date_of_birth, gender, branch_of_service,
    service_start_date, service_end_date, discharge_status,
    disability_rating_percentage, priority_group,
    address_line1, city, state, zip_code,
    last_updated_timestamp
)
VALUES
    ('123-45-6789', 'V123456', 'John', 'M', 'Smith',
     '1975-03-20', 'Male', 'Army',
     '1995-06-01', '2015-06-01', 'Honorable',
     70, 2,
     '123 Main St', 'Buffalo', 'NY', '14201',
     '2024-01-15 11:00:00'),
    ('987-65-4321', 'V789012', 'Jane', 'A', 'Doe',
     '1980-07-15', 'Female', 'Navy',
     '2000-09-01', '2020-09-01', 'Honorable',
     50, 3,
     '456 Oak Ave', 'Albany', 'NY', '12203',
     '2024-01-16 15:00:00'),
    ('555-66-7777', 'V345678', 'Robert', 'L', 'Johnson',
     '1978-11-10', 'Male', 'Marines',
     '1998-01-01', '2018-01-01', 'Honorable',
     90, 1,
     '789 Pine Rd', 'Syracuse', 'NY', '13202',
     '2024-01-17 10:00:00');

-- Verify
SELECT COUNT(*) AS vems_count FROM ods_vems_veteran;
SELECT * FROM ods_vems_veteran LIMIT 5;
```

**Checkpoint:** You should now have data in both ODS tables.

---

## Step 2a: Set Up Environment Configuration

### Why Environment Configuration?

Before proceeding, we'll set up environment configuration to make our code work seamlessly across DEV, TST, and PRD environments without hardcoding database names.

### Step 2a.1: Create Environment Configuration Table

Run this in **each environment** (VESDW_DEV, VESDW_TST, VESDW_PRD):

```sql
-- Switch to the appropriate database for your environment
USE ROLE DATA_ENGINEER_ROLE;
USE DATABASE VESDW_DEV;  -- Change to VESDW_TST or VESDW_PRD as needed
USE SCHEMA metadata;

-- Create environment configuration table
CREATE TABLE IF NOT EXISTS environment_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(500),
    description VARCHAR(1000),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- FOR DEV ENVIRONMENT: Insert these values
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MERGE INTO environment_config AS target
USING (
    SELECT 'ENVIRONMENT_NAME' AS config_key, 'DEV' AS config_value,
           'Current environment: DEV, TST, or PRD' AS description
    UNION ALL
    SELECT 'DW_DATABASE', 'VESDW_DEV',
           'Data warehouse database name'
    UNION ALL
    SELECT 'ODS_DATABASE', 'VESODS_DEV',
           'Operational data store database name'
) AS source
ON target.config_key = source.config_key
WHEN MATCHED THEN UPDATE SET
    config_value = source.config_value,
    description = source.description,
    last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (config_key, config_value, description)
    VALUES (source.config_key, source.config_value, source.description);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- FOR TST ENVIRONMENT: Use these values instead
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/*
MERGE INTO environment_config AS target
USING (
    SELECT 'ENVIRONMENT_NAME' AS config_key, 'TST' AS config_value,
           'Current environment: DEV, TST, or PRD' AS description
    UNION ALL
    SELECT 'DW_DATABASE', 'VESDW_TST',
           'Data warehouse database name'
    UNION ALL
    SELECT 'ODS_DATABASE', 'VESODS_TST',
           'Operational data store database name'
) AS source
ON target.config_key = source.config_key
WHEN MATCHED THEN UPDATE SET config_value = source.config_value
WHEN NOT MATCHED THEN INSERT (config_key, config_value, description)
    VALUES (source.config_key, source.config_value, source.description);
*/

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- FOR PRD ENVIRONMENT: Use these values instead
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/*
MERGE INTO environment_config AS target
USING (
    SELECT 'ENVIRONMENT_NAME' AS config_key, 'PRD' AS config_value,
           'Current environment: DEV, TST, or PRD' AS description
    UNION ALL
    SELECT 'DW_DATABASE', 'VESDW_PRD',
           'Data warehouse database name'
    UNION ALL
    SELECT 'ODS_DATABASE', 'VESODS_PRD',
           'Operational data store database name'
) AS source
ON target.config_key = source.config_key
WHEN MATCHED THEN UPDATE SET config_value = source.config_value
WHEN NOT MATCHED THEN INSERT (config_key, config_value, description)
    VALUES (source.config_key, source.config_value, source.description);
*/

-- Verify configuration
SELECT * FROM environment_config ORDER BY config_key;
```

### Step 2a.2: Create Helper Functions

```sql
-- Function to get current environment name
CREATE OR REPLACE FUNCTION get_environment()
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'ENVIRONMENT_NAME'
$$;

-- Function to get DW database name
CREATE OR REPLACE FUNCTION get_dw_database()
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'DW_DATABASE'
$$;

-- Function to get ODS database name
CREATE OR REPLACE FUNCTION get_ods_database()
RETURNS VARCHAR
AS
$$
    SELECT config_value
    FROM environment_config
    WHERE config_key = 'ODS_DATABASE'
$$;

-- Test the functions
SELECT
    get_environment() AS current_environment,
    get_dw_database() AS dw_database,
    get_ods_database() AS ods_database;
```

**Expected Output (in DEV):**
```
current_environment | dw_database | ods_database
--------------------|-------------|-------------
DEV                 | VESDW_DEV   | VESODS_DEV
```

**Expected Output (in TST):**
```
current_environment | dw_database | ods_database
--------------------|-------------|-------------
TST                 | VESDW_TST   | VESODS_TST
```

**Expected Output (in PRD):**
```
current_environment | dw_database | ods_database
--------------------|-------------|-------------
PRD                 | VESDW_PRD   | VESODS_PRD
```

### Using Environment Configuration

From this point forward, you have two options for writing SQL:

**Option 1: Simplified (for learning/examples)**
```sql
-- Hardcoded - easier to read, but must be changed per environment
USE DATABASE VESDW_DEV;
SELECT * FROM VESDW_DEV.warehouse.dim_veteran;
```

**Option 2: Environment-Aware (production-ready)**
```sql
-- Dynamic - works in any environment!
USE DATABASE IDENTIFIER(get_dw_database());
SELECT * FROM IDENTIFIER(get_dw_database() || '.warehouse.dim_veteran');
```

**For the rest of this guide:**
- Examples show simplified code for clarity (e.g., `VESDW_DEV`)
- Production versions are noted with "🏭 Production Version" comments
- When deploying to TST or PRD, use environment-aware code

**Checkpoint:** Environment configuration is now set up!

---

## Step 3: Create Streams for Change Data Capture

### What are Snowflake Streams?

Streams are Snowflake's **Change Data Capture (CDC)** mechanism. They automatically track:
- New rows (INSERTs)
- Updated rows (UPDATEs)
- Deleted rows (DELETEs)

**Magic of Streams:**
- No need to track "last updated" timestamps manually
- Only process changed records (incremental loading)
- Automatically managed by Snowflake
- Near real-time change detection

### How Streams Work

```
Original Table          Stream               What You See in Stream
──────────────         ──────────           ────────────────────────
[Insert Row A]    →   Captures change   →  Row A, METADATA$ACTION='INSERT'
[Update Row A]    →   Captures change   →  Row A (old), METADATA$ACTION='DELETE'
                                            Row A (new), METADATA$ACTION='INSERT'
[Delete Row A]    →   Captures change   →  Row A, METADATA$ACTION='DELETE'
```

**Key Metadata Columns:**
- `METADATA$ACTION` - INSERT, DELETE (UPDATE = DELETE + INSERT)
- `METADATA$ISUPDATE` - TRUE if part of an UPDATE
- `METADATA$ROW_ID` - Unique identifier for the change

### Step 3.1: Create Stream on OMS Table

```sql
USE DATABASE VESODS_PRDDATA_PRD;
USE SCHEMA OMS;

-- Create stream to track changes in ods_oms_veteran
CREATE OR REPLACE STREAM stream_oms_veteran
    ON TABLE ods_oms_veteran
    COMMENT = 'Captures all changes (INSERTs, UPDATEs, DELETEs) in OMS veteran data';

-- Verify stream creation
SHOW STREAMS IN SCHEMA OMS;
DESC STREAM stream_oms_veteran;
```

**What you should see:**
```
name: stream_oms_veteran
database_name: VESODS_PRDDATA_PRD
schema_name: OMS
owner: DATA_ENGINEER_ROLE
comment: Captures all changes...
table_name: ods_oms_veteran
type: DELTA
stale: false
mode: DEFAULT
```

### Step 3.2: Create Stream on VEMS Table

```sql
USE SCHEMA VEMS_CORE;

-- Create stream to track changes in ods_vems_veteran
CREATE OR REPLACE STREAM stream_vems_veteran
    ON TABLE ods_vems_veteran
    COMMENT = 'Captures all changes (INSERTs, UPDATEs, DELETEs) in VEMS veteran data';

-- Verify stream creation
SHOW STREAMS IN SCHEMA VEMS_CORE;
DESC STREAM stream_vems_veteran;
```

### Step 3.3: Test Streams (Understanding How They Work)

Let's test to understand streams before using them in production.

```sql
-- Check what's in the OMS stream (should show all initial records)
USE SCHEMA OMS;
SELECT
    veteran_id,
    first_name,
    last_name,
    METADATA$ACTION AS action_type,
    METADATA$ISUPDATE AS is_update
FROM stream_oms_veteran
LIMIT 10;
```

**Expected output:**
```
veteran_id | first_name | last_name | action_type | is_update
-----------|------------|-----------|-------------|----------
V123456    | John       | Smith     | INSERT      | FALSE
V789012    | Jane       | Doe       | INSERT      | FALSE
V345678    | Robert     | Johnson   | INSERT      | FALSE
```

Now let's make a change to see how the stream captures it:

```sql
-- Update a veteran's phone number in OMS
UPDATE ods_oms_veteran
SET phone_primary = '555-999-8888',
    ods_updated_timestamp = CURRENT_TIMESTAMP()
WHERE veteran_id = 'V123456';

-- Check the stream again
SELECT
    veteran_id,
    first_name,
    phone_primary,
    METADATA$ACTION AS action_type,
    METADATA$ISUPDATE AS is_update
FROM stream_oms_veteran
WHERE veteran_id = 'V123456';
```

**Expected output (UPDATE appears as DELETE + INSERT):**
```
veteran_id | first_name | phone_primary  | action_type | is_update
-----------|------------|----------------|-------------|----------
V123456    | John       | 555-123-4567   | DELETE      | TRUE      <- Old value
V123456    | John       | 555-999-8888   | INSERT      | TRUE      <- New value
```

**Important:** Stream data is consumed after reading! For now, let's reset:

```sql
-- Recreate streams to start fresh
CREATE OR REPLACE STREAM stream_oms_veteran ON TABLE ods_oms_veteran;
CREATE OR REPLACE STREAM VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran
    ON TABLE VESODS_PRDDATA_PRD.VEMS_CORE.ods_vems_veteran;
```

---

## Step 4: Create Staging Layer with Merge Logic

### What is the Staging Layer?

The staging layer is where we **merge data from multiple sources** before loading into dimensions.

**Why Staging?**
- Combine OMS and VEMS data intelligently
- Resolve conflicts (which source wins?)
- Cleanse and standardize data
- Business rules applied here

### Step 4.1: Create Staging Database Schema

```sql
-- Create/use the data warehouse database (ACCOUNTADMIN for DB creation)
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS VESDW_PRD
    COMMENT = 'VES Data Warehouse - Production';

-- Grant ownership to DATA_ENGINEER_ROLE
GRANT OWNERSHIP ON DATABASE VESDW_PRD TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON DATABASE VESDW_PRD TO ROLE DATA_ENGINEER_ROLE;

-- Switch to DATA_ENGINEER_ROLE for all subsequent operations
USE ROLE DATA_ENGINEER_ROLE;

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS VESDW_PRD.staging
    COMMENT = 'Staging layer for multi-source merge and cleansing';

USE DATABASE VESDW_PRD;
USE SCHEMA staging;
```

### Step 4.2: Create Staging Table

```sql
-- Create staging table that will hold merged veteran data
CREATE OR REPLACE TABLE stg_veteran_merged (
    -- Business key (from either system)
    veteran_id VARCHAR(50) NOT NULL,

    -- From VEMS (authoritative)
    veteran_ssn VARCHAR(11),

    -- Name (prefer VEMS, fallback to OMS)
    first_name VARCHAR(50),
    middle_name VARCHAR(50),
    last_name VARCHAR(100),
    suffix VARCHAR(10),

    -- Demographics (VEMS only)
    date_of_birth DATE,
    gender VARCHAR(20),
    ethnicity VARCHAR(50),
    race VARCHAR(50),
    marital_status VARCHAR(20),

    -- Military service (VEMS only)
    branch_of_service VARCHAR(50),
    service_start_date DATE,
    service_end_date DATE,
    discharge_status VARCHAR(50),
    combat_veteran_flag BOOLEAN,

    -- VA benefits (VEMS only)
    service_connected_disability_flag BOOLEAN,
    disability_rating_percentage NUMBER(3,0),
    priority_group NUMBER(1,0),
    enrollment_date DATE,

    -- Address (prefer OMS for recency, fallback to VEMS)
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR(100),

    -- Contact (prefer OMS for recency, fallback to VEMS)
    phone_primary VARCHAR(20),
    phone_secondary VARCHAR(20),
    email VARCHAR(200),
    preferred_contact_method VARCHAR(20),

    -- Status (VEMS only)
    active_duty_status VARCHAR(20),
    homeless_flag BOOLEAN,
    rural_flag BOOLEAN,

    -- Source tracking
    source_system VARCHAR(50),  -- 'VEMS_CORE', 'OMS', or 'BOTH'
    vems_last_updated TIMESTAMP_NTZ,
    oms_last_updated TIMESTAMP_NTZ,

    -- Staging metadata
    staging_loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    staging_updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging table with merged veteran data from OMS and VEMS';

-- Cluster for performance
ALTER TABLE stg_veteran_merged CLUSTER BY (veteran_id);
```

### Step 4.3: Create Merge Procedure

This is where the magic happens! We'll merge OMS and VEMS data with intelligent conflict resolution.

```sql
-- Create procedure to merge OMS and VEMS data into staging
CREATE OR REPLACE PROCEDURE sp_merge_veteran_to_staging()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_oms_records INT;
    v_vems_records INT;
    v_merged_records INT;
BEGIN
    -- Count incoming records
    SELECT COUNT(*) INTO :v_oms_records
    FROM VESODS_PRDDATA_PRD.OMS.stream_oms_veteran;

    SELECT COUNT(*) INTO :v_vems_records
    FROM VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran;

    -- Merge logic:
    -- 1. Start with VEMS as the base (source of truth)
    -- 2. Overlay OMS contact info if more recent
    -- 3. Handle veterans that exist in only one system

    MERGE INTO stg_veteran_merged AS target
    USING (
        -- Combined view of both systems
        WITH vems_changes AS (
            SELECT
                veteran_id,
                veteran_ssn,
                first_name,
                middle_name,
                last_name,
                suffix,
                date_of_birth,
                gender,
                ethnicity,
                race,
                marital_status,
                branch_of_service,
                service_start_date,
                service_end_date,
                discharge_status,
                combat_veteran_flag,
                service_connected_disability_flag,
                disability_rating_percentage,
                priority_group,
                enrollment_date,
                address_line1 AS vems_address_line1,
                address_line2 AS vems_address_line2,
                city AS vems_city,
                state AS vems_state,
                zip_code AS vems_zip_code,
                county,
                phone_primary AS vems_phone_primary,
                phone_secondary AS vems_phone_secondary,
                email AS vems_email,
                preferred_contact_method,
                active_duty_status,
                homeless_flag,
                rural_flag,
                last_updated_timestamp AS vems_last_updated,
                METADATA$ACTION AS vems_action,
                METADATA$ISUPDATE AS vems_is_update
            FROM VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran
            WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
               OR METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE
        ),
        oms_changes AS (
            SELECT
                veteran_id,
                first_name AS oms_first_name,
                middle_name AS oms_middle_name,
                last_name AS oms_last_name,
                phone_primary AS oms_phone_primary,
                email AS oms_email,
                address_line1 AS oms_address_line1,
                address_line2 AS oms_address_line2,
                city AS oms_city,
                state AS oms_state,
                zip_code AS oms_zip_code,
                last_updated_date AS oms_last_updated,
                METADATA$ACTION AS oms_action
            FROM VESODS_PRDDATA_PRD.OMS.stream_oms_veteran
            WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
               OR METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE
        )
        -- Full outer join to get veterans from both systems
        SELECT
            COALESCE(v.veteran_id, o.veteran_id) AS veteran_id,

            -- VEMS fields (authoritative)
            v.veteran_ssn,
            COALESCE(v.first_name, o.oms_first_name) AS first_name,
            COALESCE(v.middle_name, o.oms_middle_name) AS middle_name,
            COALESCE(v.last_name, o.oms_last_name) AS last_name,
            v.suffix,
            v.date_of_birth,
            v.gender,
            v.ethnicity,
            v.race,
            v.marital_status,
            v.branch_of_service,
            v.service_start_date,
            v.service_end_date,
            v.discharge_status,
            v.combat_veteran_flag,
            v.service_connected_disability_flag,
            v.disability_rating_percentage,
            v.priority_group,
            v.enrollment_date,

            -- Address: Use OMS if more recent or VEMS missing, else use VEMS
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_address_line1 IS NULL
                THEN o.oms_address_line1
                ELSE v.vems_address_line1
            END AS address_line1,
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_address_line2 IS NULL
                THEN o.oms_address_line2
                ELSE v.vems_address_line2
            END AS address_line2,
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_city IS NULL
                THEN o.oms_city
                ELSE v.vems_city
            END AS city,
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_state IS NULL
                THEN o.oms_state
                ELSE v.vems_state
            END AS state,
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_zip_code IS NULL
                THEN o.oms_zip_code
                ELSE v.vems_zip_code
            END AS zip_code,
            v.county,

            -- Contact: Use OMS if more recent or VEMS missing
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_phone_primary IS NULL
                THEN o.oms_phone_primary
                ELSE v.vems_phone_primary
            END AS phone_primary,
            v.vems_phone_secondary AS phone_secondary,
            CASE
                WHEN o.oms_last_updated > v.vems_last_updated
                  OR v.vems_email IS NULL
                THEN o.oms_email
                ELSE v.vems_email
            END AS email,
            v.preferred_contact_method,

            -- Status
            v.active_duty_status,
            v.homeless_flag,
            v.rural_flag,

            -- Source tracking
            CASE
                WHEN v.veteran_id IS NOT NULL AND o.veteran_id IS NOT NULL THEN 'BOTH'
                WHEN v.veteran_id IS NOT NULL THEN 'VEMS_CORE'
                ELSE 'OMS'
            END AS source_system,
            v.vems_last_updated,
            o.oms_last_updated
        FROM vems_changes v
        FULL OUTER JOIN oms_changes o ON v.veteran_id = o.veteran_id
    ) AS source
    ON target.veteran_id = source.veteran_id

    -- When matched, update if data changed
    WHEN MATCHED THEN UPDATE SET
        veteran_ssn = source.veteran_ssn,
        first_name = source.first_name,
        middle_name = source.middle_name,
        last_name = source.last_name,
        suffix = source.suffix,
        date_of_birth = source.date_of_birth,
        gender = source.gender,
        ethnicity = source.ethnicity,
        race = source.race,
        marital_status = source.marital_status,
        branch_of_service = source.branch_of_service,
        service_start_date = source.service_start_date,
        service_end_date = source.service_end_date,
        discharge_status = source.discharge_status,
        combat_veteran_flag = source.combat_veteran_flag,
        service_connected_disability_flag = source.service_connected_disability_flag,
        disability_rating_percentage = source.disability_rating_percentage,
        priority_group = source.priority_group,
        enrollment_date = source.enrollment_date,
        address_line1 = source.address_line1,
        address_line2 = source.address_line2,
        city = source.city,
        state = source.state,
        zip_code = source.zip_code,
        county = source.county,
        phone_primary = source.phone_primary,
        phone_secondary = source.phone_secondary,
        email = source.email,
        preferred_contact_method = source.preferred_contact_method,
        active_duty_status = source.active_duty_status,
        homeless_flag = source.homeless_flag,
        rural_flag = source.rural_flag,
        source_system = source.source_system,
        vems_last_updated = source.vems_last_updated,
        oms_last_updated = source.oms_last_updated,
        staging_updated_timestamp = CURRENT_TIMESTAMP()

    -- When not matched, insert new record
    WHEN NOT MATCHED THEN INSERT (
        veteran_id, veteran_ssn,
        first_name, middle_name, last_name, suffix,
        date_of_birth, gender, ethnicity, race, marital_status,
        branch_of_service, service_start_date, service_end_date,
        discharge_status, combat_veteran_flag,
        service_connected_disability_flag, disability_rating_percentage,
        priority_group, enrollment_date,
        address_line1, address_line2, city, state, zip_code, county,
        phone_primary, phone_secondary, email, preferred_contact_method,
        active_duty_status, homeless_flag, rural_flag,
        source_system, vems_last_updated, oms_last_updated,
        staging_loaded_timestamp, staging_updated_timestamp
    )
    VALUES (
        source.veteran_id, source.veteran_ssn,
        source.first_name, source.middle_name, source.last_name, source.suffix,
        source.date_of_birth, source.gender, source.ethnicity, source.race,
        source.marital_status,
        source.branch_of_service, source.service_start_date, source.service_end_date,
        source.discharge_status, source.combat_veteran_flag,
        source.service_connected_disability_flag, source.disability_rating_percentage,
        source.priority_group, source.enrollment_date,
        source.address_line1, source.address_line2, source.city, source.state,
        source.zip_code, source.county,
        source.phone_primary, source.phone_secondary, source.email,
        source.preferred_contact_method,
        source.active_duty_status, source.homeless_flag, source.rural_flag,
        source.source_system, source.vems_last_updated, source.oms_last_updated,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    SELECT COUNT(*) INTO :v_merged_records FROM stg_veteran_merged;

    RETURN 'Merge complete. OMS changes: ' || :v_oms_records ||
           ', VEMS changes: ' || :v_vems_records ||
           ', Total staging records: ' || :v_merged_records;
END;
$$;
```

### Step 4.4: Test the Merge Procedure

```sql
-- Run the merge procedure
CALL sp_merge_veteran_to_staging();

-- Check the staging table
SELECT
    veteran_id,
    first_name,
    last_name,
    disability_rating_percentage,
    phone_primary,
    source_system,
    vems_last_updated,
    oms_last_updated
FROM stg_veteran_merged
ORDER BY veteran_id;
```

**Expected output:**
```
veteran_id | first_name | disability_rating | phone_primary  | source_system
-----------|------------|-------------------|----------------|-------------
V123456    | John       | 70                | 555-999-8888   | BOTH
V789012    | Jane       | 50                | 555-987-6543   | BOTH
V345678    | Robert     | 90                | 555-222-3333   | BOTH
```

**Key observations:**
- All three veterans merged successfully
- `source_system = 'BOTH'` because they exist in both OMS and VEMS
- `phone_primary` comes from OMS (more recent contact info)
- `disability_rating_percentage` comes from VEMS (authoritative)

---

## Step 5: Create the dim_veteran Dimension Table

### What is SCD Type 2?

**Slowly Changing Dimension (SCD) Type 2** tracks historical changes. When a veteran's data changes, we:
1. Keep the old version with `is_current = FALSE` and `valid_to = change_date`
2. Insert new version with `is_current = TRUE` and `valid_to = NULL`

**Why SCD Type 2?**
- Track disability rating changes over time
- Audit trail for compliance
- Historical analysis (e.g., "What was his rating in 2020?")

### Step 5.1: Create Warehouse Schema

```sql
-- Create warehouse schema for dimensional model
USE DATABASE VESDW_PRD;

CREATE SCHEMA IF NOT EXISTS warehouse
    COMMENT = 'Star schema dimensional model - facts and dimensions';

USE SCHEMA warehouse;
```

### Step 5.2: Create dim_veteran Table

```sql
-- Create dimension table with SCD Type 2 support
CREATE OR REPLACE TABLE dim_veteran (
    -- Surrogate key (unique ID for each version)
    veteran_sk NUMBER(38,0) AUTOINCREMENT PRIMARY KEY,

    -- Business key (same for all versions of a veteran)
    veteran_natural_key VARCHAR(50) NOT NULL,

    -- From VEMS
    veteran_ssn VARCHAR(11),

    -- Name
    first_name VARCHAR(50) NOT NULL,
    middle_name VARCHAR(50),
    last_name VARCHAR(100) NOT NULL,
    suffix VARCHAR(10),

    -- Demographics
    date_of_birth DATE,
    gender VARCHAR(20),
    ethnicity VARCHAR(50),
    race VARCHAR(50),
    marital_status VARCHAR(20),

    -- Military service
    branch_of_service VARCHAR(50),
    service_start_date DATE,
    service_end_date DATE,
    discharge_status VARCHAR(50),
    combat_veteran_flag BOOLEAN,

    -- VA benefits
    service_connected_disability_flag BOOLEAN,
    disability_rating_percentage NUMBER(3,0),
    priority_group NUMBER(1,0),
    enrollment_date DATE,

    -- Address
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR(100),

    -- Contact
    phone_primary VARCHAR(20),
    phone_secondary VARCHAR(20),
    email VARCHAR(200),
    preferred_contact_method VARCHAR(20),

    -- Status
    active_duty_status VARCHAR(20),
    homeless_flag BOOLEAN,
    rural_flag BOOLEAN,

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,
    source_veteran_id VARCHAR(50) NOT NULL,

    -- SCD Type 2 fields
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ,

    -- Audit fields
    row_created_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    row_updated_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Veteran dimension with SCD Type 2 historical tracking';

-- Create clustering for performance
ALTER TABLE dim_veteran CLUSTER BY (veteran_natural_key, is_current);

-- Create indexes (views) for common queries
CREATE OR REPLACE VIEW vw_dim_veteran_current AS
SELECT * FROM dim_veteran WHERE is_current = TRUE
COMMENT = 'Current version of all veterans (most common query pattern)';
```

---

## Step 6: Create ETL Stored Procedure

This procedure loads data from staging into dim_veteran with SCD Type 2 logic.

### Step 6.1: Create the Load Procedure

```sql
USE SCHEMA warehouse;

-- Create procedure to load dim_veteran from staging with SCD Type 2
CREATE OR REPLACE PROCEDURE sp_load_dim_veteran()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INT DEFAULT 0;
    v_rows_updated INT DEFAULT 0;
    v_rows_unchanged INT DEFAULT 0;
BEGIN

    -- Step 1: Identify changed records
    -- Compare staging to current dimension records
    CREATE OR REPLACE TEMPORARY TABLE temp_veteran_changes AS
    WITH current_dim AS (
        SELECT
            veteran_natural_key,
            veteran_ssn,
            first_name,
            middle_name,
            last_name,
            suffix,
            date_of_birth,
            disability_rating_percentage,
            phone_primary,
            email,
            address_line1,
            city,
            state,
            zip_code
            -- Add other fields you want to track for changes
        FROM dim_veteran
        WHERE is_current = TRUE
    ),
    staging_data AS (
        SELECT
            veteran_id AS veteran_natural_key,
            veteran_ssn,
            first_name,
            middle_name,
            last_name,
            suffix,
            date_of_birth,
            disability_rating_percentage,
            phone_primary,
            email,
            address_line1,
            city,
            state,
            zip_code
        FROM VESDW_PRD.staging.stg_veteran_merged
    )
    SELECT
        s.*,
        CASE
            WHEN d.veteran_natural_key IS NULL THEN 'NEW'
            WHEN (
                -- Check if any tracked fields changed
                COALESCE(s.veteran_ssn, '') != COALESCE(d.veteran_ssn, '')
                OR COALESCE(s.first_name, '') != COALESCE(d.first_name, '')
                OR COALESCE(s.middle_name, '') != COALESCE(d.middle_name, '')
                OR COALESCE(s.last_name, '') != COALESCE(d.last_name, '')
                OR COALESCE(s.disability_rating_percentage, -1) != COALESCE(d.disability_rating_percentage, -1)
                OR COALESCE(s.phone_primary, '') != COALESCE(d.phone_primary, '')
                OR COALESCE(s.email, '') != COALESCE(d.email, '')
                OR COALESCE(s.address_line1, '') != COALESCE(d.address_line1, '')
                OR COALESCE(s.city, '') != COALESCE(d.city, '')
                OR COALESCE(s.state, '') != COALESCE(d.state, '')
                OR COALESCE(s.zip_code, '') != COALESCE(d.zip_code, '')
            ) THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM staging_data s
    LEFT JOIN current_dim d ON s.veteran_natural_key = d.veteran_natural_key;

    -- Step 2: Expire old records (set is_current = FALSE, valid_to = now)
    UPDATE dim_veteran
    SET
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP(),
        row_updated_timestamp = CURRENT_TIMESTAMP()
    WHERE veteran_natural_key IN (
        SELECT veteran_natural_key
        FROM temp_veteran_changes
        WHERE change_type = 'CHANGED'
    )
    AND is_current = TRUE;

    LET v_rows_updated := SQLROWCOUNT;

    -- Step 3: Insert new records (both NEW and CHANGED)
    INSERT INTO dim_veteran (
        veteran_natural_key,
        veteran_ssn,
        first_name,
        middle_name,
        last_name,
        suffix,
        date_of_birth,
        gender,
        ethnicity,
        race,
        marital_status,
        branch_of_service,
        service_start_date,
        service_end_date,
        discharge_status,
        combat_veteran_flag,
        service_connected_disability_flag,
        disability_rating_percentage,
        priority_group,
        enrollment_date,
        address_line1,
        address_line2,
        city,
        state,
        zip_code,
        county,
        phone_primary,
        phone_secondary,
        email,
        preferred_contact_method,
        active_duty_status,
        homeless_flag,
        rural_flag,
        source_system,
        source_veteran_id,
        is_current,
        valid_from,
        valid_to,
        row_created_timestamp,
        row_updated_timestamp
    )
    SELECT
        stg.veteran_id,
        stg.veteran_ssn,
        stg.first_name,
        stg.middle_name,
        stg.last_name,
        stg.suffix,
        stg.date_of_birth,
        stg.gender,
        stg.ethnicity,
        stg.race,
        stg.marital_status,
        stg.branch_of_service,
        stg.service_start_date,
        stg.service_end_date,
        stg.discharge_status,
        stg.combat_veteran_flag,
        stg.service_connected_disability_flag,
        stg.disability_rating_percentage,
        stg.priority_group,
        stg.enrollment_date,
        stg.address_line1,
        stg.address_line2,
        stg.city,
        stg.state,
        stg.zip_code,
        stg.county,
        stg.phone_primary,
        stg.phone_secondary,
        stg.email,
        stg.preferred_contact_method,
        stg.active_duty_status,
        stg.homeless_flag,
        stg.rural_flag,
        stg.source_system,
        stg.veteran_id,
        TRUE,  -- is_current
        CURRENT_TIMESTAMP(),  -- valid_from
        NULL,  -- valid_to (open-ended)
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM VESDW_PRD.staging.stg_veteran_merged stg
    INNER JOIN temp_veteran_changes chg
        ON stg.veteran_id = chg.veteran_natural_key
    WHERE chg.change_type IN ('NEW', 'CHANGED');

    LET v_rows_inserted := SQLROWCOUNT;

    -- Count unchanged
    SELECT COUNT(*) INTO :v_rows_unchanged
    FROM temp_veteran_changes
    WHERE change_type = 'UNCHANGED';

    -- Cleanup
    DROP TABLE IF EXISTS temp_veteran_changes;

    RETURN 'Load complete. Inserted: ' || :v_rows_inserted ||
           ', Updated (expired): ' || :v_rows_updated ||
           ', Unchanged: ' || :v_rows_unchanged;
END;
$$;
```

### Step 6.2: Test the Load Procedure

```sql
-- Run the load procedure
CALL sp_load_dim_veteran();

-- Check the dimension table
SELECT
    veteran_sk,
    veteran_natural_key,
    first_name,
    last_name,
    disability_rating_percentage,
    is_current,
    valid_from,
    valid_to
FROM dim_veteran
ORDER BY veteran_natural_key, valid_from;
```

**Expected output:**
```
veteran_sk | veteran_natural_key | first_name | disability_rating | is_current | valid_from          | valid_to
-----------|---------------------|------------|-------------------|------------|---------------------|----------
1          | V123456             | John       | 70                | TRUE       | 2024-01-18 10:00:00 | NULL
2          | V789012             | Jane       | 50                | TRUE       | 2024-01-18 10:00:00 | NULL
3          | V345678             | Robert     | 90                | TRUE       | 2024-01-18 10:00:00 | NULL
```

### Step 6.3: Test SCD Type 2 (Simulate a Change)

Let's update a veteran's disability rating to see SCD Type 2 in action!

```sql
-- Simulate a change in VEMS: John's disability rating increases to 80%
UPDATE VESODS_PRDDATA_PRD.VEMS_CORE.ods_vems_veteran
SET
    disability_rating_percentage = 80,
    last_updated_timestamp = CURRENT_TIMESTAMP(),
    ods_updated_timestamp = CURRENT_TIMESTAMP()
WHERE veteran_id = 'V123456';

-- The stream will capture this change
SELECT veteran_id, disability_rating_percentage, METADATA$ACTION
FROM VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran
WHERE veteran_id = 'V123456';

-- Run the merge to staging
CALL VESDW_PRD.staging.sp_merge_veteran_to_staging();

-- Run the dimension load
CALL sp_load_dim_veteran();

-- Check dimension table - should now have TWO versions of John!
SELECT
    veteran_sk,
    veteran_natural_key,
    first_name,
    disability_rating_percentage,
    is_current,
    valid_from,
    valid_to
FROM dim_veteran
WHERE veteran_natural_key = 'V123456'
ORDER BY valid_from;
```

**Expected output:**
```
veteran_sk | veteran_natural_key | first_name | disability_rating | is_current | valid_from          | valid_to
-----------|---------------------|------------|-------------------|------------|---------------------|---------------------
1          | V123456             | John       | 70                | FALSE      | 2024-01-18 10:00:00 | 2024-01-18 10:15:00
4          | V123456             | John       | 80                | TRUE       | 2024-01-18 10:15:00 | NULL
```

**🎉 Success!** You can see:
- Row 1: Old version (rating=70, is_current=FALSE, valid_to set)
- Row 4: New version (rating=80, is_current=TRUE, valid_to=NULL)

This is SCD Type 2 in action! Historical tracking is working!

---

## Step 7: Set Up Orchestration with Tasks

### What are Snowflake Tasks?

Tasks are Snowflake's **scheduling and orchestration** mechanism. They:
- Run SQL or stored procedures on a schedule (CRON syntax)
- Can depend on other tasks (DAG - Directed Acyclic Graph)
- Only run when conditions are met (e.g., stream has data)

### Step 7.1: Create Task to Merge Staging

```sql
USE SCHEMA VESDW_PRD.metadata;

-- Create task to merge OMS and VEMS to staging
-- Runs every hour, only if streams have data
CREATE OR REPLACE TASK task_hourly_merge_veteran_staging
    WAREHOUSE = ETL_WH
    SCHEDULE = '60 MINUTE'  -- Every hour
    WHEN SYSTEM$STREAM_HAS_DATA('VESODS_PRDDATA_PRD.OMS.stream_oms_veteran')
      OR SYSTEM$STREAM_HAS_DATA('VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran')
AS
    CALL VESDW_PRD.staging.sp_merge_veteran_to_staging();

-- Show task details
DESC TASK task_hourly_merge_veteran_staging;
```

### Step 7.2: Create Task to Load Dimension

```sql
-- Create task to load dim_veteran from staging
-- Depends on staging merge task completing first
CREATE OR REPLACE TASK task_hourly_load_dim_veteran
    WAREHOUSE = ETL_WH
    AFTER task_hourly_merge_veteran_staging  -- Dependency
AS
    CALL VESDW_PRD.warehouse.sp_load_dim_veteran();

-- Show task details
DESC TASK task_hourly_load_dim_veteran;
```

### Step 7.3: Enable Tasks (Start the Automation!)

**Important:** Tasks are created in SUSPENDED state. You must RESUME them to start.

```sql
-- Resume tasks (start from the child, then parent)
-- Resume child first
ALTER TASK task_hourly_load_dim_veteran RESUME;

-- Resume parent (this starts the automation)
ALTER TASK task_hourly_merge_veteran_staging RESUME;

-- Verify tasks are running
SHOW TASKS IN SCHEMA VESDW_PRD.metadata;

-- Check task state
SELECT
    name,
    state,
    schedule,
    warehouse,
    condition
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'task_hourly_merge_veteran_staging',
    SCHEDULED_TIME_RANGE_START => DATEADD(hour, -1, CURRENT_TIMESTAMP())
));
```

**Expected output:**
```
name                              | state    | schedule  | warehouse
----------------------------------|----------|-----------|----------
task_hourly_merge_veteran_staging | started  | 60 MINUTE | ETL_WH
task_hourly_load_dim_veteran      | started  |           | ETL_WH
```

### Step 7.4: Monitor Task Execution

```sql
-- Check recent task runs
SELECT
    name,
    scheduled_time,
    state,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -1, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'task_hourly_%'
ORDER BY scheduled_time DESC
LIMIT 10;
```

---

## Step 8: Initial Load and Testing

### Step 8.1: Manual Execution for Testing

Before relying on automated tasks, test manually:

```sql
-- 1. Add test data to ODS
USE SCHEMA VESODS_PRDDATA_PRD.VEMS_CORE;
INSERT INTO ods_vems_veteran (veteran_id, first_name, last_name,
    disability_rating_percentage, last_updated_timestamp)
VALUES ('V999999', 'Test', 'Veteran', 100, CURRENT_TIMESTAMP());

-- 2. Check stream
SELECT COUNT(*) FROM stream_vems_veteran;

-- 3. Run merge
CALL VESDW_PRD.staging.sp_merge_veteran_to_staging();

-- 4. Check staging
SELECT * FROM VESDW_PRD.staging.stg_veteran_merged
WHERE veteran_id = 'V999999';

-- 5. Run dimension load
CALL VESDW_PRD.warehouse.sp_load_dim_veteran();

-- 6. Check dimension
SELECT * FROM VESDW_PRD.warehouse.dim_veteran
WHERE veteran_natural_key = 'V999999';
```

### Step 8.2: Test Update Scenario

```sql
-- Update the test veteran
UPDATE VESODS_PRDDATA_PRD.VEMS_CORE.ods_vems_veteran
SET disability_rating_percentage = 90,
    last_updated_timestamp = CURRENT_TIMESTAMP()
WHERE veteran_id = 'V999999';

-- Run merge and load again
CALL VESDW_PRD.staging.sp_merge_veteran_to_staging();
CALL VESDW_PRD.warehouse.sp_load_dim_veteran();

-- Check for two versions (SCD Type 2)
SELECT
    veteran_sk,
    disability_rating_percentage,
    is_current,
    valid_from,
    valid_to
FROM VESDW_PRD.warehouse.dim_veteran
WHERE veteran_natural_key = 'V999999'
ORDER BY valid_from;
```

**Expected:** Two rows, one with rating=100 (is_current=FALSE), one with rating=90 (is_current=TRUE)

---

## Step 9: Monitor and Validate

### Step 9.1: Create Monitoring Views

```sql
USE SCHEMA VESDW_PRD.metadata;

-- View to check data freshness
CREATE OR REPLACE VIEW vw_veteran_data_freshness AS
SELECT
    'ODS_OMS' AS source_layer,
    COUNT(*) AS record_count,
    MAX(ods_updated_timestamp) AS last_updated
FROM VESODS_PRDDATA_PRD.OMS.ods_oms_veteran
UNION ALL
SELECT
    'ODS_VEMS' AS source_layer,
    COUNT(*) AS record_count,
    MAX(ods_updated_timestamp) AS last_updated
FROM VESODS_PRDDATA_PRD.VEMS_CORE.ods_vems_veteran
UNION ALL
SELECT
    'STAGING' AS source_layer,
    COUNT(*) AS record_count,
    MAX(staging_updated_timestamp) AS last_updated
FROM VESDW_PRD.staging.stg_veteran_merged
UNION ALL
SELECT
    'DIM_CURRENT' AS source_layer,
    COUNT(*) AS record_count,
    MAX(row_updated_timestamp) AS last_updated
FROM VESDW_PRD.warehouse.dim_veteran
WHERE is_current = TRUE;

-- View to check SCD Type 2 versions
CREATE OR REPLACE VIEW vw_veteran_version_count AS
SELECT
    veteran_natural_key,
    COUNT(*) AS version_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_count,
    MIN(valid_from) AS first_version_date,
    MAX(valid_from) AS latest_version_date
FROM VESDW_PRD.warehouse.dim_veteran
GROUP BY veteran_natural_key
HAVING COUNT(*) > 1  -- Only show veterans with multiple versions
ORDER BY version_count DESC;
```

### Step 9.2: Daily Monitoring Queries

```sql
-- Query 1: Data freshness check
SELECT * FROM VESDW_PRD.metadata.vw_veteran_data_freshness;

-- Query 2: Veterans with recent changes
SELECT
    veteran_natural_key,
    first_name,
    last_name,
    disability_rating_percentage,
    valid_from
FROM VESDW_PRD.warehouse.dim_veteran
WHERE is_current = TRUE
  AND valid_from >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY valid_from DESC;

-- Query 3: Stream status
SELECT
    'stream_oms_veteran' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('VESODS_PRDDATA_PRD.OMS.stream_oms_veteran') AS has_data
UNION ALL
SELECT
    'stream_vems_veteran' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran') AS has_data;

-- Query 4: Task execution history
SELECT
    name,
    scheduled_time,
    state,
    DATEDIFF(second, scheduled_time, completed_time) AS duration_seconds
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'task_hourly_%'
ORDER BY scheduled_time DESC
LIMIT 20;
```

---

## Troubleshooting Common Issues

### Issue 1: Task Not Running

**Symptoms:** Task shows as "started" but never executes

**Diagnosis:**
```sql
-- Check task state
SHOW TASKS LIKE 'task_hourly_merge_veteran_staging';

-- Check if condition is met
SELECT SYSTEM$STREAM_HAS_DATA('VESODS_PRDDATA_PRD.OMS.stream_oms_veteran');
```

**Solutions:**
- If condition is FALSE, task won't run (by design - no data to process)
- Check warehouse is running: `SHOW WAREHOUSES LIKE 'ETL_WH';`
- Check task schedule: `DESC TASK task_hourly_merge_veteran_staging;`
- Force run for testing: `EXECUTE TASK task_hourly_merge_veteran_staging;`

### Issue 2: Stream Shows No Data

**Symptoms:** Stream returns 0 rows but you know data changed

**Diagnosis:**
```sql
-- Check if stream exists
SHOW STREAMS IN SCHEMA VESODS_PRDDATA_PRD.OMS;

-- Check base table
SELECT COUNT(*) FROM VESODS_PRDDATA_PRD.OMS.ods_oms_veteran;
```

**Solutions:**
- Stream may have been consumed (data was read and offset advanced)
- Recreate stream: `CREATE OR REPLACE STREAM stream_oms_veteran ON TABLE ods_oms_veteran;`
- Check if table has changes since stream creation

### Issue 3: Duplicate Keys in Dimension

**Symptoms:** Multiple current versions of same veteran

**Diagnosis:**
```sql
-- Find duplicates
SELECT
    veteran_natural_key,
    COUNT(*) AS current_count
FROM VESDW_PRD.warehouse.dim_veteran
WHERE is_current = TRUE
GROUP BY veteran_natural_key
HAVING COUNT(*) > 1;
```

**Solutions:**
- Run this to fix:
```sql
-- Keep only the most recent version as current
UPDATE VESDW_PRD.warehouse.dim_veteran
SET is_current = FALSE
WHERE veteran_sk IN (
    SELECT veteran_sk
    FROM (
        SELECT
            veteran_sk,
            ROW_NUMBER() OVER (PARTITION BY veteran_natural_key
                               ORDER BY valid_from DESC) AS rn
        FROM VESDW_PRD.warehouse.dim_veteran
        WHERE is_current = TRUE
    )
    WHERE rn > 1
);
```

### Issue 4: Merge Taking Too Long

**Symptoms:** Staging merge procedure times out

**Diagnosis:**
```sql
-- Check staging table size
SELECT COUNT(*) FROM VESDW_PRD.staging.stg_veteran_merged;

-- Check stream sizes
SELECT COUNT(*) FROM VESODS_PRDDATA_PRD.OMS.stream_oms_veteran;
SELECT COUNT(*) FROM VESODS_PRDDATA_PRD.VEMS_CORE.stream_vems_veteran;
```

**Solutions:**
- Use larger warehouse: `ALTER TASK task_hourly_merge_veteran_staging SET WAREHOUSE = ETL_WH_LARGE;`
- Add clustering: `ALTER TABLE stg_veteran_merged CLUSTER BY (veteran_id);`
- Process in batches (modify procedure to use `LIMIT`)

### Issue 5: Missing Veterans in Dimension

**Symptoms:** Veterans in staging but not in dimension

**Diagnosis:**
```sql
-- Find missing veterans
SELECT stg.veteran_id
FROM VESDW_PRD.staging.stg_veteran_merged stg
LEFT JOIN VESDW_PRD.warehouse.dim_veteran dim
    ON stg.veteran_id = dim.veteran_natural_key AND dim.is_current = TRUE
WHERE dim.veteran_sk IS NULL;
```

**Solutions:**
- Run load procedure: `CALL sp_load_dim_veteran();`
- Check for errors in task history
- Verify procedure logic for edge cases

---

## Best Practices

### 1. Stream Management

✅ **DO:**
- Create one stream per source table
- Use descriptive naming: `stream_<source>_<entity>_<purpose>`
- Check `SYSTEM$STREAM_HAS_DATA()` before processing
- Monitor stream lag with `SHOW STREAMS`

❌ **DON'T:**
- Read from stream without consuming (use `CREATE TABLE AS SELECT` for testing)
- Create multiple streams on same table for same purpose
- Forget that streams consume data when read

### 2. SCD Type 2 Implementation

✅ **DO:**
- Always have exactly ONE current version per business key
- Use `is_current = TRUE` for current, `FALSE` for historical
- Set `valid_to = NULL` for current version
- Include all fields you want to track for changes in comparison logic

❌ **DON'T:**
- Update the `veteran_sk` (surrogate key should never change)
- Delete old versions (that defeats the purpose of history)
- Forget to set `valid_to` when expiring old versions

### 3. Task Orchestration

✅ **DO:**
- Start with smaller schedules for testing (e.g., 5 MINUTE)
- Use `AFTER` for task dependencies
- Add error notifications: `ALTER TASK ... SET ERROR_INTEGRATION = <integration>`
- Monitor task history regularly

❌ **DON'T:**
- Create circular dependencies (Task A depends on Task B, which depends on Task A)
- Use very small warehouses for large data volumes
- Forget to RESUME tasks after creation

### 4. Performance Optimization

✅ **DO:**
- Cluster dimension tables by business key + `is_current`
- Use appropriate warehouse sizes (XS for testing, L+ for production)
- Partition very large tables by date
- Monitor credit usage

❌ **DON'T:**
- Over-cluster (max 3-4 columns)
- Use XL warehouse for small data loads
- Query historical versions frequently (create separate view if needed)

### 5. Monitoring & Alerting

✅ **DO:**
- Check data freshness daily
- Monitor task success rates
- Alert on SLA breaches
- Track credit usage trends

❌ **DON'T:**
- Wait for users to report data issues
- Ignore failed tasks
- Let streams build up without processing

---

## Summary Checklist

Use this checklist to verify your implementation:

### Setup Phase
- [ ] ODS database and schemas created
- [ ] ODS tables created for both OMS and VEMS
- [ ] Streams created on both ODS tables
- [ ] Staging schema and table created
- [ ] Warehouse schema created
- [ ] dim_veteran table created with SCD Type 2 fields

### Procedures Phase
- [ ] Merge procedure created (`sp_merge_veteran_to_staging`)
- [ ] Load procedure created (`sp_load_dim_veteran`)
- [ ] Both procedures tested manually
- [ ] SCD Type 2 logic validated with test data

### Automation Phase
- [ ] Tasks created for merge and load
- [ ] Task dependencies configured correctly
- [ ] Tasks resumed (automation started)
- [ ] Task execution verified in history

### Monitoring Phase
- [ ] Monitoring views created
- [ ] Daily checks scheduled
- [ ] Alerting configured (if available)
- [ ] Documentation updated

### Testing Phase
- [ ] Initial load successful
- [ ] Update scenario tested (SCD Type 2)
- [ ] Delete scenario tested
- [ ] Performance acceptable
- [ ] No duplicate current records

---

## Next Steps

**Congratulations!** You've successfully set up a production-ready pipeline for loading dim_veteran from multiple sources using Snowflake's native features.

### What You've Accomplished

✅ Built a multi-source data integration pipeline
✅ Implemented CDC with Streams
✅ Created intelligent merge logic in staging
✅ Implemented SCD Type 2 for historical tracking
✅ Automated the entire process with Tasks
✅ Set up monitoring and validation

### Recommended Next Steps

1. **Apply to Other Dimensions**
   - Use this same pattern for dim_evaluator, dim_facility, etc.
   - The process is identical, just change table/column names

2. **Enhance Monitoring**
   - Set up email alerts for task failures
   - Create Snowflake dashboards
   - Integrate with external monitoring tools

3. **Optimize Performance**
   - Analyze query patterns and add clustering
   - Consider materialized views for common queries
   - Right-size warehouses based on actual usage

4. **Advanced Features**
   - Implement late-arriving fact handling
   - Add data quality checks in staging
   - Create audit tables for compliance

### Additional Resources

- [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) - Comprehensive Snowflake guide
- [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md) - Advanced CDC patterns
- [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) - Task orchestration details
- [SCD_TYPE2_DESIGN_GUIDE.md](SCD_TYPE2_DESIGN_GUIDE.md) - SCD Type 2 deep dive
- [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Performance tuning

---

## Document Information

**Author:** Mark Chappell
**Version:** 1.0
**Last Updated:** 2024-11-18
**Target Audience:** Data Engineers new to Snowflake
**Estimated Implementation Time:** 2-3 hours

**Feedback:** Submit issues or suggestions to the data engineering team.

---

**You did it! 🎉 Welcome to the world of modern data engineering with Snowflake!**
