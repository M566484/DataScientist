# VES Multi-Source Data Integration - Jira Project Plan

**Project Name:** VES Multi-Source Data Warehouse
**Delivery Timeline:** 8 weeks (2 months)
**Sprint Duration:** 2 weeks
**Total Sprints:** 4

---

## Executive Summary

### MVP Deliverables (Weeks 1-6)
**Goal:** Unified veteran and evaluator data from OMS and VEMS in a queryable dimensional model

**Core Capabilities:**
- Data ingestion from OMS and VEMS into ODS layer
- Entity matching and reconciliation
- Core dimensions (Veterans, Evaluators, Facilities)
- Essential fact tables (Evaluations, Exam Requests)
- Basic data quality monitoring
- Fundamental lineage tracking

### Non-MVP Deliverables (Weeks 7-8+)
**Goal:** Enhanced analytics, comprehensive monitoring, and business intelligence capabilities

**Enhanced Capabilities:**
- Advanced marts and pre-aggregated views
- Comprehensive data lineage dashboard
- Advanced data quality rules and alerts
- Additional fact tables (Appointments, QA Events)
- Performance optimization
- User training and documentation

---

## Phase Distribution

| Phase | Duration | Sprint | Focus | Deliverables |
|-------|----------|--------|-------|--------------|
| **Phase 1** | Weeks 1-2 | Sprint 1 | Foundation | Database setup, ODS layer, reference framework |
| **Phase 2** | Weeks 3-4 | Sprint 2 | Core Integration | Staging layer, entity matching, core dimensions |
| **Phase 3** | Weeks 5-6 | Sprint 3 | Facts & ETL | Fact tables, ETL orchestration, basic monitoring |
| **Phase 4** | Weeks 7-8 | Sprint 4 | Enhancement | Marts, advanced features, optimization |

---

# Epic Structure

## Epic 1: Infrastructure & Foundation Setup
**Priority:** Critical (MVP)
**Sprint:** 1
**Story Points:** 21

## Epic 2: ODS Layer & Multi-Source Ingestion
**Priority:** Critical (MVP)
**Sprint:** 1
**Story Points:** 34

## Epic 3: Reference Data Framework
**Priority:** Critical (MVP)
**Sprint:** 1-2
**Story Points:** 21

## Epic 4: Staging Layer & Data Reconciliation
**Priority:** Critical (MVP)
**Sprint:** 2
**Story Points:** 34

## Epic 5: Core Dimensional Model
**Priority:** Critical (MVP)
**Sprint:** 2-3
**Story Points:** 34

## Epic 6: Fact Tables & Metrics
**Priority:** Critical (MVP)
**Sprint:** 3
**Story Points:** 34

## Epic 7: ETL Orchestration & Automation
**Priority:** Critical (MVP)
**Sprint:** 3
**Story Points:** 21

## Epic 8: Data Quality & Lineage (Basic)
**Priority:** Critical (MVP)
**Sprint:** 3
**Story Points:** 21

## Epic 9: Business Intelligence Marts
**Priority:** High (Non-MVP)
**Sprint:** 4
**Story Points:** 21

## Epic 10: Advanced Monitoring & Observability
**Priority:** Medium (Non-MVP)
**Sprint:** 4
**Story Points:** 13

## Epic 11: Documentation & Training
**Priority:** High (MVP + Non-MVP)
**Sprint:** 1-4
**Story Points:** 13

---

# Detailed User Stories & Tasks

---

## EPIC 1: Infrastructure & Foundation Setup
**MVP:** Yes | **Sprint:** 1 | **Total Points:** 21

### Story 1.1: Setup Snowflake Database and Schemas
**Story Points:** 5
**Priority:** Critical
**Dependencies:** None
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need to create the foundational database structure in Snowflake so that all layers of the data pipeline have proper schema organization.

**Acceptance Criteria:**
- [ ] Database `VETERAN_EVALUATION_DW` created
- [ ] Schema `ODS_RAW` created for raw landing data
- [ ] Schema `STAGING` created for cleansed data
- [ ] Schema `WAREHOUSE` created for dimensional model
- [ ] Schema `REFERENCE` created for mapping tables
- [ ] Schema `MARTS_CLINICAL` created for business views
- [ ] Appropriate roles and permissions configured
- [ ] Deployment script validated in DEV environment

**Tasks:**
- [ ] Task 1.1.1: Create database and schemas (2 pts)
- [ ] Task 1.1.2: Configure warehouse compute resources (1 pt)
- [ ] Task 1.1.3: Setup roles and access control (2 pts)

**Technical Notes:**
- File: `snowflake/schema/00_setup_database.sql`
- Ensure proper naming conventions per VES standards

---

### Story 1.2: Setup Development & Deployment Framework
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Story 1.1
**Assignee:** DevOps Engineer

**Description:**
As a DevOps engineer, I need to establish CI/CD pipelines and deployment processes so that database changes can be deployed safely and consistently.

**Acceptance Criteria:**
- [ ] Git repository structure established
- [ ] Master deployment script created and tested
- [ ] DEV environment fully functional
- [ ] TEST environment provisioned
- [ ] Deployment runbook documented
- [ ] Rollback procedures documented

**Tasks:**
- [ ] Task 1.2.1: Setup Git branching strategy (2 pts)
- [ ] Task 1.2.2: Create master deployment script (3 pts)
- [ ] Task 1.2.3: Configure environment-specific variables (1 pt)
- [ ] Task 1.2.4: Document deployment process (2 pts)

**Technical Notes:**
- File: `snowflake/schema/02_master_deployment.sql`
- Use !source commands for modular deployment

---

### Story 1.3: Establish Naming Conventions & Standards
**Story Points:** 3
**Priority:** High
**Dependencies:** None
**Assignee:** Data Architect

**Description:**
As a data architect, I need to document and enforce naming conventions so that all database objects follow consistent standards.

**Acceptance Criteria:**
- [ ] Naming conventions document created
- [ ] Table naming standards defined (ods_, stg_, dim_, fact_, ref_)
- [ ] Column naming standards defined
- [ ] Procedure naming standards defined (sp_, fn_)
- [ ] Code review checklist includes naming validation

**Tasks:**
- [ ] Task 1.3.1: Document naming conventions (2 pts)
- [ ] Task 1.3.2: Create validation checklist (1 pt)

---

### Story 1.4: Setup Date Dimension
**Story Points:** 5
**Priority:** High
**Dependencies:** Story 1.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need to create and populate the date dimension so that all temporal analysis has a consistent calendar framework.

**Acceptance Criteria:**
- [ ] dim_dates table created with proper structure
- [ ] Date range covers 2020-2030
- [ ] All date attributes populated (fiscal year, quarter, etc.)
- [ ] Clustering configured on date_key
- [ ] Table contains 3,653+ rows

**Tasks:**
- [ ] Task 1.4.1: Create dim_dates table DDL (2 pts)
- [ ] Task 1.4.2: Populate dimension with date range (2 pts)
- [ ] Task 1.4.3: Validate fiscal calendar logic (1 pt)

**Technical Notes:**
- Files: `snowflake/dimensions/01_dim_date.sql`, `snowflake/schema/01_populate_date_dimension.sql`

---

## EPIC 2: ODS Layer & Multi-Source Ingestion
**MVP:** Yes | **Sprint:** 1 | **Total Points:** 34

### Story 2.1: Create ODS Tables for Source Systems
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Story 1.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need to create ODS landing tables that can receive data from both OMS and VEMS systems so that we preserve source fidelity.

**Acceptance Criteria:**
- [ ] 8 ODS tables created (veterans, evaluators, facilities, exam_requests, evaluations, appointments, qa_events, claims)
- [ ] All tables include source_system discriminator column
- [ ] All tables include extraction_timestamp and batch_id
- [ ] Primary keys defined on (source_system, source_record_id, extraction_timestamp)
- [ ] Tables clustered by source_system and batch_id
- [ ] DDL scripts validated and deployed

**Tasks:**
- [ ] Task 2.1.1: Create ods_veterans_source table (2 pts)
- [ ] Task 2.1.2: Create ods_evaluators_source table (2 pts)
- [ ] Task 2.1.3: Create ods_facilities_source table (1 pt)
- [ ] Task 2.1.4: Create ods_exam_requests_source table (2 pts)
- [ ] Task 2.1.5: Create ods_evaluations_source table (2 pts)
- [ ] Task 2.1.6: Create ods_appointments_source table (2 pts)
- [ ] Task 2.1.7: Create ods_qa_events_source table (1 pt)
- [ ] Task 2.1.8: Create ods_claims_source table (1 pt)

**Technical Notes:**
- File: `snowflake/ods/01_create_ods_tables.sql`
- Ensure all source-specific fields are included as VARCHAR to preserve raw values

---

### Story 2.2: Create ODS Control & Error Tables
**Story Points:** 5
**Priority:** Critical
**Dependencies:** Story 2.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need batch control and error logging tables so that ETL processes can be tracked and failures investigated.

**Acceptance Criteria:**
- [ ] ods_batch_control table created
- [ ] ods_error_log table created with VARIANT column for details
- [ ] Sample batch control record inserted for testing
- [ ] Error log retention policy defined (90 days)

**Tasks:**
- [ ] Task 2.2.1: Create ods_batch_control table (2 pts)
- [ ] Task 2.2.2: Create ods_error_log table (2 pts)
- [ ] Task 2.2.3: Define retention policies (1 pt)

**Technical Notes:**
- Included in `snowflake/ods/01_create_ods_tables.sql`

---

### Story 2.3: Coordinate with Mulesoft Team on Data Contracts
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Story 2.1
**Assignee:** Integration Engineer + Data Architect

**Description:**
As an integration engineer, I need to establish data contracts with the Mulesoft team so that ODS tables receive properly formatted data from both OMS and VEMS.

**Acceptance Criteria:**
- [ ] OMS data contract documented (field mappings, data types, frequency)
- [ ] VEMS data contract documented
- [ ] Sample data files received for both systems
- [ ] Data validation rules agreed upon
- [ ] Error handling process defined
- [ ] Mulesoft team sign-off obtained

**Tasks:**
- [ ] Task 2.3.1: Document OMS field mappings (2 pts)
- [ ] Task 2.3.2: Document VEMS field mappings (2 pts)
- [ ] Task 2.3.3: Review sample data files (2 pts)
- [ ] Task 2.3.4: Define error handling process (2 pts)

**Technical Notes:**
- Create data dictionary document
- Include field-level metadata (nullability, constraints, business rules)

---

### Story 2.4: Test ODS Data Loading
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Story 2.1, Story 2.2, Story 2.3
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need to validate that ODS tables can successfully receive data from Mulesoft so that we confirm the integration works end-to-end.

**Acceptance Criteria:**
- [ ] Sample OMS data loaded successfully
- [ ] Sample VEMS data loaded successfully
- [ ] Batch control records created correctly
- [ ] Source_system discriminator working properly
- [ ] No data type conversion errors
- [ ] Extraction timestamps captured correctly

**Tasks:**
- [ ] Task 2.4.1: Load sample OMS veteran data (2 pts)
- [ ] Task 2.4.2: Load sample VEMS veteran data (2 pts)
- [ ] Task 2.4.3: Validate batch control logging (2 pts)
- [ ] Task 2.4.4: Test error scenarios (2 pts)

---

## EPIC 3: Reference Data Framework
**MVP:** Yes | **Sprint:** 1-2 | **Total Points:** 21

### Story 3.1: Create System of Record Configuration
**Story Points:** 3
**Priority:** Critical
**Dependencies:** Story 1.1
**Assignee:** Data Architect + Business Analyst

**Description:**
As a data architect, I need to define which source system is authoritative for each entity type so that data reconciliation rules are clear and documented.

**Acceptance Criteria:**
- [ ] ref_system_of_record table created
- [ ] Configuration determined for Veterans (OMS primary)
- [ ] Configuration determined for Evaluators (VEMS primary)
- [ ] Configuration determined for Facilities (OMS primary)
- [ ] Configuration determined for Appointments (VEMS only)
- [ ] Business stakeholders approve configuration
- [ ] Configuration data loaded

**Tasks:**
- [ ] Task 3.1.1: Create ref_system_of_record table (1 pt)
- [ ] Task 3.1.2: Conduct business rule discovery sessions (1 pt)
- [ ] Task 3.1.3: Load configuration data (1 pt)

**Technical Notes:**
- File: `snowflake/reference/01_create_reference_tables.sql`

---

### Story 3.2: Create Entity Crosswalk Tables
**Story Points:** 5
**Priority:** Critical
**Dependencies:** Story 3.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need crosswalk tables to link OMS and VEMS records for the same real-world entities so that we can create unified master records.

**Acceptance Criteria:**
- [ ] ref_entity_crosswalk_veteran table created
- [ ] ref_entity_crosswalk_evaluator table created
- [ ] ref_entity_crosswalk_facility table created
- [ ] Match confidence scoring column included (0-100)
- [ ] Match method column included (SSN_EXACT_MATCH, etc.)
- [ ] Created/updated timestamps included

**Tasks:**
- [ ] Task 3.2.1: Create veteran crosswalk table (2 pts)
- [ ] Task 3.2.2: Create evaluator crosswalk table (2 pts)
- [ ] Task 3.2.3: Create facility crosswalk table (1 pt)

**Technical Notes:**
- These tables are populated by ETL procedures, not manually

---

### Story 3.3: Create Field Mapping Tables
**Story Points:** 5
**Priority:** High
**Dependencies:** Story 3.1
**Assignee:** Data Analyst + Data Engineer

**Description:**
As a data analyst, I need to document how OMS and VEMS field names map to standardized names so that transformations are consistent.

**Acceptance Criteria:**
- [ ] ref_field_mapping_oms table created
- [ ] ref_field_mapping_vems table created
- [ ] All veteran field mappings documented
- [ ] All evaluator field mappings documented
- [ ] Transformation rules documented (e.g., vet_ssn → veteran_ssn)
- [ ] Sample mappings loaded

**Tasks:**
- [ ] Task 3.3.1: Create field mapping tables (1 pt)
- [ ] Task 3.3.2: Document OMS field mappings (2 pts)
- [ ] Task 3.3.3: Document VEMS field mappings (2 pts)

**Technical Notes:**
- Include transformation_rule column with SQL snippets

---

### Story 3.4: Create Code Mapping Tables
**Story Points:** 8
**Priority:** High
**Dependencies:** Story 3.1, Story 2.3
**Assignee:** Data Analyst

**Description:**
As a data analyst, I need to map system-specific code values to standardized codes so that reporting is consistent across sources.

**Acceptance Criteria:**
- [ ] ref_code_mapping_specialty table created and populated
- [ ] ref_code_mapping_request_type table created and populated
- [ ] ref_code_mapping_appointment_status table created and populated
- [ ] All OMS specialty codes mapped
- [ ] All VEMS specialty codes mapped
- [ ] Active/inactive flag included
- [ ] Code categories defined

**Tasks:**
- [ ] Task 3.4.1: Create code mapping tables (1 pt)
- [ ] Task 3.4.2: Analyze OMS specialty codes (2 pts)
- [ ] Task 3.4.3: Analyze VEMS specialty codes (2 pts)
- [ ] Task 3.4.4: Map to standard values (2 pts)
- [ ] Task 3.4.5: Load code mappings (1 pt)

**Technical Notes:**
- Work with business SMEs to validate mappings
- Consider edge cases and unmapped codes

---

## EPIC 4: Staging Layer & Data Reconciliation
**MVP:** Yes | **Sprint:** 2 | **Total Points:** 34

### Story 4.1: Create Staging Tables
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Epic 2, Story 3.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need staging tables that can hold cleansed and reconciled data from both sources so that the warehouse receives high-quality unified data.

**Acceptance Criteria:**
- [ ] stg_veterans table created with all standardized columns
- [ ] stg_evaluators table created
- [ ] stg_facilities table created
- [ ] stg_fact_exam_requests table created
- [ ] stg_fact_evaluations table created
- [ ] All staging tables include dq_score and dq_issues columns
- [ ] All staging tables include source_record_hash for change detection
- [ ] Tables clustered appropriately

**Tasks:**
- [ ] Task 4.1.1: Create dimension staging tables (3 pts)
- [ ] Task 4.1.2: Create fact staging tables (3 pts)
- [ ] Task 4.1.3: Configure clustering and performance optimization (2 pts)

**Technical Notes:**
- File: `snowflake/staging/01_create_staging_tables.sql`

---

### Story 4.2: Build Entity Matching Procedures
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Story 3.2, Story 4.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need procedures to match OMS and VEMS records so that we can identify which records represent the same veteran, evaluator, or facility.

**Acceptance Criteria:**
- [ ] sp_build_crosswalk_veterans() procedure created
- [ ] sp_build_crosswalk_evaluators() procedure created
- [ ] sp_build_crosswalk_facilities() procedure created
- [ ] Veterans matched on SSN with 100% confidence for exact match
- [ ] Evaluators matched on NPI with 100% confidence
- [ ] Facilities matched on facility ID or name
- [ ] Match confidence calculated correctly
- [ ] Procedures tested with sample data

**Tasks:**
- [ ] Task 4.2.1: Create veteran matching procedure with SSN logic (5 pts)
- [ ] Task 4.2.2: Create evaluator matching procedure with NPI logic (4 pts)
- [ ] Task 4.2.3: Create facility matching procedure (2 pts)
- [ ] Task 4.2.4: Test matching logic with edge cases (2 pts)

**Technical Notes:**
- File: `snowflake/etl/03_etl_procedures_multi_source.sql`
- Use FULL OUTER JOIN pattern
- Handle cases where entity exists in only one system

---

### Story 4.3: Build Multi-Source Transformation Procedures
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Story 4.2, Story 3.3, Story 3.4
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need procedures to merge OMS and VEMS data using system of record rules so that staging tables contain unified, reconciled records.

**Acceptance Criteria:**
- [ ] sp_transform_multisource_ods_to_staging_veterans() created
- [ ] sp_transform_multisource_ods_to_staging_evaluators() created
- [ ] sp_transform_multisource_ods_to_staging_facilities() created
- [ ] System of record preference implemented correctly
- [ ] Most recent timestamp logic working for dynamic fields
- [ ] Field mappings applied correctly
- [ ] Code mappings applied using UDFs
- [ ] Conflict detection logging to ref_reconciliation_log
- [ ] Data quality scoring calculated
- [ ] Procedures tested end-to-end

**Tasks:**
- [ ] Task 4.3.1: Create veteran transformation procedure (5 pts)
- [ ] Task 4.3.2: Create evaluator transformation procedure (4 pts)
- [ ] Task 4.3.3: Create facility transformation procedure (2 pts)
- [ ] Task 4.3.4: Test with real OMS and VEMS data (2 pts)

**Technical Notes:**
- Implement COALESCE patterns for merging
- Use CASE statements for system of record preference
- Call crosswalk builders first

---

## EPIC 5: Core Dimensional Model
**MVP:** Yes | **Sprint:** 2-3 | **Total Points:** 34

### Story 5.1: Create Core Dimension Tables
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Story 4.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need to create dimension tables with SCD Type 2 tracking so that we can maintain history of how entities change over time.

**Acceptance Criteria:**
- [ ] dim_veterans table created with SCD Type 2 structure
- [ ] dim_evaluators table created with SCD Type 2 structure
- [ ] dim_facilities table created with SCD Type 2 structure
- [ ] Surrogate keys (_sk suffix) implemented with AUTOINCREMENT
- [ ] Natural keys (_id suffix) from crosswalk
- [ ] Effective_date and expiration_date columns included
- [ ] Is_current flag included
- [ ] Tables clustered by natural key

**Tasks:**
- [ ] Task 5.1.1: Create dim_veterans with SCD Type 2 (5 pts)
- [ ] Task 5.1.2: Create dim_evaluators with SCD Type 2 (4 pts)
- [ ] Task 5.1.3: Create dim_facilities with SCD Type 2 (2 pts)
- [ ] Task 5.1.4: Create dim_evaluation_types (1 pt)
- [ ] Task 5.1.5: Create dim_exam_request_types (1 pt)

**Technical Notes:**
- Files: `snowflake/dimensions/02_dim_veteran.sql`, `03_dim_evaluator.sql`, `04_dim_facility.sql`, `05_dim_evaluation_type.sql`, `09_dim_exam_request_types.sql`

---

### Story 5.2: Create Dimension Load Procedures
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Story 5.1, Story 4.3
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need ETL procedures to load dimensions with proper SCD Type 2 logic so that historical changes are tracked correctly.

**Acceptance Criteria:**
- [ ] sp_load_dim_veterans() procedure created
- [ ] sp_load_dim_evaluators() procedure created
- [ ] sp_load_dim_facilities() procedure created
- [ ] MD5 hash change detection working
- [ ] Old records end-dated when changes detected
- [ ] New versions inserted with new surrogate key
- [ ] Is_current flag maintained correctly
- [ ] Procedures handle inserts and updates

**Tasks:**
- [ ] Task 5.2.1: Create veteran dimension load procedure (5 pts)
- [ ] Task 5.2.2: Create evaluator dimension load procedure (4 pts)
- [ ] Task 5.2.3: Create facility dimension load procedure (2 pts)
- [ ] Task 5.2.4: Test SCD Type 2 logic with changing data (2 pts)

**Technical Notes:**
- File: `snowflake/etl/01_etl_procedures_dimensions.sql`
- Use MD5 hash of key attributes for change detection

---

### Story 5.3: Create Full Dimension ETL Pipeline
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Story 5.2, Story 4.3
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need orchestration procedures that execute the full dimension pipeline from ODS to warehouse so that I can run a single command to load all dimensions.

**Acceptance Criteria:**
- [ ] sp_etl_veterans() orchestration procedure created
- [ ] sp_etl_evaluators() orchestration procedure created
- [ ] sp_etl_facilities() orchestration procedure created
- [ ] Procedures call transformation then load in correct sequence
- [ ] Error handling implemented
- [ ] Batch ID passed through all steps
- [ ] Procedures tested end-to-end

**Tasks:**
- [ ] Task 5.3.1: Create orchestration procedures (3 pts)
- [ ] Task 5.3.2: Add error handling and logging (2 pts)
- [ ] Task 5.3.3: End-to-end integration test (3 pts)

---

## EPIC 6: Fact Tables & Metrics
**MVP:** Yes | **Sprint:** 3 | **Total Points:** 34

### Story 6.1: Create Core Fact Tables
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Epic 5
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need fact tables for evaluations and exam requests so that we can analyze the core business processes.

**Acceptance Criteria:**
- [ ] fact_evaluations_completed table created (transaction grain)
- [ ] fact_exam_requests table created (accumulating snapshot)
- [ ] Surrogate key references to dimensions working
- [ ] Fact tables clustered by date
- [ ] Measure columns include nullable flags for lifecycle milestones
- [ ] Date columns for all lifecycle events

**Tasks:**
- [ ] Task 6.1.1: Create fact_evaluations_completed (5 pts)
- [ ] Task 6.1.2: Create fact_exam_requests (5 pts)
- [ ] Task 6.1.3: Create fact_examiner_assignments (3 pts)

**Technical Notes:**
- Files: `snowflake/facts/01_fact_evaluation.sql`, `07_fact_exam_requests.sql`, `08_fact_examiner_assignments.sql`

---

### Story 6.2: Create Multi-Source Fact Transformation Procedures
**Story Points:** 13
**Priority:** Critical
**Dependencies:** Story 6.1, Story 4.2
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need transformation procedures that map source IDs to master IDs using crosswalks so that fact records reference the correct unified dimensions.

**Acceptance Criteria:**
- [ ] sp_transform_multisource_ods_to_staging_exam_requests() created
- [ ] sp_transform_multisource_ods_to_staging_evaluations() created
- [ ] Crosswalk lookups implemented for veteran_id, evaluator_id, facility_id
- [ ] Code translations applied
- [ ] Both OMS and VEMS sources handled
- [ ] Data quality scoring included

**Tasks:**
- [ ] Task 6.2.1: Create exam requests transformation (5 pts)
- [ ] Task 6.2.2: Create evaluations transformation (5 pts)
- [ ] Task 6.2.3: Test ID mapping logic (3 pts)

**Technical Notes:**
- File: `snowflake/etl/03_etl_procedures_multi_source.sql`
- Use LEFT JOIN to crosswalk tables with OR conditions for OMS/VEMS

---

### Story 6.3: Create Fact Load Procedures
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Story 6.1, Story 6.2
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need procedures to load fact tables from staging and resolve surrogate keys so that facts properly reference dimension records.

**Acceptance Criteria:**
- [ ] sp_load_fact_evaluations() procedure created
- [ ] sp_load_fact_exam_requests() procedure created
- [ ] Surrogate key lookups from dimensions working
- [ ] INSERT vs MERGE logic appropriate for grain
- [ ] Handles orphaned records (missing dimension matches)
- [ ] Error logging for data quality issues

**Tasks:**
- [ ] Task 6.3.1: Create evaluation fact load procedure (3 pts)
- [ ] Task 6.3.2: Create exam request fact load procedure (3 pts)
- [ ] Task 6.3.3: Test surrogate key resolution (2 pts)

**Technical Notes:**
- File: `snowflake/etl/02_etl_procedures_facts.sql`

---

## EPIC 7: ETL Orchestration & Automation
**MVP:** Yes | **Sprint:** 3 | **Total Points:** 21

### Story 7.1: Create Master ETL Orchestration Procedure
**Story Points:** 8
**Priority:** Critical
**Dependencies:** Epic 5, Epic 6
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need a master ETL procedure that runs the entire pipeline so that I can execute a single command to load all data.

**Acceptance Criteria:**
- [ ] sp_etl_master_pipeline_multisource() procedure created
- [ ] Calls all dimension transformations in correct order
- [ ] Calls all fact transformations after dimensions
- [ ] Batch control record created at start
- [ ] Batch control record updated with success/failure
- [ ] Error handling with try-catch
- [ ] Returns comprehensive status message

**Tasks:**
- [ ] Task 7.1.1: Create orchestration procedure (4 pts)
- [ ] Task 7.1.2: Implement batch control logic (2 pts)
- [ ] Task 7.1.3: Add comprehensive error handling (2 pts)

**Technical Notes:**
- File: `snowflake/etl/03_etl_procedures_multi_source.sql`
- Use DECLARE/BEGIN/EXCEPTION blocks

---

### Story 7.2: Create Code Translation UDFs
**Story Points:** 5
**Priority:** High
**Dependencies:** Story 3.4
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need user-defined functions to translate codes so that transformations can call consistent mapping logic.

**Acceptance Criteria:**
- [ ] fn_map_specialty_code() function created
- [ ] fn_map_request_type_code() function created
- [ ] fn_map_appointment_status_code() function created
- [ ] Functions query reference tables
- [ ] Functions handle null inputs gracefully
- [ ] Performance is acceptable (<10ms per call)

**Tasks:**
- [ ] Task 7.2.1: Create specialty code UDF (2 pts)
- [ ] Task 7.2.2: Create request type UDF (2 pts)
- [ ] Task 7.2.3: Create appointment status UDF (1 pt)

**Technical Notes:**
- File: `snowflake/etl/03_etl_procedures_multi_source.sql`

---

### Story 7.3: Setup Scheduled ETL Jobs
**Story Points:** 8
**Priority:** High
**Dependencies:** Story 7.1
**Assignee:** DevOps Engineer + Data Engineer

**Description:**
As a DevOps engineer, I need to schedule the ETL to run automatically so that data is refreshed on a regular cadence.

**Acceptance Criteria:**
- [ ] Snowflake TASK created for daily ETL (or appropriate frequency)
- [ ] Task monitors for new ODS data before running
- [ ] Email alerts configured for failures
- [ ] Monitoring dashboard shows last run status
- [ ] Task can be manually triggered for ad-hoc runs
- [ ] Task runs in TEST environment successfully

**Tasks:**
- [ ] Task 7.3.1: Create Snowflake TASK (3 pts)
- [ ] Task 7.3.2: Configure failure alerting (2 pts)
- [ ] Task 7.3.3: Test scheduled execution (2 pts)
- [ ] Task 7.3.4: Document manual trigger process (1 pt)

---

## EPIC 8: Data Quality & Lineage (Basic)
**MVP:** Yes | **Sprint:** 3 | **Total Points:** 21

### Story 8.1: Create Reconciliation Log Table
**Story Points:** 3
**Priority:** Critical
**Dependencies:** Story 3.1
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need a table to log conflicts between OMS and VEMS so that data stewards can review discrepancies.

**Acceptance Criteria:**
- [ ] ref_reconciliation_log table created
- [ ] Captures conflict type, OMS value, VEMS value, resolved value
- [ ] Resolution method documented
- [ ] Batch ID included for traceability
- [ ] Sample conflict logged during testing

**Tasks:**
- [ ] Task 8.1.1: Create reconciliation log table (2 pts)
- [ ] Task 8.1.2: Test conflict logging in transformation (1 pt)

**Technical Notes:**
- File: `snowflake/reference/01_create_reference_tables.sql`

---

### Story 8.2: Implement Conflict Logging in Transformations
**Story Points:** 5
**Priority:** High
**Dependencies:** Story 8.1, Story 4.3
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need transformations to automatically log conflicts so that we have visibility into data quality issues.

**Acceptance Criteria:**
- [ ] Veteran transformation logs disability rating conflicts
- [ ] Evaluator transformation logs specialty conflicts
- [ ] Conflict logging doesn't fail the transformation
- [ ] Conflicts visible in ref_reconciliation_log after ETL run

**Tasks:**
- [ ] Task 8.2.1: Add conflict detection to veteran transform (2 pts)
- [ ] Task 8.2.2: Add conflict detection to evaluator transform (2 pts)
- [ ] Task 8.2.3: Test conflict logging (1 pt)

---

### Story 8.3: Create Basic Lineage Views
**Story Points:** 8
**Priority:** High
**Dependencies:** Epic 6
**Assignee:** Data Engineer

**Description:**
As a data analyst, I need views to trace data lineage from warehouse back to source so that I can troubleshoot data issues.

**Acceptance Criteria:**
- [ ] vw_veteran_lineage_trace view created
- [ ] View joins warehouse → crosswalk → ODS for complete lineage
- [ ] Shows which source system provided each value
- [ ] Shows match confidence and method
- [ ] Shows any conflicts from reconciliation log
- [ ] View performs acceptably (<5 seconds for single veteran)

**Tasks:**
- [ ] Task 8.3.1: Create veteran lineage view (3 pts)
- [ ] Task 8.3.2: Create evaluator lineage view (2 pts)
- [ ] Task 8.3.3: Test lineage queries (2 pts)
- [ ] Task 8.3.4: Document usage examples (1 pt)

**Technical Notes:**
- File: `snowflake/monitoring/data_lineage_queries.sql`

---

### Story 8.4: Create Data Quality Dashboard Views
**Story Points:** 5
**Priority:** Medium
**Dependencies:** Story 8.1, Story 8.2
**Assignee:** Data Analyst

**Description:**
As a data analyst, I need views that summarize data quality metrics so that I can monitor pipeline health.

**Acceptance Criteria:**
- [ ] vw_match_quality_report view created
- [ ] vw_conflict_summary view created
- [ ] vw_data_freshness view created
- [ ] Views show trends over time
- [ ] Views identify records needing attention

**Tasks:**
- [ ] Task 8.4.1: Create match quality view (2 pts)
- [ ] Task 8.4.2: Create conflict summary view (2 pts)
- [ ] Task 8.4.3: Create freshness view (1 pt)

---

## EPIC 9: Business Intelligence Marts (NON-MVP)
**MVP:** No | **Sprint:** 4 | **Total Points:** 21

### Story 9.1: Create Clinical Analytics Mart
**Story Points:** 13
**Priority:** Medium
**Dependencies:** Epic 6
**Assignee:** BI Developer

**Description:**
As a business analyst, I need pre-aggregated views for clinical analytics so that I can quickly analyze evaluator performance and exam request metrics.

**Acceptance Criteria:**
- [ ] vw_evaluator_performance view created
- [ ] vw_exam_request_performance view created
- [ ] vw_qa_performance_metrics view created
- [ ] Views include relevant KPIs and metrics
- [ ] Views perform well (<10 seconds)
- [ ] Documentation includes example queries

**Tasks:**
- [ ] Task 9.1.1: Create evaluator performance view (4 pts)
- [ ] Task 9.1.2: Create exam request performance view (4 pts)
- [ ] Task 9.1.3: Create QA metrics view (3 pts)
- [ ] Task 9.1.4: Optimize view performance (2 pts)

**Technical Notes:**
- File: `snowflake/marts/01_create_marts_clinical.sql`

---

### Story 9.2: Create Operational Analytics Mart
**Story Points:** 8
**Priority:** Medium
**Dependencies:** Epic 6
**Assignee:** BI Developer

**Description:**
As an operations manager, I need views for facility performance and appointment analytics so that I can monitor operational efficiency.

**Acceptance Criteria:**
- [ ] vw_facility_performance_dashboard view created
- [ ] vw_appointment_lifecycle_analytics view created
- [ ] Views aggregated at appropriate grain
- [ ] Metrics aligned with business definitions
- [ ] Views tested with realistic queries

**Tasks:**
- [ ] Task 9.2.1: Create facility performance view (3 pts)
- [ ] Task 9.2.2: Create appointment analytics view (3 pts)
- [ ] Task 9.2.3: Test and validate metrics (2 pts)

---

## EPIC 10: Advanced Monitoring & Observability (NON-MVP)
**MVP:** No | **Sprint:** 4 | **Total Points:** 13

### Story 10.1: Create Comprehensive Lineage Queries
**Story Points:** 5
**Priority:** Medium
**Dependencies:** Story 8.3
**Assignee:** Data Engineer

**Description:**
As a data steward, I need comprehensive lineage queries and procedures so that I can investigate any data issue thoroughly.

**Acceptance Criteria:**
- [ ] sp_compare_source_systems() procedure created
- [ ] vw_veteran_history view created (SCD Type 2 history)
- [ ] vw_batch_lineage view created
- [ ] All monitoring views documented
- [ ] Example investigation workflow documented

**Tasks:**
- [ ] Task 10.1.1: Create source comparison procedure (2 pts)
- [ ] Task 10.1.2: Create history view (2 pts)
- [ ] Task 10.1.3: Document investigation workflows (1 pt)

**Technical Notes:**
- File: `snowflake/monitoring/data_lineage_queries.sql`

---

### Story 10.2: Create Pipeline Health Check Procedure
**Story Points:** 5
**Priority:** Medium
**Dependencies:** Epic 8
**Assignee:** Data Engineer

**Description:**
As a data engineer, I need a health check procedure that validates pipeline status so that I can quickly assess if everything is working.

**Acceptance Criteria:**
- [ ] sp_pipeline_health_check() procedure created
- [ ] Checks for recent successful batches
- [ ] Checks for failed batches in last 24 hours
- [ ] Checks for data errors
- [ ] Checks for low confidence matches
- [ ] Returns status (HEALTHY/WARNING/CRITICAL) for each check

**Tasks:**
- [ ] Task 10.2.1: Create health check procedure (3 pts)
- [ ] Task 10.2.2: Test all health check scenarios (2 pts)

---

### Story 10.3: Setup Alerting and Notifications
**Story Points:** 3
**Priority:** Low
**Dependencies:** Story 10.2
**Assignee:** DevOps Engineer

**Description:**
As a DevOps engineer, I need automated alerts for pipeline issues so that problems are detected and addressed quickly.

**Acceptance Criteria:**
- [ ] Email alerts configured for ETL failures
- [ ] Alerts configured for data quality thresholds
- [ ] Alert notification list includes appropriate stakeholders
- [ ] Alert testing completed

**Tasks:**
- [ ] Task 10.3.1: Configure email alerts (2 pts)
- [ ] Task 10.3.2: Test alerting (1 pt)

---

## EPIC 11: Documentation & Training
**MVP:** Partial | **Sprint:** 1-4 | **Total Points:** 13

### Story 11.1: Create Technical Documentation
**Story Points:** 5
**Priority:** High
**Dependencies:** Epic 4, Epic 5, Epic 6
**Assignee:** Technical Writer + Data Architect

**Description:**
As a developer, I need comprehensive technical documentation so that I can understand and maintain the data pipeline.

**Acceptance Criteria:**
- [ ] Architecture diagram created and published
- [ ] DATA_PIPELINE_ARCHITECTURE.md completed
- [ ] All ETL procedures documented with comments
- [ ] Data lineage documentation completed
- [ ] Troubleshooting guide created

**Tasks:**
- [ ] Task 11.1.1: Create architecture documentation (2 pts)
- [ ] Task 11.1.2: Document ETL procedures (2 pts)
- [ ] Task 11.1.3: Create troubleshooting guide (1 pt)

**Technical Notes:**
- Files: `DATA_PIPELINE_ARCHITECTURE.md`, inline SQL comments

---

### Story 11.2: Create Interactive User Guide
**Story Points:** 3
**Priority:** Medium
**Dependencies:** Story 11.1
**Assignee:** Technical Writer

**Description:**
As a business user, I need an easy-to-understand guide explaining the multi-source integration so that I can understand how the data is processed.

**Acceptance Criteria:**
- [ ] Interactive HTML guide created
- [ ] Guide covers all major concepts (OMS vs VEMS, reconciliation, etc.)
- [ ] Guide includes visual diagrams
- [ ] Guide includes code examples
- [ ] Guide reviewed by stakeholders

**Tasks:**
- [ ] Task 11.2.1: Create HTML guide (2 pts)
- [ ] Task 11.2.2: Review and refine with stakeholders (1 pt)

**Technical Notes:**
- File: `VES_Multi_Source_Integration_Guide.html`

---

### Story 11.3: Conduct Training Sessions
**Story Points:** 5
**Priority:** High
**Dependencies:** Story 11.1, Story 11.2
**Assignee:** Data Architect + Business Analyst

**Description:**
As a business analyst, I need training on how to use the new data warehouse so that I can effectively query and analyze the data.

**Acceptance Criteria:**
- [ ] Training materials prepared
- [ ] Data analyst training session conducted
- [ ] Developer training session conducted
- [ ] Business stakeholder overview session conducted
- [ ] Training recordings available
- [ ] Q&A documented

**Tasks:**
- [ ] Task 11.3.1: Prepare training materials (2 pts)
- [ ] Task 11.3.2: Conduct analyst training (1 pt)
- [ ] Task 11.3.3: Conduct developer training (1 pt)
- [ ] Task 11.3.4: Conduct stakeholder overview (1 pt)

---

# Dependency Matrix

| Epic | Depends On | Blocking |
|------|------------|----------|
| Epic 1 | None | Epic 2, 3, 4, 5 |
| Epic 2 | Epic 1 | Epic 4, 6 |
| Epic 3 | Epic 1 | Epic 4 |
| Epic 4 | Epic 2, 3 | Epic 5, 6 |
| Epic 5 | Epic 4 | Epic 6, 9 |
| Epic 6 | Epic 5 | Epic 9 |
| Epic 7 | Epic 5, 6 | None |
| Epic 8 | Epic 4, 6 | Epic 10 |
| Epic 9 | Epic 6 | None |
| Epic 10 | Epic 8 | None |
| Epic 11 | Epic 4, 5, 6 | None |

---

# Sprint Planning

## Sprint 1 (Weeks 1-2): Foundation
**Goal:** Build foundational infrastructure and ODS layer

**Epics:**
- Epic 1: Infrastructure & Foundation Setup (21 pts)
- Epic 2: ODS Layer & Multi-Source Ingestion (34 pts)
- Epic 3: Reference Data Framework (partial, 13 pts)

**Total Story Points:** 68
**Key Deliverables:**
- Snowflake environment configured
- All ODS tables created
- Mulesoft integration tested
- System of record defined
- Initial reference tables created

**Success Criteria:**
- [ ] Can load OMS data into ODS
- [ ] Can load VEMS data into ODS
- [ ] Batch control logging working

---

## Sprint 2 (Weeks 3-4): Data Reconciliation
**Goal:** Build staging layer with multi-source reconciliation

**Epics:**
- Epic 3: Reference Data Framework (complete remaining, 8 pts)
- Epic 4: Staging Layer & Data Reconciliation (34 pts)
- Epic 5: Core Dimensional Model (partial, 21 pts)

**Total Story Points:** 63
**Key Deliverables:**
- All reference tables populated
- Entity matching procedures working
- Multi-source transformation procedures complete
- Core dimension tables created
- Dimension load procedures working

**Success Criteria:**
- [ ] Veterans matched between OMS and VEMS
- [ ] Merged veteran data in staging
- [ ] dim_veterans loaded with SCD Type 2

---

## Sprint 3 (Weeks 5-6): Facts & Orchestration
**Goal:** Complete fact tables and automated ETL pipeline

**Epics:**
- Epic 5: Core Dimensional Model (complete remaining, 13 pts)
- Epic 6: Fact Tables & Metrics (34 pts)
- Epic 7: ETL Orchestration & Automation (21 pts)
- Epic 8: Data Quality & Lineage (21 pts)

**Total Story Points:** 89
**Key Deliverables:**
- All core fact tables created
- Fact transformation and load procedures complete
- Master ETL orchestration working
- Scheduled jobs configured
- Basic lineage and monitoring

**Success Criteria:**
- [ ] End-to-end ETL runs successfully
- [ ] Fact tables populated with correct data
- [ ] Can trace data lineage
- [ ] Automated daily ETL working

---

## Sprint 4 (Weeks 7-8): Enhancement & Polish
**Goal:** Add marts, advanced monitoring, finalize documentation

**Epics:**
- Epic 9: Business Intelligence Marts (21 pts)
- Epic 10: Advanced Monitoring & Observability (13 pts)
- Epic 11: Documentation & Training (complete remaining, 8 pts)

**Total Story Points:** 42
**Key Deliverables:**
- Clinical and operational marts
- Comprehensive monitoring views
- Health check procedures
- Complete documentation
- User training conducted

**Success Criteria:**
- [ ] Business users can query marts
- [ ] Monitoring dashboard functional
- [ ] All documentation complete
- [ ] Training sessions delivered

---

# Risk Management

## High-Risk Items

| Risk | Impact | Mitigation | Owner |
|------|--------|------------|-------|
| **Mulesoft integration delays** | Critical - blocks ODS loading | Early coordination, weekly sync meetings, fallback to manual loads | Integration Engineer |
| **OMS and VEMS schema differences larger than expected** | High - adds transformation complexity | Dedicate Sprint 1 to thorough analysis, buffer in estimates | Data Architect |
| **Matching algorithm produces low-quality matches** | High - data quality issues | Implement match confidence scoring early, manual review process | Data Engineer + SME |
| **Performance issues with large data volumes** | Medium - slower ETL | Implement clustering early, test with production-like volumes | Data Engineer |
| **Scope creep from stakeholders** | Medium - delays delivery | Clear MVP definition, change control process | Project Manager |
| **Resource availability** | Medium - delays | Cross-train team members, document as you go | Project Manager |

---

# Success Metrics

## MVP Success Criteria (End of Sprint 3)

| Metric | Target | Validation |
|--------|--------|------------|
| **Data Completeness** | 95%+ veterans matched between OMS/VEMS | Query crosswalk table |
| **Data Quality** | <5% conflicts needing manual review | Query reconciliation log |
| **ETL Performance** | Full pipeline completes in <2 hours | Monitor batch control |
| **Match Confidence** | 90%+ matches at 100% confidence | Query match quality view |
| **Lineage Coverage** | 100% fact records traceable to source | Sample lineage queries |
| **Automated Execution** | Scheduled ETL runs successfully daily | Monitor task history |

## Post-MVP Success Criteria (End of Sprint 4)

| Metric | Target | Validation |
|--------|--------|------------|
| **Mart Adoption** | 5+ business users querying marts weekly | Snowflake query history |
| **Monitoring** | Health check runs daily with no failures | Task logs |
| **Documentation** | 100% of procedures documented | Code review |
| **Training** | 15+ users trained | Training attendance |

---

# Team Composition

| Role | Allocation | Key Responsibilities |
|------|------------|---------------------|
| **Data Architect** | 50% | Design decisions, standards, review |
| **Senior Data Engineer** | 100% | ETL development, procedure creation |
| **Data Engineer** | 100% | Table creation, testing, deployment |
| **Integration Engineer** | 50% | Mulesoft coordination, ODS loading |
| **BI Developer** | 50% | Marts creation, query optimization |
| **Data Analyst** | 50% | Reference data, code mappings, testing |
| **DevOps Engineer** | 25% | Deployment automation, scheduling, monitoring |
| **Business Analyst** | 25% | Requirements, UAT, training |
| **Technical Writer** | 25% | Documentation, user guides |

---

# Notes for Jira Import

## Recommended Jira Configuration

**Custom Fields:**
- `MVP Flag` (Yes/No)
- `Sprint Target`
- `Technical Complexity` (Low/Medium/High)
- `Data Source` (OMS/VEMS/Both)

**Issue Types:**
- Epic
- Story
- Task
- Bug
- Documentation

**Workflow States:**
- To Do
- In Progress
- Code Review
- Testing
- Done

**Story Point Scale:** Fibonacci (1, 2, 3, 5, 8, 13, 21)

## Import Instructions

1. Create Project: "VES Multi-Source Data Warehouse"
2. Import Epics (11 total)
3. Import Stories under respective Epics
4. Import Tasks under respective Stories
5. Set dependencies using "Blocks" link type
6. Assign to sprints based on Sprint Planning section
7. Tag MVP items appropriately

---

**Document Version:** 1.0
**Last Updated:** 2025-11-17
**Owner:** Data Engineering Team
