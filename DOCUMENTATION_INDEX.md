# VES Data Warehouse - Complete Documentation Index

**Master Navigation Guide for All Documentation Resources**

Last Updated: 2024-11-18
Total Documentation: 456 pages
Total Documents: 27 files

---

## Table of Contents

- [Quick Navigation by Role](#quick-navigation-by-role)
- [Quick Navigation by Task](#quick-navigation-by-task)
- [Complete Alphabetical Index](#complete-alphabetical-index)
- [Documentation by Category](#documentation-by-category)
- [SQL Scripts Index](#sql-scripts-index)
- [Documentation Reading Paths](#documentation-reading-paths)

---

## Quick Navigation by Role

### 🆕 New Hire / Junior Developer

**Your First Day:**
1. [README.md](README.md) - Start here for overview (10 min read)
2. [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md) - Your 5-day program

**Your First Week:**
- Day 1: Environment setup and repository tour
- Day 2: [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Understand the data model
- Day 3: [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) - Learn Snowflake basics
- Day 4: [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Daily operations
- Day 5: Build your first feature!

**Reference During Development:**
- [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - When things go wrong
- [NAMING_CONVENTION_ALIGNMENT_REPORT.md](NAMING_CONVENTION_ALIGNMENT_REPORT.md) - Coding standards

### 👨‍💼 Data Engineer / ETL Developer

**Essential Reading (in order):**
1. [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) - End-to-end architecture (1 hour)
2. [STAGING_LAYER_IMPLEMENTATION_GUIDE.md](STAGING_LAYER_IMPLEMENTATION_GUIDE.md) - Multi-source integration (45 min)
3. [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) - Tasks & dependencies (45 min)
4. [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md) - CDC patterns (30 min)
5. [SCD_TYPE2_DESIGN_GUIDE.md](SCD_TYPE2_DESIGN_GUIDE.md) - Dimension management (30 min)

**Quick References:**
- [ORCHESTRATION_QUICKSTART_CHECKLIST.md](ORCHESTRATION_QUICKSTART_CHECKLIST.md) - Task setup checklist
- [BOTTLENECK_DETECTION_GUIDE.md](BOTTLENECK_DETECTION_GUIDE.md) - Performance analysis

**Operations:**
- [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Daily/weekly tasks
- [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - Problem solving

**Advanced Topics:**
- [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Tuning & cost reduction
- [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) - DR procedures

### 🔍 Data Analyst / BI Developer

**Get Started:**
1. [README.md](README.md) - Solution overview (10 min)
2. [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Complete data model (1 hour)
3. [ER_DIAGRAM.md](ER_DIAGRAM.md) - Visual relationships (15 min)

**Build Reports:**
- [snowflake/marts/02_executive_analytics_dashboard.sql](snowflake/marts/02_executive_analytics_dashboard.sql) - Executive dashboard views
- [snowflake/marts/01_create_marts_clinical.sql](snowflake/marts/01_create_marts_clinical.sql) - Clinical analytics

**Performance:**
- [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Query optimization (focus on Section 3)

**Find Data:**
- [DATA_DICTIONARY.md](DATA_DICTIONARY.md) - All tables and columns (coming)
- [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Table definitions

### ⚙️ Operations / DBA / DevOps

**Daily Operations:**
1. [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - **YOUR BIBLE** (read fully)
   - Section 1: Daily health check (run every morning at 9 AM)
   - Section 2: Weekly maintenance
   - Section 3: Monthly tasks
   - Section 4: Incident response

**When Things Break:**
1. [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - **YOUR FIRST STOP** (50+ scenarios)
   - Pipeline failures (Section 1)
   - Data quality issues (Section 2)
   - Performance problems (Section 3)
   - Task failures (Section 4)
   - Emergency procedures (Section 9)

**Disaster Recovery:**
1. [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md)
   - Backup strategy (Section 2)
   - Recovery procedures (Section 3)
   - DR testing (Section 6)

**Performance Tuning:**
1. [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md)
   - Clustering (Section 1)
   - Warehouse sizing (Section 4)
   - Cost optimization (Section 5)

**Monitoring:**
- [snowflake/monitoring/00_comprehensive_monitoring_dashboard.sql](snowflake/monitoring/00_comprehensive_monitoring_dashboard.sql)
- [snowflake/quality/00_advanced_data_quality_framework.sql](snowflake/quality/00_advanced_data_quality_framework.sql)

### 👔 Manager / Architect / Technical Lead

**Strategic Overview:**
1. [README.md](README.md) - Executive summary & success metrics (15 min)
2. [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) - Architecture overview (1 hour)
3. [PRODUCT_READINESS_ASSESSMENT.md](PRODUCT_READINESS_ASSESSMENT.md) - Production readiness (30 min)

**Planning & Roadmap:**
1. [JIRA_PROJECT_PLAN.md](JIRA_PROJECT_PLAN.md) - Epic and story breakdown
2. [PROJECT_ROADMAP_VISUAL.md](PROJECT_ROADMAP_VISUAL.md) - Visual timeline
3. [QA_AUTOMATION_ROADMAP.md](QA_AUTOMATION_ROADMAP.md) - Quality automation plan

**Design Documentation:**
1. [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Complete data model
2. [ER_DIAGRAM.md](ER_DIAGRAM.md) - Entity relationships
3. [PROCESS_FLOW_GAP_ANALYSIS.md](PROCESS_FLOW_GAP_ANALYSIS.md) - Gap analysis

**Operations & Governance:**
1. [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Operational runbook
2. [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) - DR plan
3. [NAMING_CONVENTION_ALIGNMENT_REPORT.md](NAMING_CONVENTION_ALIGNMENT_REPORT.md) - Standards

**Team Enablement:**
1. [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md) - New hire program
2. [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) - Training material

### 🎓 Learning / Transitioning from SQL Server/Redshift

**Your Learning Path:**
1. [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) - **START HERE** (70 pages)
   - Section 2: Key paradigm shifts
   - Section 3: Architecture fundamentals
   - Section 4: SQL differences (50+ examples)
   - Section 5: Stored procedures
   - Section 6: Performance tuning
   - Section 7: Data loading
   - Section 8: Cost management

2. [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Apply knowledge
3. [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md) - CDC (unique to Snowflake)
4. [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) - Tasks (replaces SQL Server Agent)

**Hands-On Practice:**
- [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md) - Day 3 exercises

---

## Quick Navigation by Task

### I Need To...

**Deploy the solution**
→ [README.md](README.md) - Section: Deployment Guide
→ [DEPLOYMENT_VALIDATION.sql](DEPLOYMENT_VALIDATION.sql) - Post-deployment validation

**Run daily health checks**
→ [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Section 1

**Fix a pipeline failure**
→ [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - Section 1

**Fix data quality issues**
→ [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - Section 2

**Optimize a slow query**
→ [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Section 3
→ [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - Section 3

**Reduce Snowflake costs**
→ [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Section 5

**Recover from disaster**
→ [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) - Section 3

**Understand a specific table**
→ [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Section 3 (Dimensions) or 4 (Facts)
→ [DATA_DICTIONARY.md](DATA_DICTIONARY.md) - Coming soon

**Create a new ETL job**
→ [STAGING_LAYER_IMPLEMENTATION_GUIDE.md](STAGING_LAYER_IMPLEMENTATION_GUIDE.md)
→ [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md)

**Set up CDC/incremental loading**
→ [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md)

**Implement SCD Type 2**
→ [SCD_TYPE2_DESIGN_GUIDE.md](SCD_TYPE2_DESIGN_GUIDE.md)

**Onboard a new team member**
→ [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md)

**Build an executive report**
→ [snowflake/marts/02_executive_analytics_dashboard.sql](snowflake/marts/02_executive_analytics_dashboard.sql)

**Create diagrams**
→ [DIAGRAM_TEMPLATES.md](DIAGRAM_TEMPLATES.md)
→ [LUCIDCHART_GUIDE.md](LUCIDCHART_GUIDE.md)

**Respond to an incident**
→ [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Section 4

**Plan a project**
→ [JIRA_PROJECT_PLAN.md](JIRA_PROJECT_PLAN.md)

---

## Complete Alphabetical Index

| Document | Pages | Category | Keywords |
|----------|-------|----------|----------|
| [APPOINTMENT_LIFECYCLE_DESIGN.md](APPOINTMENT_LIFECYCLE_DESIGN.md) | 12 | Design | appointment, scheduling, workflow |
| [BOTTLENECK_DETECTION_GUIDE.md](BOTTLENECK_DETECTION_GUIDE.md) | 15 | Implementation | performance, bottleneck, analysis |
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | TBD | Reference | tables, columns, metadata |
| [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) | 25 | Architecture | pipeline, architecture, flow |
| [DEPLOYMENT_VALIDATION.sql](DEPLOYMENT_VALIDATION.sql) | N/A | Deployment | validation, testing, health check |
| [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md) | 68 | Getting Started | onboarding, training, new hire |
| [DIAGRAM_TEMPLATES.md](DIAGRAM_TEMPLATES.md) | 8 | Visualization | diagrams, mermaid, templates |
| [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) | 35 | Architecture | data model, dimensions, facts |
| [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) | 50 | Operations | disaster recovery, backup, failover |
| [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) | This file | Navigation | index, navigation, catalog |
| [ER_DIAGRAM.md](ER_DIAGRAM.md) | 10 | Architecture | relationships, ER diagram, visual |
| [EVALUATION_QA_LIFECYCLE_DESIGN.md](EVALUATION_QA_LIFECYCLE_DESIGN.md) | 10 | Design | QA, evaluation, workflow |
| [JIRA_PROJECT_PLAN.md](JIRA_PROJECT_PLAN.md) | 15 | Planning | project plan, JIRA, epics |
| [LUCIDCHART_GUIDE.md](LUCIDCHART_GUIDE.md) | 6 | Visualization | lucidchart, diagramming |
| [NAMING_CONVENTION_ALIGNMENT_REPORT.md](NAMING_CONVENTION_ALIGNMENT_REPORT.md) | 8 | Governance | naming, standards, conventions |
| [ORCHESTRATION_QUICKSTART_CHECKLIST.md](ORCHESTRATION_QUICKSTART_CHECKLIST.md) | 4 | Implementation | tasks, orchestration, quickstart |
| [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) | 65 | Operations | performance, optimization, cost |
| [PROCESS_FLOW_GAP_ANALYSIS.md](PROCESS_FLOW_GAP_ANALYSIS.md) | 12 | Planning | gap analysis, process flow |
| [PRODUCT_READINESS_ASSESSMENT.md](PRODUCT_READINESS_ASSESSMENT.md) | 10 | Planning | readiness, production, assessment |
| [PROJECT_ROADMAP_VISUAL.md](PROJECT_ROADMAP_VISUAL.md) | 8 | Planning | roadmap, timeline, milestones |
| [QA_AUTOMATION_ROADMAP.md](QA_AUTOMATION_ROADMAP.md) | 12 | Planning | QA, automation, testing |
| [README.md](README.md) | 15 | Getting Started | overview, quickstart, deployment |
| [SCD_TYPE2_DESIGN_GUIDE.md](SCD_TYPE2_DESIGN_GUIDE.md) | 10 | Implementation | SCD Type 2, dimensions, history |
| [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) | 70 | Getting Started | Snowflake, SQL, transition |
| [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) | 18 | Implementation | tasks, orchestration, scheduling |
| [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md) | 12 | Implementation | streams, CDC, incremental |
| [STAGING_LAYER_IMPLEMENTATION_GUIDE.md](STAGING_LAYER_IMPLEMENTATION_GUIDE.md) | 20 | Implementation | staging, integration, multi-source |
| [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) | 55 | Operations | SOP, operations, daily tasks |
| [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) | 48 | Operations | troubleshooting, problems, solutions |
| [VES_PROCESS_FLOW_REFERENCE.md](VES_PROCESS_FLOW_REFERENCE.md) | 14 | Reference | process flow, business process |
| **TOTAL** | **456** | | |

---

## Documentation by Category

### 🎯 Getting Started (3 documents, 153 pages)

Perfect for new team members and those unfamiliar with the solution.

| Document | Pages | Time to Read | Purpose |
|----------|-------|--------------|---------|
| [README.md](README.md) | 15 | 15-30 min | Solution overview, quick start, deployment |
| [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md) | 68 | 5 days | Structured 5-day onboarding program |
| [SNOWFLAKE_DEVELOPER_GUIDE.md](SNOWFLAKE_DEVELOPER_GUIDE.md) | 70 | 4-6 hours | SQL Server/Redshift → Snowflake transition |

### 📊 Architecture & Design (5 documents, 85 pages)

Understand how the solution is designed and why.

| Document | Pages | Time to Read | Purpose |
|----------|-------|--------------|---------|
| [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) | 35 | 2-3 hours | Complete data model reference |
| [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) | 25 | 1-2 hours | End-to-end architecture |
| [ER_DIAGRAM.md](ER_DIAGRAM.md) | 10 | 30 min | Entity relationships |
| [APPOINTMENT_LIFECYCLE_DESIGN.md](APPOINTMENT_LIFECYCLE_DESIGN.md) | 12 | 30 min | Appointment workflow design |
| [EVALUATION_QA_LIFECYCLE_DESIGN.md](EVALUATION_QA_LIFECYCLE_DESIGN.md) | 10 | 30 min | QA workflow design |
| [SCD_TYPE2_DESIGN_GUIDE.md](SCD_TYPE2_DESIGN_GUIDE.md) | 10 | 30 min | Slowly Changing Dimension patterns |

### 🔧 Implementation Guides (4 documents, 65 pages)

Step-by-step guides for building and extending the solution.

| Document | Pages | Time to Read | Purpose |
|----------|-------|--------------|---------|
| [STAGING_LAYER_IMPLEMENTATION_GUIDE.md](STAGING_LAYER_IMPLEMENTATION_GUIDE.md) | 20 | 1 hour | Multi-source integration |
| [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) | 18 | 1 hour | Tasks, streams, dependencies |
| [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md) | 12 | 45 min | CDC and incremental processing |
| [BOTTLENECK_DETECTION_GUIDE.md](BOTTLENECK_DETECTION_GUIDE.md) | 15 | 45 min | Performance bottleneck analysis |
| [ORCHESTRATION_QUICKSTART_CHECKLIST.md](ORCHESTRATION_QUICKSTART_CHECKLIST.md) | 4 | 10 min | Quick reference checklist |

### 🚀 Operations & Monitoring (3 documents, 163 pages)

Day-to-day operations, troubleshooting, and disaster recovery.

| Document | Pages | Time to Read | Purpose |
|----------|-------|--------------|---------|
| [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) | 55 | 3-4 hours | Daily/weekly/monthly operations |
| [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) | 48 | 2-3 hours | 50+ problem scenarios |
| [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) | 50 | 3-4 hours | DR/BC plan with procedures |
| [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) | 65 | 4-5 hours | Optimization & cost reduction |

### 📋 Planning & Governance (5 documents, 53 pages)

Project planning, standards, and assessments.

| Document | Pages | Time to Read | Purpose |
|----------|-------|--------------|---------|
| [JIRA_PROJECT_PLAN.md](JIRA_PROJECT_PLAN.md) | 15 | 45 min | Epic and story breakdown |
| [PROJECT_ROADMAP_VISUAL.md](PROJECT_ROADMAP_VISUAL.md) | 8 | 30 min | Visual roadmap |
| [QA_AUTOMATION_ROADMAP.md](QA_AUTOMATION_ROADMAP.md) | 12 | 45 min | Quality automation plan |
| [PRODUCT_READINESS_ASSESSMENT.md](PRODUCT_READINESS_ASSESSMENT.md) | 10 | 30 min | Production readiness checklist |
| [NAMING_CONVENTION_ALIGNMENT_REPORT.md](NAMING_CONVENTION_ALIGNMENT_REPORT.md) | 8 | 20 min | Naming standards |
| [PROCESS_FLOW_GAP_ANALYSIS.md](PROCESS_FLOW_GAP_ANALYSIS.md) | 12 | 45 min | Gap analysis |

### 🎨 Visualization & Diagramming (3 documents, 28 pages)

Tools and templates for creating diagrams.

| Document | Pages | Time to Read | Purpose |
|----------|-------|--------------|---------|
| [DIAGRAM_TEMPLATES.md](DIAGRAM_TEMPLATES.md) | 8 | 30 min | Mermaid diagram templates |
| [LUCIDCHART_GUIDE.md](LUCIDCHART_GUIDE.md) | 6 | 20 min | Lucidchart integration |
| [VES_PROCESS_FLOW_REFERENCE.md](VES_PROCESS_FLOW_REFERENCE.md) | 14 | 45 min | Process flow documentation |

---

## SQL Scripts Index

### Core Implementation

| Script | Purpose | Run Order |
|--------|---------|-----------|
| [snowflake/schema/00_setup_database.sql](snowflake/schema/00_setup_database.sql) | Database & schema creation | 1 |
| [snowflake/schema/02_master_deployment.sql](snowflake/schema/02_master_deployment.sql) | **MASTER DEPLOYMENT** (runs everything) | 1 (all-in-one) |
| [snowflake/schema/01_populate_date_dimension.sql](snowflake/schema/01_populate_date_dimension.sql) | Populate dim_date (10 years) | 2 |

### Dimensions (9 files)

| Script | Table Created |
|--------|---------------|
| [snowflake/dimensions/01_dim_date.sql](snowflake/dimensions/01_dim_date.sql) | dim_date |
| [snowflake/dimensions/02_dim_veteran.sql](snowflake/dimensions/02_dim_veteran.sql) | dim_veteran (SCD Type 2) |
| [snowflake/dimensions/03_dim_evaluator.sql](snowflake/dimensions/03_dim_evaluator.sql) | dim_evaluator (SCD Type 2) |
| [snowflake/dimensions/04_dim_facility.sql](snowflake/dimensions/04_dim_facility.sql) | dim_facility (SCD Type 2) |
| [snowflake/dimensions/05_dim_evaluation_type.sql](snowflake/dimensions/05_dim_evaluation_type.sql) | dim_evaluation_type |
| [snowflake/dimensions/06_dim_medical_condition.sql](snowflake/dimensions/06_dim_medical_condition.sql) | dim_medical_condition |
| [snowflake/dimensions/07_dim_claim.sql](snowflake/dimensions/07_dim_claim.sql) | dim_claim (SCD Type 2) |
| [snowflake/dimensions/08_dim_appointment.sql](snowflake/dimensions/08_dim_appointment.sql) | dim_appointment |
| [snowflake/dimensions/09_dim_exam_request_types.sql](snowflake/dimensions/09_dim_exam_request_types.sql) | dim_exam_request_types |

### Facts (9 files)

| Script | Table Created | Grain Type |
|--------|---------------|------------|
| [snowflake/facts/01_fact_evaluation.sql](snowflake/facts/01_fact_evaluation.sql) | fact_evaluation | Transaction |
| [snowflake/facts/02_fact_claim_status.sql](snowflake/facts/02_fact_claim_status.sql) | fact_claim_status | Accumulating Snapshot |
| [snowflake/facts/03_fact_appointment.sql](snowflake/facts/03_fact_appointment.sql) | fact_appointment | Transaction |
| [snowflake/facts/04_fact_daily_snapshot.sql](snowflake/facts/04_fact_daily_snapshot.sql) | fact_daily_snapshot | Periodic Snapshot |
| [snowflake/facts/05_fact_appointment_events.sql](snowflake/facts/05_fact_appointment_events.sql) | fact_appointment_events | Transaction |
| [snowflake/facts/06_fact_evaluation_qa_events.sql](snowflake/facts/06_fact_evaluation_qa_events.sql) | fact_evaluation_qa_events | Transaction |
| [snowflake/facts/07_fact_exam_requests.sql](snowflake/facts/07_fact_exam_requests.sql) | fact_exam_requests | Accumulating Snapshot |
| [snowflake/facts/08_fact_examiner_assignments.sql](snowflake/facts/08_fact_examiner_assignments.sql) | fact_examiner_assignments | Transaction |
| [snowflake/facts/09_fact_exam_processing_bottlenecks.sql](snowflake/facts/09_fact_exam_processing_bottlenecks.sql) | fact_exam_processing_bottlenecks | Analysis |

### ETL & Orchestration

| Script | Purpose |
|--------|---------|
| [snowflake/ods/01_create_ods_tables.sql](snowflake/ods/01_create_ods_tables.sql) | ODS layer tables |
| [snowflake/staging/01_create_staging_tables.sql](snowflake/staging/01_create_staging_tables.sql) | Staging layer tables |
| [snowflake/staging/02_staging_layer_oms_vems_merge_simplified.sql](snowflake/staging/02_staging_layer_oms_vems_merge_simplified.sql) | Multi-source merge logic |
| [snowflake/etl/01_etl_procedures_dimensions.sql](snowflake/etl/01_etl_procedures_dimensions.sql) | Dimension load procedures |
| [snowflake/etl/02_etl_procedures_facts.sql](snowflake/etl/02_etl_procedures_facts.sql) | Fact load procedures |
| [snowflake/etl/03_etl_procedures_multi_source.sql](snowflake/etl/03_etl_procedures_multi_source.sql) | Cross-source integration |
| [snowflake/orchestration/01_snowflake_native_orchestration.sql](snowflake/orchestration/01_snowflake_native_orchestration.sql) | Tasks, streams, notifications |

### Monitoring & Quality

| Script | Purpose |
|--------|---------|
| [snowflake/monitoring/00_comprehensive_monitoring_dashboard.sql](snowflake/monitoring/00_comprehensive_monitoring_dashboard.sql) | Real-time monitoring (5 tables, 5 views, 2 tasks) |
| [snowflake/quality/00_advanced_data_quality_framework.sql](snowflake/quality/00_advanced_data_quality_framework.sql) | Data quality (40+ rules, anomaly detection) |
| [snowflake/monitoring/data_lineage_queries.sql](snowflake/monitoring/data_lineage_queries.sql) | Impact analysis queries |
| [snowflake/monitoring/bottleneck_analysis_queries.sql](snowflake/monitoring/bottleneck_analysis_queries.sql) | Performance bottleneck queries |
| [snowflake/monitoring/staging_layer_validation_queries.sql](snowflake/monitoring/staging_layer_validation_queries.sql) | Staging validation queries |

### Analytics & Marts

| Script | Purpose |
|--------|---------|
| [snowflake/marts/01_create_marts_clinical.sql](snowflake/marts/01_create_marts_clinical.sql) | Clinical analytics mart |
| [snowflake/marts/02_executive_analytics_dashboard.sql](snowflake/marts/02_executive_analytics_dashboard.sql) | Executive KPIs (6 views, 2 MVs, 1 task) |

### Reference & Testing

| Script | Purpose |
|--------|---------|
| [snowflake/reference/01_create_reference_tables.sql](snowflake/reference/01_create_reference_tables.sql) | Reference/lookup tables |
| [snowflake/testing/01_create_qa_framework.sql](snowflake/testing/01_create_qa_framework.sql) | Automated testing framework |

### Deployment & Validation

| Script | Purpose |
|--------|---------|
| [DEPLOYMENT_VALIDATION.sql](DEPLOYMENT_VALIDATION.sql) | Post-deployment validation (35 tests) |

---

## Documentation Reading Paths

### Path 1: "I Need to Deploy This in 1 Day"

**Time Required: 4-6 hours**

1. [README.md](README.md) - Overview & deployment (30 min)
2. [README.md](README.md) - Section: Deployment Guide (30 min)
3. Run deployment: `snowflake/schema/02_master_deployment.sql` (15 min)
4. [DEPLOYMENT_VALIDATION.sql](DEPLOYMENT_VALIDATION.sql) - Validate (5 min)
5. [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - Section 1 only (30 min)
6. [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - Skim for emergencies (30 min)

**You're now operational!** Continue learning while system runs.

### Path 2: "I'm a New Developer - 5 Day Program"

**Time Required: 5 days (40 hours)**

Follow [DEVELOPER_ONBOARDING_GUIDE.md](DEVELOPER_ONBOARDING_GUIDE.md) exactly as written.

- Day 1: Environment & overview
- Day 2: Architecture deep dive
- Day 3: Hands-on Snowflake
- Day 4: Data quality & monitoring
- Day 5: Build first feature

### Path 3: "I'm an Architect - Deep Understanding"

**Time Required: 2-3 weeks**

**Week 1: Architecture & Design**
1. [README.md](README.md) - 30 min
2. [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) - 2 hours
3. [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - 4 hours
4. [ER_DIAGRAM.md](ER_DIAGRAM.md) - 1 hour
5. [SCD_TYPE2_DESIGN_GUIDE.md](SCD_TYPE2_DESIGN_GUIDE.md) - 1 hour
6. [APPOINTMENT_LIFECYCLE_DESIGN.md](APPOINTMENT_LIFECYCLE_DESIGN.md) - 1 hour
7. [EVALUATION_QA_LIFECYCLE_DESIGN.md](EVALUATION_QA_LIFECYCLE_DESIGN.md) - 1 hour

**Week 2: Implementation & Operations**
1. [STAGING_LAYER_IMPLEMENTATION_GUIDE.md](STAGING_LAYER_IMPLEMENTATION_GUIDE.md) - 2 hours
2. [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) - 2 hours
3. [SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md](SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md) - 1 hour
4. [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - 4 hours
5. [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - 4 hours

**Week 3: Advanced Topics & Planning**
1. [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) - 4 hours
2. [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - 3 hours
3. [JIRA_PROJECT_PLAN.md](JIRA_PROJECT_PLAN.md) - 1 hour
4. [PROJECT_ROADMAP_VISUAL.md](PROJECT_ROADMAP_VISUAL.md) - 30 min
5. [PRODUCT_READINESS_ASSESSMENT.md](PRODUCT_READINESS_ASSESSMENT.md) - 1 hour

### Path 4: "I'm Operations - Make Me Operational ASAP"

**Time Required: 2-3 days**

**Day 1: Critical Operations (8 hours)**
1. [README.md](README.md) - Quick start & operations section (1 hour)
2. [STANDARD_OPERATING_PROCEDURES.md](STANDARD_OPERATING_PROCEDURES.md) - **READ FULLY** (4 hours)
3. [TROUBLESHOOTING_PLAYBOOK.md](TROUBLESHOOTING_PLAYBOOK.md) - **READ FULLY** (3 hours)

**Day 2: Disaster Recovery & Performance (8 hours)**
1. [DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md](DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md) - Focus on Sections 2-3 (4 hours)
2. [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Sections 4-5 (2 hours)
3. Practice: Run morning health check from SOP (1 hour)
4. Practice: Simulate incident response (1 hour)

**Day 3: Architecture Understanding (4 hours)**
1. [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Overview only (1 hour)
2. [DATA_PIPELINE_ARCHITECTURE.md](DATA_PIPELINE_ARCHITECTURE.md) - Data flow section (1 hour)
3. [SNOWFLAKE_ORCHESTRATION_GUIDE.md](SNOWFLAKE_ORCHESTRATION_GUIDE.md) - Tasks & scheduling (2 hours)

**You're now operationally ready!**

### Path 5: "I'm an Analyst - I Need to Query Data"

**Time Required: 1 day**

**Morning (4 hours)**
1. [README.md](README.md) - Overview (30 min)
2. [DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Sections 3-4 (3 hours)
3. [ER_DIAGRAM.md](ER_DIAGRAM.md) - Visual understanding (30 min)

**Afternoon (4 hours)**
1. Review SQL scripts: [snowflake/marts/02_executive_analytics_dashboard.sql](snowflake/marts/02_executive_analytics_dashboard.sql) (2 hours)
2. [PERFORMANCE_OPTIMIZATION_GUIDE.md](PERFORMANCE_OPTIMIZATION_GUIDE.md) - Section 3 only (1 hour)
3. Practice: Write 5 sample queries (1 hour)

**You're ready to analyze!**

---

## Document Dependencies

Some documents reference others. Here's the dependency map:

```
README.md (no dependencies - start here!)
│
├─→ DEVELOPER_ONBOARDING_GUIDE.md
│   ├─→ DIMENSIONAL_MODEL_DOCUMENTATION.md
│   ├─→ SNOWFLAKE_DEVELOPER_GUIDE.md
│   └─→ STANDARD_OPERATING_PROCEDURES.md
│
├─→ DATA_PIPELINE_ARCHITECTURE.md
│   ├─→ STAGING_LAYER_IMPLEMENTATION_GUIDE.md
│   ├─→ SNOWFLAKE_ORCHESTRATION_GUIDE.md
│   └─→ DIMENSIONAL_MODEL_DOCUMENTATION.md
│
├─→ STANDARD_OPERATING_PROCEDURES.md
│   ├─→ TROUBLESHOOTING_PLAYBOOK.md
│   ├─→ PERFORMANCE_OPTIMIZATION_GUIDE.md
│   └─→ DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md
│
└─→ DIMENSIONAL_MODEL_DOCUMENTATION.md
    ├─→ ER_DIAGRAM.md
    ├─→ SCD_TYPE2_DESIGN_GUIDE.md
    └─→ APPOINTMENT_LIFECYCLE_DESIGN.md
```

---

## Quick Search Tips

**Find documentation by keyword:**

- **Snowflake basics**: SNOWFLAKE_DEVELOPER_GUIDE.md
- **Daily operations**: STANDARD_OPERATING_PROCEDURES.md, Section 1
- **Pipeline failures**: TROUBLESHOOTING_PLAYBOOK.md, Section 1
- **Data quality**: TROUBLESHOOTING_PLAYBOOK.md, Section 2 + snowflake/quality/
- **Performance**: PERFORMANCE_OPTIMIZATION_GUIDE.md
- **Cost optimization**: PERFORMANCE_OPTIMIZATION_GUIDE.md, Section 5
- **Disaster recovery**: DISASTER_RECOVERY_AND_BUSINESS_CONTINUITY.md
- **SCD Type 2**: SCD_TYPE2_DESIGN_GUIDE.md
- **CDC/Streams**: SNOWFLAKE_STREAMS_BENEFITS_GUIDE.md
- **Tasks/Scheduling**: SNOWFLAKE_ORCHESTRATION_GUIDE.md
- **Table definitions**: DIMENSIONAL_MODEL_DOCUMENTATION.md
- **Deployment**: README.md + DEPLOYMENT_VALIDATION.sql
- **Onboarding**: DEVELOPER_ONBOARDING_GUIDE.md
- **Naming standards**: NAMING_CONVENTION_ALIGNMENT_REPORT.md
- **Project planning**: JIRA_PROJECT_PLAN.md

---

## Contributing to Documentation

When adding new documentation:

1. Add entry to this index in alphabetical order
2. Update page count and total
3. Add to appropriate category
4. Update role-based navigation if relevant
5. Add keywords for searchability
6. Update dependencies map if needed

---

## Documentation Maintenance

**Owner**: Data Engineering Team
**Review Frequency**: Quarterly
**Last Review**: 2024-11-18
**Next Review**: 2025-02-18

**Version History:**
- v1.0 (2024-11-18): Initial comprehensive index created

---

**Need help finding something?** Check [README.md](README.md) first, or ask in #data-engineering Slack channel.

**Found a broken link or outdated info?** Submit a GitHub issue with label `documentation`.

---

*This index is your navigation hub. Bookmark it!* 📚
