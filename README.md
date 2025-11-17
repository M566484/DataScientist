> **Updated to align with VES Snowflake Naming Conventions v1.0**

Veteran Evaluation Services - Dimensional Model
================================================

A comprehensive dimensional data model for veteran evaluation services, designed for reporting and analytics in Snowflake.

## Overview

This repository contains a complete Kimball-style dimensional model for tracking:
- Medical evaluations (C&P exams, IMEs, disability evaluations)
- VA disability claims processing
- Appointment scheduling and wait times
- Daily operational performance metrics

## Quick Start

### Deploy the Model

```bash
# Connect to Snowflake
snowsql -a <your_account> -u <your_username>

# Deploy all objects
!source snowflake/schema/02_master_deployment.sql
```

### Verify Installation

```sql
USE DATABASE VETERAN_EVALUATION_DW;

-- Check dimension tables
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'WAREHOUSE';

-- Check fact tables
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'WAREHOUSE';
```

## Repository Structure

```
.
├── snowflake/
│   ├── schema/
│   │   ├── 00_setup_database.sql          # Database and schema creation
│   │   ├── 01_populate_date_dimension.sql # Date dimension population
│   │   └── 02_master_deployment.sql       # Master deployment script
│   ├── dimensions/
│   │   ├── 01_dim_date.sql                # Date dimension
│   │   ├── 02_dim_veteran.sql             # Veteran dimension (SCD Type 2)
│   │   ├── 03_dim_evaluator.sql           # Evaluator dimension (SCD Type 2)
│   │   ├── 04_dim_facility.sql            # Facility dimension (SCD Type 2)
│   │   ├── 05_dim_evaluation_type.sql     # Evaluation type dimension
│   │   ├── 06_dim_medical_condition.sql   # Medical condition dimension
│   │   ├── 07_dim_claim.sql               # Claim dimension (SCD Type 2)
│   │   └── 08_dim_appointment.sql         # Appointment dimension
│   └── facts/
│       ├── 01_fact_evaluation.sql         # Evaluation fact table
│       ├── 02_fact_claim_status.sql       # Claim status fact table
│       ├── 03_fact_appointment.sql        # Appointment fact table
│       └── 04_fact_daily_snapshot.sql     # Daily snapshot fact table
├── DIMENSIONAL_MODEL_DOCUMENTATION.md     # Complete documentation
├── ER_DIAGRAM.md                          # Entity relationship diagrams
└── README.md                              # This file
```

## Key Features

### Dimension Tables (8 tables)
- **dim_dates**: Standard date dimension with VA fiscal year support
- **dim_veterans**: Veteran demographics and service history (Type 2 SCD)
- **dim_evaluators**: Medical professionals and evaluators (Type 2 SCD)
- **dim_facilities**: VA facilities and medical centers (Type 2 SCD)
- **dim_evaluation_types**: Types of medical evaluations
- **dim_medical_conditions**: Medical conditions and diagnoses
- **dim_claims**: VA disability claims (Type 2 SCD)
- **dim_appointments**: Appointment scheduling details

### Fact Tables (4 tables)
- **fact_evaluations_completed**: Transaction fact for evaluations (grain: evaluation per condition)
- **fact_claim_status_changes**: Accumulating snapshot for claim processing
- **fact_appointments_scheduled**: Transaction fact for appointments
- **fact_daily_facility_snapshot**: Periodic snapshot for daily KPIs

### Special Features
- Slowly Changing Dimensions (Type 2) for tracking historical changes
- VA-specific attributes (fiscal year, VISN, priority groups, etc.)
- Telehealth and community care support
- Comprehensive quality and performance metrics
- Wait time compliance tracking (VA standards)
- Cost and satisfaction analytics

## Documentation

For complete documentation, see:
- **[DIMENSIONAL_MODEL_DOCUMENTATION.md](DIMENSIONAL_MODEL_DOCUMENTATION.md)** - Full technical documentation
- **[ER_DIAGRAM.md](ER_DIAGRAM.md)** - Entity relationship diagrams

## Sample Queries

### Wait Time Analysis
```sql
SELECT
    f.facility_name,
    AVG(fa.total_wait_days) AS avg_wait_days,
    SUM(CASE WHEN fa.meets_va_wait_time_goal = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS compliance_pct
FROM fact_appointments_scheduled fa
JOIN dim_facilities f ON fa.facility_sk = f.facility_sk
GROUP BY f.facility_name;
```

### Evaluation Quality Metrics
```sql
SELECT
    e.specialty,
    AVG(fe.report_completeness_score) AS avg_completeness,
    SUM(CASE WHEN fe.sufficient_exam_flag = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS sufficient_rate_pct
FROM fact_evaluations_completed fe
JOIN dim_evaluators e ON fe.evaluator_sk = e.evaluator_sk
WHERE e.is_current = TRUE
GROUP BY e.specialty;
```

More examples in the [documentation](DIMENSIONAL_MODEL_DOCUMENTATION.md#common-queries).

## Technologies

- **Snowflake**: Data warehouse platform
- **SQL**: DDL scripts for table creation
- **Kimball Methodology**: Dimensional modeling approach

## Design Decisions

1. **Star Schema**: Optimized for query performance and ease of use
2. **Type 2 SCD**: Track historical changes in veteran status, evaluators, facilities, and claims
3. **Multiple Fact Grains**: Support different analysis patterns (transaction, accumulating, periodic)
4. **Conformed Dimensions**: Enable cross-process analysis
5. **VA-Specific Metrics**: Built-in support for VA standards and reporting requirements

## Contributing

This is a reference implementation. Customize as needed for your specific requirements:
- Add/modify dimensions based on your data sources
- Adjust fact table grains for your reporting needs
- Extend with additional metrics or attributes
- Implement ETL processes for your source systems

## License

This dimensional model is provided as-is for educational and implementation purposes.

## Version

**v1.0** - Initial release (2024-11-15)

---

For questions or support, refer to the comprehensive documentation included in this repository.
