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
WHERE TABLE_SCHEMA = 'DIM';

-- Check fact tables
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FACT';
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
- **DIM_DATE**: Standard date dimension with VA fiscal year support
- **DIM_VETERAN**: Veteran demographics and service history (Type 2 SCD)
- **DIM_EVALUATOR**: Medical professionals and evaluators (Type 2 SCD)
- **DIM_FACILITY**: VA facilities and medical centers (Type 2 SCD)
- **DIM_EVALUATION_TYPE**: Types of medical evaluations
- **DIM_MEDICAL_CONDITION**: Medical conditions and diagnoses
- **DIM_CLAIM**: VA disability claims (Type 2 SCD)
- **DIM_APPOINTMENT**: Appointment scheduling details

### Fact Tables (4 tables)
- **FACT_EVALUATION**: Transaction fact for evaluations (grain: evaluation per condition)
- **FACT_CLAIM_STATUS**: Accumulating snapshot for claim processing
- **FACT_APPOINTMENT**: Transaction fact for appointments
- **FACT_DAILY_SNAPSHOT**: Periodic snapshot for daily KPIs

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
    f.FACILITY_NAME,
    AVG(fa.TOTAL_WAIT_DAYS) AS avg_wait_days,
    SUM(CASE WHEN fa.MEETS_VA_WAIT_TIME_GOAL = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS compliance_pct
FROM FACT_APPOINTMENT fa
JOIN DIM_FACILITY f ON fa.FACILITY_KEY = f.FACILITY_KEY
GROUP BY f.FACILITY_NAME;
```

### Evaluation Quality Metrics
```sql
SELECT
    e.SPECIALTY,
    AVG(fe.REPORT_COMPLETENESS_SCORE) AS avg_completeness,
    SUM(CASE WHEN fe.SUFFICIENT_EXAM_FLAG = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS sufficient_rate_pct
FROM FACT_EVALUATION fe
JOIN DIM_EVALUATOR e ON fe.EVALUATOR_KEY = e.EVALUATOR_KEY
WHERE e.IS_CURRENT = TRUE
GROUP BY e.SPECIALTY;
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
