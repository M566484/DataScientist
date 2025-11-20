# Common Functions Analysis & Recommendations

## Executive Summary

This analysis identifies **8 major categories** of repeated code patterns across the codebase, with **1,181+ instances** of duplicate logic. Creating reusable SQL functions (UDFs) will:

- **Reduce code by ~35-40%** through consolidation
- **Improve consistency** in data transformations
- **Simplify maintenance** - fix once, apply everywhere
- **Enhance readability** - descriptive function names vs. complex inline code

---

## 1. STRING NORMALIZATION & CLEANING

### 1.1 Standard String Normalization

**Pattern Found:** `UPPER(TRIM(column_name))`
**Occurrences:** 150+ instances across ETL procedures

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:81, 88, 89, 94, 98, 99, 101, 102
UPPER(TRIM(gender)) AS gender,
UPPER(TRIM(address_line1)) AS address_line1,
UPPER(TRIM(city)) AS city,
UPPER(TRIM(state)) AS state,
UPPER(TRIM(service_branch)) AS service_branch,
UPPER(TRIM(discharge_status)) AS discharge_status,
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_normalize_string_upper(input_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    UPPER(TRIM(input_value))
$$;
```

**Usage:**
```sql
fn_normalize_string_upper(gender) AS gender,
fn_normalize_string_upper(city) AS city,
fn_normalize_string_upper(state) AS state
```

---

### 1.2 Email Normalization

**Pattern Found:** `LOWER(TRIM(email))`
**Occurrences:** 25+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:84
LOWER(TRIM(email)) AS email,
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_normalize_email(email_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    LOWER(TRIM(email_value))
$$;
```

---

### 1.3 Phone Number Cleaning

**Pattern Found:** `REGEXP_REPLACE(phone_primary, '[^0-9]', '')`
**Occurrences:** 15+ instances across dimension and fact ETL

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:85
-- Found in: etl/03_etl_procedures_multi_source.sql:370
REGEXP_REPLACE(phone_primary, '[^0-9]', '') AS phone
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_clean_phone_number(phone_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Removes all non-numeric characters from phone numbers. Example: (555) 123-4567 -> 5551234567'
AS
$$
    REGEXP_REPLACE(phone_value, '[^0-9]', '')
$$;
```

**Benefits:**
- Consistent phone formatting across all tables
- Easy to modify format (e.g., keep extensions, international codes)
- Self-documenting code

---

### 1.4 Zip Code Cleaning

**Pattern Found:** `REGEXP_REPLACE(zip_code, '[^0-9-]', '')`
**Occurrences:** 15+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:90
-- Found in: etl/03_etl_procedures_multi_source.sql:375
REGEXP_REPLACE(zip_code, '[^0-9-]', '') AS zip_code
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_clean_zip_code(zip_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Removes all characters except digits and hyphens from zip codes. Example: "12345-6789 USA" -> "12345-6789"'
AS
$$
    REGEXP_REPLACE(zip_value, '[^0-9-]', '')
$$;
```

---

## 2. DATA VALIDATION & CATEGORIZATION

### 2.1 Disability Rating Categorization

**Pattern Found:** Exact same CASE statement repeated 4+ times
**Occurrences:** 4 files (etl/01_etl_procedures_dimensions.sql, etl/03_etl_procedures_multi_source.sql, dimensions/02_dim_veteran.sql, staging)

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:109-116
-- Found in: etl/03_etl_procedures_multi_source.sql:394-401
CASE
    WHEN disability_rating = 0 THEN '0%'
    WHEN disability_rating BETWEEN 10 AND 30 THEN '10-30%'
    WHEN disability_rating BETWEEN 40 AND 60 THEN '40-60%'
    WHEN disability_rating BETWEEN 70 AND 90 THEN '70-90%'
    WHEN disability_rating = 100 THEN '100%'
    ELSE NULL
END AS disability_rating_category
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_categorize_disability_rating(rating NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Categorizes VA disability ratings into standard ranges: 0%, 10-30%, 40-60%, 70-90%, 100%'
AS
$$
    CASE
        WHEN rating = 0 THEN '0%'
        WHEN rating BETWEEN 10 AND 30 THEN '10-30%'
        WHEN rating BETWEEN 40 AND 60 THEN '40-60%'
        WHEN rating BETWEEN 70 AND 90 THEN '70-90%'
        WHEN rating = 100 THEN '100%'
        ELSE NULL
    END
$$;
```

**Benefits:**
- Change categorization logic once, applies everywhere
- Business rules in one place
- Easy to add new categories (e.g., split 70-90% into separate ranges)

---

### 2.2 Disability Rating Validation

**Pattern Found:** `CASE WHEN disability_rating BETWEEN 0 AND 100 THEN disability_rating ELSE NULL END`
**Occurrences:** 4+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:105-108
CASE
    WHEN disability_rating BETWEEN 0 AND 100 THEN disability_rating
    ELSE NULL
END AS current_disability_rating
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_validate_disability_rating(rating NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE WHEN rating BETWEEN 0 AND 100 THEN rating ELSE NULL END
$$;
```

---

### 2.3 VA Priority Group Validation

**Pattern Found:** `CASE WHEN priority_group BETWEEN 1 AND 8 THEN priority_group ELSE NULL END`
**Occurrences:** 3+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:122-125
CASE
    WHEN priority_group BETWEEN 1 AND 8 THEN priority_group
    ELSE NULL
END AS priority_group
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_validate_priority_group(group_number NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Validates VA priority group is between 1-8, returns NULL for invalid values'
AS
$$
    CASE WHEN group_number BETWEEN 1 AND 8 THEN group_number ELSE NULL END
$$;
```

---

## 3. DATE & TIME CALCULATIONS

### 3.1 Age Calculation

**Pattern Found:** `DATEDIFF(year, date_of_birth, CURRENT_DATE())`
**Occurrences:** 10+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:80
DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS age
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_calculate_age(birth_date DATE)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates age in years from date of birth to current date'
AS
$$
    DATEDIFF(year, birth_date, CURRENT_DATE())
$$;
```

---

### 3.2 Years of Service Calculation

**Pattern Found:** `ROUND(DATEDIFF(day, service_start_date, COALESCE(service_end_date, CURRENT_DATE())) / 365.25, 2)`
**Occurrences:** 5+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:97
-- Found in: etl/03_etl_procedures_multi_source.sql:382
ROUND(DATEDIFF(day, service_start_date, COALESCE(service_end_date, CURRENT_DATE())) / 365.25, 2) AS years_of_service
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_calculate_years_of_service(
    start_date DATE,
    end_date DATE
)
RETURNS DECIMAL(5,2)
LANGUAGE SQL
COMMENT = 'Calculates years of military service, using current date if service is ongoing. Accounts for leap years (365.25)'
AS
$$
    ROUND(DATEDIFF(day, start_date, COALESCE(end_date, CURRENT_DATE())) / 365.25, 2)
$$;
```

**Benefits:**
- Consistent handling of active vs. ended service
- Single place to adjust leap year calculation if needed
- Clear intent in calling code

---

### 3.3 SLA Cycle Time Calculations

**Pattern Found:** Multiple DATEDIFF calculations for SLA metrics
**Occurrences:** 20+ instances across fact tables

**Current Implementation:**
```sql
-- Found in: etl/02_etl_procedures_facts.sql:110-122
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
END AS sla_variance_days
```

**Proposed Functions:**
```sql
CREATE OR REPLACE FUNCTION fn_calculate_cycle_time_days(
    start_date DATE,
    end_date DATE
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates cycle time in days, using current date if end_date is NULL (for in-progress items)'
AS
$$
    DATEDIFF(day, start_date, COALESCE(end_date, CURRENT_DATE()))
$$;

CREATE OR REPLACE FUNCTION fn_check_sla_met(
    actual_days NUMBER,
    sla_days_allowed NUMBER
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE WHEN actual_days <= sla_days_allowed THEN TRUE ELSE FALSE END
$$;

CREATE OR REPLACE FUNCTION fn_calculate_sla_variance(
    actual_days NUMBER,
    sla_days_allowed NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates SLA variance in days. Positive = late, Negative = early, 0 = on time'
AS
$$
    actual_days - sla_days_allowed
$$;
```

---

## 4. HASH-BASED CHANGE DETECTION

**Pattern Found:** `MD5(CONCAT_WS('|', COALESCE(...), COALESCE(...), ...))`
**Occurrences:** 20+ instances across all dimension tables

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:128-135
-- Found in: etl/03_etl_procedures_multi_source.sql:413-420
MD5(CONCAT_WS('|',
    COALESCE(first_name, ''),
    COALESCE(last_name, ''),
    COALESCE(date_of_birth::VARCHAR, ''),
    COALESCE(disability_rating::VARCHAR, ''),
    COALESCE(email, ''),
    COALESCE(phone_primary, '')
)) AS source_record_hash
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_generate_record_hash(
    field1 VARCHAR DEFAULT '',
    field2 VARCHAR DEFAULT '',
    field3 VARCHAR DEFAULT '',
    field4 VARCHAR DEFAULT '',
    field5 VARCHAR DEFAULT '',
    field6 VARCHAR DEFAULT '',
    field7 VARCHAR DEFAULT '',
    field8 VARCHAR DEFAULT '',
    field9 VARCHAR DEFAULT '',
    field10 VARCHAR DEFAULT ''
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Generates MD5 hash from up to 10 fields for change detection. Fields are coalesced to empty string and pipe-delimited.'
AS
$$
    MD5(CONCAT_WS('|',
        COALESCE(field1, ''),
        COALESCE(field2, ''),
        COALESCE(field3, ''),
        COALESCE(field4, ''),
        COALESCE(field5, ''),
        COALESCE(field6, ''),
        COALESCE(field7, ''),
        COALESCE(field8, ''),
        COALESCE(field9, ''),
        COALESCE(field10, '')
    ))
$$;
```

**Alternative - Using VARIANT for Variable Arguments:**
```sql
CREATE OR REPLACE FUNCTION fn_generate_record_hash_v2(fields ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = 'Generates MD5 hash from array of fields for change detection'
AS
$$
    // Convert all fields to strings and handle nulls
    var values = FIELDS.map(function(f) {
        return f === null || f === undefined ? '' : String(f);
    });

    // Join with pipe delimiter
    var concatenated = values.join('|');

    // Generate MD5 (Snowflake provides this)
    return snowflake.createStatement({
        sqlText: "SELECT MD5(?)",
        binds: [concatenated]
    }).execute().next() ? snowflake.createStatement().getColumnValue(1) : null;
$$;
```

**Usage:**
```sql
-- Standard function (up to 10 fields)
fn_generate_record_hash(
    first_name,
    last_name,
    date_of_birth::VARCHAR,
    disability_rating::VARCHAR,
    email,
    phone_primary
) AS source_record_hash

-- Array version (unlimited fields)
fn_generate_record_hash_v2(
    ARRAY_CONSTRUCT(
        first_name,
        last_name,
        date_of_birth::VARCHAR,
        email,
        phone_primary
    )
) AS source_record_hash
```

---

## 5. DATA QUALITY SCORING

**Pattern Found:** Complex CASE statement summing quality scores
**Occurrences:** 11+ files with DQ scoring

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:142-151
(
    (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN date_of_birth IS NOT NULL THEN 15 ELSE 0 END) +
    (CASE WHEN veteran_va_id IS NOT NULL OR veteran_ssn IS NOT NULL THEN 20 ELSE 0 END) +
    (CASE WHEN email IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN phone_primary IS NOT NULL THEN 10 ELSE 0 END) +
    (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
    (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
) AS dq_score
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_calculate_veteran_dq_score(
    first_name VARCHAR,
    last_name VARCHAR,
    date_of_birth DATE,
    veteran_id VARCHAR,
    ssn VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    state VARCHAR,
    disability_rating NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculates data quality score for veteran records (0-100 scale)'
AS
$$
    (
        (CASE WHEN first_name IS NOT NULL THEN 15 ELSE 0 END) +
        (CASE WHEN last_name IS NOT NULL THEN 15 ELSE 0 END) +
        (CASE WHEN date_of_birth IS NOT NULL THEN 15 ELSE 0 END) +
        (CASE WHEN veteran_id IS NOT NULL OR ssn IS NOT NULL THEN 20 ELSE 0 END) +
        (CASE WHEN email IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN phone IS NOT NULL THEN 10 ELSE 0 END) +
        (CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END) +
        (CASE WHEN disability_rating BETWEEN 0 AND 100 THEN 10 ELSE 0 END)
    )
$$;
```

**Usage:**
```sql
fn_calculate_veteran_dq_score(
    first_name, last_name, date_of_birth,
    veteran_va_id, veteran_ssn,
    email, phone_primary, state, disability_rating
) AS dq_score
```

---

## 6. NULL HANDLING & DEFAULTS

### 6.1 Country Default

**Pattern Found:** `COALESCE(UPPER(TRIM(country)), 'USA')`
**Occurrences:** 10+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:91
COALESCE(UPPER(TRIM(country)), 'USA') AS country
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_default_country(country_value VARCHAR, default_country VARCHAR DEFAULT 'USA')
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    COALESCE(UPPER(TRIM(country_value)), default_country)
$$;
```

---

### 6.2 Boolean Flag Defaults

**Pattern Found:** `COALESCE(flag_field, FALSE)`
**Occurrences:** 50+ instances

**Current Implementation:**
```sql
-- Found in: etl/01_etl_procedures_dimensions.sql:100, 117, 120
COALESCE(combat_veteran_flag, FALSE) AS combat_veteran_flag,
COALESCE(service_connected_flag, FALSE) AS service_connected_flag,
COALESCE(va_enrolled_flag, FALSE) AS va_enrolled_flag
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_default_false(flag_value BOOLEAN)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS
$$
    COALESCE(flag_value, FALSE)
$$;
```

---

## 7. COMPLEX BUSINESS LOGIC

### 7.1 Complex Case Flag

**Pattern Found:** Multiple condition evaluations creating boolean flags
**Occurrences:** 15+ instances

**Current Implementation:**
```sql
-- Found in: etl/02_etl_procedures_facts.sql:89
CASE WHEN ARRAY_SIZE(SPLIT(requested_conditions, ',')) > 3 THEN TRUE ELSE FALSE END AS complex_case_flag
```

**Proposed Function:**
```sql
CREATE OR REPLACE FUNCTION fn_is_complex_case(requested_conditions VARCHAR, threshold NUMBER DEFAULT 3)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE WHEN ARRAY_SIZE(SPLIT(requested_conditions, ',')) > threshold THEN TRUE ELSE FALSE END
$$;
```

---

## 8. IMPLEMENTATION RECOMMENDATIONS

### Priority 1: High-Impact, Simple Functions (Implement First)
1. **fn_clean_phone_number** - 15+ uses, simple regex
2. **fn_clean_zip_code** - 15+ uses, simple regex
3. **fn_normalize_string_upper** - 150+ uses, very simple
4. **fn_normalize_email** - 25+ uses, very simple
5. **fn_categorize_disability_rating** - 4+ uses, high complexity reduction

**Estimated Code Reduction:** ~20% for ETL procedures

---

### Priority 2: Medium Complexity, High Value
1. **fn_calculate_years_of_service** - 5+ uses, consistent business logic
2. **fn_calculate_age** - 10+ uses
3. **fn_validate_disability_rating** - 4+ uses
4. **fn_validate_priority_group** - 3+ uses
5. **fn_default_false** - 50+ uses
6. **fn_default_country** - 10+ uses

**Estimated Code Reduction:** Additional ~10%

---

### Priority 3: Complex Functions (Plan Carefully)
1. **fn_generate_record_hash** - 20+ uses, requires testing
2. **fn_calculate_veteran_dq_score** - 11+ uses, complex scoring
3. **SLA calculation functions** - 20+ uses across multiple fact tables

**Estimated Code Reduction:** Additional ~5-10%

---

## 9. IMPLEMENTATION PLAN

### Phase 1: Create Functions Library (Week 1)
```sql
-- File: snowflake/functions/00_common_data_functions.sql
-- Contains all Priority 1 & 2 functions
-- Includes comprehensive tests
```

### Phase 2: Refactor ETL Procedures (Week 2-3)
1. Start with `01_etl_procedures_dimensions.sql` (most repetition)
2. Then `03_etl_procedures_multi_source.sql`
3. Then `02_etl_procedures_facts.sql`
4. Update staging layer tables

### Phase 3: Testing & Validation (Week 4)
1. Compare results before/after refactoring
2. Performance testing (UDFs vs. inline code)
3. Update documentation

---

## 10. EXPECTED BENEFITS

### Code Quality
- **Lines of Code Reduction:** ~35-40% in ETL procedures
- **Readability:** Function names are self-documenting
- **Consistency:** Same transformation logic everywhere

### Maintenance
- **Bug Fixes:** Fix once, applies to all uses
- **Business Rule Changes:** Update function definition only
- **Testing:** Test functions independently

### Performance
- **Potential Concern:** UDF overhead vs. inline code
- **Mitigation:** Mark functions as IMMUTABLE for optimization
- **Recommendation:** Performance test before/after

### Example Impact on dim_veterans ETL:
**Before:** 634 lines with 47 instances of repeated patterns
**After:** ~400 lines with function calls
**Code Reduction:** ~37%

---

## 11. NEXT STEPS

1. ✅ **Review this analysis** with team
2. ⬜ **Approve Priority 1 functions** for implementation
3. ⬜ **Create functions library** in `snowflake/functions/`
4. ⬜ **Implement & test** Priority 1 functions
5. ⬜ **Refactor** one ETL procedure as proof of concept
6. ⬜ **Measure** code reduction and performance impact
7. ⬜ **Roll out** to remaining ETL procedures

---

## APPENDIX A: Function Naming Convention

All common functions follow this naming pattern:
- **fn_** prefix for user-defined functions
- **Verb-noun structure:** `fn_calculate_age`, `fn_clean_phone_number`
- **Descriptive names:** Self-documenting purpose
- **Comments:** Explain purpose, provide examples

## APPENDIX B: Files Analyzed

### High-Repetition Files:
- `snowflake/etl/01_etl_procedures_dimensions.sql` (634 lines)
- `snowflake/etl/02_etl_procedures_facts.sql` (622 lines)
- `snowflake/etl/03_etl_procedures_multi_source.sql` (1,284 lines)
- `snowflake/staging/02_staging_layer_oms_vems_merge_simplified.sql` (802 lines)
- `snowflake/quality/00_advanced_data_quality_framework.sql` (692 lines)

### Pattern Counts:
- COALESCE: 384 occurrences across 10 files
- UPPER(TRIM()): 150+ occurrences
- REGEXP_REPLACE: 30+ occurrences
- MD5(CONCAT_WS()): 20+ occurrences
- DATEDIFF calculations: 80+ occurrences
