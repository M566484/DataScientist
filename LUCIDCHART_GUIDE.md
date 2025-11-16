# Lucidchart Diagrams - VES Dimensional Model
## Step-by-Step Guide for Creating Professional Data Model Diagrams

**Date**: 2025-11-16
**Standards**: VES Snowflake Naming Conventions v1.0
**Purpose**: Instructions for creating Lucidchart diagrams of the veteran evaluation dimensional model

---

## Table of Contents
1. [Overview](#overview)
2. [Recommended Diagram Types](#recommended-diagram-types)
3. [Step-by-Step Instructions](#step-by-step-instructions)
4. [Table Definitions](#table-definitions)
5. [Relationship Specifications](#relationship-specifications)
6. [Styling Guidelines](#styling-guidelines)
7. [Diagram Export Settings](#diagram-export-settings)

---

## Overview

### Diagrams to Create
1. **Star Schema Overview** - High-level view showing all dimensions around each fact table
2. **Detailed ERD** - Complete entity relationship diagram with all columns
3. **Schema Architecture** - Database and schema organization
4. **Fact Table Details** - Individual diagrams for each fact with related dimensions

### Total Tables
- **8 Dimension Tables** (in WAREHOUSE schema)
- **4 Fact Tables** (in WAREHOUSE schema)
- **12 Total Tables** to diagram

---

## Recommended Diagram Types

### Diagram 1: Star Schema Overview (Recommended First)
**Purpose**: High-level business view
**Lucidchart Template**: Use "Entity Relationship Diagram" or "Database Schema"
**Show**: Table names, primary keys, foreign keys only (no detail columns)
**Layout**: Star pattern with fact tables in center, dimensions around them

### Diagram 2: Complete ERD
**Purpose**: Technical reference with all columns
**Lucidchart Template**: "Detailed Entity Relationship Diagram"
**Show**: All columns with data types, all constraints
**Layout**: Grouped by schema, dimensions on left, facts on right

### Diagram 3: Schema Architecture
**Purpose**: Database organization
**Lucidchart Template**: "AWS Architecture" or "Database Design"
**Show**: Database → Schemas → Table counts
**Layout**: Hierarchical tree structure

### Diagram 4: Individual Fact Diagrams (4 diagrams)
**Purpose**: Detailed view of each fact with its dimensions
**Show**: One fact table with all related dimensions and complete columns
**Layout**: Fact table in center, related dimensions around it

---

## Step-by-Step Instructions

### Step 1: Set Up Lucidchart Document

1. **Create New Document**:
   - Go to Lucidchart → New Document
   - Select "Entity Relationship Diagram" template
   - Name: "VES Dimensional Model - Star Schema"

2. **Document Settings**:
   - Page Size: Letter (11" x 8.5") or A3 for larger diagrams
   - Orientation: Landscape
   - Grid: Enable snap to grid (10px spacing)
   - Background: White

3. **Enable ERD Shape Library**:
   - Click "+ Shapes" in left panel
   - Search for "Entity Relationship"
   - Enable "Entity Relationship Diagram" shape library
   - Enable "Database" shape library

---

### Step 2: Create Dimension Tables

#### For Each Dimension Table:

**2.1 Add Entity Shape**:
- Drag "Entity" shape from left panel
- Resize to appropriate size based on column count

**2.2 Configure Table Header**:
- Double-click header section
- Enter table name (e.g., `dim_veterans`)
- Format: **Bold, 12pt, Dark Blue (#1B4D89)**
- Add schema prefix if needed: `WAREHOUSE.dim_veterans`

**2.3 Add Primary Key Row**:
- Add attribute row
- **Column Name**: `veteran_sk` (use actual PK from tables below)
- **Data Type**: `INTEGER`
- **Constraints**: Check "Primary Key" checkbox or add 🔑 symbol
- Format: **Bold, 11pt**

**2.4 Add Business Key Row** (if applicable):
- Add attribute row
- **Column Name**: `veteran_id` (business key)
- **Data Type**: `VARCHAR(50)`
- **Constraints**: Add "NOT NULL"
- Format: Regular, 10pt

**2.5 Add Selected Important Columns** (for overview diagram):
- For overview: Only show 3-5 most important columns
- For detailed: Show all columns
- Format: Regular, 10pt

**2.6 Add SCD Attributes** (for Type 2 dimensions):
- `effective_start_date` - TIMESTAMP_NTZ
- `effective_end_date` - TIMESTAMP_NTZ
- `is_current` - BOOLEAN
- Format: Italic, 9pt, Gray (#666666)

**2.7 Style the Entity**:
- **Header Background**: Light Blue (#E3F2FD)
- **Border**: Dark Blue (#1B4D89), 2px
- **Body Background**: White
- **Shadow**: Enable soft shadow

---

### Step 3: Create Fact Tables

#### For Each Fact Table:

**3.1 Add Entity Shape**:
- Drag "Entity" shape
- Make larger than dimensions (facts have more columns)

**3.2 Configure Table Header**:
- Double-click header
- Enter table name (e.g., `fct_evaluations_completed`)
- Format: **Bold, 12pt, Dark Orange (#E65100)**

**3.3 Add Primary Key**:
- Add attribute row
- **Column Name**: `evaluation_fact_sk` (use actual PK)
- **Data Type**: `INTEGER`
- **Constraints**: Primary Key 🔑
- Format: **Bold, 11pt**

**3.4 Add Foreign Key Columns**:
- List all foreign keys (dimension references)
- Format with FK indicator or different color
- Examples:
  - `veteran_sk` → INTEGER (FK)
  - `evaluator_sk` → INTEGER (FK)
  - `facility_sk` → INTEGER (FK)
  - `evaluation_date_sk` → INTEGER (FK)

**3.5 Add Degenerate Dimensions** (if any):
- Transaction IDs that don't belong to dimensions
- Format: Regular, 10pt

**3.6 Add Measure Columns** (select important ones):
- Numeric facts and calculations
- Examples: `evaluation_duration_minutes`, `report_completeness_score`
- Format: Regular, 10pt

**3.7 Style the Entity**:
- **Header Background**: Light Orange (#FFF3E0)
- **Border**: Dark Orange (#E65100), 2px
- **Body Background**: White
- **Shadow**: Enable soft shadow

---

### Step 4: Create Relationships

#### For Each Foreign Key Relationship:

**4.1 Draw Relationship Line**:
- Use "Relationship" connector from ERD library
- Or use regular line with crow's foot notation

**4.2 Configure Relationship**:
- **From**: Dimension table (one side)
- **To**: Fact table (many side)
- **Line Style**: Solid line
- **Endpoint (Dimension side)**: Single line (one)
- **Endpoint (Fact side)**: Crow's foot (many)

**4.3 Add Relationship Label** (optional):
- Add text label on line
- Format: Italic, 8pt, Gray
- Examples: "has evaluations", "assigned to", "occurred on"

**4.4 Configure Line Routing**:
- Use orthogonal (right-angle) routing
- Avoid crossing lines when possible
- Use line jumps where lines must cross

**4.5 Color Coding** (optional):
- Use different colors for different relationship types
- Example: Date relationships in blue, entity relationships in black

---

### Step 5: Layout and Organization

#### Star Schema Layout (Recommended for Overview):

```
                    dim_evaluation_types
                            |
                            |
    dim_veterans -------- fct_evaluations_completed -------- dim_evaluators
                            |
                            |
                      dim_facilities
                            |
                            |
                       dim_dates
```

**Layout Steps**:
1. Place fact table in center
2. Arrange dimensions in circular pattern around fact
3. Space evenly (200-300px between centers)
4. Align vertically or horizontally where possible

#### Detailed ERD Layout:

```
[Dimensions Column - Left]          [Facts Column - Right]

dim_dates                           fct_evaluations_completed
dim_veterans                        fct_claim_status_changes
dim_evaluators                      fct_appointments_scheduled
dim_facilities                      fct_daily_facility_snapshot
dim_evaluation_types
dim_medical_conditions
dim_claims
dim_appointments
```

**Layout Steps**:
1. Create two columns
2. Place dimensions on left (grouped logically)
3. Place facts on right
4. Draw relationships across columns

---

### Step 6: Add Annotations and Legend

**6.1 Add Title**:
- Text box at top of diagram
- **Content**: "VES Dimensional Model - Star Schema"
- **Format**: Bold, 18pt, Dark Blue

**6.2 Add Metadata Box**:
- Text box in corner
- **Content**:
  ```
  Database: VETERAN_EVALUATION_DW
  Schema: WAREHOUSE
  Standards: VES Snowflake Naming Conventions v1.0
  Updated: 2025-11-16
  ```
- **Format**: 9pt, Light Gray box

**6.3 Add Legend**:
- Create small box with symbols
- **Content**:
  ```
  🔑 = Primary Key
  🔗 = Foreign Key
  ⭐ = Business Key
  📊 = Measure/Metric
  🔄 = SCD Type 2
  ```

**6.4 Add Table Count Summary**:
- Text box showing counts
- **Content**:
  ```
  📋 Summary:
  • 8 Dimension Tables
  • 4 Fact Tables
  • 19 Foreign Key Relationships
  • 450+ Column-level Comments
  ```

---

## Table Definitions

### Dimension Tables (WAREHOUSE Schema)

#### 1. dim_dates
**Type**: Type 1 Dimension
**Purpose**: Time dimension for analysis
**Primary Key**: `date_sk` (INTEGER)

**Key Columns**:
- `date_sk` 🔑 INTEGER - Primary key (YYYYMMDD format)
- `full_date` DATE - Actual date
- `fiscal_year` INTEGER - VA fiscal year
- `fiscal_quarter` INTEGER - VA fiscal quarter
- `is_weekend` BOOLEAN - Weekend flag
- `is_holiday` BOOLEAN - Federal holiday flag

**Lucidchart Color**: Light Purple (#E1BEE7)

---

#### 2. dim_veterans
**Type**: Type 2 SCD
**Purpose**: Veteran demographic and service information
**Primary Key**: `veteran_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `veteran_id` (VARCHAR(50))

**Key Columns**:
- `veteran_sk` 🔑 INTEGER - Surrogate key
- `veteran_id` ⭐ VARCHAR(50) - Business key
- `full_name` VARCHAR(255)
- `service_branch` VARCHAR(50)
- `current_disability_rating` INTEGER
- `effective_start_date` 🔄 TIMESTAMP_NTZ
- `effective_end_date` 🔄 TIMESTAMP_NTZ
- `is_current` 🔄 BOOLEAN

**Lucidchart Color**: Light Blue (#BBDEFB)

---

#### 3. dim_evaluators
**Type**: Type 2 SCD
**Purpose**: Medical professionals performing evaluations
**Primary Key**: `evaluator_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `evaluator_id` (VARCHAR(50))

**Key Columns**:
- `evaluator_sk` 🔑 INTEGER
- `evaluator_id` ⭐ VARCHAR(50)
- `full_name` VARCHAR(255)
- `specialty` VARCHAR(100)
- `credentials` VARCHAR(100)
- `npi_number` VARCHAR(10)
- `effective_start_date` 🔄 TIMESTAMP_NTZ
- `is_current` 🔄 BOOLEAN

**Lucidchart Color**: Light Green (#C8E6C9)

---

#### 4. dim_facilities
**Type**: Type 2 SCD
**Purpose**: VA facilities and medical centers
**Primary Key**: `facility_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `facility_id` (VARCHAR(50))

**Key Columns**:
- `facility_sk` 🔑 INTEGER
- `facility_id` ⭐ VARCHAR(50)
- `facility_name` VARCHAR(255)
- `station_number` VARCHAR(10)
- `visn_number` INTEGER
- `facility_type` VARCHAR(50)
- `is_current` 🔄 BOOLEAN

**Lucidchart Color**: Light Cyan (#B2EBF2)

---

#### 5. dim_evaluation_types
**Type**: Type 1 Dimension
**Purpose**: Types of medical evaluations
**Primary Key**: `evaluation_type_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `evaluation_type_id` (VARCHAR(50))

**Key Columns**:
- `evaluation_type_sk` 🔑 INTEGER
- `evaluation_type_id` ⭐ VARCHAR(50)
- `evaluation_type_name` VARCHAR(255)
- `evaluation_category` VARCHAR(100)
- `typical_duration_minutes` INTEGER
- `requires_specialist` BOOLEAN

**Lucidchart Color**: Light Yellow (#FFF9C4)

---

#### 6. dim_medical_conditions
**Type**: Type 1 Dimension
**Purpose**: Medical conditions and diagnoses
**Primary Key**: `medical_condition_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `medical_condition_id` (VARCHAR(50))

**Key Columns**:
- `medical_condition_sk` 🔑 INTEGER
- `medical_condition_id` ⭐ VARCHAR(50)
- `condition_name` VARCHAR(255)
- `icd10_code` VARCHAR(10)
- `body_system` VARCHAR(100)
- `dbq_form_number` VARCHAR(20)

**Lucidchart Color**: Light Pink (#F8BBD0)

---

#### 7. dim_claims
**Type**: Type 2 SCD
**Purpose**: VA disability claims
**Primary Key**: `claim_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `claim_id` (VARCHAR(50))

**Key Columns**:
- `claim_sk` 🔑 INTEGER
- `claim_id` ⭐ VARCHAR(50)
- `claim_number` VARCHAR(50)
- `claim_type` VARCHAR(50)
- `claim_status` VARCHAR(50)
- `is_current` 🔄 BOOLEAN

**Lucidchart Color**: Light Orange (#FFCCBC)

---

#### 8. dim_appointments
**Type**: Type 1 Dimension
**Purpose**: Appointment details
**Primary Key**: `appointment_sk` (INTEGER AUTOINCREMENT)
**Business Key**: `appointment_id` (VARCHAR(50))

**Key Columns**:
- `appointment_sk` 🔑 INTEGER
- `appointment_id` ⭐ VARCHAR(50)
- `appointment_type` VARCHAR(100)
- `visit_type` VARCHAR(50)
- `scheduled_duration_minutes` INTEGER

**Lucidchart Color**: Light Lime (#E6EE9C)

---

### Fact Tables (WAREHOUSE Schema)

#### 1. fct_evaluations_completed
**Type**: Transaction Fact Table
**Grain**: One row per evaluation per condition
**Primary Key**: `evaluation_fact_sk` (INTEGER AUTOINCREMENT)

**Foreign Keys**:
- `veteran_sk` → dim_veterans 🔗
- `evaluator_sk` → dim_evaluators 🔗
- `facility_sk` → dim_facilities 🔗
- `evaluation_type_sk` → dim_evaluation_types 🔗
- `medical_condition_sk` → dim_medical_conditions 🔗
- `claim_sk` → dim_claims 🔗
- `appointment_sk` → dim_appointments 🔗
- `evaluation_date_sk` → dim_dates 🔗

**Key Measures**:
- `evaluation_duration_minutes` 📊 INTEGER
- `report_completeness_score` 📊 DECIMAL(5,2)
- `disability_rating_percentage` 📊 INTEGER
- `evaluation_cost` 📊 DECIMAL(10,2)

**Clustering**: (`evaluation_date_sk`, `facility_sk`)

**Lucidchart Color**: Orange (#FFE0B2)

---

#### 2. fct_claim_status_changes
**Type**: Accumulating Snapshot Fact
**Grain**: One row per claim (updated as claim progresses)
**Primary Key**: `claim_status_fact_sk` (INTEGER AUTOINCREMENT)

**Foreign Keys**:
- `veteran_sk` → dim_veterans 🔗
- `claim_sk` → dim_claims 🔗
- `facility_sk` → dim_facilities 🔗
- Multiple date keys for milestones (claim_received_date_sk, exam_completed_date_sk, etc.)

**Key Measures**:
- `days_to_schedule` 📊 INTEGER
- `days_to_complete_exam` 📊 INTEGER
- `days_to_rating_decision` 📊 INTEGER
- `total_cycle_time_days` 📊 INTEGER

**Clustering**: (`claim_sk`, `rating_decision_date_sk`)

**Lucidchart Color**: Deep Orange (#FFAB91)

---

#### 3. fct_appointments_scheduled
**Type**: Transaction Fact Table
**Grain**: One row per appointment
**Primary Key**: `appointment_fact_sk` (INTEGER AUTOINCREMENT)

**Foreign Keys**:
- `veteran_sk` → dim_veterans 🔗
- `evaluator_sk` → dim_evaluators 🔗
- `facility_sk` → dim_facilities 🔗
- `evaluation_type_sk` → dim_evaluation_types 🔗
- `appointment_sk` → dim_appointments 🔗
- `claim_sk` → dim_claims 🔗
- `appointment_date_sk` → dim_dates 🔗

**Key Measures**:
- `actual_duration_minutes` 📊 INTEGER
- `wait_time_days` 📊 INTEGER
- `veteran_satisfaction_score` 📊 INTEGER
- `travel_distance_miles` 📊 DECIMAL(8,2)

**Clustering**: (`appointment_date_sk`, `facility_sk`)

**Lucidchart Color**: Amber (#FFE082)

---

#### 4. fct_daily_facility_snapshot
**Type**: Periodic Snapshot Fact
**Grain**: One row per facility per day
**Primary Key**: `daily_snapshot_sk` (INTEGER AUTOINCREMENT)
**Unique**: (`facility_sk`, `snapshot_date_sk`)

**Foreign Keys**:
- `facility_sk` → dim_facilities 🔗
- `snapshot_date_sk` → dim_dates 🔗

**Key Measures**:
- `scheduled_appointments_count` 📊 INTEGER
- `completed_evaluations_count` 📊 INTEGER
- `cancelled_appointments_count` 📊 INTEGER
- `average_wait_time_days` 📊 DECIMAL(8,2)
- `facility_utilization_rate` 📊 DECIMAL(5,2)

**Clustering**: (`snapshot_date_sk`, `facility_sk`)

**Lucidchart Color**: Yellow (#FFF59D)

---

## Relationship Specifications

### Relationships from dim_dates

| From Table | To Table | Foreign Key Column | Relationship Type | Label |
|------------|----------|-------------------|-------------------|-------|
| dim_dates | fct_evaluations_completed | evaluation_date_sk | 1:M | occurred on |
| dim_dates | fct_claim_status_changes | claim_received_date_sk | 1:M | received on |
| dim_dates | fct_claim_status_changes | exam_scheduled_date_sk | 1:M | scheduled on |
| dim_dates | fct_appointments_scheduled | appointment_date_sk | 1:M | scheduled for |
| dim_dates | fct_daily_facility_snapshot | snapshot_date_sk | 1:M | snapshot date |

### Relationships from dim_veterans

| From Table | To Table | Foreign Key Column | Relationship Type | Label |
|------------|----------|-------------------|-------------------|-------|
| dim_veterans | fct_evaluations_completed | veteran_sk | 1:M | has evaluations |
| dim_veterans | fct_claim_status_changes | veteran_sk | 1:M | filed claims |
| dim_veterans | fct_appointments_scheduled | veteran_sk | 1:M | has appointments |

### Relationships from dim_evaluators

| From Table | To Table | Foreign Key Column | Relationship Type | Label |
|------------|----------|-------------------|-------------------|-------|
| dim_evaluators | fct_evaluations_completed | evaluator_sk | 1:M | performed by |
| dim_evaluators | fct_appointments_scheduled | evaluator_sk | 1:M | assigned to |

### Relationships from dim_facilities

| From Table | To Table | Foreign Key Column | Relationship Type | Label |
|------------|----------|-------------------|-------------------|-------|
| dim_facilities | fct_evaluations_completed | facility_sk | 1:M | conducted at |
| dim_facilities | fct_claim_status_changes | facility_sk | 1:M | processed at |
| dim_facilities | fct_appointments_scheduled | facility_sk | 1:M | located at |
| dim_facilities | fct_daily_facility_snapshot | facility_sk | 1:M | metrics for |

### Relationships from Other Dimensions

| From Table | To Table | Foreign Key Column | Relationship Type | Label |
|------------|----------|-------------------|-------------------|-------|
| dim_evaluation_types | fct_evaluations_completed | evaluation_type_sk | 1:M | type of |
| dim_evaluation_types | fct_appointments_scheduled | evaluation_type_sk | 1:M | scheduled type |
| dim_medical_conditions | fct_evaluations_completed | medical_condition_sk | 1:M | evaluates |
| dim_claims | fct_evaluations_completed | claim_sk | 1:M | supports |
| dim_claims | fct_claim_status_changes | claim_sk | 1:M | tracks |
| dim_claims | fct_appointments_scheduled | claim_sk | 1:M | related to |
| dim_appointments | fct_evaluations_completed | appointment_sk | 1:M | via appointment |
| dim_appointments | fct_appointments_scheduled | appointment_sk | 1:M | appointment details |

---

## Styling Guidelines

### Color Palette

**Dimension Tables**:
- Header Background: Pastel shades of blue, green, purple
- Border: Dark version of header color (2px solid)
- Text: Dark gray (#333333)

**Fact Tables**:
- Header Background: Pastel shades of orange, amber, yellow
- Border: Dark orange (#E65100, 2px solid)
- Text: Dark gray (#333333)

**Relationships**:
- Line Color: Medium gray (#757575)
- Line Width: 2px
- Line Style: Solid
- Crow's Foot: Black, 3px

### Typography

**Table Headers**:
- Font: Arial or Helvetica
- Size: 12pt
- Weight: Bold
- Color: Dark Blue (#1565C0) for dimensions, Dark Orange (#E65100) for facts

**Primary Keys**:
- Font: Arial
- Size: 11pt
- Weight: Bold
- Color: Black (#000000)
- Prefix with 🔑 symbol

**Foreign Keys**:
- Font: Arial
- Size: 10pt
- Weight: Regular
- Color: Blue (#1976D2)
- Prefix with 🔗 symbol

**Regular Columns**:
- Font: Arial
- Size: 10pt
- Weight: Regular
- Color: Dark Gray (#424242)

**SCD Attributes**:
- Font: Arial
- Size: 9pt
- Weight: Italic
- Color: Gray (#757575)

### Spacing

- **Between Tables**: 150-250px minimum
- **Line Padding**: 4px
- **Table Padding**: 8px internal
- **Connector Offset**: 10px from table edge

---

## Diagram Export Settings

### For Presentations

**Format**: PNG or PDF
**Resolution**: 300 DPI
**Size**: Fit to page or custom (1920x1080 for slides)
**Background**: Transparent or White
**Quality**: High

### For Documentation

**Format**: PDF (vector)
**Size**: Letter or A4
**Margins**: 0.5 inch all sides
**Embed Fonts**: Yes

### For Collaboration

**Format**: Lucidchart native (.lucid)
**Share Settings**: Edit access for team, view for stakeholders
**Version**: Enable version history

---

## Quick Start Checklist

- [ ] Create new Lucidchart document
- [ ] Enable ERD shape libraries
- [ ] Import table definitions from CSV files (see lucidchart_tables.csv)
- [ ] Create 8 dimension table entities
- [ ] Create 4 fact table entities
- [ ] Draw 19 relationship connectors
- [ ] Apply color scheme and styling
- [ ] Add legend and annotations
- [ ] Add title and metadata
- [ ] Review and adjust layout
- [ ] Export to PDF/PNG
- [ ] Share with team

---

## Tips for Success

1. **Start Simple**: Create overview diagram first with just table names and keys
2. **Use Containers**: Group related tables in containers (e.g., all dims together)
3. **Align Elements**: Use Lucidchart's alignment tools for professional look
4. **Consistent Spacing**: Keep uniform spacing between all elements
5. **Color Consistency**: Use the same colors for same entity types
6. **Clear Labels**: Add descriptive labels to relationships
7. **Save Often**: Lucidchart auto-saves, but manually save versions at milestones
8. **Get Feedback**: Share draft with team before finalizing

---

## Additional Resources

- **CSV Import Files**: See `lucidchart_tables.csv` and `lucidchart_relationships.csv`
- **Complete Table Specs**: See `DIMENSIONAL_MODEL_DOCUMENTATION.md`
- **Naming Standards**: See `NAMING_CONVENTION_ALIGNMENT_REPORT.md`

---

**Document Version**: 1.0
**Author**: Data Engineering Team
**Last Updated**: 2025-11-16
