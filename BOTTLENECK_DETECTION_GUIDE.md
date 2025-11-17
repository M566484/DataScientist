# Exam Processing Bottleneck Detection System

## Overview

This system identifies and analyzes bottlenecks in the exam processing workflow, distinguishing between **internal VEMS-controlled processes** and **external dependencies** (VA systems and veteran availability). It provides comprehensive visibility into where delays occur and helps prioritize process improvement efforts.

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Internal vs External Bottleneck Classification](#internal-vs-external-bottleneck-classification)
3. [Data Model](#data-model)
4. [Using the Bottleneck Detection Queries](#using-the-bottleneck-detection-queries)
5. [Interpreting Results](#interpreting-results)
6. [Actionable Insights & Recommendations](#actionable-insights--recommendations)
7. [Query Reference](#query-reference)

---

## System Architecture

### Exam Processing Workflow

The system tracks bottlenecks across 8 stages of exam processing:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     EXAM PROCESSING WORKFLOW                             │
└─────────────────────────────────────────────────────────────────────────┘

1. INTAKE & VALIDATION (Internal VEMS)
   ├─ Request received from VA
   ├─ Eligibility verification
   └─ Missing information requests

2. QUEUE WAIT (Internal VEMS)
   ├─ Request enters assignment queue
   └─ Wait for available examiner

3. EXAMINER ASSIGNMENT (Internal VEMS)
   ├─ Examiner selection
   ├─ Acceptance/rejection
   └─ Potential reassignments

4. SCHEDULING (Mixed - Internal + External)
   ├─ Appointment coordination
   ├─ Veteran availability
   └─ Potential reschedules

5. APPOINTMENT WAIT (External - Veteran)
   └─ Wait until scheduled appointment date

6. EXAM EXECUTION (Internal VEMS)
   ├─ Veteran exam conducted
   └─ Documentation completed

7. QA REVIEW (Internal VEMS)
   ├─ Initial QA review
   ├─ Clarification cycles (if needed)
   └─ Final approval

8. VA DELIVERY (External - VA)
   └─ Results delivered to VA
```

### Data Flow

```
┌──────────────────────────┐
│  SOURCE FACT TABLES      │
│  - fact_exam_requests    │
│  - fact_examiner_assign  │
│  - fact_appointment_evt  │
│  - fact_evaluation_qa    │
│  - fact_evaluations      │
└────────────┬─────────────┘
             │
             │ Aggregation & Analysis
             ▼
┌──────────────────────────────────────┐
│  fact_exam_processing_bottlenecks    │
│  - Stage timings                     │
│  - Bottleneck classification         │
│  - Internal/External breakdown       │
│  - Capacity indicators               │
│  - Quality metrics                   │
│  - SLA tracking                      │
└────────────┬─────────────────────────┘
             │
             │ Analysis Queries
             ▼
┌──────────────────────────────────────┐
│  BOTTLENECK ANALYSIS QUERIES         │
│  - Stage-by-stage analysis           │
│  - Internal vs external comparison   │
│  - Capacity bottlenecks              │
│  - Quality bottlenecks               │
│  - SLA risk analysis                 │
│  - Root cause identification         │
└──────────────────────────────────────┘
```

---

## Internal vs External Bottleneck Classification

Understanding the distinction between internal and external bottlenecks is critical for effective process improvement.

### Internal VEMS Bottlenecks

**Definition:** Delays in processes that VEMS directly controls and can improve through operational changes.

**Stages:**
- **Intake & Validation** - Processing time for initial request review
- **Queue Wait** - Time waiting for examiner availability
- **Examiner Assignment** - Time to assign and accept examiner
- **Exam Execution** - Duration of the exam itself
- **QA Review** - Time for quality review and clarification cycles

**Improvement Strategies:**
- Add examiner capacity in bottleneck specialties
- Streamline validation procedures
- Improve QA processes to reduce rework
- Optimize workload distribution
- Enhance training to improve quality scores

### External Bottlenecks

**Definition:** Delays dependent on parties outside VEMS control (veterans, VA systems).

**Stages:**
- **Appointment Wait** - Time between scheduling and appointment (veteran availability)
- **VA Delivery** - Time for VA to receive and acknowledge results

**Improvement Strategies:**
- Proactive scheduling outreach to veterans
- Offer more appointment slots/times
- Streamline delivery mechanisms to VA
- Improve communication channels

### Mixed Bottlenecks

**Definition:** Processes involving both internal and external factors.

**Stages:**
- **Scheduling Coordination** - Requires both VEMS staff and veteran coordination

**Improvement Strategies:**
- Automated scheduling tools
- Better veteran communication
- Flexible appointment options

### Key Metrics

The `fact_exam_processing_bottlenecks` table calculates:

```sql
-- Internal process time (hours spent in VEMS-controlled stages)
internal_process_hours = validation + queue_wait + assignment + exam + qa_review

-- External dependency time (hours waiting on external parties)
external_dependency_hours = appointment_wait + va_delivery_wait

-- Percentages
internal_percentage = (internal_process_hours / total_cycle_time) * 100
external_percentage = (external_dependency_hours / total_cycle_time) * 100
```

---

## Data Model

### Fact Table: `fact_exam_processing_bottlenecks`

**Purpose:** Comprehensive bottleneck detection and analysis for exam processing
**Grain:** One row per exam request
**Type:** Accumulating snapshot (updated as requests progress)

**Key Field Categories:**

#### 1. Stage Timing Metrics
Tracks duration (in hours) at each processing stage:
- `intake_to_validation_hours`
- `queue_wait_hours`
- `time_to_examiner_response_hours`
- `assignment_to_scheduling_hours`
- `scheduled_to_appointment_hours`
- `total_qa_process_hours`
- `total_cycle_time_hours`

#### 2. Bottleneck Classification
Identifies primary and secondary bottlenecks:
- `primary_bottleneck_stage` - Stage with longest duration
- `primary_bottleneck_hours` - Duration of primary bottleneck
- `primary_bottleneck_type` - 'INTERNAL_VEMS', 'EXTERNAL_VA', 'EXTERNAL_VETERAN', or 'MIXED'
- Stage-specific flags: `queue_bottleneck_flag`, `qa_review_bottleneck_flag`, etc.

#### 3. Internal vs External Breakdown
- `internal_process_hours` - Total time in VEMS control
- `external_dependency_hours` - Total time waiting on external parties
- `internal_percentage` - % of time in internal processes
- `external_percentage` - % of time in external dependencies

#### 4. Capacity & Workload Indicators
- `examiner_overload_flag` - Examiner capacity constraint
- `examiner_workload_at_assignment` - Active cases at assignment
- `examiner_utilization_pct` - Capacity utilization
- `facility_capacity_constraint_flag` - Facility at capacity
- `specialty_shortage_flag` - Limited examiners in specialty

#### 5. Quality & Rework Indicators
- `qa_rework_flag` - Required multiple QA cycles
- `qa_cycle_count` - Number of QA review iterations
- `first_pass_approval_flag` - Approved on first review
- `clarification_count` - Number of clarification requests
- `overall_quality_score` - Quality rating (0-100)

#### 6. SLA & Performance Metrics
- `sla_met_flag` - Whether SLA was met
- `sla_breach_flag` - Whether SLA was breached
- `sla_at_risk_flag` - At risk of breach
- `days_until_sla_breach` - Remaining time
- `overall_performance_rating` - 'EXCELLENT', 'GOOD', 'AVERAGE', 'POOR', 'CRITICAL'

#### 7. Root Cause Analysis
- `likely_root_cause` - Primary cause of bottleneck
- `contributing_factors` - Additional factors
- Pattern flags: `chronic_reassignment_pattern_flag`, `quality_issue_pattern_flag`, etc.

### Population Query

The table is populated from the view `vw_populate_bottleneck_fact`, which:
1. Joins data from multiple fact tables
2. Calculates stage durations
3. Identifies primary bottlenecks
4. Computes internal/external breakdowns
5. Flags capacity and quality issues
6. Performs root cause analysis

---

## Using the Bottleneck Detection Queries

The file `snowflake/monitoring/bottleneck_analysis_queries.sql` contains pre-built queries organized into 7 categories.

### Query Categories

#### 1. Stage-by-Stage Bottleneck Analysis
**Queries:** 1.1 - 1.3
**Purpose:** Identify which processing stages cause the most delays
**Use Cases:**
- Weekly operational reviews
- Identifying systemic stage delays
- Tracking performance trends

**Example Output:**
```
processing_stage         | avg_duration_hours | bottleneck_percentage
-------------------------+-------------------+----------------------
QA Review Process        | 72.5              | 28%
Queue Wait               | 68.2              | 31%
Appointment Wait Time    | 156.8             | 18%
```

#### 2. Internal vs External Process Bottlenecks
**Queries:** 2.1 - 2.4
**Purpose:** Compare internal VEMS processes to external dependencies
**Use Cases:**
- Focus improvement efforts on controllable processes
- Understand external dependency impact
- Facility/specialty efficiency analysis

**Example Output:**
```
time_distribution_category | request_count | avg_internal_pct | avg_external_pct | sla_breach_rate
---------------------------+--------------+-----------------+-----------------+----------------
Primarily Internal (75%+)  | 1,234        | 82%             | 18%             | 12%
Mostly External (25-49%)   | 567          | 35%             | 65%             | 8%
```

#### 3. Capacity & Workload Bottlenecks
**Queries:** 3.1 - 3.3
**Purpose:** Identify capacity constraints causing delays
**Use Cases:**
- Resource planning and hiring decisions
- Workload balancing
- Specialty staffing analysis

**Example Output:**
```
examiner_name  | specialty        | overload_rate_pct | avg_workload | sla_breach_rate
---------------+------------------+------------------+-------------+----------------
Dr. Smith      | Orthopedics      | 45%              | 23.5        | 18%
Dr. Johnson    | Mental Health    | 38%              | 21.2        | 15%
```

#### 4. Quality & Rework Bottlenecks
**Queries:** 4.1 - 4.3
**Purpose:** Analyze how quality issues create delays through rework
**Use Cases:**
- Training needs identification
- QA process improvement
- Quality-performance correlation

**Example Output:**
```
qa_cycle_category          | request_count | avg_qa_hours | sla_breach_rate
---------------------------+--------------+-------------+----------------
1 - First Pass Approval    | 2,145        | 24.3        | 5%
2 - One Rework Cycle       | 876          | 48.7        | 12%
3+ - Multiple Rework       | 234          | 96.2        | 28%
```

#### 5. SLA Risk & Performance Analysis
**Queries:** 5.1 - 5.2
**Purpose:** Monitor SLA compliance and identify at-risk requests
**Use Cases:**
- Daily operational prioritization
- SLA breach prevention
- Performance monitoring

**Example Output:**
```
primary_bottleneck_stage | sla_breach_rate | avg_cycle_time_days
-------------------------+----------------+--------------------
QA Review                | 22%            | 12.5
Queue Wait               | 18%            | 11.3
Appointment Wait         | 8%             | 9.7
```

#### 6. Root Cause Analysis
**Queries:** 6.1 - 6.2
**Purpose:** Identify systemic root causes and patterns
**Use Cases:**
- Process improvement initiatives
- Strategic planning
- Pattern recognition

**Example Output:**
```
likely_root_cause              | occurrence_count | avg_cycle_time_days | sla_breach_rate
-------------------------------+-----------------+--------------------+----------------
Examiner capacity shortage     | 456             | 13.2               | 22%
Multiple QA rework cycles      | 334             | 14.5               | 25%
Extended veteran wait time     | 289             | 11.8               | 9%
```

#### 7. Dashboard Monitoring Queries
**Queries:** 7.1 - 7.3
**Purpose:** Executive dashboards and automated monitoring
**Use Cases:**
- Daily operations dashboards
- Executive reporting
- Automated alerting

---

## Interpreting Results

### Understanding Bottleneck Types

#### INTERNAL_VEMS Bottlenecks

**What it means:** The delay is in a process VEMS controls

**Where to look:**
- Examiner capacity and utilization
- Queue management efficiency
- QA review processes and quality scores
- Validation procedures

**Red flags:**
- Queue wait > 72 hours
- QA review > 72 hours
- Examiner utilization > 90%
- QA cycle count > 2
- First pass approval rate < 70%

**Action items:**
- Review examiner staffing levels
- Analyze workload distribution
- Improve QA training and standards
- Streamline validation procedures

#### EXTERNAL_VETERAN Bottlenecks

**What it means:** Delay is waiting for veteran availability

**Where to look:**
- Appointment wait times
- Reschedule rates
- No-show rates
- Scheduling coordination efficiency

**Red flags:**
- Appointment wait > 168 hours (7 days)
- Reschedule count > 2
- No-show rate > 10%
- Scheduling coordination > 48 hours

**Action items:**
- Improve veteran communication
- Offer more flexible scheduling
- Proactive outreach for appointments
- Consider telehealth options

#### EXTERNAL_VA Bottlenecks

**What it means:** Delay is waiting for VA systems/processes

**Where to look:**
- VA delivery acknowledgment times
- Request validation delays due to missing VA data

**Red flags:**
- VA delivery > 48 hours
- Frequent missing information requests

**Action items:**
- Improve VA integration
- Streamline delivery processes
- Enhance data exchange

### Performance Rating Interpretation

```
EXCELLENT (< 80% of avg cycle time)
  → Process running efficiently
  → Use as benchmark for best practices

GOOD (80-100% of avg cycle time)
  → Normal performance range
  → Monitor for trends

AVERAGE (100-120% of avg cycle time)
  → Slightly slower than typical
  → Review for improvement opportunities

POOR (120-150% of avg cycle time)
  → Significant delays occurring
  → Requires attention

CRITICAL (> 150% of avg cycle time)
  → Severe bottlenecks
  → Immediate action required
```

### SLA Risk Levels

```
BREACHED (days_until_breach < 0)
  → SLA already missed
  → Document reason, expedite completion

CRITICAL (0-1 days until breach)
  → Immediate action required
  → Escalate to management

HIGH RISK (2-3 days until breach)
  → Daily monitoring
  → Clear roadblocks

MODERATE RISK (4-5 days until breach)
  → Monitor closely
  → Prepare contingency plans

AT RISK (within 20% of SLA)
  → Track progress
  → Identify potential delays early
```

---

## Actionable Insights & Recommendations

### Daily Operations

**Morning Priority Review** (Query 5.2 - At-Risk Requests Dashboard)
```sql
-- Run daily to identify requests needing immediate attention
-- Filter for: sla_at_risk_flag = TRUE OR sla_breach_flag = TRUE
-- Action: Prioritize work queue based on risk_level
```

**Capacity Monitoring** (Query 7.3 - Actionable Alerts)
```sql
-- Run daily to detect emerging bottlenecks
-- Look for: CRITICAL or WARNING alert_status
-- Action: Redistribute workload, add capacity as needed
```

### Weekly Reviews

**Stage Performance Analysis** (Query 1.1)
```sql
-- Run weekly to identify systemic delays
-- Focus on: Stages with bottleneck_percentage > 25%
-- Action: Deep dive into high-percentage bottlenecks
```

**Internal Process Efficiency** (Query 2.2)
```sql
-- Run weekly to monitor VEMS-controlled processes
-- Watch for: bottleneck_rate_pct trends
-- Action: Address increasing internal bottleneck rates
```

**Quality Trends** (Query 4.1)
```sql
-- Run weekly to track QA performance
-- Monitor: First pass approval rate
-- Action: If < 70%, review training needs
```

### Monthly Analysis

**Capacity Planning** (Queries 3.1 - 3.3)
```sql
-- Run monthly for resource planning
-- Identify: Specialties with shortage_flag = TRUE
-- Action: Recruit/train examiners in shortage specialties
```

**Root Cause Review** (Query 6.1)
```sql
-- Run monthly to guide process improvements
-- Focus on: Top 3-5 root causes by occurrence
-- Action: Develop improvement initiatives
```

**Trend Analysis** (Query 1.3)
```sql
-- Run monthly to track changes over time
-- Compare: Month-over-month bottleneck shifts
-- Action: Validate improvement initiative effectiveness
```

### Executive Reporting

**KPI Dashboard** (Query 7.1)
```sql
-- Run monthly for board/executive reporting
-- Highlight: SLA met rate, cycle time trends, top bottlenecks
-- Action: Strategic decision-making
```

---

## Query Reference

### Quick Reference Table

| Query | Name | Frequency | Primary Users | Key Metric |
|-------|------|-----------|---------------|------------|
| 1.1 | Stage Duration Analysis | Weekly | Operations Managers | avg_duration_hours by stage |
| 1.2 | Top Bottleneck Stages | Weekly | Process Improvement | bottleneck_occurrence_count |
| 1.3 | Bottleneck Trends | Monthly | Leadership | Trend over time |
| 2.1 | Internal vs External Distribution | Weekly | Operations | internal_percentage |
| 2.2 | Internal Bottleneck Detail | Weekly | VEMS Leadership | Internal stage performance |
| 2.3 | External Dependency Detail | Weekly | Scheduling Team | External wait times |
| 2.4 | VEMS Process Efficiency | Monthly | Facility Managers | internal_bottleneck_rate |
| 3.1 | Examiner Capacity Analysis | Monthly | HR/Resource Planning | overload_rate_pct |
| 3.2 | Facility Capacity Constraints | Monthly | Facility Managers | capacity_constraint_rate |
| 3.3 | Specialty Availability | Monthly | Recruiting | shortage_rate_pct |
| 4.1 | QA Rework Impact | Weekly | QA Leadership | qa_cycle_count impact |
| 4.2 | QA Deficiency Types | Weekly | Training Team | Deficiency breakdown |
| 4.3 | Quality vs Cycle Time | Monthly | Leadership | Quality correlation |
| 5.1 | SLA Breach by Bottleneck | Weekly | Operations | sla_breach_rate |
| 5.2 | At-Risk Requests Dashboard | **DAILY** | All Teams | Current at-risk list |
| 6.1 | Top Root Causes | Monthly | Process Improvement | Root cause frequency |
| 6.2 | Pattern Analysis | Monthly | Strategic Planning | Pattern identification |
| 7.1 | Executive Dashboard | Monthly | Leadership | Overall KPIs |
| 7.2 | Weekly Trends | Weekly | Operations | Week-over-week trends |
| 7.3 | Actionable Alerts | **DAILY** | Operations | Performance degradation |

### Query Customization

All queries can be filtered by:
- Date range: `WHERE request_date_sk BETWEEN X AND Y`
- Facility: `WHERE facility_dim_sk = X`
- Specialty: `WHERE specialty_dim_sk = X`
- Exam type: `WHERE exam_type_dim_sk = X`
- Priority: `WHERE priority_level = 'HIGH'`
- Source system: `WHERE source_system IN ('OMS', 'VEMS')`

Example:
```sql
-- Focus on high-priority orthopedic exams from last month
SELECT ...
FROM datascience.warehouse.fact_exam_processing_bottlenecks fb
JOIN datascience.warehouse.dim_specialty ds ON fb.specialty_dim_sk = ds.specialty_sk
WHERE ds.specialty_name = 'Orthopedics'
  AND fb.priority_level = 'HIGH'
  AND fb.request_date_sk >= DATEADD(month, -1, CURRENT_DATE())
```

---

## Implementation Checklist

### Phase 1: Data Setup
- [ ] Create `fact_exam_processing_bottlenecks` table
- [ ] Populate table using `vw_populate_bottleneck_fact` view
- [ ] Validate data completeness and accuracy
- [ ] Create indexes for query performance

### Phase 2: Query Deployment
- [ ] Test all queries against production data
- [ ] Customize queries for your organization's needs
- [ ] Set up query scheduling (daily/weekly/monthly)
- [ ] Create query result storage for trending

### Phase 3: Dashboard Creation
- [ ] Build operational dashboard (daily monitoring)
- [ ] Build executive dashboard (monthly KPIs)
- [ ] Set up automated reports and alerts
- [ ] Train users on dashboard interpretation

### Phase 4: Process Integration
- [ ] Integrate into daily standup meetings
- [ ] Add to weekly operational reviews
- [ ] Include in monthly strategic planning
- [ ] Establish escalation procedures for critical alerts

### Phase 5: Continuous Improvement
- [ ] Baseline current performance
- [ ] Set improvement targets
- [ ] Track initiative effectiveness
- [ ] Iterate on bottleneck solutions

---

## Best Practices

### 1. Focus on Actionable Insights
- Don't just identify bottlenecks—create action plans
- Assign ownership for each identified issue
- Set measurable improvement targets
- Track improvement initiative outcomes

### 2. Prioritize Based on Impact
Use this priority matrix:

| Frequency | Impact on Cycle Time | Impact on SLA | Priority |
|-----------|---------------------|---------------|----------|
| High | High | High | **P0 - Critical** |
| High | High | Medium | **P1 - High** |
| High | Medium | High | **P1 - High** |
| Medium | High | High | **P1 - High** |
| All others | - | - | P2-P3 |

### 3. Balance Internal and External Focus
- 70% of effort on internal bottlenecks (most controllable)
- 30% of effort on external bottlenecks (influence what you can)
- Use internal efficiency gains to buffer external variability

### 4. Trend Over Time
- Single data points can be outliers
- Look for consistent patterns over 4+ weeks
- Validate improvements with before/after analysis
- Beware of seasonal variations

### 5. Drill Down Systematically
```
1. Identify top bottleneck stage (Query 1.1)
   ↓
2. Classify as internal/external (Query 2.1)
   ↓
3. If internal → Check capacity (Queries 3.x) and quality (Queries 4.x)
   ↓
4. If external → Analyze scheduling/veteran factors (Query 2.3)
   ↓
5. Identify root cause (Query 6.1)
   ↓
6. Implement improvement
   ↓
7. Monitor with trending queries (Queries 1.3, 7.2)
```

---

## Support and Maintenance

### Data Refresh Schedule
- **Real-time:** `fact_exam_processing_bottlenecks` updates as requests progress
- **Daily:** Refresh for at-risk dashboard (Query 5.2)
- **Weekly:** Full analytics refresh for trending queries

### Data Quality Checks
Run monthly to ensure data integrity:
```sql
-- Check for NULL primary bottlenecks
SELECT COUNT(*) FROM fact_exam_processing_bottlenecks
WHERE primary_bottleneck_stage IS NULL AND total_cycle_time_hours > 0;

-- Verify internal + external = total
SELECT COUNT(*) FROM fact_exam_processing_bottlenecks
WHERE ABS((internal_process_hours + external_dependency_hours) - total_cycle_time_hours) > 1;

-- Check for data completeness
SELECT request_date_sk, COUNT(*)
FROM fact_exam_processing_bottlenecks
GROUP BY request_date_sk
ORDER BY request_date_sk DESC
LIMIT 30;
```

### Troubleshooting

**Issue:** Query returns no results
**Solution:** Check date filters, ensure data is populated for date range

**Issue:** Performance is slow
**Solution:** Verify indexes exist, consider materializing frequently-used queries

**Issue:** Bottleneck classifications seem incorrect
**Solution:** Validate stage duration calculations, review classification thresholds

---

## Glossary

**Accumulating Snapshot Fact:** A fact table that tracks milestone dates throughout a process lifecycle, updated as the process progresses.

**Bottleneck:** The stage or process that limits overall throughput and causes the longest delays.

**Capacity Constraint:** A limitation in available resources (examiners, facilities) that creates delays.

**External Dependency:** Processes or delays caused by parties outside VEMS control (veterans, VA systems).

**Internal Process:** Processes directly controlled by VEMS that can be improved through operational changes.

**Primary Bottleneck:** The single stage with the longest duration in a request's lifecycle.

**QA Rework:** Additional quality review cycles required due to deficiencies in initial exam documentation.

**SLA At Risk:** A request within 20% of its SLA deadline with significant remaining work.

**Utilization Rate:** The percentage of examiner capacity currently in use (active cases / total capacity).

---

## Document Version

**Version:** 1.0
**Last Updated:** 2025-11-17
**Author:** Data Science Team
**Contact:** [Your contact information]

---

## Related Documentation

- [VES Process Flow Reference](VES_PROCESS_FLOW_REFERENCE.md) - Complete process flow diagrams
- [Dimensional Model Documentation](DIMENSIONAL_MODEL_DOCUMENTATION.md) - Data model details
- [Data Pipeline Architecture](DATA_PIPELINE_ARCHITECTURE.md) - ETL architecture
- [Evaluation QA Lifecycle Design](EVALUATION_QA_LIFECYCLE_DESIGN.md) - QA process details
