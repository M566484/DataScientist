# VES Process Flow Reference Guide

**Purpose**: Complete textual representation of VES examination processing workflow
**Source**: process_flow.png diagram
**Last Updated**: 2025-11-16

---

## Table of Contents

1. [Process Overview](#process-overview)
2. [Swim Lanes (Actors/Systems)](#swim-lanes-actorssystems)
3. [Stage 1: Exam Request Intake](#stage-1-exam-request-intake)
4. [Stage 2: Request Validation & Eligibility](#stage-2-request-validation--eligibility)
5. [Stage 3: Examiner Assignment](#stage-3-examiner-assignment)
6. [Stage 4: Appointment Scheduling](#stage-4-appointment-scheduling)
7. [Stage 5: Exam Execution](#stage-5-exam-execution)
8. [Stage 6: Documentation & Quality Assurance](#stage-6-documentation--quality-assurance)
9. [Stage 7: Delivery to VA](#stage-7-delivery-to-va)
10. [Stage 8: Payment Processing](#stage-8-payment-processing)
11. [Decision Points](#decision-points)
12. [Exception Handling](#exception-handling)
13. [Key Performance Indicators](#key-performance-indicators)

---

## Process Overview

The VES examination processing workflow manages the complete lifecycle of veteran medical evaluations from initial request receipt through final report delivery to the VA and examiner payment.

**Process Duration**: Typically 10-30 days (SLA varies by request type)

**Key Milestones**:
1. Request Received from VA
2. Eligibility Confirmed
3. Examiner Assigned
4. Appointment Scheduled
5. Exam Completed
6. QA Approved
7. Report Delivered to VA
8. Payment Processed

**Primary Actors**:
- VA (Veterans Affairs) - Request originator
- VES Operations - Request processing and coordination
- Examiners - Medical professionals conducting evaluations
- QA Team - Quality assurance reviewers
- Veterans - Exam recipients
- Finance - Payment processing

---

## Swim Lanes (Actors/Systems)

### Lane 1: VA (Veterans Affairs)
**Role**: Request origination and report receipt
**Key Actions**:
- Submit exam requests
- Receive completed evaluation reports
- Track request status
- Provide veteran information

### Lane 2: VES Operations / Intake Team
**Role**: Request processing, validation, coordination
**Key Actions**:
- Receive and log requests
- Validate eligibility
- Assign to examiners
- Schedule appointments
- Track SLAs
- Coordinate exceptions

### Lane 3: Examiners
**Role**: Conduct medical evaluations
**Key Actions**:
- Accept/reject assignments
- Schedule appointments
- Conduct examinations
- Complete DBQ forms
- Provide clarifications to QA
- Submit completed evaluations

### Lane 4: Quality Assurance Team
**Role**: Review evaluation quality and completeness
**Key Actions**:
- Review submitted evaluations
- Request clarifications
- Approve reports
- Escalate quality issues
- Release reports to VA

### Lane 5: Veterans
**Role**: Exam recipients
**Key Actions**:
- Receive appointment notifications
- Confirm appointments
- Attend examinations
- Provide medical history

### Lane 6: Finance / Payment Processing
**Role**: Examiner compensation
**Key Actions**:
- Validate exam completion
- Calculate payment amounts
- Process payments
- Track payment status

### Lane 7: Systems / Automation
**Role**: Workflow automation and tracking
**Key Actions**:
- Auto-assignment algorithms
- SLA monitoring
- Notifications and alerts
- Document management
- Reporting

---

## Stage 1: Exam Request Intake

### 1.1 Request Receipt
**Trigger**: VA submits exam request
**Input**: Exam request with veteran information
**Systems**: VES Intake System

**Process Steps**:
1. **Receive Request from VA**
   - Electronic submission via secure portal
   - Alternative channels: Mail, Fax, Direct API
   - Request includes: Veteran demographics, claim number, conditions to evaluate

2. **Log Request**
   - Assign unique VES request ID
   - Timestamp receipt
   - Link to VA request number
   - Categorize by request type

3. **Initial Data Validation**
   - Verify required fields present
   - Check data format compliance
   - Flag incomplete submissions

**Outputs**:
- Request logged in system
- Request ID assigned
- Initial validation status

**Exceptions**:
- ❌ **Incomplete Request** → Request additional information from VA
- ❌ **Duplicate Request** → Flag and consolidate with existing
- ❌ **Invalid Format** → Return to VA for correction

**Data Captured** (Maps to fct_exam_requests):
- exam_request_id
- va_request_number
- request_received_date_sk
- request_source_system
- request_channel
- request_priority

---

## Stage 2: Request Validation & Eligibility

### 2.1 Veteran Eligibility Verification
**Trigger**: Request successfully logged
**Owner**: VES Operations Team

**Process Steps**:
1. **Verify Veteran Identity**
   - Validate SSN, name, date of birth
   - Check against VA veteran database
   - Confirm active veteran status

2. **Check Eligibility Requirements**
   - Verify claim type is eligible for VES services
   - Confirm geographic service area
   - Check for service restrictions
   - Validate authorization for exam type

3. **Medical Records Review**
   - Determine if medical records needed
   - Request records from VA or veteran
   - Review existing medical history
   - Flag missing critical information

4. **Determine Exam Requirements**
   - Identify conditions to evaluate
   - Determine specialty requirements (orthopedic, psychological, etc.)
   - Assess complexity level
   - Identify if specialist needed

**Outputs**:
- Eligibility status: ELIGIBLE / NOT_ELIGIBLE / PENDING_VERIFICATION
- Required examiner specialty
- Complexity level assigned
- Missing information flagged

**Decision Points**:
- ✅ **Eligible** → Proceed to examiner assignment
- ❌ **Not Eligible** → Notify VA, close request
- ⏸️ **Pending Info** → Request additional information, hold in queue

**Exceptions**:
- Missing medical records → Request from VA/veteran
- Unclear exam requirements → Escalate to clinical team
- Authorization issues → Contact VA for clarification

**Data Captured** (Maps to fct_exam_requests):
- eligibility_status
- eligibility_confirmed_date_sk
- requested_conditions
- requested_conditions_count
- requires_specialist_flag
- required_specialty
- complex_case_flag
- missing_information_flag
- medical_records_requested

---

## Stage 3: Examiner Assignment

### 3.1 Examiner Selection
**Trigger**: Eligibility confirmed
**Owner**: VES Operations / Auto-Assignment System

**Process Steps**:
1. **Determine Assignment Method**
   - Auto-assignment if standard case
   - Manual assignment if complex/specialist required
   - Preferred examiner if veteran request
   - Emergency assignment if expedited

2. **Identify Qualified Examiners**
   - Filter by specialty match
   - Check geographic proximity
   - Verify certification/credentials
   - Review capacity and workload

3. **Apply Assignment Algorithm**
   - Calculate priority scores
   - Consider examiner utilization rates
   - Balance workload across examiners
   - Optimize for SLA compliance
   - Factor in examiner performance history

4. **Make Assignment**
   - Assign to selected examiner
   - Add to examiner's work queue
   - Set priority level
   - Calculate due date based on SLA

5. **Notify Examiner**
   - Send assignment notification (email/portal/SMS)
   - Provide case details
   - Include veteran information
   - Set acceptance deadline

**Outputs**:
- Examiner assigned
- Assignment notification sent
- Due date established
- Case added to examiner queue

**Decision Points**:
- ✅ **Examiner Accepts** → Proceed to scheduling
- ❌ **Examiner Rejects** → Reassign to different examiner
- ⏱️ **No Response** → Auto-escalate after timeout period

**Exceptions**:
- No qualified examiners available → Expand search radius
- Multiple rejections → Manual assignment by supervisor
- Capacity constraints → Escalate to operations manager
- Specialty not available → Contract with external specialist

**Data Captured** (Maps to fct_exam_requests):
- assigned_evaluator_sk
- assignment_started_date_sk
- examiner_assigned_date_sk
- assignment_status
- assignment_method
- assignment_attempts
- assignment_rejections

**Data Captured** (Maps to fct_examiner_assignments):
- assignment_event_id
- event_type (ASSIGNED, ACCEPTED, REJECTED, TRANSFERRED)
- event_timestamp
- assignment_method
- examiner_current_workload
- examiner_utilization_percentage
- specialty_match_flag
- travel_distance_miles
- time_to_response_hours

---

### 3.2 Examiner Acceptance/Rejection

**Process Steps**:
1. **Examiner Reviews Assignment**
   - Reviews case details
   - Checks schedule availability
   - Assesses specialty match
   - Considers geographic location

2. **Examiner Decision**
   - **Accept**: Confirms ability to perform exam
   - **Reject**: Provides rejection reason
   - **Transfer**: Requests transfer to colleague

3. **System Processing**
   - If accepted: Move to scheduling
   - If rejected: Return to assignment queue
   - If transferred: Process transfer request

**Rejection Reasons**:
- Scheduling conflict
- Capacity/workload concerns
- Specialty mismatch
- Geographic distance too far
- Personal reasons

**Transfer Handling**:
- Examiner requests transfer to specific colleague
- Supervisor approves/denies transfer
- If approved: New examiner receives assignment
- If denied: Original examiner must accept or reject

**Data Captured** (Maps to fct_examiner_assignments):
- is_acceptance / is_rejection
- acceptance_timestamp / rejection_timestamp
- time_to_response_hours
- rejection_reason_code
- rejection_category
- is_transfer_out / is_transfer_in
- transfer_reason_code
- transfer_initiated_by

---

## Stage 4: Appointment Scheduling

### 4.1 Initial Scheduling
**Trigger**: Examiner accepts assignment
**Owner**: Examiner or Scheduling Coordinator

**Process Steps**:
1. **Identify Available Time Slots**
   - Check examiner calendar
   - Consider veteran location
   - Account for travel time
   - Respect SLA deadlines
   - Factor in exam duration

2. **Contact Veteran**
   - Call veteran to schedule
   - Offer multiple appointment options
   - Confirm veteran availability
   - Verify location preferences
   - Discuss telehealth option if applicable

3. **Book Appointment**
   - Reserve time slot
   - Assign appointment location (clinic, home, telehealth)
   - Send confirmation to veteran
   - Add to examiner schedule
   - Set reminders

4. **Send Notifications**
   - Veteran appointment confirmation (email/mail/phone)
   - Examiner calendar update
   - Include appointment details: date, time, location, directions
   - Provide veteran preparation instructions

**Outputs**:
- Appointment scheduled
- Veteran notified
- Examiner calendar updated
- Confirmation sent

**Decision Points**:
- ✅ **Veteran Confirms** → Appointment set
- ❌ **Veteran Unavailable** → Find alternative time
- 📞 **Cannot Reach Veteran** → Retry contact attempts

**Exceptions**:
- Veteran requests telehealth → Verify eligibility, set up virtual
- Location accessibility issues → Arrange home visit
- Cannot contact veteran → Escalate to VA
- Scheduling conflicts → Find alternative examiner

**Data Captured** (Maps to fct_exam_requests):
- appointment_scheduled_date_sk
- scheduled_flag
- veteran_notified_flag
- veteran_notification_date
- telehealth_requested / telehealth_approved

**Data Captured** (Maps to fct_appointment_events):
- event_type: SCHEDULED
- appointment_id
- scheduled_date_sk
- scheduled_time
- appointment_location
- location_type (CLINIC, HOME, TELEHEALTH)
- veteran_confirmed_flag

---

### 4.2 Appointment Management

**Process Steps**:
1. **Pre-Appointment**
   - Send reminders (24-48 hours before)
   - Confirm veteran still available
   - Prepare examination materials
   - Review medical records

2. **Appointment Changes**
   - **Reschedule**: Veteran/examiner requests new time
   - **Cancellation**: Veteran/examiner cancels
   - **No-Show**: Veteran doesn't attend

**Rescheduling Process**:
1. Cancel existing appointment
2. Find new time slot
3. Create new appointment record
4. Notify all parties
5. Link appointments in chain (original → rescheduled)

**Cancellation Process**:
1. Record cancellation reason
2. Remove from calendar
3. Notify relevant parties
4. Return to assignment queue if examiner cancels
5. Contact veteran for rescheduling if veteran cancels

**No-Show Handling**:
1. Document no-show
2. Contact veteran to understand reason
3. Reschedule if appropriate
4. Report to VA if pattern of no-shows
5. May require VA reauthorization

**Data Captured** (Maps to fct_appointment_events):
- event_type: CONFIRMED, RESCHEDULED, CANCELLED, NO_SHOW
- previous_appointment_id (if rescheduled)
- new_appointment_id (if rescheduled)
- cancellation_reason_code
- cancelled_by (VETERAN, EXAMINER, VES, VA)
- no_show_reason
- rescheduling_count

---

## Stage 5: Exam Execution

### 5.1 Pre-Exam Preparation
**Trigger**: Appointment date approaching
**Owner**: Examiner

**Process Steps**:
1. **Review Case File**
   - Read veteran medical history
   - Review VA exam requirements
   - Prepare DBQ forms
   - Review previous exams if re-exam

2. **Prepare Materials**
   - Print DBQ templates
   - Prepare diagnostic equipment
   - Review specialty protocols
   - Set up exam room or telehealth

3. **Final Confirmation**
   - Confirm veteran will attend
   - Verify location/logistics
   - Ensure all materials ready

**Data Captured**:
- pre_exam_review_completed
- materials_prepared_flag

---

### 5.2 Conduct Examination
**Trigger**: Appointment time
**Owner**: Examiner

**Process Steps**:
1. **Veteran Check-In**
   - Verify veteran identity
   - Confirm consent
   - Review appointment purpose
   - Update contact information

2. **Medical History Review**
   - Interview veteran about medical history
   - Review current symptoms
   - Document service connection claims
   - Note relevant life events

3. **Physical Examination**
   - Conduct required medical tests
   - Perform specialty-specific evaluations
   - Document findings in real-time
   - Take measurements as required

4. **Diagnostic Testing** (if required)
   - Order additional tests
   - Perform in-clinic diagnostics
   - Document results

5. **Complete DBQ Form**
   - Fill out all required DBQ sections
   - Answer all mandatory questions
   - Provide nexus opinion (service connection)
   - Document functional limitations
   - Include medical rationale

6. **Veteran Debrief**
   - Explain exam process
   - Set expectations for timeline
   - Provide next steps
   - Answer veteran questions

**Outputs**:
- Exam completed
- DBQ form completed
- Findings documented
- Veteran debriefed

**Decision Points**:
- ✅ **Exam Complete** → Proceed to documentation
- ⚕️ **Additional Testing Needed** → Schedule follow-up
- ❌ **Incomplete Exam** → Document reason, may need to reschedule

**Exceptions**:
- Veteran doesn't show → Mark as no-show
- Veteran refuses exam → Document refusal, notify VA
- Medical emergency during exam → Handle emergency, reschedule
- Insufficient time to complete → Schedule continuation

**Data Captured** (Maps to fct_exam_requests):
- exam_completed_date_sk
- completed_flag

**Data Captured** (Maps to fct_evaluations_completed):
- evaluation_id
- evaluation_date_sk
- exam_start_timestamp
- exam_end_timestamp
- evaluation_duration_minutes
- exam_location_type
- incomplete_exam_flag
- incomplete_reason

**Data Captured** (Maps to fct_appointment_events):
- event_type: COMPLETED
- completion_timestamp
- exam_duration_minutes
- veteran_attended_flag

---

## Stage 6: Documentation & Quality Assurance

### 6.1 Report Preparation
**Trigger**: Exam completed
**Owner**: Examiner

**Process Steps**:
1. **Compile Examination Report**
   - Finalize DBQ form
   - Complete all required sections
   - Ensure all questions answered
   - Review for completeness

2. **Write Medical Rationale**
   - Provide detailed medical reasoning
   - Support nexus opinion with evidence
   - Reference diagnostic criteria
   - Document functional assessment

3. **Attach Supporting Documents**
   - Include test results
   - Attach relevant images
   - Reference medical records reviewed
   - Add examiner credentials

4. **Self-Review**
   - Check for completeness
   - Verify accuracy
   - Ensure clarity of writing
   - Confirm all requirements met

5. **Submit to QA**
   - Upload to QA system
   - Mark as ready for review
   - Add any examiner notes
   - Set submission timestamp

**Outputs**:
- Complete evaluation report
- DBQ form finalized
- Supporting documents attached
- Submitted to QA queue

**Data Captured** (Maps to fct_evaluations_completed):
- report_submission_date_sk
- report_page_count
- attachments_count
- dbq_completeness_percentage

**Data Captured** (Maps to fct_evaluation_qa_events):
- event_type: INITIAL_SUBMISSION
- evaluation_id
- submission_id
- event_timestamp
- document_page_count
- attachments_count
- qa_cycle_number: 1

---

### 6.2 Quality Assurance Review
**Trigger**: Report submitted by examiner
**Owner**: QA Team

**Process Steps**:
1. **QA Assignment**
   - Assign report to QA reviewer
   - Route based on specialty
   - Prioritize by SLA risk
   - Add to QA queue

2. **Initial QA Review**
   - Check DBQ completeness
   - Verify all items answered
   - Review nexus opinion quality
   - Assess medical rationale adequacy
   - Check for regulatory compliance
   - Run automated quality checks

3. **Quality Scoring**
   - Rate completeness (0-100)
   - Rate accuracy (0-100)
   - Rate clarity (0-100)
   - Rate nexus quality (0-100)
   - Calculate overall quality score

4. **QA Decision**
   - **APPROVED**: Report meets all quality standards
   - **NEEDS_CLARIFICATION**: Minor issues, need examiner input
   - **REJECTED**: Major deficiencies, return to examiner
   - **INSUFFICIENT**: Incomplete exam, may need re-examination

**Outputs**:
- QA review completed
- Quality scores assigned
- QA decision made
- Feedback documented

**Decision Points**:
- ✅ **APPROVED** → Proceed to VA delivery
- 🔄 **NEEDS_CLARIFICATION** → Request clarification from examiner
- ❌ **REJECTED** → Return to examiner for revision
- ⚠️ **INSUFFICIENT** → May require new exam

**Exceptions**:
- Complex case requires senior review → Escalate to QA manager
- Potential fraud detected → Escalate to compliance
- Regulatory concern → Legal/compliance review
- Systemic quality issues → Examiner coaching/training

**Data Captured** (Maps to fct_evaluation_qa_events):
- event_type: QA_REVIEW_STARTED, QA_REVIEW_COMPLETED
- qa_reviewer_sk
- review_started_timestamp
- review_completed_timestamp
- review_duration_minutes
- review_outcome (APPROVED, NEEDS_CLARIFICATION, REJECTED)
- completeness_score
- accuracy_score
- clarity_score
- nexus_quality_score
- overall_quality_score
- all_dbq_items_completed
- nexus_opinion_provided
- medical_rationale_adequate

---

### 6.3 Clarification Process
**Trigger**: QA requests clarification
**Owner**: QA Team → Examiner → QA Team

**Process Steps**:
1. **QA Identifies Deficiencies**
   - Document specific issues
   - Flag incomplete DBQ items
   - Note inadequate rationale
   - Identify missing nexus elements
   - Categorize severity (MINOR, MODERATE, MAJOR, CRITICAL)

2. **Request Clarification from Examiner**
   - Create clarification request
   - Specify required information
   - Set due date for response
   - Notify examiner
   - Provide detailed guidance

3. **Examiner Provides Clarification**
   - Review QA feedback
   - Provide additional explanation
   - May submit addendum
   - May revise DBQ
   - May conduct additional testing
   - Re-submit to QA

4. **QA Re-Review**
   - Review clarification response
   - Verify deficiencies addressed
   - Update quality scores
   - Make new QA decision

**Clarification Types**:
- Additional medical testing needed
- Nexus explanation required
- DBQ item completion needed
- Rationale expansion required
- Functional assessment detail needed

**Clarification Response Methods**:
- Written addendum
- Revised DBQ form
- Phone consultation with QA
- Additional veteran examination

**Iterative Process**:
- May require multiple clarification cycles
- Track cycle number (1st review, 2nd review, 3rd review, etc.)
- Each cycle follows same QA review process
- Maximum cycles typically 2-3 before escalation

**Outputs**:
- Clarification request documented
- Examiner response captured
- QA re-review completed
- Final decision made

**Data Captured** (Maps to fct_evaluation_qa_events):
- event_type: CLARIFICATION_REQUESTED
- is_clarification_request: TRUE
- clarification_type
- clarification_description
- clarification_due_date
- clarification_priority
- specific_dbq_items_flagged
- deficiency_found_flag
- deficiency_count
- deficiency_severity
- deficiency_category

- event_type: CLARIFICATION_SUBMITTED
- is_clarification_response: TRUE
- clarification_response_text
- clarification_response_complete
- clarification_response_method
- qa_cycle_number (incremented)

---

### 6.4 Final QA Approval
**Trigger**: Report meets quality standards
**Owner**: QA Team

**Process Steps**:
1. **Final Quality Check**
   - Confirm all deficiencies resolved
   - Verify DBQ completeness
   - Validate nexus opinion
   - Ensure regulatory compliance
   - Check all documentation present

2. **Approval Authorization**
   - Senior QA reviewer approval if required
   - Document approval decision
   - Add approval notes
   - Set approval timestamp

3. **Release for VA Delivery**
   - Mark as approved
   - Move to delivery queue
   - Prepare for transmission
   - Update status to "Ready for VA"

**Outputs**:
- Report approved
- Ready for VA delivery
- Approval documented
- Examiner notified of approval

**Quality Metrics**:
- First-pass approval rate (approved on first QA review)
- Average QA cycles per report
- Average clarification turnaround time
- QA approval rate by examiner

**Data Captured** (Maps to fct_evaluation_qa_events):
- event_type: APPROVED
- is_final_approval: TRUE
- approved_by
- approved_timestamp
- approval_notes
- first_pass_approval_flag
- total_qa_cycles_at_event
- days_in_qa_process

**Data Captured** (Maps to fct_evaluations_completed):
- qa_approved_date_sk
- qa_approval_timestamp
- first_pass_qa_approval
- qa_cycles_count
- days_in_qa

---

## Stage 7: Delivery to VA

### 7.1 Report Transmission
**Trigger**: QA approval completed
**Owner**: VES Operations

**Process Steps**:
1. **Prepare for Transmission**
   - Generate final report package
   - Include all supporting documents
   - Create transmission manifest
   - Encrypt sensitive data

2. **Transmit to VA**
   - Upload to VA secure portal (primary method)
   - Alternative methods: Secure email, encrypted file transfer
   - Include cover sheet with metadata
   - Generate transmission receipt

3. **VA Confirmation**
   - Receive VA acknowledgment
   - Capture confirmation number
   - Verify successful receipt
   - Update case status

4. **Close Request**
   - Mark request as completed
   - Calculate final metrics (cycle time, SLA compliance)
   - Archive case file
   - Trigger payment process

**Outputs**:
- Report delivered to VA
- Confirmation received
- Case closed
- Metrics captured

**Decision Points**:
- ✅ **VA Confirms Receipt** → Case complete
- ❌ **Transmission Failed** → Retry transmission
- ⏱️ **No Confirmation** → Follow up with VA

**Exceptions**:
- Transmission errors → Retry via alternative method
- VA rejects report → Review rejection reason, may need revision
- Missing documents → Compile and retransmit
- Encryption issues → Resolve technical issue

**Data Captured** (Maps to fct_exam_requests):
- request_closed_date_sk
- total_cycle_time_days
- sla_met_flag
- sla_variance_days

**Data Captured** (Maps to fct_evaluations_completed):
- report_delivered_to_va_date_sk
- va_delivery_timestamp
- va_delivery_method
- va_confirmation_number
- va_delivery_confirmed

**Data Captured** (Maps to fct_evaluation_qa_events):
- event_type: SENT_TO_VA
- sent_to_va_flag: TRUE
- sent_to_va_timestamp
- va_submission_method
- va_confirmation_number

---

## Stage 8: Payment Processing

### 8.1 Payment Authorization
**Trigger**: Report delivered to VA
**Owner**: Finance Team

**Process Steps**:
1. **Verify Payment Eligibility**
   - Confirm exam completed
   - Verify QA approval
   - Check VA delivery confirmation
   - Validate examiner credentials current

2. **Calculate Payment Amount**
   - Determine base exam fee
   - Apply complexity adjustment
   - Add specialist premium if applicable
   - Calculate mileage reimbursement
   - Add any additional authorized charges
   - Apply contractual rates

3. **Validate Payment**
   - Check against contract terms
   - Verify within budget
   - Confirm no duplicate payments
   - Review for anomalies

4. **Authorize Payment**
   - Approve payment amount
   - Set payment date
   - Generate payment record
   - Queue for processing

**Payment Tiers**:
- Standard exam: Base rate
- Complex exam: Base rate + complexity premium
- Specialist exam: Specialist rate
- High-complexity: Enhanced rate
- Additional charges: Mileage, tests, etc.

**Outputs**:
- Payment authorized
- Payment amount calculated
- Payment record created
- Examiner notified

**Data Captured** (Maps to fct_payments - NEW TABLE NEEDED):
- payment_id
- evaluator_sk
- evaluation_id
- payment_date_sk
- base_payment_amount
- complexity_adjustment
- specialist_premium
- mileage_reimbursement
- total_payment_amount
- payment_tier
- payment_status

---

### 8.2 Payment Execution
**Trigger**: Payment authorized
**Owner**: Finance Team

**Process Steps**:
1. **Process Payment**
   - Submit to payment system
   - Execute ACH/wire transfer
   - Generate payment confirmation
   - Update payment status

2. **Examiner Notification**
   - Notify examiner of payment
   - Provide payment details
   - Include remittance information
   - Send payment confirmation

3. **Record Keeping**
   - Archive payment record
   - Update examiner payment history
   - Track for tax reporting (1099)
   - Maintain audit trail

**Outputs**:
- Payment processed
- Examiner paid
- Records updated
- Confirmation sent

**Exceptions**:
- Payment rejected → Investigate and reprocess
- Examiner banking info invalid → Contact examiner
- Payment dispute → Research and resolve
- Tax withholding issues → Coordinate with examiner

**Data Captured** (Maps to fct_payments):
- payment_processed_timestamp
- payment_method (ACH, WIRE, CHECK)
- payment_confirmation_number
- payment_status (AUTHORIZED, PROCESSED, COMPLETED, REJECTED)
- rejection_reason

---

## Decision Points

### Critical Decision Points Throughout Process

| Stage | Decision Point | Criteria | Outcomes |
|-------|---------------|----------|----------|
| Intake | Accept Request? | Valid request format, all fields present | ✅ Accept → Validate<br>❌ Reject → Return to VA |
| Validation | Veteran Eligible? | Active veteran, geographic area, claim type | ✅ Eligible → Assign<br>❌ Not Eligible → Notify VA<br>⏸️ Pending → Request Info |
| Assignment | Examiner Accepts? | Availability, specialty, capacity | ✅ Accept → Schedule<br>❌ Reject → Reassign<br>🔄 Transfer → New Examiner |
| Scheduling | Veteran Confirms? | Veteran availability, location | ✅ Confirm → Appointment Set<br>❌ Decline → Find New Time<br>📞 No Contact → Retry |
| Appointment | Veteran Attends? | Veteran shows up | ✅ Attend → Conduct Exam<br>❌ No-Show → Contact & Reschedule |
| QA Review | Report Quality? | Completeness, accuracy, compliance | ✅ Approve → Deliver to VA<br>🔄 Clarify → Request Clarification<br>❌ Reject → Return to Examiner |
| Delivery | VA Accepts? | Proper format, complete | ✅ Accept → Close Case<br>❌ Reject → Revise & Resubmit |

---

## Exception Handling

### Common Exceptions and Resolution Paths

#### Exception 1: Incomplete Exam Request
**Trigger**: Required information missing
**Resolution**:
1. Flag as incomplete
2. Contact VA for missing info
3. Hold in pending queue
4. Set follow-up reminder
5. Close if no response after X days

**Escalation**: If VA doesn't respond in 14 days → Close and notify

---

#### Exception 2: Cannot Contact Veteran
**Trigger**: Multiple failed contact attempts
**Resolution**:
1. Try all contact methods (phone, email, mail)
2. Contact VA to verify veteran contact info
3. Request VA assistance in contacting veteran
4. Set case on hold
5. Close if no contact after 30 days

**Escalation**: If no contact after 30 days → Return to VA

---

#### Exception 3: Multiple Assignment Rejections
**Trigger**: 3+ examiners reject assignment
**Resolution**:
1. Review case for issues
2. Expand geographic search radius
3. Increase payment tier if complex
4. Manual assignment by supervisor
5. Consider contract specialist
6. May need to telehealth option

**Escalation**: If no examiner found → Escalate to operations manager

---

#### Exception 4: Exam Incomplete
**Trigger**: Examiner unable to complete exam
**Resolution**:
1. Document reason for incompletion
2. Determine if additional appointment needed
3. Schedule follow-up if required
4. If veteran refused → Document and notify VA
5. If medical emergency → Reschedule

**Escalation**: If cannot complete → Notify VA, may need different examiner

---

#### Exception 5: Multiple QA Rejections
**Trigger**: Report rejected 2+ times
**Resolution**:
1. Escalate to senior QA
2. QA manager reviews case
3. May arrange examiner coaching
4. May require phone consultation
5. May need peer review
6. Consider reassignment to different examiner

**Escalation**: If still not acceptable → Medical director review

---

#### Exception 6: SLA at Risk
**Trigger**: Case approaching SLA deadline
**Resolution**:
1. Flag as high priority
2. Expedite assignment
3. Prefer examiners with quick availability
4. Fast-track QA review
5. Prioritize in all queues
6. Daily status monitoring

**Escalation**: If SLA breach imminent → Notify VA, explain delay

---

#### Exception 7: Examiner Performance Issues
**Trigger**: Consistent quality issues, late submissions
**Resolution**:
1. QA team flags pattern
2. Review examiner performance metrics
3. Coaching and training provided
4. Probationary status if needed
5. Reduce assignment volume
6. May suspend examiner

**Escalation**: If no improvement → Terminate contract

---

#### Exception 8: VA Rejects Delivered Report
**Trigger**: VA finds issue with submitted report
**Resolution**:
1. Review VA rejection reason
2. Determine if revision possible
3. Request clarification from examiner
4. QA re-review
5. Revise and resubmit
6. May require new examination

**Escalation**: If cannot resolve → Escalate to client services

---

## Key Performance Indicators

### SLA Metrics
- **Request to Assignment**: Days from request receipt to examiner assignment
  - Target: ≤ 5 days
  - Source: fct_exam_requests.days_to_assignment

- **Assignment to Scheduling**: Days from assignment to appointment scheduled
  - Target: ≤ 3 days
  - Source: fct_exam_requests.days_to_scheduling

- **Request to Completion**: Total cycle time from request to exam complete
  - Target: ≤ 21 days (varies by request type)
  - Source: fct_exam_requests.total_cycle_time_days

- **SLA Compliance Rate**: Percentage of cases meeting SLA
  - Target: ≥ 95%
  - Source: COUNT(sla_met_flag = TRUE) / COUNT(*)

### Assignment Metrics
- **Assignment Acceptance Rate**: Percentage of assignments accepted by first examiner
  - Target: ≥ 85%
  - Source: fct_examiner_assignments (is_acceptance = TRUE on first assignment)

- **Time to Assignment Acceptance**: Hours from assignment to examiner acceptance
  - Target: ≤ 24 hours
  - Source: fct_examiner_assignments.time_to_response_hours

- **Reassignment Rate**: Percentage of cases requiring reassignment
  - Target: ≤ 10%
  - Source: fct_examiner_assignments (reassignment_number > 0)

### Appointment Metrics
- **Appointment Show Rate**: Percentage of veterans attending scheduled appointments
  - Target: ≥ 90%
  - Source: fct_appointment_events (veteran_attended_flag)

- **Rescheduling Rate**: Percentage of appointments rescheduled
  - Target: ≤ 15%
  - Source: fct_appointment_events (event_type = 'RESCHEDULED')

- **No-Show Rate**: Percentage of no-shows
  - Target: ≤ 10%
  - Source: fct_appointment_events (event_type = 'NO_SHOW')

### Quality Metrics
- **First-Pass QA Approval Rate**: Percentage approved on first QA review
  - Target: ≥ 70%
  - Source: fct_evaluation_qa_events.first_pass_approval_flag

- **Average QA Cycles**: Average number of QA cycles per evaluation
  - Target: ≤ 1.5
  - Source: fct_evaluation_qa_events.total_qa_cycles_at_event

- **QA Turnaround Time**: Hours from submission to QA decision
  - Target: ≤ 48 hours
  - Source: fct_evaluation_qa_events.turnaround_time_hours

- **Overall Quality Score**: Average quality score across all evaluations
  - Target: ≥ 85/100
  - Source: fct_evaluation_qa_events.overall_quality_score

### Examiner Performance
- **Examiner Utilization Rate**: Percentage of examiner capacity utilized
  - Target: 70-85%
  - Source: fct_examiner_assignments.examiner_utilization_percentage

- **Examiner Quality Score**: Average quality score by examiner
  - Target: ≥ 85/100
  - Source: AVG(overall_quality_score) by evaluator_sk

- **Examiner On-Time Rate**: Percentage of exams completed on time
  - Target: ≥ 95%
  - Source: fct_evaluations_completed (exam_date ≤ sla_due_date)

### Financial Metrics
- **Average Payment per Exam**: Average examiner payment amount
  - Source: fct_payments.total_payment_amount

- **Payment Processing Time**: Days from exam completion to payment
  - Target: ≤ 14 days
  - Source: payment_date - exam_completion_date

---

## Process Flow Summary

```
VA Submits Request
    ↓
VES Receives & Logs Request
    ↓
Validate Veteran Eligibility
    ↓
[DECISION: Eligible?]
    ├─ Yes → Assign Examiner
    └─ No → Notify VA, Close
         ↓
Assign to Qualified Examiner
    ↓
Examiner Reviews Assignment
    ↓
[DECISION: Accept?]
    ├─ Yes → Schedule Appointment
    └─ No → Reassign to Different Examiner
         ↓
Contact Veteran to Schedule
    ↓
Appointment Scheduled
    ↓
Send Reminders & Confirmations
    ↓
[DECISION: Veteran Attends?]
    ├─ Yes → Conduct Examination
    └─ No → Reschedule or Close
         ↓
Examiner Conducts Exam
    ↓
Complete DBQ Form
    ↓
Submit Report to QA
    ↓
QA Reviews Report
    ↓
[DECISION: Quality Acceptable?]
    ├─ Approved → Deliver to VA
    ├─ Needs Clarification → Request from Examiner → Re-Review
    └─ Rejected → Return to Examiner → Revise → Re-Review
         ↓
Prepare Report Package
    ↓
Transmit to VA
    ↓
VA Confirms Receipt
    ↓
Close Case
    ↓
Authorize Payment
    ↓
Process Payment to Examiner
    ↓
COMPLETE
```

---

## Mapping to Dimensional Model

### Key Fact Tables Supporting This Process

| Process Stage | Primary Fact Table | Key Metrics Captured |
|---------------|-------------------|---------------------|
| Request Intake | fct_exam_requests | Request received, validated, eligibility |
| Examiner Assignment | fct_examiner_assignments | Assignment events, acceptance/rejection, transfers |
| Appointment Scheduling | fct_appointment_events | Scheduled, confirmed, rescheduled, cancelled, no-show, completed |
| Exam Execution | fct_evaluations_completed | Exam date, duration, location, completeness |
| QA Process | fct_evaluation_qa_events | Submissions, reviews, clarifications, approvals |
| VA Delivery | fct_evaluations_completed<br>fct_evaluation_qa_events | Delivery timestamp, confirmation, method |
| Payment | fct_payments (NEW) | Payment amounts, dates, status |

### Process Flow Coverage Analysis

✅ **Well Covered**:
- Exam request tracking (fct_exam_requests)
- Examiner assignments (fct_examiner_assignments)
- Appointment lifecycle (fct_appointment_events)
- QA review process (fct_evaluation_qa_events)
- Evaluation outcomes (fct_evaluations_completed)

⚠️ **Partially Covered** (Priority 2):
- Document management events
- Communication/notification tracking
- Exception handling events

❌ **Not Yet Covered** (Priority 2-3):
- Payment transaction details (fct_payments needed)
- Detailed document lifecycle (fct_document_events needed)
- Communication audit trail (fct_communication_events needed)

---

## Document Change Log

| Date | Author | Change Description |
|------|--------|-------------------|
| 2025-11-16 | Claude | Initial creation from process_flow.png |

---

**End of VES Process Flow Reference Guide**
