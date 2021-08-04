with sla_member_info as (

  select * from {{ ref('thpp_sla_member_info') }}

),

sla_outreach as (

  select * from {{ ref('thpp_sla_outreach') }}

),

sla_mailers as (
    select * from {{ ref('thpp_sla_mailers') }}
),

reporting_month as (

   select * from {{ ref('thpp_sla_reporting_month') }}
),

sla_discharges as (
    select * from {{ ref('thpp_sla_discharges') }}
),


-- SLA 1: Percentage Of Members Outreached Within 48 Hours Of An Inpatient Transfer
-- 0 because Cityblock doesn't currently track info on inpatient transfers
--        0 as sla1Num,
--    0 as sla1Den,

-- SLA 2: Percentage Of Members Outreached Within 48 Hours Of Facility Discharge
sla2 as (

  select
    patientId,
    '2' as slaId,
    'outreach' as service,
    'outreach within 48 hours' as serviceLevel,
    dischargeDate as serviceDate,
    date_add(dischargeDate, interval 2 day) as serviceDueDate,
    outreachWithin48Hours as slaMet

  from sla_discharges

  inner join reporting_month
  -- runs metric between first day and last day of previous month
  on dischargeDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate
),


-- -- SLA 3: Percentage Of Members Outreached Within 24 Hours Of Acute Hospital Admission
--
--sla3 as (
--
--  select
--    a.patientId,
--    '3' as slaId,
--    'outreach' as service,
--    'outreach within 24 hours' as serviceLevel,
--    admitDate as serviceDate,
--    date_add(a.admitDate, interval 1 day) as serviceDueDate,
--    a.outreachWithin24Hours as slaMet
--
--  from {{ ref('thpp_sla_admits') }}  a
--
--  inner join reporting_month
--  -- runs metric between first day and last day of previous month
--  on a.admitDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate
--),

-- SLA 4: Percentage Of Members Outreached Within 30 Days Of Becoming CBH Member
sla4 as (

  select
    m.patientId,
    '4' as slaId,
    'outreach' as service,
    'action before due date' as serviceLevel,
    min(o.outreachDate) as serviceDate,
    date_add(m.tuftsEffectiveDate, interval 30 day) as serviceDueDate,
    min(o.outreachDate) is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on tuftsEffectiveDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  left join sla_outreach o
  on
    m.patientId = o.patientId and
    date_diff(outreachDate, tuftsEffectiveDate, day) between 0 and 30

  where date_diff(current_date(), tuftsEffectiveDate, day) >= 30
         and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

  group by m.patientId, m.tuftsEffectiveDate

),

---- SLA 5: Percentage Of Members Outreached Within 30 Days Of Being Attributed to CBH
--sla5 as (
--
--  select
--    m.patientId,
--    '5' as slaId,
--    'outreach' as service,
--    'action before due date' as serviceLevel,
--    min(o.outreachDate) as serviceDate,
--    date_add(m.tuftsEffectiveDate, interval 30 day) as serviceDueDate,
--    min(o.outreachDate) is not null as slaMet
--
--  from sla_member_info m
--
--  left join sla_outreach o
--  on
--    m.patientId = o.patientId and
--    date_diff(outreachDate, tuftsEffectiveDate, day) between 0 and 30
--
--  where date_diff(current_date(), tuftsEffectiveDate, day) >= 30
--  and tuftsEffectiveDate >= '2020-03-02'
--
--  group by m.patientId, m.tuftsEffectiveDate
--
--),

-- SLA 6: Percentage of Members With Both Tufts Assessments Completed Within 1 Year Of Intial Assessment Or Reassessment
--Not reported on 6 , we only report on sla 6A; so haven't made changes to this
--WE DONOT REPORT ON SLA 6
--sla6 as (
--
--  select
--    patientId,
--    '6' as slaId,
--    'comprehensive assessment' as service,
--    'action before due date' as serviceLevel,
--    cbComprehensiveAssessmentDate as serviceDate,
--    tuftsNextAssessmentDueDate as serviceDueDate,
--    cbComprehensiveAssessmentDate <= tuftsNextAssessmentDueDate as slaMet
--
--  from sla_member_info m
--
--  inner join reporting_month
--  on tuftsNextAssessmentDueDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate
--
--  where
--    tuftsNextAssessmentDueDate is not null and
--    current_date() >= tuftsNextAssessmentDueDate and
--    cbHardToReachStatus is false and
--    tuftsNextAssessmentDueDate >= '2020-03-02' and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null)
--
--),

 -- SLA 6a: Percentage of Members With Both Tufts Assessments Completed Within 1 Year Of Initial Assessment Or Reassessment
  -- comparison version of measure incorporating updated MDS assessment logic

sla6a as (

  select
    patientId,
    '6a' as slaId,
    'both Tufts assessments' as service,
    'action before due date' as serviceLevel,
    tuftsLastCompletedAssessmentDate as serviceDate,
    tuftsNextAssessmentDueDate as serviceDueDate,
    coalesce(cbMaxComprehensiveAssessmentDate <= tuftsNextAssessmentDueDate and
      cbMaxMdsAssessmentDate <= tuftsNextAssessmentDueDate, false) as slaMet

  from sla_member_info m

  inner join reporting_month
  on tuftsNextAssessmentDueDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
    tuftsNextAssessmentDueDate is not null and
    current_date() >= tuftsNextAssessmentDueDate and
    (cbHardToReachStatus is false and refusalAssessment is false ) and
    tuftsNextAssessmentDueDate >= '2020-03-02' and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 7: Percentage of Unable To Reach Members Who Need Reassessment With 3 Outreach Attempts and 1 Letter Sent
sla7 as (

  select
    patientId,
    '7' as slaId,
    'outreach attempt' as service,
    'date not null' as serviceLevel,
    date(null) as serviceDate,
    tuftsNextAssessmentDueDate as serviceDueDate,
    cbHardToReachDate is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on cbHardToReachDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
    cbTotalOutreachAttempts >= 3 and
    cbTotalMailers >= 1 and
    tuftsNextAssessmentDueDate is not null and
    cbHardToReachStatus is true and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
),

-- SLA 8: Percentage of Unable To Reach Members With 1 Outreach Attempt Within 30 Days Before the Intial Assessment Is Due
sla8 as (

  select distinct
    m.patientId,
    '8' as slaId,
    'outreach attempt' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    o.patientId is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on date_sub(tuftsNextAssessmentDueDate, interval 30 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  left join sla_outreach o
  on
    m.patientId = o.patientId and
    date_diff(tuftsNextAssessmentDueDate, outreachDate, day) <= 30

  where
    tuftsNextAssessmentDueDate is not null and
    cbHardToReachStatus is true and
    current_date() >= tuftsNextAssessmentDueDate and
    tuftsNextAssessmentDueDate >= '2020-03-02' and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

  union all

  select distinct
    m.patientId,
    '8' as slaId,
    'outreach attempt' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    o.patientId is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on date_sub(tuftsNextAssessmentDueDate, interval 30 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  left join sla_outreach o
  on
    m.patientId = o.patientId and
    date_diff(date_add(tuftsEffectiveDate, interval 90 day), outreachDate, day) <= 30

  where
    tuftsNextAssessmentDueDate is null and
    cbHardToReachStatus is true and
    current_date() >= date_add(tuftsEffectiveDate, interval 90 day) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),
-- SLA 9: Percentage of Members Who Refused Reassessment With 1 Outreach Attempt and 1 Letter Sent Within 90 Days Of Reassessment Due Date
sla9 as (

  select distinct
    m.patientId,
    '9' as slaId,
    'Reassessment' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    tuftsNextAssessmentDueDate as serviceDueDate,
    refusalAssessment as slaMet

  from sla_member_info  m
  left join sla_outreach o
    on m.patientId = o.patientId and
      date_diff(o.outreachDate, date_sub(m.tuftsNextAssessmentDueDate, interval 90 day), day) between 0 and 90
  left join sla_mailers ma
    on m.patientId = ma.patientId and
      date_diff(ma.outreachDate, date_sub(m.tuftsNextAssessmentDueDate, interval 90 day), day) between 0 and 90
  inner join reporting_month
  on tuftsNextAssessmentDueDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
      cbComprehensiveAssessmentDate is null and
      cbMdsAssessmentDate is null and
      cbMemberAssessmentRefusalDate is not null and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

  -- SLA 10: Percentage of Members With Complete Care Plan Within 90 Days During Assessment
sla10 as (

  select
    patientId,
    '10' as slaId,
    'first care plan task' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    cbHasCarePlan as slaMet

  from sla_member_info m

  inner join reporting_month
  on date_add(tuftsEffectiveDate, interval 90 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
    tuftsLastCompletedAssessmentDate is null and
    date_diff(current_date(), tuftsEffectiveDate, day) >= 90 and
    (cbComprehensiveAssessmentDate is not null or
    cbMdsAssessmentDate is not null) and
    (cbHardToReachStatus is false  and refusalAssessment is false and refusalCarePlan is false ) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 11: Percentage of Members With Complete Care Plan Within 90 Days During Assessment
sla11 as (

  select
    patientId,
    '11' as slaId,
    'first care plan task date' as service,
    'date not null' as serviceLevel,
    cbCarePlanInitiationDate as serviceDate,
    date(null) as serviceDueDate,
    cbCarePlanInitiationDate is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on cbHardToReachDate  between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where cbHardToReachStatus is true and refusalCarePlan is false and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 12: Percentage of Members With Intial Care Plan Agreement
sla12 as (

  select
    patientId,
    '12' as slaId,
    'Care plan not refused' as service,
    'date not null' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    cbMemberCarePlanRefusalDate is null as slaMet

  from sla_member_info m

  inner join reporting_month
  on date_add(tuftsEffectiveDate, interval 90 day)  between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where cbMemberCarePlanRefusalDate is null and
      tuftsLastCompletedAssessmentDate is null and (cbHardToReachStatus is false
       and refusalCarePlan is false) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 13: Percentage of Members With Completed Annual Care Plan Review As A Result of Reassessment
sla13 as (

  select
    patientId,
    '13' as slaId,
    'first case conference' as service,
    'date not null' as serviceLevel,
    cbMinCaseConferenceAt as serviceDate,
    date(null) as serviceDueDate,
    cbMinCaseConferenceAt is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on tuftsNextAssessmentDueDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
    tuftsNextAssessmentDueDate is not null and
    cbComprehensiveAssessmentDate is not null and
    (cbHardToReachStatus is false and refusalCarePlan is false) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
),

-- SLA 14: Percentage of Members With Annual Care Plan Agreement
sla14 as (

  select
    patientId,
    '14' as slaId,
    'Annual Care Plan Agreement' as service,
    'date not null' as serviceLevel,
    date(null) as serviceDate,
    tuftsNextAssessmentDueDate as serviceDueDate,
    cbMemberCarePlanRefusalDate is null as slaMet

  from sla_member_info m

  inner join reporting_month
  on tuftsNextAssessmentDueDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
    tuftsNextAssessmentDueDate is not null and
    (cbHardToReachStatus is false and refusalCarePlan is false and refusalAssessment is false) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
),


---- SLA 15: Percentage of Members With Comprehensive Assessment Completed Within 90 Days of Enrollment
--sla15 as (
--
--  select
--    patientId,
--    '15' as slaId,
--    'Comprehensive assessments' as service,
--    'action before due date' as serviceLevel,
--    cbComprehensiveAssessmentDate as serviceDate,
--    date_add(tuftsEffectiveDate, interval 90 day) as serviceDueDate,
--    cbComprehensiveAssessmentDate is not null as slaMet
--
--  from sla_member_info m
--
--  inner join reporting_month
--  on date_add(tuftsEffectiveDate, interval 90 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate
--
--  where
--    tuftsLastCompletedAssessmentDate is null and
--    date_diff(current_date(), tuftsEffectiveDate, day) >= 90 and
--    (cbHardToReachStatus is false and refusalAssessment is false) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
--    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
--
--),

 -- SLA 15v2: Percentage of Members With Both Tufts Assessments Completed Within 90 Days of Enrollment
sla15v2 as (

  select
    patientId,
    '15v2' as slaId,
    'both Tufts assessments' as service,
    'action before due date' as serviceLevel,
    Least(cbComprehensiveAssessmentDate, cbMdsAssessmentDate) as serviceDate,
    --date_add(tuftsEffectiveDate, interval 90 day) as serviceDueDate,
    -- tuftsSlaReportEndDate is always = date_add(tuftsEffectiveDate, interval 90 day) which is last date of the month
    tuftsSlaReportEndDate as serviceDueDate,
    coalesce((tuftsEffectiveDate<=tuftsSlaReportEndDate and
    (Least(cbComprehensiveAssessmentDate, cbMdsAssessmentDate) between date_sub(date_trunc(tuftsSlaReportEndDate, month), interval 2 month) and tuftsSlaReportEndDate)),false)
      as slaMet

  from sla_member_info m

  inner join reporting_month
  on date_sub(date_trunc(tuftsSlaReportEndDate, month), interval 2 month) = date_trunc(tuftsEffectiveDate, month)

  where
    date_diff(current_date(), tuftsEffectiveDate, day) >= 90 and
    (cbHardToReachStatus is false and refusalAssessment is false) and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 16: Percentage of Unable To Reach Members With 3 Outreach Attempts and 1 Letter Sent Within Within 90 Days of Enrollment
sla16 as (

  select
    m.patientId,
    '16' as slaId,
    'outreach attempt' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    cbHardToReachStatus as slaMet

  from sla_member_info m

  inner join reporting_month r
  on cbHardToReachDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate
  where
    cbTotalOutreachAttempts >= 3 and
    cbTotalMailers >= 1 and
    tuftsNextAssessmentDueDate is null and
    date_diff(current_date(), tuftsEffectiveDate, day) >= 90 and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 17: Percentage of Members Who Refused Tufts Annual Assessment With 1 Outreach Attempt and 1 Letter Sent Within 90 Days Of Enrollment
sla17 as (

  select
    m.patientId,
    '17' as slaId,
    'Annual Assessment' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date_add(m.tuftsEffectiveDate, interval 90 day) as serviceDueDate,
    o.patientId is not null as slaMet

  from sla_member_info m
  inner join sla_outreach o
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), o.outreachDate, day) between 0 and 90
  inner join sla_mailers ma
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), ma.outreachDate, day) between 0 and 90
  inner join reporting_month r
  on date_add(tuftsEffectiveDate, interval 90 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate
  where
    cbTotalOutreachAttempts >= 1 and
    cbTotalMailers >= 1 and
    cbComprehensiveAssessmentDate is null and
    cbMdsAssessmentDate is null and
    tuftsAssignedMemberAssessmentRefusalStatus is true and (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
),

-- SLA 18: Percentage of Members Who Refused Tufts Annual Assessment But Given Care Plan Development With 1 Outreach Attempt and 1 Letter Sent Within 90 Days Of Enrollment
sla18 as (

  select
    m.patientId,
    '18' as slaId,
    'Annual Assessment' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date_add(m.tuftsEffectiveDate, interval 90 day) as serviceDueDate,
    o.patientId is not null as slaMet

  from sla_member_info m
  inner join sla_outreach o
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), o.outreachDate, day) between 0 and 90
  inner join sla_mailers ma
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), ma.outreachDate, day) between 0 and 90
  inner join reporting_month r
  on date_add(tuftsEffectiveDate, interval 90 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate
  where
      cbComprehensiveAssessmentDate is null and
      cbMdsAssessmentDate is null and
      tuftsAssignedMemberAssessmentRefusalStatus is true  and
      cbCarePlanInitiationDate is not null and
      cbTotalOutreachAttempts = 1 and
      cbTotalMailers = 1 and
      (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
),
-- SLA 19: Percentage of Hard To Engage Members With 1 Outreach Attempt Every Month

sla19 as (

  select
    m.patientId,
    '19' as slaId,
    'HTR with outreach attempt' as service,
    'date not null' as serviceLevel,
    max(o.outreachDate) as serviceDate,
    date(null) as serviceDueDate,
    max(o.outreachDate) is not null as slaMet

  from sla_member_info m

  inner join reporting_month r
  on m.cbHardToReachDate <= r.tuftsSlaReportStartDate

  left join sla_outreach o
  on
    m.patientId = o.patientId and
    o.outreachDate between r.tuftsSlaReportStartDate and r.tuftsSlaReportEndDate

  where m.cbHardToReachStatus is true and
  (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
  group by m.patientId

),

-- SLA 20: Percentage of Members Who Refused Assessment And/Or Care Plan With 1 Outreach Attempt Every Month

sla20 as (

  select
    m.patientId,
    '20' as slaId,
    'Refusal Assessment/Careplan with outreach attempt' as service,
    'date not null' as serviceLevel,
    max(o.outreachDate) as serviceDate,
    date(null) as serviceDueDate,
    max(o.outreachDate) is not null as slaMet

  from sla_member_info m

  inner join reporting_month r
  on
    (m.cbMemberAssessmentRefusalDate between r.tuftsSlaReportStartDate and r.tuftsSlaReportEndDate) or
    (m.cbMemberCarePlanRefusalDate between r.tuftsSlaReportStartDate and r.tuftsSlaReportEndDate)

  left join sla_outreach o
  on
    m.patientId = o.patientId and
    o.outreachDate between r.tuftsSlaReportStartDate and r.tuftsSlaReportEndDate

  where
    cbMemberAssessmentRefusalDate is not null or
    cbMemberCarePlanRefusalDate is not null and
    (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

  group by m.patientId

),

-- SLA 21: Percentage of Members Who Refused Assessment With Care Plan Development
sla21 as (

  select
    patientId,
    '21' as slaId,
    'care plan initiation' as service,
    'date not null' as serviceLevel,
    cbCarePlanInitiationDate as serviceDate,
    date(null) as serviceDueDate,
    cbCarePlanInitiationDate is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on cbMemberAssessmentRefusalDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
    cbComprehensiveAssessmentDate is null and
    cbMdsAssessmentDate is null and
    cbMemberAssessmentRefusalDate is not null and
    (cbHardToReachStatus is false and refusalCarePlan is false) and
    (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 22: Percentage of Members Assessed In Current Month Who Were Offered an LTSC
sla22 as (

  select
    patientId,
    '22' as slaId,
    'LTSC offer' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    cbLtscOfferedStatus as slaMet

  from sla_member_info m
  inner join reporting_month
  on (tuftsNextAssessmentDueDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate)

  where (cbHardToReachStatus is false and refusalAssessment is false) and
  (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 23: Percentage of Members Assessed in reporting Month Who Had An LTSC Invited To Participate in the Intial Assessment as yes
sla23 as (

  select
    patientId,
    '23' as slaId,
    'LTSC invited' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date(null) as serviceDueDate,
    cbLtscInvitedToAssessmentStatus is not null as slaMet

  from sla_member_info m

  inner join reporting_month
  on cbMaxComprehensiveAssessmentDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate
  where cbLtscOfferedAnswer = 'yes' and  (cbHardToReachStatus is false and refusalAssessment is false) and
  (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

-- SLA 24: Percentage of Members With Major Change In Condition That Had Assessment Completed After Change
  -- 0 because Cityblock hasn't automated this metric

-- SLA 25: Percentage Of Members Outreached Within 48 Hours Of Emergency Department (ED) Discharge
sla25 as (

  select
    patientId,
    '25' as slaId,
    'Outreach' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date_add(dischargeDate, interval 2 day) as serviceDueDate,
    outreachWithin48Hours as slaMet

  from sla_discharges

  inner join reporting_month
   -- runs metric between first day and last day of previous month
  on
    dischargeDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate and
    dischargeType = 'ed'
),

 -- SLA 26: Percentage Of Members Outreached Within 48 Hours Of Inpatient (IP) Discharge
sla26 as (

  select
    patientId,
    '26' as slaId,
    'Outreach' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date_add(dischargeDate, interval 2 day) as serviceDueDate,
    outreachWithin48Hours as slaMet

  from sla_discharges

  inner join reporting_month
   -- runs metric between first day and last day of previous month
  on
    dischargeDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate and
    dischargeType = 'ip'
),

-- SLA 27: Percentage of Members Who Refused Cityblock Intial Assessment With 1 Outreach Attempt and 1 Letter Sent Within 90 Days Of Enrollment
sla27 as (

  select
    distinct
    m.patientId,
    '27' as slaId,
    'Refusal Assessment with outreach attempt' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date_add(m.tuftsEffectiveDate, interval 90 day) as serviceDueDate,
    o.patientId is not null as slaMet

  from sla_member_info m
  inner join sla_outreach o
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), o.outreachDate, day) between 0 and 90

  inner join sla_mailers ma
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), ma.outreachDate, day) between 0 and 90

  inner join reporting_month
    on date_add(m.tuftsEffectiveDate, interval 90 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
      cbComprehensiveAssessmentDate is null and
      cbMdsAssessmentDate is null and
      cbMemberAssessmentRefusalDate is not null and
      (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))
),

 -- SLA 28: Percentage of Members Who Refused Cityblock Intial Assessment But Given Care Plan Development With 1 Outreach Attempt and 1 Letter Sent Within 90 Days Of Enrollment
sla28 as (

  select
    m.patientId,
    '28' as slaId,
    'Refusal Assessment with outreach attempt' as service,
    'yes / no' as serviceLevel,
    date(null) as serviceDate,
    date_add(m.tuftsEffectiveDate, interval 90 day) as serviceDueDate,
    o.patientId is not null as slaMet

  from sla_member_info m
  inner join sla_outreach o
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), o.outreachDate, day) between 0 and 90

  inner join sla_mailers ma
    on m.patientId = o.patientId and
      date_diff(date_add(m.tuftsEffectiveDate, interval 90 day), ma.outreachDate, day) between 0 and 90

  inner join reporting_month
    on date_add(m.tuftsEffectiveDate, interval 90 day) between tuftsSlaReportStartDate and tuftsSlaReportEndDate

  where
      cbComprehensiveAssessmentDate is null and
      cbMdsAssessmentDate is null and
      cbMemberAssessmentRefusalDate is not null and
      cbCarePlanInitiationDate is not null and
      (m.disenrolledAtDate > tuftsSlaReportEndDate or m.disenrolledAtDate is null
    or (currentState not in ("disenrolled", "disenrolled_after_consent") and m.disenrolledAtDate is not null))

),

final as (

  select * from sla2
  union all
  select * from sla4
  union all
  select * from sla6a
  union all
  select * from sla7
  union all
  select * from sla8
  union all
  select * from sla9
  union all
  select * from sla10
  union all
  select * from sla11
  union all
  select * from sla12
  union all
  select * from sla13
  union all
  select * from sla14
  union all
  select * from sla15v2
  union all
  select * from sla16
  union all
  select * from sla17
  union all
  select * from sla18
  union all
  select * from sla19
  union all
  select * from sla20
  union all
  select * from sla21
  union all
  select * from sla22
  union all
  select * from sla23
  union all
  select * from sla25
  union all
  select * from sla26
  union all
  select * from sla27
  union all
  select * from sla28

)

select * from final

