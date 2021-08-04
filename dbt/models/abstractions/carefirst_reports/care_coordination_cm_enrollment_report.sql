-- carefirst report #2 case management

--need to make sure we are taking their state until the last day of last month
with currentstate_last_month as (
  select
    patientid,
    historicalState,
    date(patientStateCreatedAt) as patientStateCreatedAtdate
  from  {{ ref('member_historical_states') }}
  where date_sub(date_trunc(current_date, month), interval 1 day) = calendardate
),

-- base table of ALL members
members as (
  select
    m.patientId,
    m.memberId as Medicaid_ID,
    m.lastName as Enrollee_Last_Name,
    m.firstName as Enrollee_First_Name,
    m.dateOfBirth as Date_of_Birth,
    case when  historicalState in ('disenrolled_after_consent', 'disenrolled')
        then patientStateCreatedAtdate
    else NULL end as Inactive_Date,
  from {{ ref('member') }} m
  left join {{ ref('member_states') }} ms
    using(patientId)
  left join currentstate_last_month using (patientid)
  -- all CareFirst members
    where m.partnerName = 'CareFirst'
),

-- comprehensive assessment
assessments as (
  select
    patientId,
    max(date(scoredAt)) as Date_of_Last_Completed_Assessment,
    min(date(createdAt)) as CM_Enrollment_Date,
  from {{ source('commons', 'patient_screening_tool_submission') }}
  where
    screeningToolSlug in ('comprehensive', 'baseline-nyc-ct', 'comprehensive-dc') and
    scoredAt is not null and
    date(scoredAt) < date_trunc(current_date, month) and
    date(createdAt) < date_trunc(current_date, month)
  group by 1
),

-- any task counts as a "care plan". Last task created as of end of last month.
care_plans as (
  select
    patientId,
    max(date(taskCreatedAt)) as Date_of_Last_Care_Plan
  from {{ ref('member_goals_tasks') }}
  where date(taskCreatedAt) < date_trunc(current_date, month)
  group by 1
),

-- borrowed from compiled dbt/sql of latest_member_interactions
latest_attempted_connections as (
  select
    patientId,
    max(date(eventTimestamp)) as Date_of_Last_Contact_Attempt
  from {{ ref('member_interactions') }}
  where isAttemptedConnection is true
    and date(eventTimestamp) < date_trunc(current_date, month)
  group by patientId
),

latest_successful_connections as (
  select
    patientId,
    array_agg(
      struct(
        date(eventTimestamp) as Date_of_Last_CM_Contact,
        -- certain types of interactions count, regardless of the staff type
        case
          when eventType in ('phoneCall', 'Telehealth', 'Telephonic Check In', 'Telemedicine Note', 'Telephone Visit Note', 'App Visit Note') then 'Phone'
          when eventType in ('inPersonVisitOrMeeting', 'videoCall', 'Office Visit Note', 'Office Visit', 'House Call Visit Note', 'Home Care Visit', 'RN Home Visit', 'RN Hypertension Visit', 'SNF Visit Note', 'Visit Note') then 'Face to Face'
          when eventType in ('email') then 'E-mail'
          when eventType in ('text') then 'Text'
        else null end as Contact_Type
      )
      order by eventTimestamp desc limit 1
    )[offset(0)].*
  from {{ ref('member_interactions') }}
  where isSuccessfulConnection is true
    and eventType is not null
    and eventType not in ('research', 'caseConference', 'legacyUnlabeled')
    and date(eventTimestamp) < date_trunc(current_date, month)
  group by patientId
),

-- combining attempts and successes
contacts as (
  select
    patientId,
    Date_of_Last_CM_Contact,
    Contact_Type,
    Date_of_Last_Contact_Attempt
  from latest_attempted_connections lac
  left join latest_successful_connections lsc
    using(patientId)
),

--12/2020 we arent using these CTEs but keeping just incase we need them in the future
-- default to regular acuity, but use recommended acuity as a backup
--acuity as (
--  select
--    patientId,
--    coalesce(a.currentMemberAcuityDescription, ra.recommendedMemberAcuityDescription) as currentMemberAcuityDescription,
--    a.currentMemberAcuityDescription is null as isRecommendedAcuity
--  from {{ ref('member_recommended_acuity') }} ra
--  left join {{ ref('member_current_acuity') }} a
--    using(patientId)
--),

-- complex care status, as internally defined by CBH
--complex as (
--  select
--    patientId,
--    fieldValue as isComplex,
--  from {{ ref('cf_complex_care_management') }}
--),

--reasons why member is inactive
reason as (
  select
    patientId,
    note,
    reason
  from {{ ref('latest_disenrollment_reason') }}
  where date(createdAt) < date_trunc(current_date, month)
),

-- resulting output table
result as (
  select
    *,
    'CM-High'as CM_Level,
    'Yes' as Complex_Case_Management,
    'Cityblock Health' as Delegated_CM,
  from members m
  left join assessments a
    using(patientId)
  left join care_plans cp
    using(patientId)
  left join contacts c
    using(patientId)
  left join reason
    using(patientId)
  --  left join acuity ac
--    using(patientId)
--  left join complex co
--    using(patientId)
),

-- thin down output, reorder columns, and create status value based on other fields
final as (
  select
    Medicaid_ID,
    Enrollee_Last_Name,
    Enrollee_First_Name,
    Date_of_Birth,
    CM_Enrollment_Date,
    Delegated_CM,
    case
        when CM_Enrollment_Date is not null
           and Date_of_Last_Completed_Assessment is not null
           and Inactive_date is null
          then 'Active'
        When Medicaid_ID is not null
            and Inactive_date is null
          then 'Enrolled'
        When Inactive_Date is not null
          then'Inactive'
    else 'Inactive' end as Status,
    Inactive_Date,
    Date_of_Last_Completed_Assessment,
    Date_of_Last_Care_Plan,
    CM_Level,
    Complex_Case_Management,
    Date_of_Last_CM_Contact,
    Contact_Type,
    Date_of_Last_Contact_Attempt,
    reason||', '||note as Reason_Why_Inactive
  from result
)

select * from final order by 1

--this is incase we need to ever break out to CM and RM

--cm_final as (
--  select
--    *
--  from final
--)

-- note as of 11/20 we don't have any members in a Stable state, thus none in RM
-- we will not have to report patients in RM, since we never have any patient in RM but keeping code incase things change
--rm_final as (
--  select
--    Medicaid_ID,
--    Enrollee_Last_Name,
--    Enrollee_First_Name,
--    Date_of_Birth,
--    CM_Enrollment_Date as RM_Enrollment_Date,
--    Status,
--    Inactive_Date,
--    CM_Level as  RM_Level,
--    Date_of_Last_CM_Contact as Date_of_Last_RM_Contact,
--    Contact_Type,
--    Date_of_Last_Contact_Attempt
--  from final
--  where CM_Level = 'RM-Level 1 Pop Health'
--)
