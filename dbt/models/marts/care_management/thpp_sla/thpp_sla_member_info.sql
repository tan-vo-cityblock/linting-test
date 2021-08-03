with patient_attributes as (

  select
    patientId,
    
    case
      when type = 'declinedAssessments' and value = 'true' then date(createdAt)
      else null
    end as cbMemberAssessmentRefusalDate,
      
    case
      when type = 'declinedActionPlan' and value = 'true' then date(createdAt)
      else null
    end as cbMemberCarePlanRefusalDate
      
    from {{ source('commons','patient_attribute') }}
    
    where deletedAt is null

),

aggregated_attributes as (

  select 
    patientId, 
    array_agg(cbMemberAssessmentRefusalDate ignore nulls)[safe_offset(0)] as cbMemberAssessmentRefusalDate,
    array_agg(cbMemberCarePlanRefusalDate ignore nulls)[safe_offset(0)] as cbMemberCarePlanRefusalDate

from patient_attributes 

group by patientId

),

member_info as (
  
  select distinct
    m.patientId,
    mdi.externalId as memberId,
    m.currentState,
    
    -- needs to be updated when new tufts cohort is added
    case 
      when m.cohortGoLiveDate is not null then m.cohortGoLiveDate
      when m.cohortGoLiveDate is null and m.tuftsEnrollmentDate <= date('2020-03-02') then date('2020-03-02')
      when m.cohortGoLiveDate is null and m.tuftsEnrollmentDate > date('2020-03-02') then date('2020-04-01')
    end as tuftsCohortGoLiveDate,
    m.tuftsEnrollmentDate,
    m.disenrolledAtDate,
    mds.tuftsDisenrollmentDate,
    mds.lastAttemptedAssessmentDate as tuftsLastAttemptedAssessmentDate,
    -- per kristen's logic to produce old members, using min of comp for both contractual
    coalesce(least(coalesce(date(mcc.maxComprehensiveAssessmentAt),date(mcc.latestMdsInterviewCompletedAt)),coalesce(date(mcc.latestMdsInterviewCompletedAt),date(mcc.maxComprehensiveAssessmentAt))), mds.lastCompletedAssessmentDate) as tuftsLastCompletedAssessmentDate,
    coalesce(date(timestamp_add(mcc.bothTuftsContractualAssessmentsAt, interval 365 day)), mds.nextAssessmentDueDate) as tuftsNextAssessmentDueDate,
    case
      when mds.htrStatus in ('Hard to engage','Unable to contact') then true
      else false
    end as tuftsAssignedHardToReachStatus,
    
    case
      when mds.htrStatus = 'Member refused' then true
      else false
    end as tuftsAssignedMemberAssessmentRefusalStatus,
    
    aa.cbMemberAssessmentRefusalDate,
    aa.cbMemberCarePlanRefusalDate, 
    
    coalesce(mcc.hasGeneratedCarePlan, false) as cbHasCarePlan,
    date(mcc.minTaskAt) as cbCarePlanInitiationDate,
    date(mcc.minComprehensiveAssessmentAt) as cbComprehensiveAssessmentDate,
    date(mcc.maxComprehensiveAssessmentAt) as cbMaxComprehensiveAssessmentDate,
    date(mcc.maxGoalUpdatedAt) as cbCarePlanGoalUpdatedLatestDate,
    date(mcc.earliestMdsInterviewCompletedAt) as cbMdsAssessmentDate,
    date(mcc.latestMdsInterviewCompletedAt) as cbMaxMdsAssessmentDate,
    date(mcc.earliestHraCompletedAt) as cbMinHraCompletedDate,
    date(mcc.latestHraCompletedAt) as cbMaxHraCompletedDate,
    date(mcc.bothTuftsContractualAssessmentsAt) as cbBothTuftsAssessmentsDate,
    date(mcc.minCaseConferenceAt) as cbMinCaseConferenceAt,
    date(mcc.maxCaseConferenceAt) as cbMaxCaseConferenceAt
    
  from {{ ref('member') }} m
  left join {{ source('member_index', 'member_datasource_identifier') }} mdi
  on
    m.patientId = mdi.memberId and
    mdi.current is true and
    mdi.deletedAt is null and
    mdi.datasourceId not in (28, 29)
  left join {{ ref('member_commons_completion') }} mcc
  using (patientId) 
  left join {{ source('care_management','thpp_cm_mds_05142020') }} mds
  on m.nmi = mds.memberId
  left join aggregated_attributes aa
  on m.patientId = aa.patientId
  left join {{ source('commons','patient_document') }} pd
   on m.patientId = pd.patientId and pd.deletedAt is null
  
  where (m.patientHomeMarketName = 'Massachusetts') and 
 -- m.currentState not in ('disenrolled', 'disenrolled_after_consent') and
  m.tuftsEnrollmentDate is not null
  and (m.cohortName not in ("Tufts Cohort Virtual 1","Tufts Cohort 5", "Tufts Cohort 6")
  or cohortName is null)
),

cohort_info as (
  
  select distinct
  patientId,
  
  case
    when tuftsCohortGoLiveDate = '2020-03-02' then 'cohort_1'
    when tuftsCohortGoLiveDate = '2020-04-01' then 'cohort_2'
  end as tuftsCohort,
  
  case
    when tuftsCohortGoLiveDate = '2020-03-02' then date('2020-03-02')
    when tuftsCohortGoLiveDate >= '2020-04-01' then tuftsEnrollmentDate
  end as tuftsEffectiveDate

from member_info

),

ltsc_offer_info as (
  select distinct
    m.patientId,
    
    max(case
      when q.questionSlug = 'would-you-like-a-long-term-services-coordinator' then answerSlug
      else null
    end) as cbLtscOfferedAnswer,
        
    max(case
      when q.questionSlug = 'would-you-like-a-long-term-services-coordinator' and q.answerSlug in ('yes','no') then true
      else false
    end) as cbLtscOfferedStatus,

   from member_info m
   left join {{ ref('questions_answers_current') }} q
   using (patientId)
   
   group by m.patientId
   
),

ltsc_invitation_info as (
  select distinct
    m.patientId,

    max(case
      when q.questionSlug = 'would-you-like-the-ltsc-to-be-present-during-your-initial-assessment'  then answerSlug
      else null
    end) as cbLtscInvitedToAssessmentStatus,
    
   from member_info m
   left join ltsc_offer_info
   using (patientId)
   left join {{ ref('questions_answers_current') }} q
   using (patientId)

   where cbLtscOfferedAnswer = 'yes'

   group by m.patientId
   
),

outreach_attempts as (
  select
    patientId, 
    count(*) as cbTotalOutreachAttempts
    
  from {{ ref('thpp_sla_outreach') }}
  group by patientId
  
),

mailers as (
  select
    patientId, 
    count(*) as cbTotalMailers

  from {{ ref('thpp_sla_mailers') }}  
  group by patientId
  
),

total_outreach as (
  
  select distinct
  oa.patientId,
  coalesce(cbTotalOutreachAttempts, 0) as cbTotalOutreachAttempts,
  coalesce(cbTotalMailers, 0) as cbTotalMailers,

  from outreach_attempts oa
  left join mailers m
  using (patientId)
  
),

hard_to_reach_flag as (
   select distinct
   patientId,
   case
     when (isHardToReachAssessment or isHardToReachCarePlanning)
        then true
     else false
   end as cbHardToReachStatus

   from {{ ref('thpp_sla_hard_to_reach') }}

),

refusal_flag as (

    select    patientId,
              hasDeclinedAssessments as refusalAssessment,
              hasDeclinedActionPlan refusalCarePlan

        from {{ ref('patient_attribute_member_types') }}
),

hard_to_reach_date as (
   select distinct
   m.patientId,
   
   case
    when htrf.cbHardToReachStatus is true and o.outreachDate >= ma.outreachDate then o.outreachDate
    when htrf.cbHardToReachStatus is true and o.outreachDate < ma.outreachDate then ma.outreachDate
   end as cbHardToReachDate
    
   from member_info m
   left join hard_to_reach_flag htrf
   on m.patientId = htrf.patientId
   left join {{ ref('thpp_sla_outreach') }} o
   on htrf.patientId = o.patientId and isThirdOutreachAt is true
   left join {{ ref('thpp_sla_mailers') }}  ma
   on m.patientId = ma.patientId and isFirstMailer is true
  
),

kpi_ltss_flag as (
  select distinct
    patientId, 
  
    max(case
      when questionSlug = 'does-the-member-have-any-mental-illness-mds' and answerSlug != 'no' then true
      when questionSlug = 'how-well-client-made-decision-about-organizing-day' and answerSlug != 'independent-answer' then true
      when questionSlug = 'meal-preparation-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'ordinary-housework-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'managing-finance-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'managing-medications-difficulty-code' and answerSlug != 'no-difficulty' then true
      when questionSlug = 'phone-use-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'shopping-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'transportation-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'mobility-in-bed-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'transfer-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'locomotion-in-home-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'locomotion-outside-of-home-self-performance-code' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'dressing-upper-body-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'dressing-lower-body-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'eating-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'toilet-use-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'personal-hygiene-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'bathing-self-performance' and answerSlug != 'independent-did-on-own' then true
      when questionSlug = 'alzheiemers' and answerSlug != 'not-present' then true
      when questionSlug = 'dementia-other-than-alzheiemers-disease' and answerSlug != 'not-present' then true
      when questionSlug = 'presence-of-pressure-ulcer' and answerSlug != 'no-ulcer' then true
      when questionSlug = 'presence-of-statis-ulcer' and answerSlug != 'no-ulcer' then true
      when questionSlug = 'wound-or-ulcer-care' and answerSlug != 'none' then true
      when questionSlug = 'home-health-aide-days-hours-mins' and answerText not in ('0/0/0','0','none') then true
      when questionSlug = 'visiting-nurse-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'homemaking-services-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'meals-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'volunteer-services-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'physical-therapy-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'occupational-therapy-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'speech-therapy-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'day-care-or-day-hospital-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'social-worker-at-home-days-hours-mins' and answerSlug != 'not-present' then true
      when questionSlug = 'iv-infusion-central-treatment' and answerSlug != 'not-applicable-treatment' then true
      when questionSlug = 'iv-infusion-peripheral-treatment' and answerSlug != 'not-applicable-treatment' then true
      when questionSlug = 'occupational-therapy' and answerSlug != 'not-applicable-treatment' then true
      when questionSlug = 'physical-therapy' and answerSlug != 'not-applicable-treatment' then true
      when questionSlug = 'daily-nurse-monitoring' and answerSlug != 'not-applicable-treatment' then true
      when questionSlug = 'nurse-monitoring-less-than-daily' and answerSlug != 'not-applicable-treatment' then true
      when questionSlug = 'oxygen-equipment' and answerSlug != 'not-used-equipment' then true
      when questionSlug = 'iv-equipment' and answerSlug != 'not-used-equipment' then true
      when questionSlug = 'catheter-equipment' and answerSlug != 'not-used-equipment' then true
      when questionSlug = 'ostomy-equipment' and answerSlug != 'not-used-equipment' then true
      else false
      end) as kpiLtssFlag
    
  from {{ ref('questions_answers_current') }}

  where assessmentSlug = 'minimum-data-set'
  
  group by patientId
  
), 

kpi_ltss_info as (
  select 
    m.patientId,
    coalesce(k.kpiLtssFlag, false) as kpiLtssFlag
    
  from member_info m
  left join kpi_ltss_flag k
  using (patientId)

),

final as (
  select distinct
    mi.patientId,
    mi.memberId,
    mi.currentState,
    ci.tuftsCohort,
    mi.tuftsCohortGoLiveDate,
    ci.tuftsEffectiveDate,
    mi.tuftsEnrollmentDate,
    mi.tuftsDisenrollmentDate,
    mi.disenrolledAtDate,
    mi.tuftsLastAttemptedAssessmentDate,
    mi.tuftsLastCompletedAssessmentDate,
    mi.tuftsNextAssessmentDueDate,
    mi.tuftsAssignedHardToReachStatus,
    mi.tuftsAssignedMemberAssessmentRefusalStatus,
    mi.cbMemberAssessmentRefusalDate,
    mi.cbMemberCarePlanRefusalDate,
    mi.cbHasCarePlan,
    mi.cbCarePlanInitiationDate,
    mi.cbComprehensiveAssessmentDate,
    mi.cbMaxComprehensiveAssessmentDate,
    mi.cbCarePlanGoalUpdatedLatestDate,
    mi.cbMdsAssessmentDate,
    mi.cbMaxMdsAssessmentDate,
    mi.cbBothTuftsAssessmentsDate,
    mi.cbMinHraCompletedDate,
    mi.cbMaxHraCompletedDate,
    mi.cbMinCaseConferenceAt,
    mi.cbMaxCaseConferenceAt,
    loi.cbLtscOfferedStatus,
    loi.cbLtscOfferedAnswer,
    lii.cbLtscInvitedToAssessmentStatus,
    t.cbTotalOutreachAttempts,
    t.cbTotalMailers,
    htrf.cbHardToReachStatus,
    coalesce(r.refusalAssessment, false) as refusalAssessment,
    coalesce(r.refusalCarePlan, false) as refusalCarePlan,
    htrd.cbHardToReachDate,
    kli.kpiLtssFlag
    
  from member_info mi
  left join cohort_info ci
  using (patientId)
  left join ltsc_offer_info loi
  using (patientId)
  left join ltsc_invitation_info lii
  using (patientId)
  left join total_outreach t
  using (patientId)
  left join hard_to_reach_flag htrf
  using (patientId)
  left join refusal_flag r
  using (patientId)
  left join hard_to_reach_date htrd
  using (patientId)
  left join kpi_ltss_info kli
  using (patientId)
  
)

select * from final
