with outreach_attempts as (

  select distinct
    md5(concat(patientId, modality, date(attemptedAt))) as id,
    patientId,
    date(attemptedAt) as attemptedDate,
    personReached,
    modality,
    case when modality = "mail"
    -- Email and Text modalities are not considered as unsuccessful as the patient might want to communicate via texts or emails
          then null
         when (personReached != "member"
                                            or (personReached = "member" and outcome in
                                                                        ("hungUp" , "notInterested")))
         then true
         else false
    end as unsuccessful_attempt,
    dense_rank() over(partition by patientId order by date(attemptedAt) desc) as rnk

  from {{ ref('outreach_attempt') }}
  inner join {{ ref('thpp_sla_reporting_month') }} on date(attemptedAt) <= tuftsSlaReportEndDate

  where direction = 'outbound'

),

refusal as (

    select    patientId,
              hasDeclinedAssessments,
              hasDeclinedActionPlan

        from {{ ref('patient_attribute_member_types') }}
),

interaction_attempts as (

  select distinct
    patientId,
    md5(concat(patientId, eventType, eventTimestamp)) as progressNoteId,
    eventTimestamp as eventDate,
    eventType,
    status,
    dense_rank() over(partition by patientId order by eventTimestamp desc) as interaction_attempt_rnk
  
  from {{ ref('member_interactions') }}
  inner join {{ ref('thpp_sla_reporting_month') }} on date(eventTimestamp) <= tuftsSlaReportEndDate
  
  where

 -- This filters out the statuses where there was a successful interaction and which leads to false flagging of member being HTR even after successful recent interaction.
--    (status = 'attempt' or eventType = 'mail' and
    hasMemberAttendee

),

member_states as (

    select *

    from  {{ ref('thpp_disenrolled_member_states') }}

),

tufts_enrollment_dates as (

  select
    t.patientId,
    case
      when m.isReattributed
        then m.latestAttributedDate
      else t.tuftsEnrollmentDate
    end as tuftsEnrollmentDate,
    case
        when m.isReattributed
            then date_add(m.latestAttributedDate, interval 90 day)
        else date_add(t.tuftsEnrollmentDate, interval 90 day)
    end as enrollmentPeriodEndDate

  from {{ ref('tufts_enrollment_date') }} t
  inner join member_states m
  using (patientId)

),

consent_dates as (

  select
    patientId,
    consentDate

  from member_states
  inner join {{ ref('thpp_sla_reporting_month') }} on consentDate <= tuftsSlaReportEndDate
),

assessment_dates as (

  select 
    patientId, 
    maxComprehensiveAssessmentAt

  from {{ ref('member_commons_completion') }}
  inner join {{ ref('thpp_sla_reporting_month') }}
      on date(maxComprehensiveAssessmentAt) <= tuftsSlaReportEndDate


),

assessed_members as (

  select patientId,
         ad.maxComprehensiveAssessmentAt

  from tufts_enrollment_dates ted

  left join assessment_dates ad
  using (patientId)

  where date(ad.maxComprehensiveAssessmentAt) >= ted.tuftsEnrollmentDate

),

enrollment_period_outreach_without_consent_or_assessment as (

  select oa.*
  
  from outreach_attempts oa
  
  inner join tufts_enrollment_dates ted
  using (patientId)
  
  left join consent_dates cd
  using (patientId)

  left join assessed_members am
  using (patientId)
  
  where
    (ted.enrollmentPeriodEndDate < cd.consentDate or cd.consentDate is null) and
    am.patientId is null

),

outreach_counts as (

  select 
    patientId,
    count(distinct case when unsuccessful_attempt = false and rnk <=4 then id else null end) as numOutreachSuccesses,
    count(distinct case when unsuccessful_attempt and rnk <=4 then id else null end) as numOutreachAttempts,
    count(distinct case when unsuccessful_attempt is null then id else null end) as numMailModalityOutreachCount,
    count(distinct case when rnk <=4 then modality else null end) as numModalityOutreachCounts
    
  from enrollment_period_outreach_without_consent_or_assessment
  group by patientId
  
),

members_with_zero_outreach_successes as (

  select patientId
  from outreach_counts  
  where numOutreachSuccesses = 0

),

member_with_mailer_outreach_attempt as (

  select patientId
  from outreach_counts
  where numMailModalityOutreachCount >=1 and
  numOutreachAttempts >= 3

),

members_with_four_outreach_attempts as (

  select patientId
  from outreach_counts  
  where numOutreachAttempts >= 4 and
        numModalityOutreachCounts >=2

),

unconsented_hard_to_reach_members as (

  select patientId
  from
  members_with_zero_outreach_successes  o
  left join
  members_with_four_outreach_attempts f
  using (patientId)
  left join
  member_with_mailer_outreach_attempt m
  using (patientId)

  where
  f.patientId is not null or m.patientId is not null

),

interaction_attempts_raw as (
 select
    ia.patientId,
    count(distinct case when (status = 'attempt' or eventType = "mail" ) and  interaction_attempt_rnk <=4 then eventType else null end ) as numModalityCount,
    count(distinct case when (status = 'attempt' or eventType = "mail" ) and interaction_attempt_rnk <=4 then progressNoteId else null end) as numInteractionAttempts,
    count(distinct case when (status = 'attempt' or eventType = "mail" ) and interaction_attempt_rnk <=3 then progressNoteId else null end) as top3InteractionAttempts,
        -- for mailer or text or email at any point in time preconsent/postconsent
    count(distinct case when unsuccessful_attempt is null then id
                        when eventType = 'mail' then progressNoteId
                        else null end ) as numMailModalityCount

  from interaction_attempts ia

  left join consent_dates cd
  using (patientId)

  left join outreach_attempts o
  using (patientId)

  where
    (date(ia.eventDate) > cd.consentDate) or (cd.consentDate is null)

  group by ia.patientId
),

--HTR with no/prior assessment or consented HTR
consented_hard_to_reach_members as (

  select 
    patientId

  from interaction_attempts_raw iar
  left join assessed_members am
  using (patientId)

  where
    am.patientId is null and (
    (numInteractionAttempts >= 4 and numModalityCount >= 2) or
    (numMailModalityCount >=1 and top3InteractionAttempts >= 3)
    )

),

-- for HTR care planning after assessment

member_goals_tasks_raw as (

    select distinct patientId
    from {{ ref('member_goals_tasks') }} gt

    where goalLabel != "Engagement"
),

interaction_attempt_counts_after_assessment as (

  select
    am.patientId

  from interaction_attempts_raw iar

  inner join assessed_members am
  using (patientId)

  left join member_goals_tasks_raw  gt
  using (patientId)

  where
    gt.patientId is null and (
    (numInteractionAttempts >=4 and numModalityCount >= 2) or
    (numMailModalityCount >=1  and top3InteractionAttempts >= 3)
    )

),

reassessment_raw as (

  select patientId,
         timestamp_add(bothTuftsContractualAssessmentsAt, interval 365 day) as reassessmentDueEndDate,
         timestamp_sub((timestamp_add(bothTuftsContractualAssessmentsAt, Interval 365 day)) , interval 90 day) as reassessmentBeginDate
  from {{ ref('member_commons_completion') }}
  inner join {{ ref('thpp_sla_reporting_month') }} on
           date(timestamp_add(bothTuftsContractualAssessmentsAt, interval 365 day)) <= tuftsSlaReportEndDate

),

reassessed_members as (
-- only counting members who are assessed once already
 select r.patientId

  from  reassessment_raw r

  left join assessed_members a
  using (patientId)

   where
    a.patientId is not null and
    (a.maxComprehensiveAssessmentAt between reassessmentBeginDate and r.reassessmentDueEndDate)

),

reassessed_after_reassessment_window as (
-- if a member is reassessed after reassessment window and before the tufts sla report end date then not HTR, so for april reporting the member is assessed after reassessmnet due end date but before tufts reporting end date so this logic is not gonna work. this should be either greater than
select patientId,
 from  reassessment_raw r

  left join assessed_members a
  using (patientId)

   where
    a.patientId is not null and
    a.maxComprehensiveAssessmentAt > reassessmentDueEndDate
    --(date(a.maxComprehensiveAssessmentAt) between date(r.reassessmentDueEndDate) and LAST_DAY(date(r.reassessmentDueEndDate) , MONTH)
    --**** and last date of reassessment end date month  but does reassessment window month has to be the tufts sla reporting month
    ),

reassessment_attempt as (

  select
        patientId,
        count(distinct case when (status = 'attempt' or eventType = "mail" ) and  interaction_attempt_rnk <=4 then eventType else null end ) as   numModalityCountReassessment,
        count(distinct case when (status = 'attempt' or eventType = "mail" ) and interaction_attempt_rnk <=4 then progressNoteId else null end) as numInteractionAttempts,
        count(distinct case when (status = 'attempt' or eventType = "mail" ) and interaction_attempt_rnk <=3 then progressNoteId else null end) as top3InteractionAttempts,
        -- for mailer or text or email at any point in time preconsent/postconsent
        count(distinct case when unsuccessful_attempt is null then id
                        when eventType = "mail" then progressNoteId
                        else null end ) as numMailModalityCountReassessment


  from interaction_attempts ia
  inner join reassessment_raw rr
  using (patientId)
  --checks if the member is assessed/re-assessed before or not
  left join reassessed_members r
  using (patientId)
  left join reassessed_after_reassessment_window rw
  using (patientId)
  left join outreach_attempts o
  using (patientId)
  where r.patientId is null
  and rw.patientId is null -- the member is not assessed within tufts reporting time then the member is HTR for reassessment

  group by patientId
),

reassessment_hard_to_reach_members as (

  select patientId
  from reassessment_attempt
  where (numInteractionAttempts >= 4 and
        numModalityCountReassessment >=2) or (numMailModalityCountReassessment >=1  and top3InteractionAttempts >= 3)

),

--HTRAssessment
hard_to_reach_members as (

  (select patientId from unconsented_hard_to_reach_members
  union all
  select patientId from consented_hard_to_reach_members )
  union distinct
  select patientId from reassessment_hard_to_reach_members
),

--HTRCarePlanning

htr_care_planning as (
    select patientId from hard_to_reach_members
    union distinct
    select patientId from interaction_attempt_counts_after_assessment
),

final as (

  select 
    t.patientId,
    htr.patientId is not null as isHardToReachAssessment,
    htrcp.patientId is not null as isHardToReachCarePlanning,
    t.tuftsEnrollmentDate,
    r.hasDeclinedAssessments as refusalAssessment,
    r.hasDeclinedActionPlan as refusalCarePlan,
    t.enrollmentPeriodEndDate,
    cd.consentDate,
    date(ad.maxComprehensiveAssessmentAt) as maxComprehensiveAssessmentDate,
    oc.numOutreachSuccesses,
    oc.numOutreachAttempts,
    ia.numInteractionAttempts 
  
  from tufts_enrollment_dates t
  
  left join hard_to_reach_members htr
  using (patientId)
  
  left join consent_dates cd
  using (patientId)
  
  left join assessment_dates ad
  using (patientId)
  
  left join outreach_counts oc
  using (patientId)
  
  left join interaction_attempts_raw ia
  using (patientId)

  left join htr_care_planning htrcp
  using (patientId)

  left join refusal r
  using (patientId)
 
)

select * from final
