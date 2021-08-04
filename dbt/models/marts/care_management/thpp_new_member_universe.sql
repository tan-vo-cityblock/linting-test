with crosswalk as (

  select patientId,
         memberId
  from {{ ref('member') }}

),

patient_screening_tool_submission as (
        select  patientId,
                userId,
                createdAt,
                screeningToolSlug,
                scoredAt
        from {{ source('commons', 'patient_screening_tool_submission') }}
        where scoredAt is not null and
        screeningToolSlug ='comprehensive'
),

mds_interviews as (

  select distinct
    patientId,
    userId,
    patientAnswerCreatedAt as createdAt,
    'minimum-data-set' as screeningToolSlug,
    patientAnswerCreatedAt as scoredAt

  from {{ ref('questions_answers_all') }}

  where
    questionSlug = 'ready-for-review' and
    assessmentSlug = 'minimum-data-set'

),

thpp_cm_mds_05142020 as (
         select patientId,
                lastCompletedAssessmentDate
         from {{ source('care_management','thpp_cm_mds_05142020') }}
         inner join crosswalk using(memberId)
),


hra_assessments_raw as (
    select
    patientId,
    userId,
    patientAnswerCreatedAt as createdAt,
    'health-risk-assessment' as screeningToolSlug,
    answerValue,
    rank() over(partition by patientId order by answerValue desc) as rnk,
    rank() over(partition by patientId order by answerValue) as ascRnk
  from {{ ref('questions_answers_all') }}

  where
    questionSlug = 'assessment-reference-date-time' and
    assessmentSlug = 'health-risk-assessment-ma'

),


hra_assessments_final as (

  select patientId,
         max(case when rnk = 2 then date(answerValue) else null end ) as prevHraAssessmentDate,
         max(case when rnk = 1 then date(answerValue) else null end ) as  currentHraAssessmentDate,
         max(case when ascRnk = 1 then date(answerValue) else null end ) as initialHraAssessmentDate

  from hra_assessments_raw
  group by 1

),

user_MDS_HRA_raw as (
    select patientId,
           userId,
           createdAt

        from mds_interviews as p


        where p.screeningToolSlug ='minimum-data-set'


   union all


    select patientId,

           UserId,
           createdAt

        from hra_assessments_raw as hra


        where hra.screeningToolSlug ='health-risk-assessment'

 ) ,


user_MDS_HRA as (


    select patientId,
           userName

    from (
    select patientId,
           userName,
           dense_rank() over(partition by patientId order by createdAt desc) as rnk
    from
            user_MDS_HRA_raw p

        left join {{ ref('user') }} as u
                                        on p.userId = u.userId

        )


    where rnk = 1
),


cbh_thpp_assessment_raw as (

        select   patientId,
                 screeningToolSlug,
                 scoredAt as assessmentDate
        from patient_screening_tool_submission

union all
        select  patientId,
                screeningToolSlug,
                scoredAt as assessmentDate
        from mds_interviews

union all

      select patientId,
             'minimum-data-set' as screeningToolSlug,
             CAST(lastCompletedAssessmentDate AS TIMESTAMP) as assessmentDate
      from 	thpp_cm_mds_05142020

union all

      select patientId,
             'comprehensive' as screeningToolSlug,
             CAST(lastCompletedAssessmentDate AS TIMESTAMP) as assessmentDate
      from 	thpp_cm_mds_05142020
),

cbh_thpp as (

    select patientId,
           assessmentDate,
           screeningToolSlug,
           dense_rank() over(partition by patientId,screeningToolSlug order by assessmentDate desc) as rnk
    from cbh_thpp_assessment_raw
 ),

cbh_thpp_final as (

     select patientId,
            cast(min(case when screeningToolSlug = 'comprehensive' then assessmentDate else null end) as date) as initialCompCompletionDate,
            cast(min(case when screeningToolSlug = 'minimum-data-set' then assessmentDate else null end) as date) as initialMdsCompletionDate,
            cast(max(case when screeningToolSlug = 'minimum-data-set' then assessmentDate else null end) as date) as currentMdsCompletionDate,
            cast(max(case when screeningToolSlug = 'comprehensive' then assessmentDate else null end) as date) as currentCompCompletionDate,
            cast(max(case
                   when rnk=2 and screeningToolSlug = 'minimum-data-set'
                      then assessmentDate
                   else null
                end ) as date) as prevMdsAssessmentDate,
            cast(max(case
                    when rnk=2 and screeningToolSlug = 'comprehensive'
                        then assessmentDate
                    else null
                 end) as date) as prevCompAssessmentDate

     from cbh_thpp
     group by 1
),

assessments_combo as (

  select patientId,
         coalesce(initialCompCompletionDate,initialHraAssessmentDate) as initialCompCompletionDate,
         coalesce(initialMdsCompletionDate,initialHraAssessmentDate) as initialMdsCompletionDate,
         coalesce(currentHraAssessmentDate,currentMdsCompletionDate) as currentMdsCompletionDate,
         coalesce(currentHraAssessmentDate,currentCompCompletionDate) as currentCompCompletionDate,

       -- case when hra is present and comp or mds is also present
         case when currentHraAssessmentDate is not null then coalesce(prevHraAssessmentDate,currentMdsCompletionDate) else
         -- when hra is not present, prevHraAssessmentDate will be always null
                prevMdsAssessmentDate end as prevMdsAssessmentDate,
          case when currentHraAssessmentDate is not null then coalesce(prevHraAssessmentDate,currentCompCompletionDate) else
                    prevCompAssessmentDate end  as prevCompAssessmentDate

  from cbh_thpp_final

 -- Using full join as the new members will not have individual comp or mds but the combo assessment which leads to cbh_thpp_final.patientId as null and the cases are not captured.
  full outer join hra_assessments_final  using (patientId)

),

outreach_connection_attempts_raw as (
    select patientId,
           case when modality ='phone' then replace(modality,'phone','phoneCall') else modality end as modality,
           cast(attemptedAt as date) as outreachDate
    from {{ ref('outreach_attempt') }}
    where direction = 'outbound'

union distinct


   select patientId,
          case when eventType= 'phone' then replace(eventType,'phone','phoneCall') else eventType end as modality,
          cast(eventTimestamp as date) as outreachDate
   from {{ ref('member_interactions') }}
   where isAttemptedConnection
),


outreach_connection_attempts_final_V1 as (
        select patientId,
               outreachDate,
               cast(row_number() over(partition by patientId order by outreachDate desc) as STRING) as row
        from outreach_connection_attempts_raw
),

outreach_connection_attempts_final_V2 as (
        select patientId,
               {{ dbt_utils.pivot(
                    'row',
                    [1,2,3,4,5],
                     agg='min',
                     prefix='Outreach',
                     then_value= 'outreachDate',
                     else_value= 'NULL' )
               }}
        from outreach_connection_attempts_final_V1
        group by patientId
),

final as (
select
             m.firstName,
             m.lastName,
             m.memberId,
             m.tuftsEnrollmentDate,
             coalesce(m.cohortGoLiveDate, m.tuftsEnrollmentDate) as MemberAssignedtoCBDate,
             case
               when m.cohortName = 'Tufts Cohort 1'
                 then 'Y'
               else 'N'
             end as transitionFromthp,
             ctf.initialCompCompletionDate,
             ctf.initialMdsCompletionDate,
             ctf.currentCompCompletionDate,
             ctf.currentMdsCompletionDate,
             ctf.prevCompAssessmentDate,
             ctf.prevMdsAssessmentDate,
             mcc.hasGeneratedCarePlan as ICPcompletedOrNot,
             date(mcc.maxCompletedTaskAt) AS currentICPCompletedDate,
             mca.currentMemberAcuityDescription AS careLevel,
             user_MDS_HRA.userName AS assessmentCompletedBy,
             case
                 when date_diff(current_date(), ctf.currentCompCompletionDate, day) > 365
                   then 'Late'
                 when ctf.currentCompCompletionDate is null and date_diff(current_date(),m.tuftsEnrollmentDate ,day) > 90
                   then 'Late'
                 else 'No'
             end as assessmentOverdueIndicator,
             case
                when thpp.isHardToReachAssessment
                    then 'HTRAssessment'
                else ' '
             end as isHardToReachAssessment,
             case
                when thpp.isHardToReachCarePlanning
                    then 'HTRCarePlanning'
                else ' '
             end as isHardToReachCarePlanning,
             case
                when thpp.refusalAssessment
                    then 'refusalAssessment'
                else ' '
             end as refusalAssessment,
             case
                when thpp.refusalCarePlan
                    then 'refusalCarePlan'
                else ' '
             end as refusalCarePlan,
             v2.outreach1 as firstOutreachDate,
             v2.outreach2 as secondOutreachDate,
             v2.outreach3 as thirdOutreachDate,
             v2.outreach4 as fourthOutreachDate,
             v2.outreach5 as fifthOutreachDate,
             ms.currentState,
             date(ms.disenrolledAt) as disenrolledDate
from {{ ref('member') }} as m
left join {{ ref('member_commons_completion') }} as mcc on m.patientID = mcc.patientID
left join thpp_cm_mds_05142020 as ta on ta.patientId = m.patientID
left join {{ ref('member_current_acuity') }} as mca on m.patientID = mca.patientID
left join outreach_connection_attempts_final_V2 as v2 on v2.patientId =m.patientId
left join assessments_combo as ctf on ctf.patientId = m.patientId
left join user_MDS_HRA on user_MDS_HRA.patientId = m.patientId
left join {{ ref('member_states') }} ms on ms.patientId = m.patientId
left join {{ ref('thpp_sla_hard_to_reach') }} thpp on thpp.patientId = m.patientId
where m.patientHomeMarketName = 'Massachusetts' and m.partnerName ='Tufts'
)


select * from final
