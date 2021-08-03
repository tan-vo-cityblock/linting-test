{{
 config(
   materialized='table'
 )
}}

with current_attributes as (

  select
    patientId,
    type,
    value,
    createdAt
  from {{ source('commons', 'patient_attribute') }}
  where deletedAt is null
),

members_no_longer_interested_or_lost_contact as (

  select
    *
  from current_attributes
  where ((type = 'noLongerInterested' and value = 'true') or
    (type = 'lostContact' and value = 'true'))
),

members_who_declined_assessments as (

  select
    *
  from current_attributes
  where (type = 'declinedAssessments' and value = 'true')
),

members_who_declined_action_plan_collaboration as (

  select
    *
  from current_attributes
  where (type = 'declinedActionPlan' and value = 'true')
),

final as (
  select
    distinct(current_attributes.patientId) as patientId,
    case when (members_no_longer_interested_or_lost_contact.type ='noLongerInterested')
      then true else false END isNoLongerInterested,
    case when (members_no_longer_interested_or_lost_contact.type ='noLongerInterested')
      then members_no_longer_interested_or_lost_contact.createdAt else null END noLongerInterestedAt,
    case when (members_no_longer_interested_or_lost_contact.type ='lostContact')
      then true else false END as isLostContact,
    case when (members_no_longer_interested_or_lost_contact.type ='lostContact')
      then members_no_longer_interested_or_lost_contact.createdAt else null END as lostContactAt,
    case when (members_who_declined_assessments.type ='declinedAssessments')
      then true else false END hasDeclinedAssessments,
    case when (members_who_declined_assessments.type ='declinedAssessments')
      then members_who_declined_assessments.createdAt else null END as declinedAssessmentsAt,
    case when (members_who_declined_action_plan_collaboration.type ='declinedActionPlan')
      then true else false END hasDeclinedActionPlan,
    case when members_who_declined_action_plan_collaboration.type ='declinedActionPlan'
      then members_who_declined_action_plan_collaboration.createdAt else null END declinedActionPlanAt,

  from current_attributes

  left join members_no_longer_interested_or_lost_contact
  using (patientId)

  left join members_who_declined_assessments
  using (patientId)

  left join members_who_declined_action_plan_collaboration
  using (patientId)
)

select * from final
