
with outreach_attempts as (

  select
    id as followUpId,
    'outreach_attempts' as followUpSource,
    patientId,
    userId as followUpUserId,
    attemptedAt as followUpAt,
    modality as followUpModality,
    modality like 'careTransitions%' as isTocFollowup,
    outcome as followUpOutcome,
    'outreach' as followUpType,

    case
      when personReached = 'noOne'
        then 'attempt'
      else 'success'
    end as followUpStatus,

    personReached as followUpRecipients

  from {{ source('commons', 'outreach_attempt') }}

),

ref_member_interactions as (

  select
    eventSource,
    progressNoteGroupId as followUpId,
    'member_interactions' as followUpSource,
    patientId,
    userId as followUpUserId,
    eventTimestamp as followUpAt,
    eventType as followUpModality,
    isTransitionOfCare as isTocFollowup,
    string(null) as followUpOutcome

  from {{ ref('member_interactions') }}

),

member_interaction_types as (

  select
    progressNoteGroupId as followUpId,
    interactionType as followUpType,
    interactionStatus as followUpStatus

  from {{ ref('abs_member_interaction_types') }}

),

member_interaction_attendees as (

  select
    eventGroupId as followUpId,
    attendees as followUpRecipients

  from {{ ref('abs_event_attendee') }}

),

member_interactions as (

  select
    rmi.* except (eventSource),
    mit.followUpType,
    mit.followUpStatus,
    case
      when rmi.eventSource in ('epic_notes', 'elation_notes')
        then 'Member'
      else mia.followUpRecipients
    end as followUpRecipients

  from ref_member_interactions rmi

  left join member_interaction_types mit
  using (followUpId)

  left join member_interaction_attendees mia
  using (followUpId)

),

screening_tool_submissions as (

  select * from {{ ref('abs_toc_tool') }}

),

followups as (

  select * from outreach_attempts
  union all
  select * from member_interactions
  union all
  select * from screening_tool_submissions

)

select * from followups
