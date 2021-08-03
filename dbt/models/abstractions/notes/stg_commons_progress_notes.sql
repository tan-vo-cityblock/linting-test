with progress_notes as (

  select * from {{ ref('timeline_event') }}

),

event_attendees as (

  select
    eventGroupId as groupId,
    attendees

  from {{ ref('abs_event_attendee') }}

),

original_text as (

  select
    eventGroupId as groupId,
    templateSlug,
    text as originalText

  from {{ ref('abs_commons_note_text') }}

),

revised_text as (

  select
    timelineEventId as id,
    text as revisedText

  from {{ source('commons', 'note_revision') }}

),

final as  (

  select
    n.id,
    n.groupId,
    n.patientId,
    n.createdById as userId,
    'progress' as noteType,
    n.direction,
    n.eventType as modality,
    n.eventType as groupedModality,
    n.location,
    n.status,
    n.isLegacy,
    n.isTransitionOfCare,
    n.isReferral,
    n.referral,
    n.referralReason,
    cast(null as boolean) as isScheduledOutreach,
    cast(null as string) as outreachOutcome,
    cast(null as string) as outreachNotInterestedReason,
    n.eventTimestamp as interactionAt,
    n.createdAt,
    n.deletedAt,
    a.attendees,
    t0.templateSlug,
    coalesce(t1.revisedText, t0.originalText) as noteText

  from progress_notes n

  left join event_attendees a
  using (groupId)

  inner join original_text t0
  using (groupId)

  left join revised_text t1
  using (id)

)

select * from final
