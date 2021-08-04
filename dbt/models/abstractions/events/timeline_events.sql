
{{
  config(
    materialized='view'
  )
}}


with progress_note_events as (

  select
    eventType,
    id,
    createdById,
    patientId,
    createdAt,
    location,
    eventTimestamp as startedAt,
    case 
      when location in ('Messaging', 'Email') 
        then 'messages'
      when 
        eventType = 'phoneCall' or 
        location = 'Telephone' 
          then 'calls'
      when 
        eventType in ('inPersonVisitOrMeeting', 'videoCall') or
        location in ('Member Home', 'Video Conference') 
          then 'meets'
      when eventType = 'caseConference' 
        then 'reviews'
      else 'services' 
    end as verb

  from {{ ref('timeline_event') }}

),

{% set event_config = {
  'source': {
    'ids': '[id]',
    'idField': "'id'",
    'project': "'cityblock-analytics'",
    'dataset': "'mrt_commons'",
    'table': "'timeline_event'"
  },
  'subject': {
    'type': "'userId'",
    'key': 'createdById',
    'display': 'string(null)'
  },
  'verb': 'verb',
  'direct_object': {
    'type': "'patientId'",
    'key': 'patientId',
    'display': 'string(null)'
  },
  'indirect_object': {
    'type': "'setting'",
    'key': """case when location in ('Telephone', 'Messaging', 'Email', 'Video Conference', 'Other') or eventType in ('phoneCall', 'videoCall') or verb = 'reviews' then null
         when regexp_contains(location, 'Hub') then 'Hub'
         when location = 'Member Home' then 'Home'
         when regexp_contains(location, 'Clinic') then 'Clinic'
         when location = 'Emergency Room' then 'ED'
         else location end""",
  },
  'prepositional_object': {
    'type': 'string(null)',
    'key': 'string(null)'
  },
  'timestamp': {
    'createdAt': 'createdAt',
    'completedAt': 'timestamp(null)',
    'manualEventAt': 'startedAt',
    'receivedAt': 'timestamp(null)'
  },
  'purposes': [{
    'type': "'timeline event'",
    'key': 'eventType'
  }],
  'outcomes': [{
    'type': 'string(null)',
    'key': 'string(null)'
  }]
}
%}

final as (

    select
      {{  create_event(**event_config)  }}

    from progress_note_events

)

select * from final
