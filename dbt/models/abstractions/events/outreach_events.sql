
{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('commons', 'outreach_attempt') }}

),

{% set event_config = {
  'source': {
    'ids': '[id]',
    'idField': "'id'",
    'project': "'cbh-db-mirror-prod'",
    'dataset': "'commons_mirror'",
    'table': "'outreach_attempt'"
  },
  'subject': {
    'type': "'userId'",
    'key': 'userId',
    'display': 'STRING(NULL)'
  },
  'verb': """case
      when modality IN ('phone', 'careTransitionsPhone', 'calledCityblockMain') then 'calls'
      when modality = 'smsMessage' then 'messages'
      when modality IN ('homeVisit', 'hub', 'careTransitionsInPerson', 'ehr') then 'meets'
      when modality in ('other', 'community', 'outreachEvent') then 'services'
      else 'services' end """,
  'direct_object': {
    'type': "'patientId'",
    'key': 'patientId',
    'display': 'STRING(NULL)'
  },
  'timestamp': {
    'createdAt': 'cast(createdAt as TIMESTAMP)',
    'completedAt': 'TIMESTAMP(NULL)',
    'manualEventAt': 'TIMESTAMP(NULL)',
    'receivedAt': 'TIMESTAMP(NULL)'
  },
  'purposes': [{
    'type': "'note'",
    'key': "'outreach'"
  }],
  'outcomes': [{
    'type': "'personReached'",
    'key': 'personReached'
  },
  {
    'type': "'detail'",
    'key': 'outcome'
  }]
}
%}

final as (

    select
      {{  create_event(**event_config)  }}

    from source

)

select * from final
