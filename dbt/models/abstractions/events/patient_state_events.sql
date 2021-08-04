

{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('commons', 'patient_state') }}

),

patient as (

    select * from {{ source('commons', 'patient') }}

),

market as (

    select * from {{ source('commons', 'market') }}

),

{% set event_config = {
  'source': {
    'ids': '[ps.id]',
    'idField': "'id'",
    'project': "'cbh-db-mirror-prod'",
    'dataset': "'commons_mirror'",
    'table': "'patient_state'"
  },
  'subject': {
    'type': "'patientId'",
    'key': 'patientId',
    'display': 'STRING(NULL)'
  },
  'verb': "'transitions'",
  'prepositional_object': {
    'type': "'market'",
    'key': 'mkt.name'
  },
  'timestamp': {
    'createdAt': 'cast(ps.createdAt as TIMESTAMP)',
    'completedAt': 'TIMESTAMP(NULL)',
    'manualEventAt': 'TIMESTAMP(NULL)',
    'receivedAt': 'TIMESTAMP(NULL)'
  },
  'outcomes': [{
    'type': "'engagementState'",
    'key': 'currentState'
  }]
}
%}

final as (

    select
      {{  create_event(**event_config)  }}

    from source as ps

    left join patient as pt
      on ps.patientId = pt.id

    left join market as mkt
      on mkt.id = pt.homeMarketId

)

select * from final
