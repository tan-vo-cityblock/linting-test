
{# test the constructor on a pure map #}

{% set test_map = {'a': "'b'", 'c': 'd'} %}

{{ map_to_struct(test_map) }}

{# test the constructor on a list of maps #}

{% set test_list = [{'a': "'b'", 'c': 'd'}, {'r': "'x'", 'e': 'w'}] %}

{{ list_to_struct_array(test_list) }}

{% set event_config = {
  'source': {
    'ids': '[id]',
    'idField': "'id'",
    'project': "'cbh-db-mirror-prod'",
    'dataset': "'commons_mirror'",
    'table': "'progress_note'"
  },
  'subject': {
    'type': "'userId'",
    'key': 'userId',
    'display': 'STRING(NULL)'
  },
  'verb': 'verb',
  'direct_object': {
    'type': "'patientId'",
    'key': 'patientId',
    'display': 'STRING(NULL)'
  },
  'indirect_object': {
    'type': "'setting'",
    'key': """case when location IN ('Telephone', 'Messaging', 'Email', 'Video Conference', 'Other') or title IN ('Telephone','Call Center') or verb = 'reviews' then NULL
         when REGEXP_CONTAINS(location, 'Hub') then 'Hub'
         when REGEXP_CONTAINS(lower(location), 'home') then 'Home'
         when REGEXP_CONTAINS(location, 'Clinic') then 'Clinic'
         when location = 'Emergency Room' then 'ED'
         else location end""",
  },
  'prepositional_object': {
    'type': 'STRING(NULL)',
    'key': 'STRING(NULL)'
  },
  'timestamp': {
    'createdAt': 'createdAt',
    'completedAt': 'completedAt',
    'manualEventAt': 'startedAt',
    'receivedAt': 'TIMESTAMP(NULL)'
  },
  'purposes': [{
    'type': "'note'",
    'key': 'title'
  }],
  'outcomes': [{
    'type': "'worryScore'",
    'key': 'cast(worryScore as string)'
  }]
}
%}


final as (

    select
      {{  create_event(**event_config)  }}

    from source

)

select * from final
