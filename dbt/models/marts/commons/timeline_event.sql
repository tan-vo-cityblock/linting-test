{%- set metadata_fields = ['isTransitionOfCare', 'direction', 'referral', 'referralReason'] -%}

with timeline_event as (
  select
    * except (metadata),
   {% for field in metadata_fields %}

    json_value(metadata, '$.{{ field }}') as {{ field }},

{% endfor %}

from {{ source('commons', 'timeline_event') }}

where
  dataSource = 'user' and
  deletedAt is null and
  isPublished is true and
  markedAsErrorReason is null
),

final as (

  select * except {{ list_to_sql(metadata_fields, quoted=False) }},
    coalesce(cast(isTransitionOfCare as boolean), false) as isTransitionOfCare,
    direction,
    referral is not null as isReferral,
    referral,
    referralReason

  from timeline_event

)

select * from final
