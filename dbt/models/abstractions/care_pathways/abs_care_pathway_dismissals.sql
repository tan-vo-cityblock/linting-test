with dismissals as (

  select *
  from {{ ref('src_commons_event') }}
  where type = 'pathwaySuggestionDismissed'

)

select
  id,

  {% for field in ['patientId', 'userId', 'pathwaySlug', 'reason'] %}

    replace(json_extract(body, "$.{{ field }}"), "\"", "") as {{ field }},

  {% endfor %}
    
  createdAt
  
from dismissals
