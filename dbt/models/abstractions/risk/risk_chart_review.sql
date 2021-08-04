
{{ config(
    materialized='incremental',
    tags=['evening']
  )
}}


with review as (

select distinct
s.* ,
current_timestamp as createdAt
from
{{source('abs_risk', 'prospective_review')}} s

{% if is_incremental() %}

left join {{ this }} t
on s.patientId = t.patientId 
and s.conditionCategoryCode = t.conditionCategoryCode
and s.conditionStatus = t.conditionStatus
and s.conditionType = t.conditionType

 where
    (s.conditionStatus != t.conditionStatus 
    or t.conditionStatus is null)
    and s.conditionStatus is not null

{% endif %}


)

select * from review where conditionCategoryCode is not null
