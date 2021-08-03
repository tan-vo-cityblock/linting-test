{{
  config(
    materialized='incremental'
  )
}}

with results as (

  select
  acf.id,
  acf.patientId,
  acf.fieldSlug,
  acf.fieldType,
  acf.fieldValue,
  acf.evaluatedResource,
  scs.isProduction as isProductionField,
  acf.createdAt

  from {{ ref('all_computed_fields') }} acf
  left join {{ source('computed_reference', 'src_computed_status') }} scs
  using (fieldSlug)

)

{% if is_incremental() %}
,

latest_results as (

select
latest.*
from (

  select array_agg(results order by createdAt desc limit 1)[offset(0)] latest

  from {{ this }} results
  group by patientId, fieldSlug)

)

{% endif %}

select
r.*

from results r

{% if is_incremental() %}

left join latest_results  as lr

on r.patientId = lr.patientId
and r.fieldSlug = lr.fieldSlug

where r.fieldValue != lr.fieldValue
or lr.fieldValue is null

{% endif %}
