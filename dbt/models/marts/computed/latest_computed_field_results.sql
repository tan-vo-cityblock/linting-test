
select latest.*, concat(latest.patientId, '-', latest.fieldSlug) as memberSlug
from (

  select array_agg(results order by createdAt desc limit 1)[offset(0)] latest
  from {{ ref('computed_field_results') }} results
  group by patientId, fieldSlug

  )
