{%- macro encounter_evidence(slug,
      ed_visits=None,
      unplanned_admits=None,
      min_day_count=1) -%}

with abs_encounters as (

  select
  patientId,
  date,
  id,
  'id' as key,
  'abs_encounters' as model,
  case
    when ed is true then "ed encounter"
    when resultsInInpatient is true then "unplanned admit"
    else null
  end as code,
  date as validDate

  from {{ ref('abs_encounters') }}
  where date > date_sub(current_date, interval 1 year)

  {% if ed_visits == True %}

  and ed is true

  {% elif unplanned_admits == True %}

  and resultsInInpatient is true

  {% endif %}

),

aggregated_resources as (

  {{ aggregate_computed_field_resources(table='abs_encounters', min_field='date', min_value=min_day_count) }}

),

cf_status as (

  {{ derive_computed_field_values(table='aggregated_resources') }}

),

final as (

  {{ computed_field(slug=slug, type='encounter_evidence', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select *
from final

{%- endmacro -%}
