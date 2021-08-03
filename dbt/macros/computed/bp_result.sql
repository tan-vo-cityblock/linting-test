
{% macro bp_result(

  slug,
  vitals_code,
  vitals_code_2,
  severe_min,
  severe_min_2,
  moderate_min,
  moderate_min_2,
  mild_min,
  mild_min_2,
  min_result_threshold=1,
  timestamps_min=1,
  period="1 year",
  vitals_table=ref('vitals'),
  cpt_table=ref('abs_procedures')

  )
%}

{{ construct_combined_bp(

  vitals_code, 
  vitals_code_2, 
  severe_min, 
  severe_min_2, 
  moderate_min, 
  moderate_min_2, 
  mild_min, 
  mild_min_2,
  period=period,
  vitals_table=vitals_table,
  cpt_table=cpt_table

  )
}}

elev_hist as (

  select patientId
  from combined_vitals
  where acuity > {{ min_result_threshold }}
  group by patientId
  having count(timestamp) > {{ timestamps_min }}

),

cf_status as (

  select 
    patientId,
    case
      when patientId in (select * from elev_hist)
        then 'true'
      else 'false'
    end as value
  from combined_vitals
  group by patientId

),

final as (

  {{ computed_field(slug=slug, type='bp_result', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
