
{% macro bp_presence (

  slug,
  vitals_code,
  vitals_code_2,
  stable_cpt,
  stable_cpt_1,
  stable_cpt_2,
  period="1 year",
  vitals_table=ref('vitals'),
  cpt_table=ref('abs_procedures')

  )
%}

{{ construct_combined_bp(

  vitals_code, 
  vitals_code_2, 
  stable_cpt=stable_cpt,
  stable_cpt_1=stable_cpt_1,
  stable_cpt_2=stable_cpt_2,
  period=period,
  vitals_table=vitals_table,
  cpt_table=cpt_table

  )
}}

members_w_vitals as (

  select distinct patientId from all_vitals

),

combined_table as (

  select patientId from {{ vitals_table }}
  union distinct
  select memberIdentifier from {{ cpt_table }} where memberIdentifierField = 'patientId'

),

cf_status as (

  select
    patientId,
    case
      when patientId in (select * from members_w_vitals)
        then 'true'
      else 'false'
    end as value
  from combined_table

),

final as (

  {{ computed_field(slug=slug, type='bp_presence', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
