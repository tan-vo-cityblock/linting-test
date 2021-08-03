
{% macro vitals_result(slug,
                       vitals_code,
                       min_vitals_value,
                       period="1 year",
                       vitals_table=ref('vitals'),
                       member_table=var('cf_member_table')
                       )%}

with members_w_elig_result as (

  select patientId
  from {{ ref('vitals') }}
  where
    code = '{{vitals_code}}' and
    date(timestamp) >= date_sub(current_date, interval {{ period }}) and
    value < 1000
  group by patientId
  having avg(value) >= {{ min_vitals_value }}

),

cf_status as (

  select
    patientId,
    case
      when patientId in (select * from members_w_elig_result)
        then 'true'
      else 'false'
    end as value
  from {{ member_table }}

),

final as (

  {{ computed_field(slug=slug, type='vitals_result', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
