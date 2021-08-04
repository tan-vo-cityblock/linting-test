{%- macro member_list (slug,
      source_name,
      source_table,
      condition_field = None,
      condition_value = None,
      member_table = var('cf_member_table')
) -%}

{% set source_relation = source(source_name, source_table) %}

with listed_members as (

  select
  patientId

  from {{ source_relation }}

  {% if condition_field and condition_value %}

  where {{ condition_field }} = '{{ condition_value }}'

  {% endif %}

  group by patientId

),

cf_status as (

  select
    patientId,

    case
      when patientId in (select * from listed_members)
        then 'true'
      else 'false'
    end as value

  from {{ member_table }}

),

final as (

    {{ computed_field(slug=slug, type='member_list', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
