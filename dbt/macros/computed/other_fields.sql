
{% macro other_fields(
    slug,
    slug_value_list=[],
    else_value='stable',
    member_table=var('cf_member_table')
  ) 
%}

{% set cte_list = [] %}
{% set value_list = [] %}

with 

{% for pair in slug_value_list %}

  {% set slug = pair[0] | replace("-", "_") %}

  {% set _ = cte_list.append(slug) %}
  {% set _ = value_list.append(slug ~ '.value') %}

  {{ slug }} as (

    select patientId, '{{ pair[1] }}' as value
    from {{ slug_to_ref(pair[0])}} 
    where fieldValue = 'true'

  ),

{% endfor %}

cf_status as (

  select
    patientId,
    coalesce(

      {% for value in value_list %}

        {{ value ~ ',' }}

      {% endfor %}

      '{{ else_value }}'

    ) as value

  from {{ member_table }}

  {% for cte in cte_list %}

    left join {{ cte }}
    using (patientId)

  {% endfor %}

),

final as (

    {{ computed_field(slug=slug, type='other_fields', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}