
{% macro finish_computed_field_query(

    slug,
    type,
    final_cte_name = 'members_w_included_conditions',
    member_table = var('cf_member_table'),
    member_table_type = 'var',
    partner_name = None,
    partner_field = "partnerName",
    partner_op = "="

  )
%}

cf_status as (

  select 
    patientId,

    case
      when patientId in (select * from {{ final_cte_name }})
        then 'true'
      else 'false'
    end as value

  {% if member_table_type == 'ref' %}

    from {{ ref(member_table) }}

  {% else %}

    from {{ member_table }}

  {% endif %}

  {% if partner_name %}

    where {{ partner_field }} {{ partner_op }} '{{ partner_name }}'

  {% endif %}

),

final as (

    {{ computed_field(slug=slug, type=type, value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
