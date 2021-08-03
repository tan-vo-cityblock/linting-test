{%- macro member_evidence(slug,
      min_age=None,
      max_age=None,
      member_field=None,
      member_values=None,
      member_info_table='member_info',
      member_table=var('cf_member_table')
  ) -%}

with member_info as (

    select
    patientId,

    {% if member_info_table == 'member_info' %}
    id,

    {% else %}
    string(null) as id,
    {% endif %}

    'id' as key,
    '{{ member_info_table }}' as model,

    {% if min_age or max_age %}
    cast(dateOfBirth as string) as code,

    {% else %}
    {{ member_field }} as code,

    {% endif %}

    current_date as validDate

    from {{ ref(member_info_table) }}

    {% if min_age %}
    where dateOfBirth <= date_sub(current_date, interval {{ min_age ~ ' year' }} )

    {% elif max_age %}
    where dateOfBirth >= date_sub(current_date, interval {{ max_age ~ ' year' }} )

    {% else %}
    where {{ member_field }} in {{ list_to_sql(member_values) }}

    {% endif %}

),

aggregated_resources as (

  {{ aggregate_computed_field_resources(table='member_info') }}

),

cf_status as (

  {{ derive_computed_field_values(table='aggregated_resources') }}

),

final as (

  {{ computed_field(slug=slug, type='member_evidence', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select *
from final

{%- endmacro -%}
