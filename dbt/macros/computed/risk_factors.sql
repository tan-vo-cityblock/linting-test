
{% macro risk_factors(
    slug,
    slug_list=[],
    member_table=var('cf_member_table')
  ) 
%}

with members_w_risk_fields as (

  {% for slug in slug_list %}

    select patientId, fieldSlug
    from {{ slug_to_ref(slug) }}
    where fieldValue = 'true'

      {% if not loop.last %}

        union distinct

      {% endif %}

  {% endfor %}

),

num_risk_fields as (

  select patientId, cast(count(*) as string) as numRiskFields
  from members_w_risk_fields
  group by patientId

),

cf_status as (

  select 
    m.patientId,
    coalesce(nrf.numRiskFields, "0") as value

  from {{ member_table }} m

  left join num_risk_fields nrf
    using (patientId)

),

final as (

    {{ computed_field(slug=slug, type='risk_factors', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
