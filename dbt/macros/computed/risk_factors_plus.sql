
{% macro risk_factors_plus (
    slug,
    risk_factor_slug,
    additional_required_slug=None,
    min_risk_factors=1,
    member_table=var('cf_member_table')
  )
%}

with members_w_risk_factors as (

  select patientId
  from {{ slug_to_ref(risk_factor_slug) }}
  where cast(fieldValue as int64) >= {{min_risk_factors}}

),

{% if additional_required_slug %}

  members_w_additional_requirement as (

    select patientId
    from {{ slug_to_ref(additional_required_slug) }}
    where fieldValue = 'true'

  ),

{% endif %}

members_meeting_criteria as (

  select patientId
  from members_w_risk_factors

  {% if additional_required_slug %}

    inner join members_w_additional_requirement
    using (patientId)

  {% endif %}

),

cf_status as (

  select 
    patientId,

    case
      when patientId in (select * from members_meeting_criteria)
        then 'true'
      else 'false'
    end as value

  from {{ member_table }}

),

final as (

    {{ computed_field(slug=slug, type='risk_factors_plus', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
