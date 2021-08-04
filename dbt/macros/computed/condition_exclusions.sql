
{% macro condition_exclusions (

  slug,
  condition_slugs,
  excluded_condition_slugs,
  all_conditions_required=True,
  member_table=var('cf_member_table')

  )
%}

with

{{ find_members_w_field_value(condition_slugs, all_slugs=all_conditions_required) }}

{{ find_members_w_field_value(excluded_condition_slugs, final_cte_name="members_w_excluded_conditions") }}

filtered_members as (

  select patientId from members_w_included_conditions
  except distinct
  select patientId from members_w_excluded_conditions

),

cf_status as (

  select
    patientId,

    case
      when patientId in (select * from filtered_members)
        then "true"
      else "false"
    end as value

  from {{ member_table }}

  group by patientId

  ),

final as (

    {{ computed_field(slug=slug, type='condition_exclusions', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
