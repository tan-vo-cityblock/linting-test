
{% macro test_absence(slug,
                      condition_slugs,
                      test_slugs,
                      all_conditions=False
                      )
%}

with all_members as (

  {% for slug in condition_slugs %}

    select patientId from {{ slug_to_ref(slug) }}

    {% if not loop.last %}

      union distinct

    {% endif %}

  {% endfor %}

),

  {{ find_members_w_field_value(condition_slugs, all_slugs=all_conditions) }}

  {{ find_members_w_field_value(test_slugs, final_cte_name="members_w_included_tests") }}

members_needing_tests as (

  select patientId from members_w_included_conditions
  except distinct
  select patientId from members_w_included_tests

),

cf_status as (

  select
    am.patientId,
    case
      when mnt.patientId is not null 
        then 'true'
      else 'false'
    end as value
  from all_members am
  left join members_needing_tests mnt
  using (patientId)

),

final as (

  {{ computed_field(slug=slug, type='test_absence', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
