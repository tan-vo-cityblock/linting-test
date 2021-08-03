
{% macro test_groups_relative_rank(model, groupColName, rankColName, upperGroup, lowerGroup) %}

  with both_groups as (

    select *
    from {{ model }}
    where {{ groupColName }} in ('{{ upperGroup }}', '{{ lowerGroup }}')
      
  ),

  errors as (

    select 1
    from both_groups g1
    inner join both_groups g2
    on 
      g1.{{ groupColName }} = '{{ upperGroup }}' and 
      g2.{{ groupColName }} = '{{ lowerGroup }}' and 
      g1.{{ rankColName }} >= g2.{{ rankColName }}

  )

  select count(*) from errors

{% endmacro %}
