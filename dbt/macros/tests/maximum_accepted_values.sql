{% macro test_maximum_accepted_values(model, column_to_count, max_values) %}

with value_count as (

  select count(distinct {{ column_to_count }}) as valueCount
  from {{ model }}

),

validation_errors as (

  select count(*)
  from value_count
  where valueCount > {{ max_values }}

)

select * from validation_errors

{% endmacro %}
