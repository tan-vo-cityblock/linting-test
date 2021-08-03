{%- macro test_accepted_length(model, column_name, column_length) -%}

with validation_errors as (

  select count(*)
  from {{ model }}
  where length({{ column_name }}) != {{ column_length }}

)

select * from validation_errors

{% endmacro %}
