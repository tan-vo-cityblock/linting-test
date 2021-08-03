
{% macro test_maximum_distinct_values(model, columns_to_group, column_to_count, max_values = 1, where = None) %}

  with validation_errors as (

    select {{ columns_to_group }}

    from {{ model }}

    {% if where %}

      where {{ where }}

    {% endif %}

    group by {{ columns_to_group }} 

    having count(distinct {{ column_to_count }}) > {{ max_values }}

  )

select count(*) from validation_errors

{% endmacro %}
