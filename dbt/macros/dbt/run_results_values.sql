
{# macro to format the results of a dbt run to insert into table #}

{% macro run_results_values(results) %}
  {% for res in results -%}
    {% if loop.index > 1 %},{% endif %}
    ('{{ res.node.alias }}', '{{ res.status }}', {{ res.execution_time }}, CURRENT_DATETIME())
  {% endfor %}
{% endmacro %}
