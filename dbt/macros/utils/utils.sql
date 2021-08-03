

{% macro chain_comparator(operator, column, comparator, values, quoted=True) %}
  {% for value in values %}
    {% if loop.index > 1 %} {{ operator }} {% endif %}
      {% if quoted %}
        {{ column }} {{ comparator }} '{{ value }}'
      {% else %}
        {{ column }} {{ comparator }} {{ value }}
      {% endif %}
  {% endfor %}
{% endmacro %}

{% macro chain_or(column, comparator, values, quoted=True) %}
  {{ chain_comparator("or", column, comparator, values, quoted) }}
{% endmacro %}

{% macro chain_and(column, comparator, values) %}
  {{ chain_comparator("and", column, comparator, values) }}
{% endmacro %}

{% macro list_to_sql(ls, quoted=True) %}
  {% if quoted %}
  ( '{{ ls | join("', '") }}' )
  {% else %}
  ( {{ ls | join(", ") }} )
  {% endif %}

{% endmacro %}

{% macro chain_operator(columns, operator) %}

  {% for column in columns %}

    {{ column }} {% if not loop.last %} {{ operator }} {% endif %}

  {% endfor %}

{% endmacro %}
