
{% macro map_to_struct(mapping) %}
{# Given a map of keys and values create a struct statement #}
  STRUCT(
    {% for key, value in mapping.items() -%}
    {{ value }} as {{ key }}{% if not loop.last %},{% endif %}
    {% endfor -%}
  )

{%- endmacro %}

{% macro list_to_struct_array(obj) %}
{# Given a list of maps that contain of keys and values create a array of
struct statements #}
    [
      {% for item in obj %}
         {{ map_to_struct(item) }}{% if not loop.last %},{% endif %}
      {% endfor -%}
    ]

{%- endmacro %}

{% macro obj_to_struct(obj) %}
{# Given an arbitrary object route to the correct constructor #}
    {% if obj is mapping %}
        {{ map_to_struct(obj) }}
    {% else %}
        {{ list_to_struct_array(obj) }}
    {% endif %}

{%- endmacro %}
