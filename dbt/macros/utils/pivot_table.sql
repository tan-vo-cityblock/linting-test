
{% macro pivot_table(index,
                     column,
                     column_values,
                     value,
                     table,
                     prefix='',
                     suffix='',
                     aggfunc='any_value') %}

SELECT
    {% for index_value in index %} {{index_value}} ,
    {% endfor %}
   
    {% for column_value in column_values %}

    {{aggfunc}}(if({{ column }} like "{{ column_value }}", {{ value }}, 0)) as {{ prefix ~ column_value ~ suffix}}{% if not loop.last %},{% endif %}
    {% endfor %}

    

FROM {{ table }}

GROUP BY {% for index_value in index %} {{index_value}} {% if not loop.last %},{% endif %}
{% endfor %}

{% endmacro %}
