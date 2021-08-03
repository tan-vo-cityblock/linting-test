{% macro select_latest(table, time_column) %}

select *
from {{ table }}
where {{ time_column }} = (select max({{ time_column }}) from {{ table }})

{% endmacro %}