
{% macro map_cdps_categories(diagnosis_column, index_columns, table_name,
                 version, group_by=False) %}

with base as (

    select
        {% for index_column in index_columns %}
        {{ index_column }},
        {% endfor %}
        {{ diagnosis_column }} as diagnosis

    from {{ table_name }}

),


mapping as (

    select * 

    from 
    {{ source('cdps', 'cdps_maps') }}

    where 
    version = '{{ version }}'

),


mapped as (

    select distinct
        {% for index_column in index_columns %}
        {{ index_column }},
        {% endfor %}
        cdps.category as cdpsCategory

    from 
    base

    left join 
    mapping as cdps
    ON base.diagnosis = cdps.dx
)

{% if group_by %}
,

grouped as (

{% set cdps_list = dbt_utils.get_column_values(table=source('cdps', 'cdps_maps'), column='category') %}

    select
        {% for index_column in index_columns %}
        {{ index_column }},
        {% endfor %}

        {% for cdps in cdps_list %}
        {% if cdps %}
        max(case when cdpsCategory = '{{ cdps }}' then 1 else 0 end) as {{ 'cdps' ~ cdps }} {% if not loop.last %},{% endif %}
        {% endif %}
        {% endfor %}

    from 
    mapped

    group by
      {% for index_column in index_columns %}
      {{ index_column }}{% if not loop.last %},{% endif %}
      {% endfor %}

)

select * from grouped

{% else %}

select * from mapped

{% endif %}

{% endmacro %}
