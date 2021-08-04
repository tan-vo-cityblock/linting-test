
{% macro map_hcc_categories(diagnosis_column, index_columns, table_name,
          version='v24', group_by=None) %}

with base as (

    select
        {% for index_column in index_columns %}
        {{ index_column }},
        {% endfor %}
        {{ diagnosis_column }} as diagnosis

    from {{ table_name }}

),

mapped as (

    select distinct
        {% for index_column in index_columns %}
        {{ index_column }},
        {% endfor %}
        hcc.hcc_{{ version }} as hccCategory,
        hcc.hcc_{{ version }}_is_chronic as hccCategoryIsChronic,
        hcc.hcc_{{ version }}_disease_group as hccCategoryDiseaseGroup,
        hcc.hcc_{{ version }}_disease_group_is_chronic as hccCategoryDiseaseGroupIsChronic

    from base

    left join {{ source('codesets', 'hcc_2020') }} as hcc
      ON base.diagnosis = hcc.diagnosis_code

)

{% if group_by %}

,
grouped as (

{% set hcc_list = dbt_utils.get_column_values(table=source('codesets', 'hcc_2020'), column='hcc_v24') %}

    select
        {% for index_column in index_columns %}
        {{ index_column }},
        {% endfor %}

        {% for hcc in hcc_list %}
        {% if hcc %}
        max(case when hccCategory = '{{ hcc }}' then 1 else 0 end) as {{ 'hcc' ~ "%03d" % hcc }}{% if not loop.last %},{% endif %}
        {% endif %}
        {% endfor %}

    from mapped

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
