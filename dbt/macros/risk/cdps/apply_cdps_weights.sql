
{% macro apply_cdps_weights(table_name, model, model_type, population, score_type, version) %}

with base as (

    select *

    from {{ table_name }}

),

--# get the set of weights based on versin, population and model
weights as (

    select
        partner,
        'cdps' as model,
        param as cdpsCategory,
        case when {{ score_type }} like '%-%' then safe_cast(replace({{ score_type }},"-","") as numeric)*-1
             when {{ score_type }} not like '%-%' then cast({{ score_type }} as numeric)  end as weight

    from {{ source('cdps', 'cdps_weights') }}

    where model = '{{ model }}'
      and type = '{{ model_type }}'
      and population = '{{ population }}'
      and version = '{{ version }}'

),

--# to pivot the cdps we need to get all of the possible values from the table
{% set cdps_list = dbt_utils.get_column_values(table=source('cdps', 'cdps_maps'), column='category') %}

pivot_weights as (


    {{ pivot_table(index=['model','partner'],
                   column='cdpsCategory',
                   column_values=cdps_list,
                   value='weight',
                   table='weights',
                   prefix='weight',
                   aggfunc='sum'

    )}}

),

--# the step takes the pivoted weights and merges them onto the base dataset
apply as (

    select
        *,

        {% for cdps in cdps_list %}
        {% if cdps %}
        {{ 'cdps' ~ cdps }} * {{ 'weight' ~ cdps }} as {{ 'score' ~ cdps }} {% if not loop.last %},{% endif %}
        {% endif %}
        {% endfor %}

    from base

    cross join pivot_weights
where lower(base.partnerName) = lower(pivot_weights.partner)
),

# this multiplying the exists of the cdps factor with the weight and sums the score
summed as (

    select
        *,

        (
          {% for cdps in cdps_list %}
          {% if cdps %}
          IFNULL({{ 'score' ~ cdps }}, 0) {% if not loop.last %}+{% endif %}
          {% endif %}
          {% endfor %}
        ) as cdpsScore

      from apply
)

select * from summed

{% endmacro %}
