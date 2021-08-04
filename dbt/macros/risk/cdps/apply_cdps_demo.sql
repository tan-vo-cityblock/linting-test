
{% macro apply_cdps_demo(table_name, model, model_type, population, score_type, version) %}


with demo as (

SELECT  
partner,
param as demo_category,
case when {{ score_type }}  like '%-%' then safe_cast(replace({{ score_type }} ,"-","") as numeric)*-1
             when {{ score_type }}  not like '%-%' then cast({{ score_type }}  as numeric)  end as demoScore

FROM 
{{ source('cdps', 'cdps_weights') }}

where 
param like '%a_%'
and model = '{{ model }}'
and type = '{{ model_type }}'
and population = '{{ population }}'
and version = '{{ version }}'
)


--# this multiplying the exists of the cdps factor with the weight and sums the score


select * from demo

{% endmacro %}
