
{{
  config(
    materialized='view',
    tags='payer_list'
  )
}}

{%- set payer_list = var('payer_list') -%}

with unioned as (

    {% for source_name in payer_list %}

    select * from {{ source( source_name, 'Facility') }}

    {% if not loop.last -%}union all {%- endif %}
    {% endfor %}

)

select * from unioned
