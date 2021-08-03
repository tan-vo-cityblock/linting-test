
{{
  config(
    materialized='table'
  )
}}

with base as (

  select distinct * from {{ ref('stg_pharmacy_costs_categories_base') }}

)

select * from base
