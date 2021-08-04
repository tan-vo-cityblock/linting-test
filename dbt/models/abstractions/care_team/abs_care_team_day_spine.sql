
{{
  config(
    materialized='table'
  )
}}

with base as (

    select distinct
        userId,
        dd.yearMonth,
        dd.year,
        dd.month,
        dd.quarter,
        dd.firstDayOfMonth,
        dd.lastDayOfMonth,
        dd.dateDay

    from {{ ref('user') }}

    cross join {{ ref('date_details') }} as dd

)

select * from base
