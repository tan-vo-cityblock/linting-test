
{{
  config(
    materialized='ephemeral'
  )
}}

with base as (

  select *

  from {{ ref('stg_facility_costs_categories_base') }}

  where costCategory = 'snf'

),

categorized as (

    select distinct

        claimId,

        case
          when subAcuteServiceFlag = false then 'acute'
          when subAcuteServiceFlag = true then 'subAcute'
          else null
          end
        as costSubCategory,

        cast(null as string) as costSubCategoryDetail

    from base

)

select * from categorized
