
{{
  config(
    materialized='table'
  )
}}


with base as (

    select distinct
        claimId,
        costCategory

    from {{ ref('stg_facility_costs_categories_base') }}

),

categories as (

    select * from {{ ref('stg_facility_costs_categories_inpatient') }}

    union all

    select * from {{ ref('stg_facility_costs_categories_outpatient') }}

    union all

    select * from {{ ref('stg_facility_costs_categories_snf') }}

),

final as (

    select
        base.claimId,
        base.costCategory,
        categories.costSubCategory,
        categories.costSubCategoryDetail

    from base

    left join categories
      on base.claimId = categories.claimId

)

select * from final
