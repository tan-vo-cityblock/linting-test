
{{
  config(
    materialized='ephemeral'
  )
}}

with base as (

    select
        claimId,
        fillNumber,
        brandIndicator

    from {{ ref('abs_pharmacy_flat') }}

),

subcats as (

    select
        base.claimId,
        base.fillNumber,
        'pharmacy' as costCategory,
        'pharmacy' as costSubCategory,
        brandIndicator as costSubCategoryDetail

    from base

)

select * from subcats
