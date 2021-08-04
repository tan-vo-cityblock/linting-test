
{{
  config(
    materialized='ephemeral'
  )
}}

with base as (

    select *

    from {{ ref('stg_facility_costs_categories_base') }}

    where costCategory = 'inpatient'

),

categorized as (

    select distinct

        claimId,

        case
          when inpatientHierarchy = 5 then 'acute'
          when inpatientHierarchy = 1 then 'maternity'
          when inpatientHierarchy = 3 then 'psych'
          when inpatientHierarchy = 4 then 'rehab'
          when inpatientHierarchy = 2 then 'hospice'
          when inpatientHierarchy = 6 then 'other'
          else null
          end
        as costSubCategory,

        case
          when (inpatientHierarchy = 5 and icuServiceFlag = true) then 'icu'
          when (inpatientHierarchy = 5 and icuServiceFlag = false) then 'acute'
          when (inpatientHierarchy = 1 and mdc = '14') then 'childbirth'
          when (inpatientHierarchy = 1 and mdc = '15') then 'newborn'
          when (inpatientHierarchy = 3 and substanceAbuseConditionFlag = true) then 'substanceAbuse'
          when (inpatientHierarchy = 3 and substanceAbuseConditionFlag = false) then 'psych'
          when (inpatientHierarchy = 4 and substanceAbuseConditionFlag = true) then 'substanceAbuse'
          when (inpatientHierarchy = 5 and substanceAbuseConditionFlag = false) then 'ptOt'
          when inpatientHierarchy in (2, 6) then 'other'
          else null
          end
        as costSubCategoryDetail

    from base

)

select * from categorized
