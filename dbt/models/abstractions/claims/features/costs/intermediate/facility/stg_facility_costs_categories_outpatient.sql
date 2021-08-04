
{{
  config(
    materialized='ephemeral'
  )
}}


with base as (

    select *

    from {{ ref('stg_facility_costs_categories_base') }}

    where costCategory = 'outpatient'

),

cost_category as (

    select
        *,

        case
          when (outpatientHierarchy = 1 and outpatientLocationCategory != 'dialysis') then 'ed'
          when (outpatientHierarchy = 2 and dialysisServiceFlag = false) then 'surgery'
          when outpatientHierarchy = 3 then 'obs' 
          when (outpatientHierarchy = 4 or dialysisServiceFlag = true) then 'dialysis'

          when ((outpatientHierarchy = 5 or homeHealthServiceFlag = true) and
            typeOfBill not like '013%') then 'homeHealth'

          when outpatientHierarchy = 6 then 'behavioralHealth'
          when outpatientHierarchy = 7 then 'infusion'

          when (
            (outpatientHierarchy = 99 or
              (outpatientHierarchy = 5 and typeOfBill like '013%')) and
             homeHealthServiceFlag = false) then 'other'

          when outpatientHierarchy = 9 then 'radiology'
          when outpatientHierarchy = 10 then 'labs'
          when outpatientHierarchy = 11 then 'ambulance'
          when outpatientHierarchy = 12 then 'dme'
          when outpatientHierarchy = 13 then 'hospice'

          else null
          end
        as costSubCategory

    from base

),

cost_sub_category as (

  select

      *,

      case
        when (costCategory = 'ed' and obsServiceFlag = true) then 'obs'
        when (costCategory = 'ed' and obsServiceFlag = false) then 'ed'

        when (costCategory = 'surgery' and outpatientLocationCategory = 'hospital') then 'hospital'
        when (costCategory = 'surgery' and outpatientLocationCategory = 'asc') then 'asc'

        when (costCategory = 'dialysis' and outpatientLocationCategory = 'hospital') then 'hospital'
        when (costCategory = 'dialysis' and outpatientLocationCategory = 'snf') then 'snf'
        when (costCategory = 'dialysis' and outpatientLocationCategory = 'dialysis') then 'dialysis'

        when (costCategory = 'behavioralHealth' and substanceAbuseConditionFlag = true and methadoneServiceFlag = false) then 'substanceAbuse'
        when (costCategory = 'behavioralHealth' and psychConditionFlag = true) then 'psych'
        when (costCategory = 'behavioralHealth' and methadoneServiceFlag = true) then 'methadone'

        when (costCategory = 'infusion' and outpatientLocationCategory = 'hospital') then 'hospital'
        when (costCategory = 'infusion' and outpatientLocationCategory != 'hospital') then 'other'

        when (costCategory = 'other' and ptServiceFlag = true) then 'pt'
        when (costCategory = 'other' and clinicServiceFlag = true) then 'clinic'

        when (costCategory = 'ambulance' and emergentAmbulanceServiceFlag = true) then 'emergent'
        when (costCategory = 'ambulance' and emergentAmbulanceServiceFlag = false) then 'nonEmergent'

        when costSubCategory = 'homeHealth' and outpatientHomeHealthHierarchy = 1 then 'Infusions'
        when costSubCategory = 'homeHealth' and outpatientHomeHealthHierarchy = 2 then 'VNS'
        when costSubCategory = 'homeHealth' and outpatientHomeHealthHierarchy = 3 then 'PT-OT-ST'
        when costSubCategory = 'homeHealth' and outpatientHomeHealthHierarchy = 4 then 'PCA'
        when costSubCategory = 'homeHealth' and outpatientHomeHealthHierarchy = 5 then 'Other Services'

        else null
        end
      as costSubCategoryDetail

  from cost_category

),

final as (

    select distinct
        claimId,
        costSubCategory,
        costSubCategoryDetail

    from cost_sub_category

)

select * from final
