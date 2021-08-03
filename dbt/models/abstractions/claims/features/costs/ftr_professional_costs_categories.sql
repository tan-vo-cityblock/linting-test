
{% set hierarchy_column='professionalLineGroupHierarchy' %}
{% set line_hierarchy_column='professionalLineHierarchy' %}

with base as (

  select * from {{ ref('stg_professional_costs_categories_base') }}

),

subcats as (

    select
        *,

        case
          when placeOfService = '21'
            and (
              (duringInpatientMaternityFlag = true)
              or
              (onInpatientMaternityStartOrEndFlag = true
                and maternityConditionFlag = true)
              ) then 'inpatientMaternity'

          when placeOfService = '21'
            and (
              (duringInpatientPsychFlag = true)
              or
              (onInpatientPsychStartOrEndFlag = true
                and psychConditionFlag = true)
              ) then 'inpatientPsych'

          when placeOfService = '51' then 'inpatientPsych'
          when placeOfService = '21' then 'inpatient'
          when placeOfService = '61' then 'rehab'
          when placeOfService in ("31", "32", "33", "13") then 'snf'
          when placeOfService = '23' then 'ed'
          when placeOfService = '41' then 'ambulance'
          when placeOfService = '81' then 'labs'
          when placeOfService = '65' then 'dialysis'
          when placeOfService = '53' then 'behavioralHealth'
          else null
          end
        as placeOfcostCategory,

        case
          when {{ hierarchy_column }} = 1 then 'ed'
          when {{ hierarchy_column }} = 2 then 'surgery'
          when {{ hierarchy_column }} = 3 then 'obs'
          when {{ hierarchy_column }} = 4 then 'dialysis'
          when {{ hierarchy_column }} = 5 then 'homeHealth'
          when {{ hierarchy_column }} = 6 then 'behavioralHealth'
          when {{ hierarchy_column }} = 7 then 'infusion'
          when {{ hierarchy_column }} = 8 and placeOfService = '12' then 'homeHealth'
          when {{ hierarchy_column }} = 8 then 'other'
          when {{ hierarchy_column }} = 9 then 'radiology'
          when {{ hierarchy_column }} = 10 then 'labs'
          when {{ hierarchy_column }} = 11 then 'ambulance'
          when {{ hierarchy_column }} = 12 then 'dme'
          when {{ hierarchy_column }} = 13 then 'hospice'
          else null
          end
        as hierarchyCategory,

        case
          when {{ line_hierarchy_column }} = 1 then 'ed'
          when {{ line_hierarchy_column }} = 2 then 'surgery'
          when {{ line_hierarchy_column }} = 3 then 'obs'
          when {{ line_hierarchy_column }} = 4 then 'dialysis'
          when {{ line_hierarchy_column }} = 5 then 'homeHealth'
          when {{ line_hierarchy_column }} = 6 then 'behavioralHealth'
          when {{ line_hierarchy_column }} = 7 then 'infusion'
          when {{ line_hierarchy_column }} = 8 and placeOfService = '12' then 'homeHealth'
          when {{ line_hierarchy_column }} = 8 then 'other'
          when {{ line_hierarchy_column }} = 9 then 'radiology'
          when {{ line_hierarchy_column }} = 10 then 'labs'
          when {{ line_hierarchy_column }} = 11 then 'ambulance'
          when {{ line_hierarchy_column }} = 12 then 'dme'
          when {{ line_hierarchy_column }} = 13 then 'hospice'
          else null
          end
        as lineHierarchyCategory,

        case
          when providerServicingSpecialty in ("01", "08", "11", "37", "38", "84") then 'primaryCare'
          when {{ hierarchy_column }} = 8 and providerServicingSpecialty is not null then 'specialtyCare'
          when {{ hierarchy_column }} = 8 and providerServicingSpecialty is null then 'other'
          else null
          end
        as providerSpecialtyCategory

    from base

),

service as (

    select
        *,

        case
          when placeOfcostCategory is not null then placeOfcostCategory
          when (placeOfService = '11' and {{ hierarchy_column }} = 8)
            and providerSpecialtyCategory is not null then providerSpecialtyCategory
          when hierarchyCategory is not null then hierarchyCategory
          else null
          end
        as costSubCategory,

        case
          when placeOfcostCategory is not null then placeOfcostCategory
          when (placeOfService = '11' and {{ line_hierarchy_column }} = 8)
            and providerSpecialtyCategory is not null then providerSpecialtyCategory
          when hierarchyCategory is not null then hierarchyCategory
          else null
          end
        as costSubCategoryLine

    from subcats

),

subservice as (

    select
        claimId,
        lineId,
        costCategory,
        costCategory as costCategoryLine,
        costSubCategory,

        case
          when (costSubCategory = 'ambulance' and emergentAmbulanceServiceFlag = true) then 'emergent'
          when (costSubCategory = 'ambulance' and emergentAmbulanceServiceFlag = false) then 'nonEmergent'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 1 then 'Infusions'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 2 then 'VNS'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 3 then 'PT-OT-ST'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 4 then 'PCA'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 5 then 'Other Services'
          when costCategory is not null then costCategory
          else null
          end
        as costSubCategoryDetail,

        costSubCategoryLine,

        case
          when (costSubCategoryLine = 'ambulance' and emergentAmbulanceServiceFlag = true) then 'emergent'
          when (costSubCategoryLine = 'ambulance' and emergentAmbulanceServiceFlag = false) then 'nonEmergent'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 1 then 'Infusions'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 2 then 'VNS'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 3 then 'PT-OT-ST'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 4 then 'PCA'
          when costSubCategory = 'homeHealth' and professionalHomeHealthHierarchy = 5 then 'Other Services'          when costCategory is not null then costCategory
          else null
          end
        as costSubCategoryDetailLine,

    from service

)

select * from subservice
