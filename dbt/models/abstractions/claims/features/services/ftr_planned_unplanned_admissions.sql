
{{
  config(
    materialized='table'
  )
}}

    select
        base.patientId,
        base.claimId,
        stays.stayGroup,
        buckets.costCategory,
        buckets.costSubCategory,
        serviceFlags.edServiceFlag,

        case
          when edServiceFlag = true Then "Unplanned"
          else "Planned"
          end
        as PlanOrUnplan,

        min(base.dateAdmit) as dateAdmit

    from {{ ref('abs_facility_flat') }} as base

    left join {{ ref('ftr_facility_stays') }} as stays
      on base.claimId = stays.claimId

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId

    left join {{ref('ftr_facility_services_flags')}} as serviceFlags
       on base.claimId = serviceFlags.claimId

    where base.claimLineStatus = 'Paid'
          and costCategory = 'inpatient'
          and costSubCategory = 'acute'

    group by patientId, stayGroup, claimId, costCategory, costSubCategory,edServiceFlag
