
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      base.patientId,
      stays.stayGroup,
      buckets.costCategory,
      buckets.costSubCategory,


      min(base.dateFrom) as dateFrom

    from {{ ref('abs_facility_flat') }} as base

    left join {{ ref('ftr_facility_stays') }} as stays
      on base.claimId = stays.claimId

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId

    where base.claimLineStatus = 'Paid'

    group by patientId, stayGroup, costCategory, costSubCategory

),

grouped as (

    select
        mm.patientId,
        mm.yearMonth,
        mm.year,
        mm.month,
        mm.quarter,
        base.costCategory,
        base.costSubCategory,

        sum(
          case
            when base.costCategory is not null then 1
            else 0
          end
        ) as admissionCount


    from base

    left join {{ ref('date_details') }} as dd
      on base.dateFrom = dd.dateDay

    right join {{ ref('abs_member_month_spine') }} as mm
      on base.patientId = mm.patientId
        and dd.yearMonth = mm.yearMonth

    group by patientId, yearMonth, year,month, quarter, costCategory, costSubCategory
)

select * from grouped
