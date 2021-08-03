
{{
  config(
    materialized='table'
  )
}}

with facility as (

    select
      base.patientId,
      base.claimId,
      base.amountAllowed,
      buckets.costCategory,
      buckets.costSubCategory,
      dd.yearMonth

    from {{ ref('abs_facility_flat') }} as base

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId

    left join {{ ref('date_details') }} as dd
      on base.dateAdmit = dd.dateDay

    where base.claimLineStatus = 'Paid'

),

professional as (

    select
      base.patientId,
      base.claimId,
      base.amountAllowed,
      buckets.costCategory,
      buckets.costSubCategory,
      dd.yearMonth

    from {{ ref('abs_professional_flat') }} as base

    left join {{ ref('ftr_professional_costs_categories') }} as buckets
      on base.claimId = buckets.claimId
        and base.lineId = buckets.lineId

    left join {{ ref('date_details') }} as dd
      on base.dateFrom = dd.dateDay

    where base.claimLineStatus = 'Paid'

),

data as (

    select * from facility

    union all

    select * from professional

),

grouped as (

    select
        mm.patientId,
        mm.yearMonth,
        costCategory,
        costSubCategory,
        sum(ifnull(amountAllowed, 0)) as amountAllowed

    from data

    right join {{ ref('abs_member_month_spine') }} as mm
      on data.patientId = mm.patientId
        and data.yearMonth = mm.yearMonth

    group by patientId, yearMonth, costCategory, costSubCategory

)

select * from grouped
