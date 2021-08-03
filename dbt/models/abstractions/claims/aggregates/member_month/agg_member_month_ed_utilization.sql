
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      base.patientId,
     
      buckets.costCategory,
      buckets.costSubCategory,
      base.dateFrom

      

    from {{ ref('abs_facility_flat') }} as base

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId


    where base.claimLineStatus = 'Paid'

    group by patientId, costCategory, costSubCategory, dateFrom

),

grouped as (

    select
        mm.patientId,
        mm.yearMonth,
        mm.year,
        mm.month,
        mm.quarter,
        

        sum(
          case
            when base.costCategory ='outpatient' and costSubCategory ='ed'  then 1
            else 0
          end
        ) as edVisits
  {#      concat(costCategory,'_',costSubCategory) as concatenated #}

    from base

    left join {{ ref('date_details') }} as dd
      on base.dateFrom = dd.dateDay

    right join {{ ref('abs_member_month_spine') }} as mm
      on base.patientId = mm.patientId
        and dd.yearMonth = mm.yearMonth

    where costCategory is not null
    group by patientId, yearMonth, year,month, quarter
    

)



select patientId,year,month, quarter, edVisits from grouped
