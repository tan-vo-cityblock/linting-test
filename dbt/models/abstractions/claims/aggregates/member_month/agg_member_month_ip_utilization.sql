
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
      edServiceFlag,

      min(base.dateAdmit) as dateAdmit

    from {{ ref('abs_facility_flat') }} as base

    left join {{ ref('ftr_facility_stays') }} as stays
      on base.claimId = stays.claimId

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId

    left join {{ref('ftr_facility_services_flags')}} as serviceFlags
       on base.claimId = serviceFlags.claimId


    where base.claimLineStatus = 'Paid'

    group by patientId, stayGroup, costCategory, costSubCategory,edServiceFlag

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
        base.edServiceFlag,

        sum(
          case
            when base.costCategory is not null then 1
            else 0
          end
        ) as admissionCount,
        concat(costCategory,'_',costSubCategory,'_',cast(edServiceFlag as string)) as concatenated

    from base

    left join {{ ref('date_details') }} as dd
      on base.dateAdmit = dd.dateDay

    right join {{ ref('abs_member_month_spine') }} as mm
      on base.patientId = mm.patientId
        and dd.yearMonth = mm.yearMonth

    where costCategory is not null
    group by patientId, yearMonth, year,month, quarter, costCategory, costSubCategory,edServiceFlag
    

)



{%- set column_values = ["inpatient_acute_true","inpatient_acute_false"] -%}
{%- set index_values = ["patientId","year", "month", "quarter"] -%}

select patientId,year,month, quarter, inpatient_acute_True as inpatientAcuteAdmissionsEmergent, 
       inpatient_acute_False as inpatientAcuteAdmissionsNonEmergent from
(
{{pivot_table(index_values,column= "concatenated",column_values  = column_values, value = "admissionCount", table = "grouped", aggfunc="sum", suffix= '')}}

)
