

{{
  config(
    materialized='table'
  )
}}


with data as (
    select
      base.patientId,
      base.claimId,
      service.serviceCategory,
      service.serviceSubCategory,
      dd.yearMonth

    from {{ ref('abs_professional_flat') }} as base

    left join {{ ref('ftr_professional_services_categories') }} as service
      on base.claimId = service.claimId
        and base.lineId = service.lineId

    left join {{ ref('date_details') }} as dd
      on base.dateFrom = dd.dateDay

    where base.claimLineStatus = 'Paid'

),

grouped as (

    select
        mm.patientId,
        mm.yearMonth,
        data.serviceCategory,
        data.serviceSubCategory,

        sum(
          case
            when data.claimId is not null then 1
            else 0
          end
        ) as serviceCount

    from data

    right join {{ ref('abs_member_month_spine') }} as mm
      on data.patientId = mm.patientId
        and data.yearMonth = mm.yearMonth

    group by patientId, yearMonth, serviceCategory, serviceSubCategory

)

select * from grouped
