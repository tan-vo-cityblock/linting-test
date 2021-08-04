

{{
  config(
    materialized='table'
  )
}}


with data as (

    select distinct
      base.patientId,
      base.claimId,
      provider.providerCategory,
      dd.yearMonth,
      1 as visitCount

    from {{ ref('abs_professional_flat') }} as base

    left join {{ ref('ftr_professional_provider_categories') }} as provider
      on base.claimId = provider.claimId
        and base.lineId = provider.lineId

    left join {{ ref('date_details') }} as dd
      on base.dateFrom = dd.dateDay

    where base.claimLineStatus = 'Paid'

),

grouped as (

    select
        mm.patientId,
        mm.yearMonth,
        providerCategory,
        sum(ifnull(visitCount, 0)) as visitCount

    from data

    right join {{ ref('abs_member_month_spine') }} as mm
      on data.patientId = mm.patientId
        and data.yearMonth = mm.yearMonth

    group by patientId, yearMonth, providerCategory

)

select * from grouped
