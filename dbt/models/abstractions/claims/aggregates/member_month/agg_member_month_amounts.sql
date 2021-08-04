{{
  config(
    materialized='table'
  )
}}
with fac_mm_level as (
select partner, patientId, sum(amountAllowed) as amountAllowed ,sum(amountPlanPaid) as amountPaid  , 
       extract(month from dateFrom) as service_month, 
        extract(quarter from dateFrom) as service_quarter , 
       extract(year from dateFrom) as service_year 
from {{ ref('abs_facility_flat')}}
where claimLineStatus ='Paid' and patientId is not null
group by partner, patientId, service_month, service_quarter, service_year
)

,prof_mm_level as (
select partner, patientId, sum(amountAllowed) as amountAllowed ,sum(amountPlanPaid) as amountPaid  , extract(month from dateFrom) as service_month, extract(quarter from dateFrom) as service_quarter , extract(year from dateFrom) as service_year 
from {{ ref('abs_professional_flat')}}
where claimLineStatus ='Paid' and patientId is not null
group by partner, patientId, service_month, service_quarter, service_year
)

,pharma_mm_level as (
select memberIdentifier.partner, memberIdentifier.patientId, sum(amount.allowed) as amountAllowed ,sum(amount.planPaid) as amountPaid   , extract(month from date.filled) as service_month, extract(quarter from date.filled) as service_quarter , extract(year from date.filled) as service_year 
from {{ source('emblem','Pharmacy')}} 
where claimLineStatus ='Paid' and memberIdentifier.patientId is not null
group by partner, patientId, service_month, service_quarter, service_year
)
,
mm_level as (
select a.*, b.amountAllowed as amountAllowedProfessional , c.amountAllowed as amountAllowedPharmacy ,
       b.amountPaid as amountPlanPaidProfessional , c.amountPaid as amountPlanPaidPharmacy

from fac_mm_level as a 
full join prof_mm_level as b 
on a.partner = b.partner and a.patientId = b.patientId and a.service_month = b.service_month and a.service_year = b.service_year
full join pharma_mm_level as c
on a.partner = c.partner and a.patientId = c.patientId and a.service_month = c.service_month and a.service_year = c.service_year

)



,grouped as (
select patientId, service_year,service_quarter, service_month, 
sum(amountAllowed) as amountAllowedFacility , sum(amountPaid) as amountPlanPaidFacility,
sum(amountAllowedProfessional) as amountAllowedProfessional , sum(amountPlanPaidProfessional) as amountPlanPaidProfessional,
sum(amountAllowedPharmacy) as amountAllowedPharmacy , sum(amountPlanPaidPharmacy) as amountPlanPaidPharmacy

from mm_level 
group by patientId,service_year,service_quarter,service_month
)

select patientId, service_year, service_quarter,service_month, 
       coalesce(amountAllowedFacility,0) as amountAllowedFacility , 
       coalesce(amountPlanPaidFacility,0) as amountPlanPaidFacility ,
       coalesce(amountAllowedProfessional,0) as amountAllowedProfessional , 
       coalesce(amountPlanPaidProfessional,0) as amountPlanPaidProfessional ,
        coalesce(amountAllowedPharmacy,0) as amountAllowedPharmacy , 
       coalesce(amountPlanPaidPharmacy,0) as amountPlanPaidPharmacy 

       from grouped 
       {#
      #  right join {{ ref('abs_member_month_spine')}} as mm
      #  on grouped.patientId = mm.patientId
      #  and grouped.service_year = mm.year
      #  and grouped.service_month = mm.month
#}
order by patientId,service_year,service_quarter,service_month






