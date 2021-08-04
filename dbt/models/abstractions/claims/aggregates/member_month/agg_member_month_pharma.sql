{{
  config(
    materialized='table'
  )
}}
with pharma_claim_level as (
select memberIdentifier.partner, claimLineStatus,memberIdentifier.patientId,  drug.partnerPrescriptionNumber,drug.ndc,amount.allowed ,amount.planPaid as amountPaid  , date.filled , extract(month from date.filled) as service_month, extract(quarter from date.filled) as service_quarter ,
extract(year from date.filled) as service_year
from {{source('emblem','Pharmacy')}} 
where memberIdentifier.patientId is not null 
)
select 'emblem' as partner,mm.patientId,partnerPrescriptionNumber, ndc, sum(allowed) as amountAllowed, sum(amountPaid) as amountPlanPaid,filled as dateFilled, service_month, service_quarter, service_year 
from pharma_claim_level as pcl

right join {{ref('abs_member_month_spine')}}  as mm
        on pcl.patientId = mm.patientId
        and pcl.service_year = mm.year
        and pcl.service_month = mm.month
        
where mm.patientId is not null
group by partner, patientId,partnerPrescriptionNumber, ndc, dateFilled,service_month, service_quarter, service_year 
order by patientId,service_year,service_quarter,service_month






