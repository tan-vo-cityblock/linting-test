select  stayGroup, 
max(case when icdCode is null then false else true end) as surgical
from {{ref('ftr_facility_stays')}} left join {{ref('abs_facility_flat')}} using(claimId) 
left join {{source('claims_mappings','icdPcsSurg')}} on icdCode = principalProcedureCode

group by 1
