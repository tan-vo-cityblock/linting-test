
-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--load member list
with diagnoses as (

select distinct * 
from
{{ ref('risk_claims_patients') }} 
),


-------------------------------------
--- Section Two: Dx History ---
-------------------------------------


dx_claims as (

select distinct 
evidenceDate,
EXTRACT(YEAR FROM evidenceDate) as clmYear,
patientID,
partner,
lineOfBusiness,
dxCode,
claimType,
dxDescription,
hcc,
HCCDescription,
memhcccombo

from
diagnoses

where 
EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date())
)


select * from dx_claims
