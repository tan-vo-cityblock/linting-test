-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--load hcc codeset
with hcc_codes_2020 as (

select distinct *

from
{{ source('codesets', 'hcc_2020') }} 

where
lower(cast(hcc_v24_2020 as string)) = 'yes'
),


--HCC description data
risk_hcc_descript as (

select distinct 
hcc.*

from 
{{ source('codesets', 'risk_hcc_descript') }} as hcc

where 
version = 'v24'
),


--provider NPI info
npidata as (

select *

from
{{ source('nppes','npidata_20190512')}}
),


--members with required RA related columns
member as (

select distinct
patientId , 
memberId as id,
partner,
lineOfBusiness,
firstname,
lastname,
dateOfBirth

from
{{ ref('hcc_risk_members') }}

where
eligYear = extract(Year from current_date())
and eligNow = true
and lineOfBusiness in( 'medicare','dsnp','duals')
),


--MOR files for DSNP members
NMI_MBI_Crosswalk as (

select *

from
{{ source('Settlement_CCI','NMI_MBI_Crosswalk')}}
),


--MOR files for DSNP members
mor_data as (

select list.*

from
{{ ref('mor_data')}} list

where
cast(paymentYear as numeric) < extract(year from current_Date())
),


dx_history as (

select *
from(
select distinct
evidenceDate,
clmYear,
claimType,
patientID,
partner,
lineOfBusiness,
dxCode,
claimType,
dxDescription,
hcc,
providerNpi,
HCCDescription,
memhcccombo,
dense_rank ()OVER (PARTITION BY patientId, hcc ORDER BY cast(evidenceDate as timestamp) desc, dxCode,providerName nulls last)  as ranked

from
{{ ref('risk_claims_patients') }} 

where 
EXTRACT(YEAR FROM  evidenceDate ) between EXTRACT(YEAR FROM  current_date() )  - 3 and EXTRACT(YEAR FROM  current_date() )
and hcc is not null
)
where ranked = 1
),


closed as (

select distinct
max(evidenceDate) as evdate,
concat(patientId, hcc) as category

from
dx_history

group by
concat(patientId, hcc)
),


final as (

select distinct 
mem.partner,
mem.patientId,
mem.lineOfBusiness,
'PERS' as conditionType, 
'mor_file' as conditionSource,
'HCC' as conditionCategory,
cast(r.hcc as string)  as conditionCategoryCode,  
hccdescr as conditionName,
case when (extract(year from evdate) = extract(year from current_date()) 
       or  extract(year from evdate) = extract(year from current_date() -3)) then 'CLOSED' 
       else 'OPEN'
          end as conditionStatus, 
d.dxcode as underlyingDxCode,
'HIGH' as confidenceLevel,
2 as confidenceNumeric,
dxCode as supportingEvidenceCode,
'HCC' as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue,
'present in MOR file' as supportingEvidenceName,
'claims' as supportingEvidenceSource,
current_date() as dateUploaded,
Provider_First_Name as providerFirstName, 
Provider_Last_Name_Legal_Name as providerLastNameLegalName,
count(distinct d.evidenceDate) as clmCount,
max(d.evidenceDate) as clmLatest
from
member mem

inner join
mor_data r
on mem.patientID = r.patientID

inner join
risk_hcc_descript descr
on r.hcc = descr.hcc

left join 
dx_history d
on r.hcc = d.hcc
and r.patientId = d.patientID

left join
closed
on d.memhcccombo = closed.category

left join 
npidata prov 
on cast(prov.npi as string) =  d.providerNpi 

group by
mem.patientId,
mem.partner,
mem.lineOfBusiness,
r.hcc,  
hccdescr,
d.dxcode,
conditionStatus,
Provider_First_Name, 
Provider_Last_Name_Legal_Name
)

select * from final
