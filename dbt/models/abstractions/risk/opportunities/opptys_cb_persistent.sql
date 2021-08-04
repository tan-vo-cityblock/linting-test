
-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--member, payer, and lob info
with member as (

select distinct
patientId, 
memberId,
partner,
lineOfBusiness,
eligYear,
virtualProgram,
cohortName,
currentState,
consentedEnrolled

from
{{ ref('hcc_risk_members') }}

where 
eligYear = extract(Year from current_date())
and eligNow = true
and lineOfBusiness in( 'medicare','dsnp','duals')
),


--NPI Data
npidata as (

select distinct 
Provider_First_Name as providerFirstName, 
Provider_Last_Name_Legal_Name as providerLastNameLegalName,
cast(npi as string) as npi

from
{{ source('nppes','npidata_20190512')}}
),


--dx info all sources
abs_diagnoses as (

select distinct 
patientID,
clmYear,
evidenceDate,
encounterType,
dxCode,
claimType,
dxDescription,
providerNPI,
providerName,
hcc,
HCCDescription,
memhcccombo,
HCCLookbackPeriod,
hccRollup

from 
{{ ref('risk_claims_patients') }} 

where
hcc is not null
and excludeICD is null
and excludeAcuteHCC is null
and clmyear between extract(Year from current_date() ) -3 and extract(Year from current_date() )
),



--ICD Codes with HCCs
ICDCodesProvDate as (

select * from
(select distinct
patientId,
hcc,
dxCode as underlyingDxCode,
dxDescription,
providerNPI,
providerFirstName, 
providerLastNameLegalName,
(EXTRACT(Month from TIMESTAMP(FORMAT_TIMESTAMP('%F %H:%M:%E*S', cast(evidenceDate as timestamp) , 'America/New_York'))) ) as FirstcodedMonth,
(TIMESTAMP(FORMAT_TIMESTAMP('%F %H:%M:%E*S', cast(evidenceDate as timestamp) , 'America/New_York')))  as FirstcodedDate,
dense_rank ()OVER (PARTITION BY patientId, hcc ORDER BY cast(evidenceDate as timestamp) desc, dxCode,providerLastNameLegalName nulls last,
providerFirstName) as ranked

from
abs_diagnoses

left join 
npidata
on providerNPI = npi

)
where ranked = 1
),


HCCCodeCount as (

select distinct
patientId,
hcc,
count(distinct evidenceDate) as codedCount

from
abs_diagnoses

left join 
npidata
on
providerNPI = npi

group by
patientId,
hcc
),


hcc_claim_hx as (

select distinct
mem.eligYear,
mem.virtualprogram,
mem.partner,
mem.cohortName,
mem.patientId,
mem.currentState,
mem.consentedEnrolled ,
mem.lineOfBusiness,
pat.clmyear,
pat.hcc,
hccRollup,
HCCLookbackPeriod,
HCCDescription as hccdescr,
memhcccombo as patienthcc,
max(EXTRACT(Month from TIMESTAMP(FORMAT_TIMESTAMP('%F %H:%M:%E*S', cast(evidenceDate as timestamp) , 'America/New_York'))) ) as FirstcodedMonth,
max(TIMESTAMP(FORMAT_TIMESTAMP('%F %H:%M:%E*S', cast(evidenceDate as timestamp) , 'America/New_York')) ) as FirstcodedDate

from
abs_diagnoses pat

inner join
member mem
on mem.patientId = pat.patientId

GROUP BY
mem.eligYear,
mem.virtualprogram,
mem.partner,
mem.cohortName,
mem.patientId,
mem.currentState,
mem.consentedEnrolled,
mem.lineOfBusiness,
pat.clmyear,
pat.hcc,
hccRollup,
HCCLookbackPeriod,
HCCDescription,
memhcccombo
),



hcc_icd_claim_index as (

select distinct
extract(Year from current_date() ) as yearOfRecap,
virtualprogram,
partner as partner_index,
cohortName as cohort_index,
patientId as patientId_index ,
currentState  as patientState_index,
consentedEnrolled as Consented_enrolled_index ,
lineOfBusiness as lob_index,
hcc as hcc_index,
hccRollup as rollup_index,
hccdescr as conditionName,
patienthcc as patient_hcc_index

from
hcc_claim_hx

where  
clmyear between  extract(Year from current_date() ) -3 and extract(Year from current_date() ) -1
and (extract(year from current_date())  - clmYear  <= cast(HCCLookbackPeriod as numeric) ) 
)
,


-------2019 recap hccs-----
hcc_icd_claim_recap as (

select distinct
cast(clmyear as string)  as yearOfRecap,
virtualprogram as virtualprogramRecap,
partner as partner_recap,
cohortName as cohort_recap,
patientId as patientId_recap,
currentState  as patientState_recap,
consentedEnrolled as Consented_enrolled_recap,
lineOfBusiness as lob_recap,
hcc as hcc_recap,
hccRollup as rollup_recap,
hccdescr as hcc_descrip_recap,
patienthcc as patient_hcc_recap
from
hcc_claim_hx

where 
clmyear = extract(Year from current_date())
),


recap as (

select distinct
patientId_index as patientID,
partner_index as partner,
lob_index as lineOfBusiness,
hcc_index as conditionCategoryCode,
rollup_index,
conditionName,
'cityblock' as conditionsource,
'PERS' as conditionType,
case when hccs.patient_hcc_recap is null then 'OPEN' else 'CLOSED' end as conditionStatus,
"HCC" conditionCategory,
current_date() as dateUploaded

from 
hcc_icd_claim_index

inner join
hcc_icd_claim_recap person
on hcc_icd_claim_index.patientId_index = person.patientId_recap

left join
hcc_icd_claim_recap hccs
--on hcc_icd_claim_index.patient_hcc_index = hccs.patient_hcc_recap
on hcc_icd_claim_index.hcc_index >= hccs.hcc_recap
and hcc_icd_claim_index.rollup_index = hccs.rollup_recap
and hcc_icd_claim_index.patientId_index = hccs.patientId_recap
),


in_claims as (

select distinct 
r.partner,
r.patientId,
r.lineOfBusiness,  
r.conditionType,  
r.conditionSource,
r.conditionCategory,
r.conditionCategoryCode,  
rollup_index,
r.conditionName,
conditionStatus, 
provdate.underlyingDxCode,
case when d.codedCount >=50 then "VERY HIGH"
     when d.codedCount >= 4 and d.codedCount < 50 then "HIGH"
     when d.codedCount = 3 then "MEDIUM"
     when d.codedCount = 2 then "LOW"
     when d.codedCount = 1 then "VERY LOW"
		else "MEDIUM"
		end as confidenceLevel,
case when d.codedCount >=50 then 1
     when d.codedCount >= 4 and d.codedCount < 50 then 2
     when d.codedCount = 3 then 3
     when d.codedCount = 2 then 4
     when d.codedCount = 1 then 5
		else 3
		end as confidenceNumeric,
provdate.underlyingDxCode as supportingEvidenceCode, 
"ICD" as supportingEvidenceCodeType,
'' as supportingEvidenceValue,
dxDescription as supportingEvidenceName,
'claims' as supportingEvidenceSource,
r.dateUploaded,
provdate.providerFirstName, 
provdate.providerLastNameLegalName,
d.codedCount as clmCount,
cast(provdate.FirstcodedDate as date) as clmLatest

from 
recap r 

inner join 
HCCCodeCount d
on r.conditionCategoryCode  = d.hcc
and r.patientId = d.patientID

left join
ICDCodesProvDate provdate
on r.conditionCategoryCode = provdate.hcc
and r.patientId = provdate.patientID
),


final as (
select * except (rollup_index, ranked)
from
(
select distinct
*,
dense_rank ()OVER (PARTITION BY patientId, rollup_index ORDER BY conditionCategoryCode) as ranked
from
in_claims
)
where ranked = 1
)


select * from final
