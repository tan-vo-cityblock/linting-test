
----------------------------
--- Section One: Reference Tables ---
-------------------------------------


--load current hcc codeset
with hcc_codes as (

select distinct 
year,
DiagnosisCode as dxCode,
cast(CMSHCCModelCategoryV24or23 as string) as hcc

from
{{ source('codesets', 'ICD10toHCCcrosswalk') }} 

where
lower(cast(CMSHCCModelCategoryV24forCurrentYear as string)) = 'yes'
),


--ICD Descriptions
ICDdescript as (

select distinct

Code as diagCode,
Description as dxDescription

from
{{ source('codesets', 'cms_icd10_descriptions') }}
),


--load current hcc descriptions
risk_hcc_descript as (

select distinct
cast( hcc as string)  as hcc,
hccdescr as HCCDescription

from
{{ source('codesets', 'risk_hcc_descript') }} 

where
version = 'v24'
),


specialties as (

select distinct * 

from
{{ source('codesets', 'RASpecialtiesAcceptable') }} 
),


--load snomed mapping (clinical team created/maintains)
snomed_to_icd10 as (

select distinct * 

from 
{{ source('code_maps', 'snomed_to_icd10') }}
),


--load member list
member as (

select distinct
patientId as id, 
memberId,
eligYear,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
isHARP = false
),


--valid claims
valid as (

select distinct
claimId, 
procCode,  
hccEligibleCpt,  
billType,
hccEligibleBilltype,
paidDate,
validClaim,
paidStatus

from
{{ ref('risk_valid_claims') }}

where
validClaim = true
),


--load diagnosis list
abs_diagnoses as (

select distinct 
dx.* except( diagnosisTier,  claimLineStatuses,  placesOfService) 

from 
{{ ref('abs_diagnoses') }} dx

where 
memberIdentifierField = 'patientId'
and extract(year from serviceDateFrom) between extract(year from current_date()) - 5 and extract(year from current_date()) 
),


--acute hccs for removal
acute_hccs as (

select distinct 
hcc_v24 as excludeHCC,
excludeAcuteHCC,
HCCLookbackPeriod

from 
{{ source('codesets', 'hcc_lookback_periods') }}
),


--acute icds for exclusion
acute_icds as (

select distinct 
dagnosisCode as excludeICD,
excludeICD10,
ICD10Replacement

from 
{{ source('codesets', 'hcc_acute_exclusion') }}
),


--rolls up hccs to categories
hcc_rollup_2020 as (

select distinct 
hcc,
hcc_rollup

from 
{{ source('codesets', 'hcc_rollup_2020') }}
),


hccs as (

select distinct 
hcc_codes.Year,
hcc_codes.dxCode,
hcc_codes.hcc,
risk_hcc_descript.HCCDescription

from
hcc_codes

left join
risk_hcc_descript
on  hcc_codes.hcc = risk_hcc_descript.hcc
),


--from abs_diagnosis
clm as 
(select distinct
   diag.*,
   case when diagnosisCodeset = 'snomedct' then replace(replace(mapTarget,'?',''),'.','')
   else replace(replace(diagnosisCode,'?',''),'.','') end as diagnosisCode2

   from
   abs_diagnoses diag

   left join
   snomed_to_icd10 sno
   on cast(diag.diagnosisCode as string) = cast(sno.referencedComponentId as string)

   where
   memberIdentifierField = 'patientId'
   and
   (
    (lower(sourceType) like '%claim%' and source in ('facility','professional'))
  or
    (lower(encounterType) in (
       'office visit',
       'initial consult',
       'follow-up',
       'home care visit',
       'house call visit note',
       'office visit note',
       'telehealth',
       'procedure visit',
       'telemedicine',
       'telemedicine note',
       'visit note')
        and lower(source) like '%ccd%'
        and partnerName in ('acpny', 'elation'))
  or 
    (lower(source) = "hie")
  )         
  ),
-------------------------------------
--- Section Three: HCC/Dx History ---
-------------------------------------


final as (

select distinct
memberIdentifier as patientID,
lineOfBusiness, 
partner,
extract(year from serviceDateFrom) as clmYear,
serviceDateFrom as evidenceDate,
encounterType,
replace(clm.diagnosisCode2,'.','')  as dxCode,
source as claimType,
dxDescription,
hccs.hcc,
HCCDescription,
providerName,
providerNpi,
cityblockProvider,
cityblockProviderType,
cityblockProviderActiveInMarket,
concat(memberIdentifier,hccs.hcc) as memhcccombo,
HCCLookbackPeriod,
case when excludeICD is not null then true 
        end as excludeICD,
case when excludeAcuteHCC is not null then true 
        end as excludeAcuteHCC,
case when hcc_rollup is null then hccs.hcc 
        else hcc_rollup 
        end as hccRollup,
case when lower(sourceType) like '%claim%' and source in ('facility','professional') then hccEligibleCpt 
     when lower(sourceType) not like '%claim%' then true
        end as hccEligibleCpt,  

case when lower(sourceType) like '%claim%' and source = 'facility' then hccEligibleBilltype
     when lower(sourceType) like '%claim%' and source = 'professional' then true
     when lower(sourceType) not like '%claim%' then true
        end as hccEligibleBilltype,

case when lower(sourceType) like '%claim%' then validClaim 
     when lower(sourceType) not like '%claim%' then true
        end as validClaim,  

providerSpecialty,        
case when clm.providerSpecialty is not null and specialties.code is not null then true
     when clm.providerSpecialty is null and specialties.code is null then true
        end as hccEligibleSpecialist,
paidDate,
paidStatus

from
member

left join
clm
on member.id = clm.memberIdentifier

left join
ICDdescript
on
replace(clm.diagnosisCode2,'.','')  = diagCode

left join
hccs
on replace(clm.diagnosisCode2,'.','') = hccs.dxCode
and  extract(year from serviceDateFrom) = cast(hccs.Year as numeric)

left join
acute_hccs
on 
cast(hccs.hcc as string)  = acute_hccs.excludeHCC

left join
acute_icds
on 
replace(clm.diagnosisCode2,'.','') = acute_icds.excludeICD

left join
hcc_rollup_2020 roll
on 
cast(hccs.hcc as string) = roll.hcc

left join
valid 
on clm.sourceID = valid.claimId

left join
specialties
on clm.providerSpecialty = specialties.code
and cast(extract(year from serviceDateFrom) as string) = specialties.paymentYear
)


select distinct * from final
