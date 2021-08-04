
with hcc_codes_2020 as (

select distinct 
year,
DiagnosisCode,
Description,
CMSHCCModelCategoryV24or23 as hcc_v24

from
{{ source('codesets', 'ICD10toHCCcrosswalk') }} 

where
lower(cast(CMSHCCModelCategoryV24forCurrentYear as string)) = 'yes'
),


--load current hcc descriptions
risk_hcc_descript as (

select distinct * 

from
{{ source('codesets', 'risk_hcc_descript') }} 

where
version = 'v24'
),


hccs as (

select distinct 
Year,
DiagnosisCode as dxCode, 
description as dxDescription,
cast(hcc_v24 as string) as hcc, 
hccdescr as HCCDescription

from
hcc_codes_2020

left join
risk_hcc_descript
on  hcc_codes_2020.hcc_v24 =cast( hcc as string) 
),


member as (

select distinct
patientId, 
memberId,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}
),


emblem_sup as (

SELECT distinct
memberid,
DiagCode,
HCC,
HCCVersion,
ReviewType,
POCode,
POName,
PracticeCode,
PracticeName,
NPI,
ProviderName,
trim(upper(split(ProviderName, ',')[ordinal(1)])) as lastName,
trim(upper(split(ProviderName, ',')[ordinal(2)])) as firstName,
PARSE_DATE("%m/%d/%Y","12/31/2019") as From_Date,
2019 as Year

FROM
{{ source('abs_risk', 'risk_supplemental_2020_emblem') }}
),


tufts_sup as (

select distinct

Line_Certification,
Reference_Nbr,
Provider_NPI,
Provider_Leg_ID,
PARSE_DATE("%m/%d/%Y",From_Date) as From_Date,
Thru_Date,
Member_HICN,
Subscriber_Nbr,
Member_Last_Name,
Member_First_Name,
Member_MI_Name,
Patient_DOB,
Rev_Code,
Bill_Type,
CPT_Code,
Facility_Name,
Provider_Last_Name,
Provider_First_Name,
CMS_Speciality_Type,
Place_of_Service,
replace(Dx_Code,'.','') as Dx_Code,
Admit_Dx_Indicator,
ICD_Type_Indicator,
Claim_Type_Indicator,
Adjustment_Code,
Record_Comment,
Attributing_Program,
Risk_Assessment_Code,
2020 as Year

from
{{ source('abs_risk', 'risk_supplemental_2020_tufts') }} 
),


cci_sup as (

select distinct 

Vendor,
MemberID,
NMI,
MBI,
PatientFirstName,
PatientLastName,
PARSE_DATE("%Y%m%d",DOSFrom) as DOSFrom,
DOSThru,
replace(DXCode,'.','') as DXCode,
ProvType,
Claim,
RenderingProviderLastName,
RenderingProviderFirstName,
ProviderNPI,
Deletes,
2019 as Year

from
{{ source('abs_risk', 'risk_supplemental_2020_cci') }}
),


supplemental as (

select distinct
Subscriber_Nbr as MemberID,
Provider_Last_Name,
Provider_First_Name,
Provider_NPI as ProviderNPI,
Dx_Code,
From_Date,
'Retrospective Review' as supportingEvidence,
Year

from 
tufts_sup

union all

select distinct
MemberID,
RenderingProviderLastName,
RenderingProviderFirstName,
ProviderNPI,
DXCode,
DOSFrom,
'Retrospective Review' as supportingEvidence,
Year

from 
cci_sup

union all

select distinct
MemberID,
lastName,
firstName,
NPI,
DiagCode,
From_Date,
concat(trim((split(ReviewType, ' - ')[ordinal(2)])) ,' Review') as supportingEvidence,
Year

from
emblem_sup

),


final as (

select distinct 
p.partner, 
p.patientId, 
p.lineOfBusiness,  
'PERS' conditionType, 
'supplemental_file' as conditionSource, 
'HCC' as conditionCategory, 
hccs.hcc as conditionCategoryCode,
HCCDescription as conditionName, 
case when extract(year from From_Date) = extract(year from current_date()) then 'CLOSED' 
     else 'OPEN' 
          end as conditionStatus, 
replace(trim(supplemental.Dx_Code),'.','') as underlyingCode,  
'VERY HIGH' as confidenceLevel,
1 as confidenceNumeric, 
Dx_Code as supportingEvidenceCode, 
'ICD' as supportingEvidenceCodeType,  
cast(null as string) as supportingEvidenceValue, 
supportingEvidence as supportingEvidenceName,
'ehr' as supportingEvidenceSource, 
PARSE_DATE("%m/%d/%Y","12/01/2020") as date_uploaded, --add date updated,
Provider_First_Name as Provider_First_Name, 
Provider_Last_Name as Provider_Last_Name_Legal_Name,
null as clmCount,
From_Date as clmLatest

from
supplemental

left join
hccs
on replace(trim(supplemental.Dx_Code),'.','') = replace(trim(hccs.dxCode ),'.','')
and  extract(year from From_Date) = cast(hccs.Year as numeric)

INNER JOIN 
member p 
on cast(supplemental.MemberID as string)  = p.memberId 

)

select * from final where conditionCategoryCode is not null


