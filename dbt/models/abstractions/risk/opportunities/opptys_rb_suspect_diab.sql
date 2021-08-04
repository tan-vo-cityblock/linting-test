
-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--HCC reference data
with hcc_codes as (

select distinct *

from
{{ source('codesets', 'hcc_2020') }} 

where
lower(cast(hcc_v24_2020 as string)) = 'yes'
),


--hcc descriptions
risk_hcc_descript as (

select distinct

hcc.*

from 
{{ source('codesets', 'risk_hcc_descript') }} as hcc

where 
version = 'v24'
),


risk_claims_dx as (

select *

from
{{ ref('risk_claims_dx') }}

where
EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date())
),


risk_claims_lab as (

select *

from
{{ ref('risk_claims_lab') }}
),


risk_claims_cpt as (

select *

from
{{ ref('risk_claims_cpt') }} 
),


risk_claims_pharm as (

select *

from
{{ ref('risk_claims_pharm') }} 
),


hedis_codesets as (

select *

from
{{ source('hedis_codesets', 'mld_med_list_to_ndc_2020') }} 
),


abs_rxnorm_ndc_crosswalk as (

select *

from
{{ source('abs_ref', 'abs_rxnorm_ndc_crosswalk') }} 
),


risk_claims_patients1918 as (

select distinct
patientId

from
{{ ref('risk_claims_patients') }} 

where 
hcc in ('19','18')
and (extract(year from evidenceDate) between extract (year from current_date()) - 2 and extract (year from current_date()) )
),


--member and lob info
member as (

select distinct
patientId as id, 
memberId,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
eligYear = extract(Year from current_date())
and eligNow = true
and isHARP = false
and lineOfBusiness <> 'commercial'
),


------------------------------------------------------------------------------------
diabetes_dx_codes as (

select distinct
patientId as PatientId,
cast(null as string) as underlyingDxCode,
'HIGH' as confidenceLevel,
dxCode as cupportingEvidenceCode,
'ICD' as cupportingEvidenceCodeType,
cast(null as string) as SupportingEvidenceValue,
case when dxCode = 'R7303' then 'prediabetes dx code' 
       when dxCode = 'Z833' then 'family h/o diabetes dx code' 
       when dxCode = 'O24' then 'gestational diabetes dx code'
       when dxCode = 'R358' then 'polyuria dx code'
       when dxCode = 'R730' then 'Abnormal glucose dx code'
          end as cupportingEvidenceName,
'claims' as cupportingEvidenceSource,
evidenceDate ,
current_date() as dateUploaded

from
risk_claims_dx  dxs

where 
dxs.dxCode  in ('R7303','Z833','O24', 'R358', 'R730')
),


diabetes_lab_test as (
select distinct   
patientId,
cast(null as string) as underlyingDxCode,
'HIGH' as confidenceLevel,
loinc as supportingEvidenceCode,
'LOINC' as supportingEvidenceCodeType,
cast(resultNumeric as string) as supportingEvidenceValue,
case when loinc in('45484','45492','178566','592618','718759', '623884') 
        then 'diabetes-a1c'
     when loinc in('15180','15586','62856') 
        then 'diabetes-glucose-ogtt'
     when loinc = '23457'
        then 'diabetes-random-glucose'
     when loinc in ('31060','31075','31061','31060','31052','67778','31053','31061','31060','30000','32000','31014','31052','31053','33438','30651','31052','31075') 
         then  concat(lower(name),'-test')
     when loinc in( '909960' ,'15545','15578','15586','178657','104505')  
         then 'diabetes-fasting-glucose'
          end as supportingEvidenceName,
  'lab' as supportingEvidenceSource,
          evidenceDate,
  current_date()  as dateUploaded

from
risk_claims_lab

where
(((trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) in('15180','15586','62856') and resultNumeric >199)
or (trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) = '23457' and resultNumeric >200)
or (trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) in( '909960' ,'15545','15578','15586','178657','104505') and resultNumeric >125)
or ( lower(name) like '%glucose%' and trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) in
('31060','31075','31061','31060','31052','67778','31053','31061','31060','30000','32000','31014','31052','31053','33438','30651','31052','31075') and resultNumeric >125))
or
(trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) in('45484','45492','178566','592618','718759', '623884') and resultNumeric >6.4) )
),


diabetes_ndc_codes as (

select distinct
patientId ,
cast(null as string) as underlyingDxCode,
'HIGH' as confidenceLevel,
drug.ndc as supportingEvidenceCode,
'NDC' as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue,
lower( ingredient ) as supportingEvidenceName,
'pharmacy' as supportingEvidenceSource,
evidenceDate,
current_date() as dateUploaded

from
risk_claims_pharm drug

left join
    (select distinct 
    hedis.* ,
    ndc as new_ndc 
    from  
    hedis_codesets hedis

    left join
    abs_rxnorm_ndc_crosswalk xwalk
    on ndc_code = sxdPckRxcui) hedis 
  on drug.ndc   =  ifnull(hedis.ndc_code, hedis.new_ndc)

where
((drug.NDC like '%00027510%'  or drug.NDC like '%00027511%'  or drug.NDC like '%00027512%'  or drug.NDC like '%00027516%'  or drug.NDC like '%00027712%'  or drug.NDC like '%00027714%'  or drug.NDC like '%00027715%'  or drug.NDC like '%00028215%'  or drug.NDC like '%00028315%'  
  or drug.NDC like '%00028501%'  or drug.NDC like '%00028715%'  or drug.NDC like '%00028797%'  or drug.NDC like '%00028798%'  or drug.NDC like '%00028799%'  or drug.NDC like '%00028803%'  or drug.NDC like '%00028805%'  or drug.NDC like '%00028824%'  or drug.NDC like '%00245761%'  
  or drug.NDC like '%00245869%'  or drug.NDC like '%00245871%'  or drug.NDC like '%00245924%'  or drug.NDC like '%00245925%'  or drug.NDC like '%00882219%'  or drug.NDC like '%00882220%'  or drug.NDC like '%00882500%'  or drug.NDC like '%00882502%'  or drug.NDC like '%00885020%'  
  or drug.NDC like '%00885021%'  or drug.NDC like '%01691833%'  or drug.NDC like '%01691834%'  or drug.NDC like '%01691837%'  or drug.NDC like '%01692550%'  or drug.NDC like '%01692660%'  or drug.NDC like '%01692911%'  or drug.NDC like '%01693201%'  or drug.NDC like '%01693204%'  
  or drug.NDC like '%01693303%'  or drug.NDC like '%01693685%'  or drug.NDC like '%01693687%'  or drug.NDC like '%01693696%'  or drug.NDC like '%01696338%'  or drug.NDC like '%01696339%'  or drug.NDC like '%01696438%'  or drug.NDC like '%01697501%'  or drug.NDC like '%4791874%' 
  or drug.NDC like '%500900353%'  or drug.NDC like '%500900352%'  or drug.NDC like '%548682746%'  or drug.NDC like '%548683474%'  or drug.NDC like '%548683619%'  or drug.NDC like '%548684381%'  or drug.NDC like '%548684626%'  or drug.NDC like '%548685108%'  or drug.NDC like '%548685201%'  
  or drug.NDC like '%548685327%'  or drug.NDC like '%548685765%'  or drug.NDC like '%548685836%'  or drug.NDC like '%548685883%'  or drug.NDC like '%548685899%'  or drug.NDC like '%548686231%'  or drug.NDC like '%550453602%'  or drug.NDC like '%550453685%'  or drug.NDC like '%647252220%' 
  or drug.NDC like '%647251837%'  or drug.NDC like '%647251834%'  or drug.NDC like '%647251833%'  or drug.NDC like '%647250750%'  or drug.NDC like '%682588899%'  or drug.NDC like '%682588977%'
  ) 
 or
(medication_list = 'Diabetes Medications'
 or medication_list = 'ACE Inhibitor and ARB Medications'
 or medication_list in ('Amlodipine Atorvastatin High Intensity Medications',
'Amlodipine Atorvastatin Moderate Intensity Medications',
'Atorvastatin High Intensity Medications',
'Atorvastatin Moderate Intensity Medications',
'Ezetimibe Simvastatin High Intensity Medications',
'Ezetimibe Simvastatin Low Intensity Medications',
'Ezetimibe Simvastatin Moderate Intensity Medications',
'Fluvastatin Low Intensity Medications',
'Fluvastatin Moderate Intensity Medications',
'Lovastatin Low Intensity Medications',
'Lovastatin Moderate Intensity Medications',
'Pitavastatin Low Intensity Medications',
'Pitavastatin Moderate Intensity Medications',
'Pravastatin Low Intensity Medications',
'Pravastatin Moderate Intensity Medications',
'Rosuvastatin High Intensity Medications',
'Rosuvastatin Moderate Intensity Medications',
'Simvastatin High Intensity Medications',
'Simvastatin Low Intensity Medications',
'Simvastatin Moderate Intensity Medications'))
)
),


diabetes_cpt_codes as (

select distinct
patientId,
cast(null as string) as underlyingDxCode,
case when code ='0488T' then 'LOW' else 'HIGH' end as confidenceLevel,
code as supportingEvidenceCode,
codeset as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue,
case when code in ('95250', '95251') then 'Continuous glucose monitoring'
     when code = '3046F' then 'Most recent hemoglobin A1c level > 9.0%'
     when code = '3045F' then 'Most recent hemoglobin A1c level 7.0% to 9.0%'
     when code in ('97802', '97803', '97804') then 'Medical Nutritional Therapy'
     when code in ('G0108', 'G0109') then 'Diabetes Self-management Teaching' 
     when code = '0488T' then 'Diabetes Prevention Program'
       end as supportingEvidenceName,
'claims' as supportingEvidenceSource,
evidenceDate,
current_date()  as dateUploaded

from
risk_claims_cpt clms

where 
code in ('95250', '95251','3046F','3045F','97802', '97803', '97804','G0108', 'G0109', '0488T')
),


diabetes_suspects_mem as (

select distinct
p.partner, 
dxs.patientID, 
p.lineOfBusiness, 
'SUSP' as conditionType, 
'internal_rules' as conditionSource,
case when lower(p.lineOfBusiness) = 'medicaid' and partner <> 'emblem' then 'CDPS'
     when lower(p.lineOfBusiness) = 'medicaid' and partner = 'emblem' then '3M CRG'
     else 'HCC'
           end as ConditionCategory,
case when lower(p.lineOfBusiness) = 'medicaid' and partner <> 'emblem' then 'DIA2L'
     when lower(p.lineOfBusiness) = 'medicaid' and partner = 'emblem' then '424'
     else '19'
           end as conditionCategoryCode,
case when lower(p.lineOfBusiness) = 'medicaid' and partner <> 'emblem' then 'Type 2 or unspecified diabetes without complications'
     when lower(p.lineOfBusiness) = 'medicaid' and partner = 'emblem' then 'Diabetes'
     else 'Diabetes without Complications'
           end as ConditionName,
case when pat.patientID is null then 'OPEN' 
     else 'CLOSED' 
          end as ConditionStatus,
underlyingDxCode ,
confidenceLevel,
case when upper(confidenceLevel) = 'VERY HIGH' then 1
     when upper(confidenceLevel) = 'HIGH' then 2
     when upper(confidenceLevel) = 'MEDIUM' then 3
     when upper(confidenceLevel) = 'LOW' then 4
     when upper(confidenceLevel) = 'VERY LOW' then 5
     else 5 
          end as confidenceNumeric,
supportingEvidenceCode ,
supportingEvidenceCodeType,  
supportingEvidenceValue, 
supportingEvidenceName ,
supportingEvidenceSource ,
dateUploaded,
cast(null as string) as Provider_First_Name, 
cast(null as string) as Provider_Last_Name_Legal_Name,
count(distinct dxs.evidenceDate) as clmCount,
max(dxs.evidenceDate) as clmLatest

from
(
select * from diabetes_lab_test
union all
select * from diabetes_ndc_codes
union all
select * from diabetes_cpt_codes
union all
select * from diabetes_dx_codes
) dxs

INNER JOIN 
member p 
on dxs.patientID = p.id 

LEFT JOIN 
risk_claims_patients1918 pat
on dxs.patientID = pat.patientID  

group by  
p.partner, 
dxs.patientId,
p.lineOfBusiness,
conditionCategory, 
conditionStatus,
underlyingDxCode, 
confidenceLevel, 
supportingEvidenceCode,
supportingEvidenceCodeType, 
supportingEvidenceValue, 
supportingEvidenceName, 
supportingEvidenceSource,
dateUploaded
)

--final
select distinct * from diabetes_suspects_mem
