
-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


-- load hcc codeset
with hcc_codes_2020 as (

select distinct * 

from
{{ source('codesets', 'hcc_2020') }} 

where
lower(cast(hcc_v24_2020 as string)) = 'yes'
),


--member base
member as (

select distinct
patientId as id, 
memberId,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
isHARP = false
),


--DS model extract (recent data in table)
model_output_new as (

select distinct *

from 
{{ source('src_risk', 'model_output') }}

where 
diagnosis_slug in ('diabetes', 'diabetes_w_complications', 'chronic_kidney_disease', 'congestive_heart_failure', 'copd', 'depression_bipolar_paranoid', 'dementia', 'vascular_disease')
),


--Older static data
model_output_static as (

select distinct * 

from 
{{ source('src_risk', 'model_output_static') }}

where 
diagnosis_slug not in ('diabetes', 'diabetes_w_complications', 'chronic_kidney_disease', 'congestive_heart_failure', 'copd', 'depression_bipolar_paranoid', 'dementia', 'vascular_disease')
),


--Combined table
model_output as (

select * from model_output_new
union all
select * from model_output_static
),


--add confidence based on quintiles of shap values
quint as (

select distinct 
memid as quint_mem, 
diagnosis_slug as quint_slug, 
code as quint_code, 
ntile(5) over (order by shap_value desc) as tile

from 
model_output t

order by 
quint_mem, 
quint_slug,tile, 
quint_code
),


--Historical dx codes
dx_history as (

select distinct 
dxCode, 
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
memhcccombo 

from
{{ ref('risk_claims_patients') }} 

where 
EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 3 and EXTRACT(YEAR FROM current_date())
),


--max year coded (to remove currently closed suspects)
closed as (

select * from (

select distinct 
max(evidenceDate) as evdateClosed,
concat(patientId, hcc) as category 

from 
dx_history 

group by 
concat(patientId, hcc)
)
where
extract(year from evdateClosed) = extract(year from current_date())

),


closedPrior as (

select * from (

select distinct
max(evidenceDate) as evdateClosed,
concat(patientId, hcc) as category

from
dx_history

group by
concat(patientId, hcc)
)
where
extract(year from evdateClosed) < extract(year from current_date())

),


--- agg dx
ml_suspects as (
  
select distinct
p.partner, 
cast( memid as string) as patientId,
p.lineOfBusiness, 
condition_type as conditionType,
condition_source as conditionSource,
condition_Category as conditionCategory,  
case when diagnosis_slug = 'depression_bipolar_paranoid' then '59'
     when diagnosis_slug = 'diabetes' then '19' 
     when diagnosis_slug = 'diabetes_w_complications' then '18' 
     when diagnosis_slug = 'dementia' then '52'
     when diagnosis_slug = 'copd' then '111'
     when diagnosis_slug = 'vascular_disease' then '108'
     when diagnosis_slug = 'chronic_kidney_disease' then '138'
     when diagnosis_slug = 'congestive_heart_failure' then '85' 
     when diagnosis_slug = 'heart_arrhythmias' then '96'           
          end as conditionCategoryCode,
case when diagnosis_slug = 'depression_bipolar_paranoid' then 'Major Depressive, Bipolar, and Paranoid Disorders'
     when diagnosis_slug = 'diabetes' then 'Diabetes without Complications' 
     when diagnosis_slug = 'diabetes_w_complications' then 'Diabetes with Chronic Complications' 
     when diagnosis_slug = 'dementia' then 'Dementia without Complication'
     when diagnosis_slug = 'copd' then 'Chronic Obstructive Pulmonary Disease'
     when diagnosis_slug = 'vascular_disease' then 'Vascular Disease'
     when diagnosis_slug = 'chronic_kidney_disease' then 'Chronic Kidney Disease, Moderate (Stage 3)'
     when diagnosis_slug = 'congestive_heart_failure' then 'Congestive Heart Failure'
     when diagnosis_slug = 'heart_arrhythmias' then 'Specified Heart Arrhythmias'           
          end as conditionName,
'OPEN' as conditionStatus,  
cast(null as string) as underlyingCode , --to be added in Elation step
case when tile = 1 then 'VERY HIGH'
     when tile = 2 then 'HIGH'
     when tile = 3 then 'MEDIUM'
     when tile = 4 then 'LOW'
     when tile = 5 then 'VERY LOW'
          end as confidence , --how do we want to standardize this?
tile as confidenceNumeric,
code as supportingEvidenceCode,
code_type as supportingEvidenceCodeType ,
cast(value as string) as supportingEvidenceValue , 
code_name as supportingEvidenceName,
case when lower(code_type) = 'cpt' then concat(evidence_type,' - claims')
     when lower(code_type) = 'icd' then concat(evidence_type,' - claims')
     when lower(code_type) = 'ndc' then concat(evidence_type,' - pharmacy')
     when lower(code_type) = 'loinc' then concat(evidence_type,' - lab')
     when lower(code_type) = 'gender' then concat(evidence_type,' - demographic')
     when lower(code_type) = 'age' then concat(evidence_type,' - demographic')
     else 'cost'
          end as supportingEvidenceSource,
current_date()  as date_uploaded, --add date updated,
cast(null as string) as Provider_First_Name, 
provider as Provider_Last_Name_Legal_Name,
null as clmCount,
last_billed as clmLatest

from  
model_output a

INNER JOIN 
member p 
on cast( memid as string)  = p.id 

left join 
quint
on a.memid = quint.quint_mem
and a.code = quint.quint_code
and a.diagnosis_slug = quint.quint_slug
),


final as (

select distinct 
partner, 
patientId, 
lineOfBusiness,  
conditionType, 
conditionSource, 
conditionCategory, 
conditionCategoryCode,
conditionName, 
case when closed.evdateClosed is not null then 'CLOSED'
     else 'OPEN' 
          end as conditionStatus, 
underlyingCode,  
confidence,  
confidenceNumeric, 
supportingEvidenceCode, 
supportingEvidenceCodeType,  
supportingEvidenceValue, 
supportingEvidenceName,  
supportingEvidenceSource, 
date_uploaded,
Provider_First_Name,
Provider_Last_Name_Legal_Name, 
clmCount, 
closed.evdateClosed as clmLatest

from
ml_suspects

left join
closed
on concat(patientId, conditionCategoryCode) = closed.category

left join
closedPrior
on concat(patientId, conditionCategoryCode) = closedPrior.category

where
closedPrior.category is null

)



--final
 select * from final
