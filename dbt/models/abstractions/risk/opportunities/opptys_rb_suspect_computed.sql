
--------------------------------------------------
--- Section One: Suspects from Computed Fields ---
--------------------------------------------------

with suspects as (

select * from {{ ref('cf_suspect_alcohol') }}
union all
select * from {{ ref('cf_suspect_drug_abuse') }}
union all
select * from {{ ref('cf_suspect_depression') }}
union all
select * from {{ ref('cf_suspect_morbid_obesity') }}
union all
select * from {{ ref('cf_unsafe_or_unstable_housing') }} where fieldValue = 'true'
union all
select * from {{ ref('cf_homelessness_assessment') }} where fieldValue = 'true'
union all
select * from {{ ref('cf_food_insecurity') }} where fieldValue = 'true'

),


--- Reference / Member Data --------

member as (

select distinct
patientId,
memberId,
eligYear,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
lineOfBusiness in ('medicare', 'duals', 'dnsp', 'medicaid')
and eligYear = extract(Year from current_date())
and eligNow = true
and isHARP = false
and lineOfBusiness <> 'commercial'
),


codes as (

select distinct
patientID,
max(clmYear) as clmYear,
max(evidenceDate) as evidenceDate,
count(distinct evidenceDate) as evidenceCount,
dxCode

from
{{ ref('risk_claims_patients') }}

group by
patientID,
dxCode
),


cf_suspects as (

select distinct
case when lower(partner) like '%connecticare%' then 'connecticare'
     when lower(partner) like  '%emblem%' then 'emblem'
     else lower(partner) 
          end as partner,
mem.patientId,
lineOfBusiness,
case when fieldslug in ( 'unsafe-or-unstable-housing', 'food-insecurity','homelessness-assessment')  then 'PERS'
     else 'SUSP'
          end as conditionType,
'computed_field' as conditionSource,
case when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' then '3M CRG'
     when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' then 'CDPS'
     else 'HCC'
          end as conditionCategory,
case when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' and fieldslug = 'suspect-drug-abuse' then 'SUBL'
     when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' and fieldslug = 'suspect-depression' then 'PSYML'
     when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' and fieldslug = 'suspect-alcohol' then 'SUBVL'

     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'suspect-drug-abuse' then '786'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'suspect-depression' then '755'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'suspect-alcohol' then '784'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'suspect-morbid-obesity' then '451'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'unsafe-or-unstable-housing' then '594000'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'food-insecurity' then '590000'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'homelessness-assessment' then '599000'

     when mem.lineOfBusiness <> 'medicaid' and  fieldslug = 'suspect-drug-abuse' then '56'
     when mem.lineOfBusiness <> 'medicaid' and  fieldslug = 'suspect-morbid-obesity' then '22'
     when mem.lineOfBusiness <> 'medicaid' and  fieldslug = 'suspect-alcohol' then '55'
     when mem.lineOfBusiness <> 'medicaid' and  fieldslug = 'suspect-depression' then '59'
          end as conditionCategoryCode,

case when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' and  fieldslug = 'suspect-drug-abuse' then 'Opioid, barbiturate, cocaine, amphetamine abuse or dependence, drug psychoses'
     when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' and  fieldslug = 'suspect-alcohol' then 'Alcohol abuse, dependence, or psychosis'
     when mem.lineOfBusiness =  'medicaid' and partner <> 'emblem' and  fieldslug = 'suspect-depression' then 'Major depressive disorder, bipolar II disorder, manic episode, other recurrent depressive disorders'

     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and  fieldslug = 'suspect-drug-abuse' then 'Drug Abuse/Dependence NOS/NEC'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and  fieldslug = 'suspect-morbid-obesity' then 'Morbid Obesity'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and  fieldslug = 'suspect-alcohol' then 'Chronic Alcohol Abuse/Dependence'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and  fieldslug = 'suspect-depression' then 'Depression'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'unsafe-or-unstable-housing' then 'Unsafe or Unstable Housing'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'homelessness-assessment' then 'Homelessness Assessment'
     when mem.lineOfBusiness =  'medicaid' and partner = 'emblem' and fieldslug = 'food-insecurity' then 'Food Insecurity'

     when mem.lineOfBusiness <>  'medicaid' and  fieldslug = 'suspect-drug-abuse' then 'Substance Use Disorder, Mild, Except Alcohol and Cannabis'
     when mem.lineOfBusiness <>  'medicaid' and  fieldslug = 'suspect-morbid-obesity' then 'Morbid Obesity'
     when mem.lineOfBusiness <>  'medicaid' and  fieldslug = 'suspect-alcohol' then 'Alcohol Dependence'
     when mem.lineOfBusiness <>  'medicaid' and  fieldslug = 'suspect-depression' then 'Major Depressive, Bipolar, and Paranoid Disorders'
          end as conditionName,
case when fieldslug = 'unsafe-or-unstable-housing' then 'Z594'
     when fieldslug = 'food-insecurity'  then 'Z590'
     when fieldslug = 'homelessness-assessment' then 'Z599'
     else cast(null as string)
          end as underlyingCode,
case when fieldslug in ( 'unsafe-or-unstable-housing', 'food-insecurity' , 'homelessness-assessment') then 'VERY HIGH'
     else 'HIGH'
          end as confidenceLevel,
case when fieldslug in ( 'unsafe-or-unstable-housing', 'food-insecurity' , 'homelessness-assessment') then 1
     else 2
          end confidenceNumeric,
case when fieldslug = 'unsafe-or-unstable-housing' then 'Z594'
     when fieldslug = 'food-insecurity' then 'Z590'
     when fieldslug = 'homelessness-assessment' then 'Z599'
     else cast(null as string)
          end as supportingEvidenceCode,
'score' as supportingEvidenceCodeType,
case when fieldslug = 'suspect-drug-abuse' then 'DAST >= 3'
     when fieldslug = 'suspect-morbid-obesity' then 'BMI > 40 or > 35 and has DM'
     when fieldslug = 'suspect-alcohol' then 'AUDIT >= 8'
     when fieldslug = 'suspect-depression' then 'PHQ-9 >= 10'
          else 'no score'
          end as supportingEvidenceValue,
case when fieldslug = 'suspect-drug-abuse' then 'DAST Score'  
     when fieldslug = 'suspect-morbid-obesity' then 'BMI > 40 or > 35 and has DM'
     when fieldslug = 'suspect-alcohol' then 'AUDIT Score'  
     when fieldslug = 'suspect-depression' then 'PHQ-9 Score'
     when fieldslug in ('unsafe-or-unstable-housing' ,'homelessness-assessment', 'food-insecurity' ) then 'Commons Assessment'
          end as supportingEvidenceName,
'commons' as supportingEvidenceSource

from 
member mem

inner join
suspects
on mem.patientID = suspects.patientid
),

 
final as (

select distinct
mem.partner,
mem.patientId,
lineOfBusiness,
conditionType,
conditionSource,
conditionCategory,
conditionCategoryCode,
conditionName,
case when extract(Year from current_date()) = codes.clmYear and conditionCategoryCode in ('594000', '590000','599000') and lineOfBusiness = 'medicaid'  then 'CLOSED'
     else 'OPEN'
          end as conditionStatus,
underlyingCode,
confidenceLevel,
confidenceNumeric,
supportingEvidenceCode,
supportingEvidenceCodeType,
supportingEvidenceValue,
supportingEvidenceName,
supportingEvidenceSource,
current_date() as date_uploaded,
cast(null as string) as Provider_First_Name,
cast(null as string) as Provider_Last_Name_Legal_Name,
codes.evidenceCount as clmCount,
codes.evidenceDate as clmLatest

from
cf_suspects mem

left join
codes
on mem.patientID = codes.patientid
and mem.supportingEvidenceCode = codes.dxCode

where
conditionCategoryCode is not null
)

select * from final
