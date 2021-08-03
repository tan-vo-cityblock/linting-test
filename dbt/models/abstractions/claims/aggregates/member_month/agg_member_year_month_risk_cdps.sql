

{{
  config(
    materialized='table'
  )
}}


with dx as (

select *

from
{{ ref('abs_diagnoses') }}

where 
sourceType = 'claim'
and memberIdentifierField = 'patientId'
),


mm as (
  
select distinct
patientID,
lower(partnerName) as partnerName,
gender,
DATE_DIFF(current_date(),dateOfBirth, YEAR)
- 
IF(EXTRACT(MONTH FROM dateOfBirth)*100 + EXTRACT(DAY FROM dateOfBirth) > EXTRACT(MONTH FROM current_date())*100 + EXTRACT(DAY FROM current_date()),1,0) AS age,
eligDate,
eligYear,
eligMonth,
lineOfBusinessGrouped as lineOfBusiness

from
{{ ref('master_member_v1') }}

where 
isCityblockMemberMonth is true
),


lob as (

select distinct 
* 
,case 
 when age between 15 and 24 and gender = "F" then "a_15_24f" 
 when age between 15 and 24 and gender = "M" then "a_15_24m"
 when age between 1  and  4 and gender in ("F", "M") then "a_1_4"
 when age between 25 and 44 and gender = "F" then "a_25_44f"
 when age between 25 and 44 and gender = "M" then "a_25_44m"
 when age between 45 and 64 and gender = "F" then "a_45_64f"
 when age between 45 and 64 and gender = "M" then "a_45_64m"
 when age between 5  and 14 and gender = "F" then "a_5_14f"
 when age between 5  and 14 and gender = "M" then "a_5_14m"
 when age >= 65 and  gender in ("F", "M") then "a_65"
 when age < 1 and gender in ("F", "M") then "a_under1"
 end as demo_category

from
mm
),


data as (

select distinct  
patientID as memberId, 
lower(lob.partnerName) as partnerName,
lob.demo_category,
lob.lineOfBusiness,
lob.gender,
lob.age,
dx.serviceDateFrom as dateFrom,
dx.diagnosisCode as diagnosis,
coalesce(extract(year from dx.serviceDateFrom), eligYear) as year,
coalesce(extract(month from dx.serviceDateFrom), eligMonth) as month

from 
lob

left join
dx
on
dx.memberIdentifier = patientID 
-- and extract(month from dx.serviceDateFrom) = eligMonth 
-- and extract(year from dx.serviceDateFrom) = eligYear

),


mapped as (

    {{ map_cdps_categories(
      diagnosis_column='diagnosis',
      table_name='data',
      index_columns=['memberId', 'partnerName', 'demo_category','lineOfBusiness', 'year','month', 'age', 'gender'],
      group_by=True,
      version = 6.4
    ) }}

),


heir as (

  {{ apply_cdps_hierarchies('mapped') }}

),


weighted as (

  {{ apply_cdps_weights(table_name='heir',
            model='CDPS+Rx',
            model_type='PROSPECTIVE',
            population='DISABLED',
            score_type='acute',
            version='6.4'
            )
  }}

),


demo as (

  {{ apply_cdps_demo(table_name='heir',
            model='CDPS+Rx',
            model_type='PROSPECTIVE',
            population='DISABLED',
            score_type='acute',
            version='6.4'
            )
  }}

)


select
year ,
partner,
age,
demo_category,
gender,
lineOfBusiness,
memberId,
model,
partnerName,
cdpsAIDSH,

case when age >1 then 0 else cdpsBABY1 end as cdpsBABY1,
case when age >1 then 0 else cdpsBABY2 end as cdpsBABY2,
case when age >1 then 0 else cdpsBABY3 end as cdpsBABY3,
case when age >1 then 0 else cdpsBABY4 end as cdpsBABY4,
case when age >1 then 0 else cdpsBABY5 end as cdpsBABY5,
case when age >1 then 0 else cdpsBABY6 end as cdpsBABY6,
case when age >1 then 0 else cdpsBABY7 end as cdpsBABY7,
case when age >1 then 0 else cdpsBABY6 end as cdpsBABY8,

cdpsCANH,
cdpsCANL,
cdpsCANM,
cdpsCANVH,
cdpsCAREL,
cdpsCARL,
cdpsCARM,
cdpsCARVH,
cdpsCERL,
cdpsCNSH,
cdpsCNSL,
cdpsCNSM,
cdpsDDL,
cdpsDDM,
cdpsDIA1H,
cdpsDIA1M,
cdpsDIA2L,
cdpsDIA2M,
cdpsEYEL,
cdpsEYEVL,
cdpsGENEL,
cdpsGIH,
cdpsGIL,
cdpsGIM,
cdpsHEMEH,
cdpsHEML,
cdpsHEMM,
cdpsHEMVH,
cdpsHIVM,
cdpsHLTRNS,
cdpsINFH,
cdpsINFL,
cdpsINFM,
cdpsMETH,
cdpsMETM,
cdpsMETVL,
cdpsPRGCMP,
cdpsPRGINC,
cdpsPSYH,
cdpsPSYL,
cdpsPSYM,
cdpsPSYML,
cdpsPULH,
cdpsPULL,
cdpsPULM,
cdpsPULVH,
cdpsRENEH,
cdpsRENL,
cdpsRENM,
cdpsRENVH,
cdpsSKCL,
cdpsSKCM,
cdpsSKCVL,
cdpsSKNH,
cdpsSKNL,
cdpsSKNVL,
cdpsSUBL,
cdpsSUBVL,
scoreAIDSH,

case when age >1 then 0 else scoreBABY1 end as scoreBABY1,
case when age >1 then 0 else scoreBABY2 end as scoreBABY2,
case when age >1 then 0 else scoreBABY3 end as scoreBABY3,
case when age >1 then 0 else scoreBABY4 end as scoreBABY4,
case when age >1 then 0 else scoreBABY5 end as scoreBABY5,
case when age >1 then 0 else scoreBABY6 end as scoreBABY6,
case when age >1 then 0 else scoreBABY7 end as scoreBABY7,
case when age >1 then 0 else scoreBABY8 end as scoreBABY8,

scoreCANH,
scoreCANL,
scoreCANM,
scoreCANVH,
scoreCAREL,
scoreCARL,
scoreCARM,
scoreCARVH,
scoreCERL,
scoreCNSH,
scoreCNSL,
scoreCNSM,
scoreDDL,
scoreDDM,
scoreDIA1H,
scoreDIA1M,
scoreDIA2L,
scoreDIA2M,
scoreEYEL,
scoreEYEVL,
scoreGENEL,
scoreGIH,
scoreGIL,
scoreGIM,
scoreHEMEH,
scoreHEML,
scoreHEMM,
scoreHEMVH,
scoreHIVM,
scoreHLTRNS,
scoreINFH,
scoreINFL,
scoreINFM,
scoreMETH,
scoreMETM,
scoreMETVL,
scorePRGCMP,
scorePRGINC,
scorePSYH,
scorePSYL,
scorePSYM,
scorePSYML,
scorePULH,
scorePULL,
scorePULM,
scorePULVH,
scoreRENEH,
scoreRENL,
scoreRENM,
scoreRENVH,
scoreSKCL,
scoreSKCM,
scoreSKCVL,
scoreSKNH,
scoreSKNL,
scoreSKNVL,
scoreSUBL,
scoreSUBVL,
weightAIDSH,
weightBABY1,
weightBABY2,
weightBABY3,
weightBABY4,
weightBABY5,
weightBABY6,
weightBABY7,
weightBABY8,
weightCANH,
weightCANL,
weightCANM,
weightCANVH,
weightCAREL,
weightCARL,
weightCARM,
weightCARVH,
weightCERL,
weightCNSH,
weightCNSL,
weightCNSM,
weightDDL,
weightDDM,
weightDIA1H,
weightDIA1M,
weightDIA2L,
weightDIA2M,
weightEYEL,
weightEYEVL,
weightGENEL,
weightGIH,
weightGIL,
weightGIM,
weightHEMEH,
weightHEML,
weightHEMM,
weightHEMVH,
weightHIVM,
weightHLTRNS,
weightINFH,
weightINFL,
weightINFM,
weightMETH,
weightMETM,
weightMETVL,
weightPRGCMP,
weightPRGINC,
weightPSYH,
weightPSYL,
weightPSYM,
weightPSYML,
weightPULH,
weightPULL,
weightPULM,
weightPULVH,
weightRENEH,
weightRENL,
weightRENM,
weightRENVH,
weightSKCL,
weightSKCM,
weightSKCVL,
weightSKNH,
weightSKNL,
weightSKNVL,
weightSUBL,
weightSUBVL,
demoScore,
cdpsScore,
ifnull(demoScore, 0)+ifnull(cdpsScore, 0) as scoreWithDemo

from 
weighted 

left join 
demo 
using(partner, demo_category )


