
{{
  config(
    materialized='incremental',
       tags=["evening"]
       )
}}


with rolledup as (
select distinct * from {{source('codesets','hcc_rollup_2020')}}
),


opptys_recapture_all_distinct as (
select distinct * from {{ ref('opptys_recapture_all') }}
),


opptys_rb_suspect_diab_distinct as (
select distinct * from {{ ref('opptys_rb_suspect_diab') }}
),


opptys_rb_suspect_computed as (
select distinct * from {{ ref('opptys_rb_suspect_computed') }}
),


opptys_ml_suspect_all_distinct as (
select distinct * from {{ ref('opptys_ml_suspect_all') }}
),


opptys_rb_suspect_diab_comp_distinct as (
select distinct * from {{ ref('opptys_rb_suspect_diab_comp') }}
),


opptys_chart_review as (
select distinct * from {{ ref('opptys_chart_review') }}
),


risk_chart_review as (
select distinct * from {{ ref('risk_chart_review') }}
where safe.PARSE_DATE('%m/%d/%Y', reviewDate) > PARSE_DATE('%m/%d/%Y', '4/7/2021')
),


opptys_cms_mor_distinct as (
select distinct * from {{ ref('opptys_cms_mor') }}
),


opptys_cb_persistent as (
select distinct * from {{ ref('opptys_cb_persistent') }}
),


opptys_supplemental as (
select distinct * from {{ ref('opptys_supplemental') }}
),


codesets_hcc_hierarchies_xwalk as (
select * from {{ source('codesets', 'hcc_hierarchies_xwalk') }}
),


closed as (
select *  from  {{ ref('all_suspects_closed') }}
),


acute_hccs as (

select distinct 
hcc_v24 as excludeHCC,
excludeAcuteHCC,
HCCLookbackPeriod

from
{{ source('codesets', 'hcc_lookback_periods') }}
),


acute_icds as (

select distinct
dagnosisCode as excludeICD,
description,
excludeICD10,
ICD10Replacement

from
{{ source('codesets', 'hcc_acute_exclusion') }}
),


risk_claims_patients as (

select distinct
max(clmYear) as maxHccYear,
patientId,
hcc

from 
{{ ref('risk_claims_patients') }}

group by 
patientId,
hcc
),


unioned as (

select * from opptys_recapture_all_distinct
union all
select * from opptys_rb_suspect_diab_distinct
union all
select * from opptys_ml_suspect_all_distinct
union all
select * from opptys_rb_suspect_diab_comp_distinct
union all
select * from opptys_rb_suspect_computed
union all
select * from opptys_cms_mor_distinct
union all
select * from opptys_chart_review
union all
select * from opptys_cb_persistent
union all
select * from opptys_supplemental
),


all_suspects as (

select distinct 
partner, 
unioned.patientId,  
lineOfBusiness, 
conditionType,  
conditionSource,  
conditionCategory,  
conditionCategoryCode,
coalesce(rolledup.hcc_rollup , unioned.conditionCategoryCode) as hcc_rollup,
conditionName,  
conditionStatus,  
underlyingDxCode, 
confidenceLevel,
confidenceNumeric,
coalesce(ICD10Replacement , supportingEvidenceCode) as supportingEvidenceCode,
supportingEvidenceCodeType, 
supportingEvidenceValue,  
supportingEvidenceName, 
supportingEvidenceSource, 
dateUploaded, 
providerFirstName,  
providerLastNameLegalName,  
clmCount, 
clmLatest,
case when conditionType = 'SUSP' 
          and elig.hcc is null 
          and conditionSource in ('tufts_file', 'roger_dashboard','digital_load','payer_emblem') 
          and confidenceLevel = 'VERY HIGH'  then 'yes'

     when conditionType = 'SUSP' 
          and elig.hcc is null 
          and conditionSource in ('internal_rules','computed_field','internal_predictive') 
          and confidenceLevel in ('HIGH','VERY HIGH') then 'yes'

     when conditionType = 'SUSP' 
          and elig.hcc is null and conditionSource in ('prospective-review' ,'supplemental_file') then 'yes'

     when excludeICD is null
          and excludeAcuteHCC is null 
          and conditionType = 'PERS' 
          and (maxHccYear is null or maxHccYear between extract(year from current_date()) - 3 and extract(year from current_date()) - 1  )
          and (maxHccYear is null or extract(year from current_date())  - maxHccYear  <= cast(HCCLookbackPeriod as numeric) ) then 'yes' 
  else 'no'
          end as suspectHccElig,
cast(maxHccYear as string)  as maxHccYear

from
unioned

left join
rolledup
on unioned.conditionCategoryCode = rolledup.hcc

left join
risk_claims_patients elig
on unioned.patientId = elig.patientId
and unioned.conditionCategoryCode = elig.hcc

left join
acute_hccs
on unioned.conditionCategoryCode = acute_hccs.excludeHCC

left join
acute_icds
on unioned.supportingEvidenceCode = acute_icds.excludeICD
),


FINAL as (

Select distinct
partner,
a.patientId,
lineOfBusiness,
conditionType,
conditionSource,
conditionCategory,
a.conditionCategoryCode,
a.HCC_Rollup,
conditionName,
case when closed.category is null then 'no' else 'yes' end as hccClosedFlag,
suspectHccElig,
maxHccYear,
conditionStatus,
underlyingDxCode,
confidenceLevel,
supportingEvidenceCode,
supportingEvidenceCodeType,
supportingEvidenceValue,
supportingEvidenceName,
supportingEvidenceSource,
dateUploaded,
providerFirstName,
providerLastNameLegalName,
clmCount,
cast(clmLatest as string) as clmLatest,
current_datetime()  AS runDate,
cast(closedDate as string) as closedDate,
closedSource,
closed.conditionCategoryCode AS closedCode

from
all_suspects a

left join
closed
on extract(year from current_date()) = extract(year from closedDate)
and a.patientID = closed.patientID
and a.conditionCategoryCode >= closed.conditionCategoryCode
and a.hcc_rollup = closed.hcc_rollup
)


select * from final
