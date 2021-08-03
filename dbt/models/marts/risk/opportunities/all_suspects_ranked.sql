
{{
  config(
    materialized='incremental',
       tags=["evening"]
       )
}}


with opptys_recapture_all_distinct as (

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


codesets_hcc2020 as (

select * from {{ source('codesets', 'hcc_2020') }}
),


acute_hccs as (

select distinct
hcc_v24 as excludeHCC,
excludeAcuteHCC,
HCCLookbackPeriod

from
{{ source('codesets', 'hcc_lookback_periods') }}
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


acute_icds as (

select distinct
dagnosisCode as excludeICD,
description,
excludeICD10,
ICD10Replacement

from
{{ source('codesets', 'hcc_acute_exclusion') }}
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
conditionName,
conditionStatus,
underlyingDxCode,
confidenceLevel,
confidenceNumeric,
case when ICD10Replacement is null then supportingEvidenceCode
     else ICD10Replacement
          end as supportingEvidenceCode,
supportingEvidenceCodeType,
supportingEvidenceValue,
supportingEvidenceName,
supportingEvidenceSource,
dateUploaded,
providerFirstName,
providerLastNameLegalName,
clmCount,
clmLatest,
null as rownum1,
cast(null as string) as clmFlg,
case when conditionType = 'SUSP'
          and elig.hcc is null
          and conditionSource in ('tufts_file', 'roger_dashboard','digital_load','payer_emblem')
          and confidenceLevel = 'VERY HIGH'  then 'yes'

     when conditionType = 'SUSP'
          and elig.hcc is null
          and conditionSource in ('internal_rules','computed_field','internal_predictive')
          and confidenceLevel in ('HIGH','VERY HIGH') then 'yes'

     when conditionType = 'SUSP'
          and elig.hcc is null and conditionSource = 'prospective-review'  then 'yes'

     when excludeICD is null
          and excludeAcuteHCC is null
          and conditionType = 'PERS'
          and (maxHccYear is null or maxHccYear between extract(year from current_date()) - 3 and extract(year from current_date()) - 1  )
          and (maxHccYear is null or extract(year from current_date())  - maxHccYear  <= cast(HCCLookbackPeriod as numeric) ) then 'yes'

     else 'no'
          end as suspectHccElig,
cast(maxHccYear as string)  as maxHccYear

from
  (select * from opptys_recapture_all_distinct
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
  ) unioned

left join
risk_claims_patients elig
on unioned.patientId = elig.patientId
and unioned.conditionCategoryCode = elig.hcc

left join
acute_hccs acuteHCCs
on unioned.conditionCategoryCode = acuteHCCs.excludeHCC

left join
acute_icds acuteICDS
on unioned.supportingEvidenceCode = acuteICDS.excludeICD
),


closed as (

select * ,
from (
select distinct * ,
DENSE_RANK() OVER
(PARTITION BY category  ORDER BY tier, closedDate desc nulls last) AS ranked
from
(select *
from
(SELECT distinct
"elation" as closedSource,
2 as tier,
concat(patientId, hcc) as category,
min(date_trunc(appt_time,day)) as closedDate

FROM
{{ref('mrt_elation_provider_code_decision')}}

where
extract(year from appt_time)  = extract(year from current_date())
and result in ('DISCONFIRMED', 'RESOLVED')

group by
concat(patientId, hcc)
)

union distinct

(select distinct
"payer_mor_other" as closedSource,
4 as tier,
concat(patientId, conditionCategoryCode) as category,
min(case when extract(year from clmLatest) = extract(year from current_date()) then clmlatest end) as closedDate

from
all_suspects

where
ConditionStatus in ('REJECTED','CLOSED')
and extract(year from dateUploaded) = extract(year from current_Date())

group by
concat(patientId, conditionCategoryCode)
)

union distinct

(select distinct
"chart_review" as closedSource,
1 as tier,
concat(patientId, conditionCategoryCode) as category,
min(case when extract(year from clmLatest) = extract(year from current_date()) then clmlatest end) as closedDate

from
opptys_chart_review

where
ConditionStatus in ('REJECTED','CLOSED')
and extract(year from dateUploaded) = extract(year from current_Date())

group by
concat(patientId, conditionCategoryCode)
)

union distinct

(select distinct
'risk_claims_patients' as closedSource,
3 as tier,
concat(patientId, hcc) as category,
min(evidenceDate) as closedDate

FROM
{{ref('risk_claims_patients')}}

where
HCC is not null
and clmyear = extract(year from current_date())
and hccEligibleSpecialist = true
and validClaim = true

group by
concat(patientId, hcc)
)
)
)
where ranked = 1
),


HCCranking as (

Select distinct
partner,
patientId,
lineOfBusiness,
conditionType ,
case when conditionType = 'PERS' then 1
     when conditionType = 'SUSP' then 2
           end as conditionTypeValue,
conditionSource,
conditionCategory,
conditionCategoryCode,
conditionName,
suspectHccElig,
maxHccYear,
conditionStatus,
underlyingDxCode,
confidenceLevel,
confidenceNumeric,
supportingEvidenceCode,
supportingEvidenceCodeType,
supportingEvidenceValue ,
supportingEvidenceName,
supportingEvidenceSource,
dateUploaded,
providerFirstName,
providerLastNameLegalName,
clmCount,
clmLatest,
rowNum1,
clmFlg,
minHcc1,
case when cast(minhcc1 as string) is null then concat(ConditionCategory,ConditionCategoryCode)
       else concat(ConditionCategory,cast(minhcc1 as string))  end as HCCRollup,
hcc1New,
hccClosedFlag,
closedSource,
closedDate,
rownum,
runDate

from
(
select distinct
a.*
,case when closed.category is null then 'no' else 'yes' end as hccClosedFlag
,closedDate
,closedSource
,ROW_NUMBER() OVER(PARTITION BY patientId, hcc1New ORDER BY patientId, hcc1New, confidenceLevel, ConditionCategoryCode, ifnull(clmCount,0) desc , clmLatest desc, case when providerLastNameLegalName is null then 'z' else  ProviderLastNameLegalName end asc)  AS rowNum
from
(select distinct
 partner,
 patientId,
 lower(lineOfBusiness) as lineOfBusiness,
 conditionType, ConditionSource,
 conditionCategory,
 conditionCategoryCode,
 conditionName,
 suspectHccElig,
 maxHccYear,
 conditionStatus,
 underlyingDxCode,
 case when confidenceLevel = 'VERY HIGH' then 1
      when confidenceLevel = 'HIGH' then 2
      when confidenceLevel = 'MEDIUM' then 3
      when confidenceLevel = 'LOW' then 4
      when confidenceLevel = 'VERY LOW' then 5
       end as confidenceLevel,
 confidenceNumeric,
 supportingEvidenceCode,
 supportingEvidenceCodeType,
 supportingEvidenceValue,
 supportingEvidenceName,
 supportingEvidenceSource,
 dateUploaded ,ProviderFirstName,
 providerLastNameLegalName,
 ifnull(clmCount,0) as clmCount,
 clmLatest,
 null as rownum1,
 clmFlg ,
 minhcc1,
case when cast(minhcc1 as string) is null then conditionCategoryCode
      else cast(minhcc1 as string)
           end as hcc1New,
           current_date() as runDate

          from
          all_suspects r

          left join
          (select distinct
           min(hcc1) as minhcc1,
           hcc2
           from codesets_hcc_hierarchies_xwalk

           where
           version = 'v24' group by hcc2
           ) h
           on r.ConditionCategoryCode = cast(h.hcc2 as string)

           left join
           codesets_hcc2020 ref
           on r.ConditionCategoryCode = ref.hcc_v24

           where
            (ref.hcc_v24_is_chronic = 'Yes' and conditionType = 'PERS')
            or
            (ref.hcc_v24_is_chronic is null and conditionType = 'SUSP' or ref.hcc_v24_is_chronic = 'Yes' and conditionType = 'SUSP')

order by
patientId, conditionCategoryCode
) a
left join
closed
on
concat(a.patientID, a.conditionCategoryCode)  = closed.category
and
extract(year from runDate) = extract(year from closedDate)
)
),


types_susp as (

select distinct
patientID as patientID1,
HCCRollup as hccNew1,
count(distinct conditionType) as conditiontypecounts,
min(case when conditionType ='PERS' then rownum end) as maxPERS,
min(case when conditionType ='SUSP' then rownum end) as maxSUSP

from
HCCranking
group by
patientId,
HCCRollup
--having count(distinct conditionType) > 1
),


prioritized_suspects as (
select distinct
a.*,
conditiontypecounts,
maxPERS,
maxSUSP,
rownum as newRownum

from
HCCranking a

left join
types_susp b
on patientID = patientID1
and HCCRollup = hccNew1
order by patientId, conditionCategoryCode
),


final as (

select distinct
partner,
patientId,
lineOfBusiness,
conditionType,
conditionTypeValue,
conditionSource,
conditionCategory,
conditionCategoryCode,
hcc1New,
HCCRollup,
conditiontypecounts,
conditionName,
suspectHccElig,
maxHccYear,
hccClosedFlag,
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
clmFlg,
case
    when
    (suspectHccElig = 'no'
    and conditionTypeCounts = 1 ) then null
    when
    hccClosedFlag = 'yes'
    then null
else newRownum
end as newRownum,
runDate,
cast(closedDate as string) as closedDate,
closedSource

from
prioritized_suspects

order by
patientID,
ConditionCategoryCode,
newRownum
)

select * from final