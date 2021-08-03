
----------------------------
--- Section One: Configs ---
----------------------------


--ref payer list
{{
  config(
        tags=["payer_list"]
  )
}}


--tables only exist in silver
{%- set payer_list = ["emblem_silver", "cci_silver"] -%}


-------------------------------------
--- Section Two: Reference Tables ---
-------------------------------------


--HCC reference data
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


--member, payer, and lob info
member as (
select distinct
patientId, 
memberId,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
isHARP = false
),


--able hcc gaps
hcc_gaps as (
select distinct *
from
{{ ref('hcc_gaps')}}
),


--Commons patient Data
commons_patient as (
select * 
from
{{ source('commons','patient')}} 
),


--provider NPI info
npidata as (
select * 
from
{{ source('nppes','npidata_20190512')}}
),


--dx info all sources
abs_diagnoses as (
select distinct 
patientID,
lineOfBusiness, 
partner,
clmYear,
evidenceDate,
encounterType,
dxCode,
claimType,
dxDescription,
providerNPI,
hcc,
HCCDescription,
memhcccombo
from 
{{ ref('risk_claims_patients') }} 
where hcc is not null
),


--gaps from tufts hcc oppty source (to be replaced)
tuftsgaps as (
SELECT * 
FROM
{{ source('abs_risk','opptys_tufts_temp')}} 
where 
sus_year = cast(extract(year from current_date()) as string)
),


--gaps from emblem hcc oppty source specific to Digital Cohort (to be replaced)
digitalgaps as (
SELECT * 
FROM
{{ source('abs_risk','opptys_emblem_digital_temp')}} 
),


---------------
---Able Data---
---------------


---NMI to Patient ID Crosswalk
crosswalk as (
SELECT distinct
patientId,
NMI as nmiId
FROM 
{{ ref('master_member_v1') }}

where 
NMI is not null
),


--List of able diagnoses in 2020
-- able as ( 
-- SELECT distinct
-- ab.patientID as patientId,
-- mem.partner, 
-- mem.lineOfBusiness,
-- 'able' as source,
-- 'PERS' as conditionType,
-- cast(ab.hccs as string) as conditionCategoryCode,
-- replace(codeValue,".","") as underlyingDxCode,
-- codeDescription,
-- 'HIGH' as confidenceLevel,
-- opportunityStatus,
-- case when lower(mem.lineOfBusiness) = 'medicare' then 'HCC'
--      when lower(mem.lineOfBusiness) = 'm' then 'HCC'
--      when lower(mem.lineOfBusiness) in ('dsnp', 'duals') then 'HCC'
--      when lower(mem.lineOfBusiness) = 'hmo' then 'HHS'
--      when lower(mem.lineOfBusiness) = 'ps' then 'HHS'
--      when lower(mem.lineOfBusiness) = 'commercial' then 'HHS'
--      when lower(mem.lineOfBusiness) = 'medicaid' then '3M CRG'
--           end as conditionCategory,
-- extract(date from ab.createdAt)  as dateUploaded
-- FROM  
-- hcc_gaps ab

-- INNER join 
-- member mem
-- on ab.patientid = mem.patientid

-- INNER JOIN  
-- commons_patient p 
-- on ab.patientid = p.id

-- WHERE
-- extract(YEAR from ab.createdAt) = extract(YEAR from current_date())
-- and lower(mem.lineOfBusiness) not in ('m','medicare','duals','dsnp','dual')
-- and (((lower(partner) = 'tufts' and p.homeMarketID = '76150da6-b62a-4099-9bd6-42e7be3ffc62')
-- or
-- (lower(partner) = 'connecticare' and p.homeMarketID = '9065e1fc-2659-4828-9519-3ad49abf5126' )
-- or
-- (lower(partner) = 'emblem' and p.homeMarketID = '3d8e4e1d-bdb5-4676-9e25-9b6fa248101f')))
-- ),


-- --List of able diagnoses in 2020, limited to the most recent file
-- able2 as (
-- select
-- patientId,
-- partner,
-- lineOfBusiness,
-- source,
-- conditionType,
-- conditionCategory,
-- underlyingDxCode,
-- codeDescription,
-- confidenceLevel,
-- opportunityStatus,
-- dateUploaded,
-- split(conditionCategoryCode, ', ') as hccs1,
-- from 
-- able c
-- where 
-- dateUploaded in (select max(dateUploaded) as dateUploaded from able)
-- ),


-- --Able data has multiple HCCs per line. Unnesting the array we just created, to get one row per member-hcc
-- able3 as (
-- select distinct * except (hccs1)
-- from 
-- able2, 
-- unnest(hccs1) as conditionCategoryCode
-- ),
 
 
--  --Final list of Able HCCs Suspects for most recent data. Also includes HCC Description  
-- suspects_able as (
-- select distinct
-- partner  ,
-- patientId,
-- lineOfBusiness  ,
-- conditionType,
-- source as ConditionSource,
-- conditionCategory,
-- conditionCategoryCode,
-- case when hccdescr = 'Diabetes without Complication' then 'Diabetes without Complications'
--      else hccdescr
--           end as ConditionName,
-- opportunityStatus as conditionStatus,
-- underlyingDxCode,
-- confidenceLevel,
-- 1 as confidenceNumeric,
-- underlyingDxCode as SupportingEvidenceCode,
-- 'ICD' as supportingEvidenceCodeType,
-- cast(null as string) as supportingEvidenceValue,
-- codeDescription as supportingEvidenceName,
-- 'claims' as supportingEvidenceSource,
-- dateUploaded 
-- from 
-- able3 as ab

-- LEFT JOIN 
-- hcc_codes_2020 ref 
-- on ab.conditionCategoryCode = ref.hcc_v24 

-- LEFT JOIN 
-- risk_hcc_descript hcc 
-- on ab.conditionCategoryCode = hcc.hcc

-- where 
-- ((lower(lineOfBusiness) in ('medicare','commercial','duals','dsnp') 
-- and ref.hcc_v24_is_chronic = 'Yes') or lineOfBusiness = 'medicaid')
-- ),


-- --pull min status og HCC
-- recapture_suspects_able as (
-- select 
-- partner ,
-- patientId,
-- lineOfBusiness,
-- conditionType,
-- ConditionSource,
-- conditionCategory,
-- conditionCategoryCode,
-- ConditionName,
-- minstatus as conditionStatus,
-- underlyingDxCode,
-- confidenceLevel,
-- confidenceNumeric,
-- SupportingEvidenceCode,
-- supportingEvidenceCodeType,
-- supportingEvidenceValue,
-- supportingEvidenceName,
-- supportingEvidenceSource,
-- dateUploaded 
-- from 
-- suspects_able a

-- inner join
--   (select distinct
--    patientId as minid,  
--    conditionCategoryCode as mincc, 
--    min(conditionStatus) as minstatus 
--    from 
--    suspects_able 
--    group by 
--    patientId,  
--    conditionCategoryCode
--    ) b
-- on patientId = minid
-- and conditionCategoryCode = mincc
-- ),

--------------
--Payer Data--
--------------

--using macro to include all payers: This will be more effective when data is in gold schema

{% for source_name in payer_list %}

{{ source_name }}_claims as (

select * from

---Emblem Data---
  (SELECT distinct
  nmi,
  'emblem' as partner,
  'payer_emblem' as source,
  conditionType,
  conditionCategoryCode,
  case when trim(conditionCategoryDescription) = 'Diabetes without Complication'  then 'Diabetes without Complications'
       else conditionCategoryDescription
            end as conditionName,
  underlyingDxCode,
  description as underlyingDxCodeDescription,
  confidenceLevel,
   case when confidenceLevel = 'VERY HIGH' then 1
        when confidenceLevel = 'HIGH' then 2
        when confidenceLevel = 'MEDIUM' then 3
        when confidenceLevel = 'LOW' then 4
        when confidenceLevel = 'VERY LOW' then 5
             end as confidenceNumeric,
  riskAdjustmentModel,
  'OPEN' as opportunityStatus,
  dateUploaded
    from
        (select distinct  
         case when _TABLE_SUFFIX like '%med%' then 'med' 
              else 'other'  
                   end as alltypes,
          data.nmi,
          data.conditionType,
          data.confidenceLevel,
          data.conditionCategoryCode,
          data.conditionCategoryDescription,
          data.underlyingDxCode ,
          data.riskAdjustmentModel,
          PARSE_DATE('%Y%m%d', data.dateUploaded)  as dateUploaded
         from
          {{ source('emblem_silver', 'SuspectCodes_*') }} clm 
          where data.conditionCategoryCode is not null 
          and data.dateUploaded <> 'dateUploaded') fullset
         left join 
          hcc_codes_2020
          on diagnosis_code = underlyingDxCode
          inner join
         (select distinct
          max(PARSE_DATE('%Y%m%d', data.dateUploaded )) as max_date,
          case when _TABLE_SUFFIX like '%med%' then 'med' else 'other' end as max_type
          FROM 
                 {{ source('emblem_silver', 'SuspectCodes_*') }} clm 
          where data.conditionCategoryCode is not null 
          and data.dateUploaded <> 'dateUploaded'

         group by 
         case when _TABLE_SUFFIX like '%med%' then 'med' else 'other' end) maxdate
           on  fullset.dateUploaded = maxdate.max_date
           and fullset.alltypes = maxdate.max_type)

union all

----CCI Data---
  (SELECT distinct
  nmi,
  'connecticare' as partner,
  'payer_connecticare' as source,
  "PERS" as conditionType,
  LTRIM(REPLACE(conditionCategoryCode,"HCC",""),"0") as conditionCategoryCode,
  conditionCategoryDescription as ConditionName,
  underlyingDxCode,
  underlyingDxCodeDescription,
  'HIGH' as confidenceLevel,
  1 as confidenceNumeric,
  riskAdjustmentModel,
  'OPEN' as opportunityStatus,
  dateUploaded
    from
        (select distinct  
         case when _TABLE_SUFFIX like '%med%' then 'med' 
              else 'other' 
                   end as alltypes,
         data.nmi  ,
         data.conditionCategoryCode  ,
         data.conditionCategoryDescription,
         data.underlyingDxCode ,
         data.underlyingDxCodeDescription,
         data.riskAdjustmentModel,
         PARSE_DATE('%Y%m%d', data.dateUploaded )  as dateUploaded
         from
         {{ source('cci_silver', 'SuspectCodes_*') }} clm 
         where data.conditionCategoryCode is not null 
         and data.dateUploaded <> 'dateUploaded'
        ) fullset
        inner join
        (select distinct
         max(PARSE_DATE('%Y%m%d', data.dateUploaded )) as max_date,
         case when _TABLE_SUFFIX like '%med%' then 'med' else 'other' end as max_type
         FROM 
               {{ source('cci_silver', 'SuspectCodes_*') }} clm 
         where data.conditionCategoryCode is not null 
         and data.dateUploaded <> 'dateUploaded'
         group by 
         case when _TABLE_SUFFIX like '%med%' then 'med' else 'other' end) maxdate
         on  fullset.dateUploaded = maxdate.max_date
         and fullset.alltypes = maxdate.max_type)
)
{% if not loop.last -%} , {%- endif %}
{% endfor %}
,



--Final list of Payer HCCs Suspects for most recent data. Also includes HCC Description  
recapture_suspects_payer as ( 
select distinct
sus.partner,
c.patientID as patientId,
p.lineOfBusiness,
conditionType, 
sus.source as conditionSource,
case when lower(p.lineOfBusiness) = 'medicare' then 'HCC'
     when lower(p.lineOfBusiness) = 'hmo' then 'HHS'
     when lower(p.lineOfBusiness) = 'dsnp' then 'HCC'
     when lower(lineOfBusiness) like '%dual%' then 'HCC'
     when lower(p.lineOfBusiness) = 'medicaid' then '3M CRG'
     when lower(p.lineOfBusiness) = 'ps' then 'HHS'
     when lower(p.lineOfBusiness) = 'commercial' then 'HHS'
     when lower(p.lineOfBusiness) = 'm' then 'HCC'
          end as conditionCategory,
conditionCategoryCode,
case when lower(p.lineOfBusiness) = 'medicaid' then ConditionName
     when lower(p.lineOfBusiness) = 'commercial' then ConditionName
     else hccs.hccdescr end as conditionName,
opportunityStatus as conditionStatus,
underlyingDxCode,
confidenceLevel,
confidenceNumeric,
underlyingDxCode as supportingEvidenceCode,
'ICD' as supportingEvidenceCodeType,
cast(null as string)   as supportingEvidenceValue,
underlyingDxCodeDescription as supportingEvidenceName,
'claims' as supportingEvidenceSource,
dateUploaded
FROM 
   ({% for source_name in payer_list -%}
     select distinct *
     from  {{ source_name }}_claims  
   {% if not loop.last -%} union all {%- endif %}
   {% endfor %}
   ) sus

LEFT JOIN risk_hcc_descript hccs 
on sus.conditionCategoryCode = hccs.hcc

INNER JOIN crosswalk c
on sus.nmi = c.nmIid 

INNER JOIN member p 
on c.patientID = p.patientid 
),


recapture_suspects_digital as (
select distinct
'emblem' as partner ,
p.patientId,
p.lineOfBusiness,
ConditionType as conditionType  ,
'digital_load' conditionSource,
case when p.lineOfBusiness = 'medicaid' then '3M CRG' 
     when p.lineOfBusiness = 'commercial' then 'HHS' 
     else 'HCC' end as conditionCategory,
cast(c.Condition as string) as conditionCategoryCode,
case when p.lineOfBusiness = 'medicaid' then conditionDescription 
     when p.lineOfBusiness = 'commercial' then conditionDescription 
     else hccs.hccdescr 
          end as conditionName,
'OPEN' as conditionStatus ,
cast(null as string) as underlyingCode ,
confidenceLevel as confidence,
case when upper(confidenceLevel) = 'VERY HIGH' then 1
     when upper(confidenceLevel) = 'HIGH' then 2
     when upper(confidenceLevel) = 'MEDIUM' then 3
     when upper(confidenceLevel) = 'LOW' then 4
     when upper(confidenceLevel) = 'VERY LOW' then 5
          else 5 end as confidenceNumeric,  
SPLIT(underlying_Cd,'_')[SAFE_OFFSET(1)] as supportingEvidenceCode,
SPLIT(underlying_Cd,'_')[SAFE_OFFSET(0)] as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue ,
cast(null as string) as supportingEvidenceName,
cast(null as string) as supportingEvidenceSource,
PARSE_DATE("%m/%d/%Y", "12/28/2021")  as dateUploaded 
from digitalgaps c

LEFT JOIN risk_hcc_descript hccs 
on cast(c.Condition as string) = hccs.hcc

LEFT JOIN member p 
on c.memberId = p.memberId 
),


recapture_suspects_tufts as (
select 
'tufts' partner ,
p.patientId,
p.lineOfBusiness,
case when Reason_code = 'Previously Coded' then 'PERS' else 'SUSP' end as conditionType ,
'tufts_file'conditionSource,
'HCC'  as conditionCategory,
cast(c.HCC as string) as conditionCategoryCode,
hccs.hccdescr as conditionName,
'OPEN' as conditionStatus ,
cast(null as string) as underlyingCode  ,
upper(likelihood) as confidence,
case when upper(likelihood) = 'VERY HIGH' then 1
     when upper(likelihood) = 'HIGH' then 2
     when upper(likelihood) = 'MEDIUM' then 3
     when upper(likelihood) = 'LOW' then 4
     when upper(likelihood) = 'VERY LOW' then 5
     else 5 
          end as confidenceNumeric,
cast(null as string) as supportingEvidenceCode,
case when upper(Reason_code) like '%RX%' then 'NDC'
     when upper(Reason_code) like '%DX%' then 'ICD'
     when upper(Reason_code) like '%PREVIOUSLY CODED%' then 'ICD'
     when upper(Reason_code) like '%MANIFEST%' then 'ICD'
     when upper(Reason_code) like '%CPT%' then 'CPT' 
     else upper(reason_code) 
          end as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue,
cast(Reason_description as string) as supportingEvidenceName,
cast(null as string) as supportingEvidenceSource,
PARSE_DATE("%m/%d/%Y","07/01/2021") as dateUploaded 
from tuftsgaps c

LEFT JOIN risk_hcc_descript hccs 
on c.HCC = hccs.hcc

LEFT JOIN member p 
on c.mem_no = p.memberId 
),


diagnoses_all_recapped as (
-- select * from recapture_suspects_able
-- union all
select * from recapture_suspects_payer
union all 
select * from recapture_suspects_tufts
union all
select * from recapture_suspects_digital
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
r.conditionName,
case when evYear = extract(year from current_date()) 
       or evYear = extract(year from current_date() - 3) then 'CLOSED' 
     else 'OPEN' 
           end as conditionStatus, 
r.underlyingDxCode,
r.confidenceLevel,
r.confidenceNumeric,
r.supportingEvidenceCode, 
r.supportingEvidenceCodeType,
r.supportingEvidenceValue,
r.supportingEvidenceName,
r.supportingEvidenceSource,
r.dateUploaded,
Provider_First_Name as providerFirstName, 
Provider_Last_Name_Legal_Name as providerLastNameLegalName,
1 as clmCount,
max(d.evidenceDate) as clmLatest

from diagnoses_all_recapped r 

inner join 
abs_diagnoses d
on r.underlyingDxCode  = d.dxCode
and r.patientId = d.patientID

left join
  (select distinct 
  extract(year from max(evidenceDate)) as evYear,
  concat(patientId, hcc) as category 
  from abs_diagnoses 
  group by 
  concat(patientId, hcc)
  ) closed
  on concat(r.patientId, r.conditionCategoryCode) = closed.category

left join 
npidata prov 
on cast(prov.npi as string) = d.providerNpi 

group by          
r.partner, r.patientId, r.lineOfBusiness, r.conditionType, r.conditionSource,r.conditionName,
r.ConditionCategory,r.ConditionCategoryCode, 
case when evYear = extract(year from current_date()) 
       or evYear = extract(year from current_date() - 3) then 'CLOSED' 
     else 'OPEN' 
           end, 
evYear,
r.underlyingDxCode,r.confidenceLevel,r.confidenceNumeric,r.supportingEvidenceCode,r.supportingEvidenceCodeType,
r.supportingEvidenceValue, r.supportingEvidenceName, r.supportingEvidenceSource,
r.dateUploaded, prov.Provider_First_Name, prov.Provider_Last_Name_Legal_Name
order by r.patientID, r.underlyingDxCode 
),


missing_claims as (
select distinct 
r.partner,
r.patientId,
r.lineOfBusiness,  
r.conditionType,  
r.conditionSource,
r.conditionCategory,
r.conditionCategoryCode,  
r.conditionName,
r.conditionStatus,
r.underlyingDxCode,
r.confidenceLevel,
r.confidenceNumeric,
r.supportingEvidenceCode, 
r.supportingEvidenceCodeType,
r.supportingEvidenceValue,
r.supportingEvidenceName,
r.supportingEvidenceSource,
r.dateUploaded,
Provider_First_Name as providerFirstName, 
Provider_Last_Name_Legal_Name as providerLastNameLegalName,
0 as clmCount,
cast(null as date) as clmLatest
from 
diagnoses_all_recapped r 

left join 
abs_diagnoses d
on r.underlyingDxCode = d.dxCode
and r.patientId = d.patientID

left join 
npidata   prov 
on cast(prov.npi as string) =  d.providerNpi 

where 
d.dxCode is null
and concat(r.patientId, r.ConditionCategoryCode)  not in (select distinct concat(patientId, conditionCategoryCode) as concat from in_claims)
),


diagnoses_all as (
select alll.* 
from
  (select * from missing_claims
  union all
  select * from in_claims
  ) alll

where 
patientID is not null
)


select * from diagnoses_all
