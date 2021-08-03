with patients as (

SELECT distinct
patientid

FROM
{{ ref('member') }}
),


cdps as (

select distinct
*
FROM
{{ ref('agg_member_year_risk_cdps') }}
),


UNPIVOTED AS (

SELECT
memberId as patientid,
year,
age,
gender,
case when partner = 'Emblem Health' then 'emblem'
     when partner = 'CareFirst' then 'carefirst'
          else lower(partner) end as partner,
  lower(lineofbusiness) lineofbusiness,
  [ STRUCT("scoreSKCL" AS Metric, scoreSKCL AS Data),
    STRUCT("scoreSKCM" AS Metric, scoreSKCM AS Data),
    STRUCT("scoreSKCVL" AS Metric, scoreSKCVL AS Data),
    STRUCT("scorePRGINC" AS Metric, scorePRGINC AS Data),
    STRUCT("scoreCARL" AS Metric, scoreCARL AS Data),
    STRUCT("scorePRGCMP" AS Metric, scorePRGCMP AS Data),
    STRUCT("scoreCNSL" AS Metric, scoreCNSL AS Data),
    STRUCT("scoreSKNVL" AS Metric, scoreSKNVL AS Data),
    STRUCT("scoreCANH" AS Metric, scoreCANH AS Data),
    STRUCT("scoreCANL" AS Metric, scoreCANL AS Data),
    STRUCT("scoreCANM" AS Metric, scoreCANM AS Data),
    STRUCT("scoreGIL" AS Metric, scoreGIL AS Data),
    STRUCT("scoreCNSM" AS Metric, scoreCNSM AS Data),
    STRUCT("scoreCERL" AS Metric, scoreCERL AS Data),
    STRUCT("scoreEYEVL" AS Metric, scoreEYEVL AS Data),
    STRUCT("scoreEYEL" AS Metric, scoreEYEL AS Data),
    STRUCT("scoreINFM" AS Metric, scoreINFM AS Data),
    STRUCT("scoreGIM" AS Metric, scoreGIM AS Data),
    STRUCT("scoreSUBL" AS Metric, scoreSUBL AS Data),
    STRUCT("scoreMETM" AS Metric, scoreMETM AS Data),
    STRUCT("scorePULL" AS Metric, scorePULL AS Data),
    STRUCT("scoreINFL" AS Metric, scoreINFL AS Data),
    STRUCT("scoreGENEL" AS Metric, scoreGENEL AS Data),
    STRUCT("scoreMETH" AS Metric, scoreMETH AS Data),
    STRUCT("scoreDIA2M" AS Metric, scoreDIA2M AS Data),
    STRUCT("scorePULM" AS Metric, scorePULM AS Data),
    STRUCT("scoreMETVL" AS Metric, scoreMETVL AS Data),
    STRUCT("scoreRENM" AS Metric, scoreRENM AS Data),
    STRUCT("scoreCARM" AS Metric, scoreCARM AS Data),
    STRUCT("scorePSYL" AS Metric, scorePSYL AS Data),
    STRUCT("scoreHEMM" AS Metric, scoreHEMM AS Data),
    STRUCT("scoreBABY5" AS Metric, scoreBABY5 AS Data),
    STRUCT("scoreSKNL" AS Metric, scoreSKNL AS Data),
    STRUCT("scoreDDL" AS Metric, scoreDDL AS Data),
    STRUCT("scorePSYM" AS Metric, scorePSYM AS Data),
    STRUCT("scorePSYML" AS Metric, scorePSYML AS Data),
    STRUCT("scoreBABY6" AS Metric, scoreBABY6 AS Data),
    STRUCT("scorePSYH" AS Metric, scorePSYH AS Data),
    STRUCT("scoreSKNH" AS Metric, scoreSKNH AS Data),
    STRUCT("scoreRENL" AS Metric, scoreRENL AS Data),
    STRUCT("scoreINFH" AS Metric, scoreINFH AS Data),
    STRUCT("scoreCNSH" AS Metric, scoreCNSH AS Data),
    STRUCT("scoreHEML" AS Metric, scoreHEML AS Data),
    STRUCT("scoreGIH" AS Metric, scoreGIH AS Data),
    STRUCT("scoreCANVH" AS Metric, scoreCANVH AS Data),
    STRUCT("scoreBABY4" AS Metric, scoreBABY4 AS Data),
    STRUCT("scoreDIA1M" AS Metric, scoreDIA1M AS Data),
    STRUCT("scoreHIVM" AS Metric, scoreHIVM AS Data),
    STRUCT("scoreDIA2L" AS Metric, scoreDIA2L AS Data),
    STRUCT("scoreCARVH" AS Metric, scoreCARVH AS Data),
    STRUCT("scoreSUBVL" AS Metric, scoreSUBVL AS Data),
    STRUCT("scoreAIDSH" AS Metric, scoreAIDSH AS Data),
    STRUCT("scoreCAREL" AS Metric, scoreCAREL AS Data),
    STRUCT("scorePULVH" AS Metric, scorePULVH AS Data),
    STRUCT("scoreBABY1" AS Metric, scoreBABY1 AS Data),
    STRUCT("scorePULH" AS Metric, scorePULH AS Data),
    STRUCT("scoreBABY3" AS Metric, scoreBABY3 AS Data),
    STRUCT("scoreRENEH" AS Metric, scoreRENEH AS Data),
    STRUCT("scoreBABY2" AS Metric, scoreBABY2 AS Data),
    STRUCT("scoreBABY8" AS Metric, scoreBABY8 AS Data),
    STRUCT("scoreBABY7" AS Metric, scoreBABY7 AS Data),
    STRUCT("scoreRENVH" AS Metric, scoreRENVH AS Data),
    STRUCT("scoreDIA1H" AS Metric, scoreDIA1H AS Data),
    STRUCT("scoreDDM" AS Metric, scoreDDM AS Data),
    STRUCT("scoreHEMVH" AS Metric, scoreHEMVH AS Data),
    STRUCT("scoreHEMEH" AS Metric, scoreHEMEH AS Data),
    STRUCT("scoreHLTRNS" AS Metric, scoreHLTRNS AS Data),
    STRUCT("demoScore" AS Metric, demoScore AS Data)
  ] AS Metrics_Data

FROM
cdps

inner join
patients
on memberid = patientid
),


cdps_final as (

select DISTINCT
partner,
lineOfBusiness,
patientid,
year,
age,
gender,
--cdps_final.LOB = (Medicaid (HARP), Medicaid (Non-Harp). When left joining hcc_risk_members on LOB they should match (hcc_risk_members.lob = medicaid)
case when lineofbusiness like '%medicaid%' then 'medicaid'
     else null end as lineofbusiness_medicaid,
case when UNMETRICS.Metric = 'demoScore' then 'DEMO'
     else regexp_replace(UNMETRICS.Metric,'score','') end as CDPSCode,
UNMETRICS.Data AS CDPSCoefficient,
case when UNMETRICS.Metric like '%demo%' then "DEMO"
     else "CDPS GROUP" end as component,
UNMETRICS.Metric as cdpsRawCodeWithScore

from
UNPIVOTED ,
    UNNEST(METRICS_DATA) AS UNMETRICS

WHERE
UNMETRICS.Data IS NOT NULL and UNMETRICS.Data <> 0
),


descriptions_hccrisk as (

select
year,
cdps_final.partner,
cdps_final.lineOfBusiness,
monthCount,
cdps_final.patientid,
age,
cdps_final.gender,
case when component = 'DEMO' then 'DEMO'
    else "NEW" end as conditiontype,
CDPSCode as cdpsCode,
case when component = 'DEMO' then 'DEMO'
    else description end as CDPSDescription,
CDPSCoefficient,
component,
"CDPS" as model

from
cdps_final

left join
{{ source('cdps', 'cdps_cat_descriptions') }}
    on CDPSCode = category

left join
{{ref('hcc_risk_members')}}
    on cdps_final.patientid = hcc_risk_members.patientid
    and cdps_final.partner = hcc_risk_members.partner
    and cdps_final.lineofbusiness_medicaid = hcc_risk_members.lineofbusiness
    and cdps_final.year = hcc_risk_members.eligYear
    and cdps_final.gender = hcc_risk_members.gender

where
cdps_final.lineofbusiness like '%medicaid%'
),


final as (

select
distinct
year,
partner,
lineOfBusiness,
monthCount,
cast(null as string) as esrdStatus,
cast(null as string) as hospiceStatus,
cast(null as string) as institutionalStatus,
cast(null as string) as disenrolledYear,
cast(null as string) as newEnrollee,
component,
conditiontype,
concat(component,' ',conditiontype) as combinedTitle,
patientid,
age,
gender,
cdpsCode,
CDPSDescription,
concat(patientID,cdpsCode) as memberCDPScombo,
CDPSCoefficient,
cast(null as string) as Original_Reason_for_Entitlement_Code_OREC,
model

from
descriptions_hccrisk
)


select * from final
