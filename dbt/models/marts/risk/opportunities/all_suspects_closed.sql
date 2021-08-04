{{
  config(
       tags=["evening"]
       )
}}

with rolledup as (

select *
from
{{source('codesets','hcc_rollup_2020')}}
),

elation as (

SELECT distinct
"elation" as closedSource,
2 as tier,
patientId,
hcc as conditionCategoryCode,
concat(patientId, hcc) as category,
date_trunc(appt_time,day) as closedDate

FROM
{{ref('mrt_elation_provider_code_decision')}}

where
extract(year from appt_time)  = extract(year from current_date())
and result in ('DISCONFIRMED', 'RESOLVED')
),


patients as (

select distinct
'risk_claims_patients' as closedSource,
3 as tier,
patientId,
hcc,
concat(patientId, hcc) as category,
evidenceDate as closedDate

FROM
{{ ref('risk_claims_patients') }}

where
HCC is not null
and clmyear = extract(year from current_date())
and hccEligibleSpecialist = true
and validClaim = true
),


chart_review as (

select distinct
"chart_review" as closedSource,
1 as tier,
patientId,
conditionCategoryCode,
concat(patientId, conditionCategoryCode) as category,
case when extract(year from safe.PARSE_DATE('%m/%d/%Y', reviewDate)) = extract(year from current_date()) then parse_date('%m/%d/%Y', reviewDate) end as closedDate

from
{{ ref('risk_chart_review') }}

where
ConditionStatus in ('REJECTED','CLOSED')
and extract(year from safe.PARSE_DATE('%m/%d/%Y', reviewDate))= extract(year from current_Date())
and safe.PARSE_DATE('%m/%d/%Y', reviewDate) > PARSE_DATE('%m/%d/%Y', '4/7/2021')
),


unioned as (

select * from elation
union distinct
select * from chart_review
union distinct
select * from patients
),


grouped as (

select distinct dx.* ,
coalesce(rolledup.hcc_rollup, dx.conditionCategoryCode) as hcc_rollup

from
unioned dx

LEFT JOIN
rolledup
on dx.conditionCategoryCode = rolledup.hcc
),


final as (

select * from
(select distinct *,
DENSE_RANK() OVER
(PARTITION BY patientId,hcc_rollup  ORDER BY conditionCategoryCode, tier,  closedDate desc nulls last) AS ranked

from
grouped
) dx

where
ranked = 1

)

select *
except (ranked)
from final