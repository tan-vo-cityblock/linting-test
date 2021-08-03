
{{ config(
materialized = "incremental",
tags = ["evening"]
)
}}

------pull in rollup

with rolledup as (

select *
from
{{source('codesets','hcc_rollup_2020')}}
),


--closed claims history current year
newAddsAndClosures as (

SELECT DISTINCT
cast(format_date("%m/%d/%Y",current_date()) as string) as date,
mrn.mrn as patient_id,
"Elation" as patient_system,
upper(p.firstName) as first_name,
upper(p.lastName) as last_name,
cast(format_date("%m/%d/%Y",dateOfBirth) as string) as date_of_birth,
CASE WHEN pi.gender = "male" then "M" WHEN pi.gender = "female" then "F" else "U" END AS gender,


concat(case when conditionType = 'PERS' and underlyingDxCode is not null then CONCAT("CB_",dx.conditionCategoryCode ,"_",underlyingDxCode)
     when conditionType = 'PERS' and underlyingDxCode is null then CONCAT("CB_SUSP_", dx.conditionCategoryCode)
     when conditionType = 'SUSP' then CONCAT("CB_SUSP_", dx.conditionCategoryCode) end,"_2021") as measure_code,

case when conditionStatus = 'CLOSED' then 'CLOSED' else 'OPEN' end as measure_status,
case when partner = 'tufts' then '1538441167'
     when partner = 'connecticare' then '1487713947' end as provider_assigned_npi,
case when partner = 'tufts' then 'Taylor, James'
     when partner = 'connecticare' then 'Lennon, Stephanie' end as provider_assigned,
case when partner = 'tufts' then 'Cityblock - Massachusetts'
     when partner = 'connecticare' then 'Cityblock - Connecticut' end as practice_name,
evidence,
current_datetime() as rundate,
dx.conditionCategoryCode,
dx.patientID

FROM
{{ref('mrt_risk_opportunities')}}  dx

left JOIN
{{source('codesets','ICD10toHCCcrosswalk')}} icd
on icd.DiagnosisCode  = dx.underlyingDxCode

LEFT JOIN
{{ source('member_index', 'member') }} m
on m.id = dx.patientID

LEFT JOIN
{{ source('member_index', 'mrn') }} mrn
on mrn.id = m.mrnId

LEFT JOIN
{{ source('commons', 'patient') }} p
on p.id = dx.patientid

LEFT JOIN
{{ source('commons', 'patient_info') }} pi
on pi.patientid = dx.patientid

where
lower(partner) in ( 'connecticare', 'tufts')
and lower(p.lineOfBusiness) in ( 'medicare', 'dsnp','dual','duals')

order by
mrn.mrn
),


-- call incremental table
{% if is_incremental() %}

existingtable as (

select distinct
date,
patient_id,
patient_system,
first_name,
last_name,
date_of_birth,
gender,
measure_code,
measure_status,
provider_assigned_npi,
provider_assigned,
practice_name,
evidence,
rundate,
conditionCategoryCode,
patientID

from
{{ this }}

),

{% endif %}


-- add grouping to existing table
{% if is_incremental() %}

existing as (

select
existingtable.* ,
case when rolledup.hcc_rollup is null then conditionCategoryCode
          else rolledup.hcc_rollup
               end as hcc_rollup_existing

from
existingtable

LEFT JOIN
rolledup
on conditionCategoryCode = rolledup.hcc
),

{% endif %}


closedInClaims as (

select distinct
patientId, hcc,
concat(patientId, hcc) as category,
concat(ifnull(dxCode,''), ' last documented ', ifnull(cast(evidenceDate as string),' date unknown') ,' by ', ifnull(providerName ,'an unknown provider')) as evidence,
2 as tier

from
(select distinct
patientID,
evidenceDate,
dxCode,
hcc,
providerName,
DENSE_RANK() OVER
(PARTITION BY patientID, hcc  ORDER BY evidenceDate desc, dxCode, providerName  nulls last ) AS Rank

FROM
{{ref('risk_claims_patients')}}

where
HCC is not null
and clmyear = extract(year from current_date())
and lineOfBusiness <> 'commercial'
and hccEligibleSpecialist = true
and validClaim = true
)

where
rank = 1
),


--elation decision this year
elationDecision as (

SELECT distinct
patientId, hcc,
concat(patientId, hcc) as category,
concat(Result , " on " ,extract(date from signed_time) ," by ", first_name, " ", last_name,". Decision: ",description) as evidence,
1 as tier

FROM
{{ref('mrt_elation_provider_code_decision')}}

where
extract(year from signed_time)  = extract(year from current_date())
and result in ('DISCONFIRMED', 'RESOLVED')
),


--closed in suspect Ranked
rankedClosed as (

select distinct
patientId, conditionCategoryCode,
concat(patientId, conditionCategoryCode) as category,
evidence,
3 as tier

from
newAddsAndClosures

where
measure_status in ('REJECTED','CLOSED')
),


joined as (

select * from elationDecision
union distinct
select * from rankedClosed
union distinct
select * from closedInClaims
),


--pull in closed HCCs anywhere
closed as (

select dx.*,
mrn.mrn as elationID,
case when rolledup.hcc_rollup is null then dx.hcc
          else rolledup.hcc_rollup
               end as hcc_rollup_closed

from
(select *
from
(select distinct * ,
DENSE_RANK() OVER
(PARTITION BY category  ORDER BY tier, evidence ) AS ranked

from
joined
)
where ranked =1 ) dx

LEFT JOIN
rolledup
on dx.hcc = rolledup.hcc

LEFT JOIN
{{ source('member_index', 'member') }} m
on m.id = dx.patientID

LEFT JOIN
{{ source('member_index', 'mrn') }} mrn
on mrn.id = m.mrnId
),


{% if is_incremental() %}

removal as (

select distinct
existing.date,
existing.patient_id,
existing.patient_system,
existing.first_name,
existing.last_name,
existing.date_of_birth,
existing.gender,
existing.measure_code,
"CLOSED" as measure_status,
existing.provider_assigned_npi,
existing.provider_assigned,
existing.practice_name,
closed.evidence as additional_context,
current_datetime() as rundate,
existing.conditionCategoryCode,
existing.patientID

from
existing

left join
closed
on existing.patient_ID = closed.elationID
and existing.hcc_rollup_existing = closed.hcc_rollup_closed
and existing.conditionCategoryCode >= closed.hcc

where
measure_status ="OPEN"
and closed.category is not null

),

{% endif %}


newAdds as (

select a.*

from
newAddsAndClosures a

{% if is_incremental() %}

left join
removal b
on a.patientID = b.patientID
and a.conditionCategoryCode = b.conditionCategoryCode

where
b.conditionCategoryCode is null

{% endif %}
),



combined as (

select * from newAdds

{% if is_incremental() %}

union distinct

select * from removal

{% endif %}
),



final as (

select p.*
from
combined p

{% if is_incremental() %}
left join
existingtable l
using (Patient_ID, Measure_Code,Measure_Status,evidence)

where
l.evidence is null

{% endif %}
)

select * from final