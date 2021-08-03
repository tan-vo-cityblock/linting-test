{{ config(
        materialized = "incremental",
        tags = ["evening"]
   )
}}

With pers as (
SELECT distinct
 case when lower(partner) = 'connecticare' then "ConnectiCare"
      when lower(partner)  = 'tufts' then 'Tufts' end as plan,


concat(case when conditionType = 'PERS' and underlyingDxCode is not null then CONCAT("CB_",conditionCategoryCode ,"_",underlyingDxCode)
     when conditionType = 'PERS' and underlyingDxCode is null then CONCAT("CB_SUSP_", conditionCategoryCode) end,"_2021") as measure_code,

CONCAT("HCC:", conditionCategoryCode) as additional_codes,

case when underlyingDxCode is not null then RTRIM(RPAD(CONCAT("Assess ", icd.description ),58))
     else RTRIM(RPAD(CONCAT("Member suspected to have ", p.conditionName ),58))
          end as name,


case when underlyingDxCode is not null then CONCAT("Available claims data suggests that this member was diagnosed with ", icd.description, " in the past 2 years")
     else CONCAT("Based on available member data, we suspect that member may have ", p.conditionName)
          end as details,

case when underlyingDxCode is not null  and BYTE_LENGTH(p.underlyingDxCode) >3 then concat(SUBSTR(p.underlyingDxCode,1,3),'.',SUBSTR(p.underlyingDxCode,4,6))
     when underlyingDxCode is null then  CONCAT("HCC",p.conditionCategoryCode ," CONFIRMED")
          else p.underlyingDxCode
               end as code_1,

"Yes, member has this condition" as code_1_description,

case when conditionType = 'PERS' and underlyingDxCode is not null then "problem_with_assessment_billing"
     when conditionType = 'PERS' and underlyingDxCode is null then "doctag_by_code"
          end as code_1_action,

case when underlyingDxCode is not null then CONCAT(p.underlyingDxCode," DISCONFIRMED")
     else CONCAT("HCC",p.conditionCategoryCode ," DISCONFIRMED")
          end as code_2,

"No, member never had this condition." as code_2_description,
"doctags_by_code" as code_2_action,

case when underlyingDxCode is not null then CONCAT(p.underlyingDxCode," RESOLVED")
     else CONCAT("HCC",p.conditionCategoryCode ," RESOLVED")
          end as code_3,

"No, member had this condition but it is resolved." as code_3_description,
"doctags_by_code" as code_3_action,
cast(null as string) as code_4,
cast(null as string) as code_4_description,
cast(null as string) as code_4_action,
cast(null as string) as code_5,
cast(null as string) as code_5_description,
cast(null as string) as code_5_action,
cast(null as string) as code_6,
cast(null as string) as code_6_description,
cast(null as string) as code_6_action,
current_datetime() as rundate

FROM
{{ref('mrt_risk_opportunities')}} p

left JOIN
{{source('codesets','ICD10toHCCcrosswalk')}} icd
on icd.DiagnosisCode  = p.underlyingDxCode

where
upper(conditionType) = 'PERS'
and lower(partner) in ( 'connecticare', 'tufts')
and lower(p.lineOfBusiness) in ( 'medicare', 'dsnp','dual','duals')
),


susp as (
SELECT
 case when lower(partner) = 'connecticare'then "ConnectiCare"
      when lower(partner) = 'tufts' then 'Tufts' end as plan,
CONCAT("CB_SUSP_", conditionCategoryCode,"_2021") as measure_code,
CONCAT("HCC:", conditionCategoryCode) as additional_codes,
RTRIM(RPAD(CONCAT("Member suspected to have ", p.conditionName ),58)) as name,
CONCAT("Based on available member data, we suspect that member may have ", p.conditionName) as details,
CONCAT("HCC",p.conditionCategoryCode ," CONFIRMED")  as code_1,
"Yes, member has this condition (PLEASE ADD PROPER ICD CODE TO BILL)" as code_1_description,
"doctags_by_code" as code_1_action,
CONCAT("HCC",p.conditionCategoryCode ," DISCONFIRMED") as code_2,
"No, member never had this condition." as code_2_description,
"doctags_by_code" as code_2_action,
CONCAT("HCC",p.conditionCategoryCode ," RESOLVED") as code_3,
"No, member had this condition but it is resolved." as code_3_description,
"doctags_by_code" as code_3_action,
cast(null as string) as code_4,
cast(null as string) as code_4_description,
cast(null as string) as code_4_action,
cast(null as string) as code_5,
cast(null as string) as code_5_description,
cast(null as string) as code_5_action,
cast(null as string) as code_6,
cast(null as string) as code_6_description,
cast(null as string) as code_6_action,
current_datetime() as rundate

FROM
{{ref('mrt_risk_opportunities')}} p

left JOIN
{{source('codesets','ICD10toHCCcrosswalk')}} icd
on icd.DiagnosisCode  = p.underlyingDxCode

where
upper(conditionType) = 'SUSP'
and lower(partner) in ( 'connecticare', 'tufts')
and lower(p.lineOfBusiness) in ( 'medicare', 'dsnp','dual','duals')
),


final as (

SELECT * from pers
union distinct
SELECT * from susp
)

select final.*

from
final

inner join
{{ref('mrt_elation_clinical_reminder_int')}} p
on final.measure_code = p.measure_code