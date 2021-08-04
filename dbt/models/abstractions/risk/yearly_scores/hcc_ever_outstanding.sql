
with current_member as (

select distinct * from {{ ref('hcc_demo') }}

),


all_suspects_ranked as (

select distinct
case when cast(extract(Year from runDate) as string) in ('2016','2017','2018') then 2019
     when cast(extract(Year from runDate) as string) in ('2017','2018','2019') then 2020
     when cast(extract(Year from runDate) as string) in ('2018','2019','2020') then 2021
        end as evidenceyear,
patientId,
case when conditionName = 'Diabetes without Complication' then 'Diabetes without Complications'
     when conditionName = 'COPD' then 'Chronic Obstructive Pulmonary Disease'
     when conditionName = 'Chronic Kidney Disease' then 'Chronic Kidney Disease, Moderate (Stage 3)'
        else conditionName
          end as ConditionName,
conditionCategoryCode,
min(conditiontype) as conditiontype,
concat(patientID,conditionCategoryCode) as patHCCCombo
from
{{ ref('all_suspects_ranked') }}

where 
conditionCategoryCode is not null
and lineOfBusiness in ('medicare','duals','dsnp')
and conditionStatus = 'OPEN'
and (confidenceLevel < 3 )

group by
evidenceyear,
patientId,
conditionName,
conditionCategoryCode

order by
patientId,
conditionName,
patHCCCombo
),


hcc_claim_hx as (

select distinct
case when cast(clmyear as string) in ('2016','2017','2018') then 2019
     when cast(clmyear as string) in ('2017','2018','2019') then 2020
     when cast(clmyear as string) in ('2018','2019','2020') then 2021
        end as evidenceyear,
patientId ,
case when HCCDescription = 'Diabetes without Complication' then 'Diabetes without Complications'
     when HCCDescription = 'COPD' then 'Chronic Obstructive Pulmonary Disease'
     when HCCDescription = 'Chronic Kidney Disease' then 'Chronic Kidney Disease, Moderate (Stage 3)'
     else HCCDescription
        end as ConditionName,
hcc as conditionCategoryCode,
'PERS' as conditiontype,
memhcccombo as patHCCCombo
from
{{ ref('risk_claims_patients') }}

where 
lineOfBusiness in ('medicare','duals','dsnp')
and cast(clmyear as string)   in ('2016','2017','2018','2019','2020')
and excludeAcuteHCC is null
and excludeICD is null
and hcc is not null
and (extract(year from current_date())  - clmyear  <= cast(HCCLookbackPeriod as numeric) )
),


conditions as (

select distinct * from hcc_claim_hx
union distinct
select distinct * from all_suspects_ranked
),


HCC_coeff_xwalk_v24 as (

select distinct
replace(variable, "HCC","") as code,
Description,    
CNA,    
CND,    
CFA,    
CFD,    
CPA,    
CPD,    
Institutional,  
Year

from 
{{ source('codesets', 'hcc_coefficient_values') }}
),


everOutstanding as ( 

select distinct 
dx.*,	
case when coefficientCategory ='CND' then CND
     when coefficientCategory ='CFA' then CFA
     when coefficientCategory ='CFD' then CFD
     when coefficientCategory ='CPA' then CPA
     when coefficientCategory ='CPD' then CPD
     else CNA
       end as coefficient
from 
(select
partner, 
lineOfBusiness, 
coefficientCategory,
evidenceyear,
demo.patientID,
ConditionName,
conditionCategoryCode,
conditiontype,
patHCCCombo as suspcombo

from
current_member demo

inner join
conditions

on demo.patientID = conditions.patientId
and eligYear = conditions.evidenceyear
) dx

inner join
HCC_coeff_xwalk_v24 hccs
on conditionCategoryCode = hccs.code 
and cast(dx.evidenceyear as string) = cast(hccs.year as string)
)


select * from everOutstanding