
with captured as (

select distinct
partner, 
lineofbusiness, 
patientId, 
coefficientCategory,
conditionType, 
HCCDescription, 
safe_cast(capturedHCC as numeric) as hcc1,
safe_cast(capturedGroup as numeric) as capturedGroup,
safe_cast(capturedCoefficient as numeric) as coefficient,
component,
'captured' as source

from 
{{ref('hcc_captured_current_year')}}

where
clmYear = extract(year from current_date)
),


currentoutstanding as (

select distinct
partner, 
lineofbusiness, 
patientID, 
coefficientCategory, 
conditionType,
conditionName,
safe_cast(conditionCategoryCode as numeric) as hcc1,
conditionGroup,
safe_cast( coefficient as numeric) as coefficient,
component,
'outstanding' as source

from 
{{ref('hcc_current_outstanding')}} 
),


unioned as (

select * from captured
union all
select * from currentoutstanding
),


hccsrankedpossible as (

select distinct *

from
unioned
)


select * from hccsrankedpossible