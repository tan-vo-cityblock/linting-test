
with current_member as (

select distinct
patientId,
coefficientCategory

from
{{ ref('hcc_demo') }} dx

where
eligYear = extract(year from current_date())
and (lower(dx.lineOfBusiness) like '%medicare%'
or lower(dx.lineOfBusiness) like '%dsnp%'
or lower(dx.lineOfBusiness) like '%dual%')
),


hcc_rollup as (

select distinct
cast(hcc as numeric) as hcc,
cast(hcc_rollup as numeric)  as conditionGroup

from
{{ source('codesets', 'hcc_rollup_2020') }}
),


all_suspects_to_providers as (

select distinct
partner, 
lineOfBusiness, 
conditionName,
patientId,
safe_cast(conditionCategoryCode as numeric) as conditionCategoryCode,
case when safe_cast(conditionGroup as numeric)  is null then  safe_cast(conditionCategoryCode as numeric)
     else  safe_cast(conditionGroup as numeric)
          end as conditionGroup,
'OUTSTANDING' as component,
conditionType,
underlyingDxCode, 
recaptureEvidence,
pharmEvidence,  
labEvidence,
claimsEvidence,
otherEvidence,
riskFactors,  
runDate

from 
{{ ref('all_suspects_to_providers_int') }} dx

left join
hcc_rollup
on safe_cast(dx.conditionCategoryCode as numeric) = hcc_rollup.hcc

where
(lower(dx.lineOfBusiness) like '%medicare%' 
or lower(dx.lineOfBusiness) like '%dsnp%' 
or lower(dx.lineOfBusiness) like '%dual%')
and conditionStatus in ('OPEN', 'NO RECORDS')
),


HCC_coeff_xwalk_v24 as (

select distinct
replace(variable, "HCC","") as code,
coeffType,
coeffValue as coefficient

from
{{ source('codesets', 'HCC_coeff_xwalk_v24') }}
),


captured as (

select distinct
a.*,
hccs.coefficient as capturedCoeff

from 
{{ ref('hcc_captured_current_year') }} a

left join
HCC_coeff_xwalk_v24 hccs
on safe_cast(capturedHCC as numeric) = safe_cast(hccs.code as numeric)
and lower(a.coefficientCategory) = lower(hccs.coeffType)

where
(lower(a.lineOfBusiness) like '%medicare%'
or lower(a.lineOfBusiness) like '%dsnp%'
or lower(a.lineOfBusiness) like '%dual%')
and clmYear = extract(year from current_date())
),


highest as (

select *
from
(select distinct
dx.partner,
dx.lineOfBusiness,
dense_RANK() OVER (PARTITION BY dx.patientId, dx.conditionGroup ORDER BY dx.conditionCategoryCode) AS rank,
dx.conditionName,
dx.conditionGroup,
dx.patientId,
a.coefficientCategory,
hccs.coefficient,
dx.conditionCategoryCode,
'OUTSTANDING' as component,
dx.conditionType,
dx.underlyingDxCode,
dx.recaptureEvidence,
dx.pharmEvidence,
dx.labEvidence,
dx.claimsEvidence,
dx.otherEvidence,
dx.riskFactors,
dx.runDate

from
current_member a

inner join
all_suspects_to_providers dx
on a.patientId = dx.patientID

left join
HCC_coeff_xwalk_v24 hccs
on safe_cast(conditionCategoryCode as numeric) = safe_cast(hccs.code as numeric)
and lower(a.coefficientCategory) = lower(hccs.coeffType)
)

where
rank =1
),


currentoutstanding as (

select
* 
from
(select distinct
a.partner,
a.lineOfBusiness,
dense_RANK() OVER (PARTITION BY extract(year from a.runDate) ,a.patientId, a.partner ORDER BY a.lineOfBusiness desc) AS rank,
a.conditionName,
a.patientId,
a.coefficientCategory,
case when cast(captured.capturedHCC as numeric) < cast(a.conditionCategoryCode as numeric)  then 0
     when cast(captured.capturedHCC as numeric) > cast(a.conditionCategoryCode as numeric)   then  round( a.coefficient- captured.capturedCoefficient ,5)
     when cast(captured.capturedHCC as numeric) is null then cast(a.coefficient as numeric)
     end as coefficient,
a.conditionCategoryCode,
safe_cast(a.conditionGroup as numeric)  as conditionGroup,
'OUTSTANDING' as component,
a.conditionType,
a.underlyingDxCode,
a.recaptureEvidence,
a.pharmEvidence,
a.labEvidence,
a.claimsEvidence,
a.otherEvidence,
a.riskFactors,
a.runDate

from
highest a

left join
captured
on a.patientID = captured.patientID
and safe_cast(a.conditionGroup as numeric)  = safe_cast(captured.capturedGroup as numeric)

where
captured.capturedHCC is null
or safe_cast(a.conditionCategoryCode as numeric) < safe_cast(captured.capturedHCC as numeric)
)
where rank =1
)


select * from currentoutstanding
