
{{
  config(
       tags=["evening"]
  )
}}


with opptys as (

select distinct
partner,
lineOfBusiness,
patientId,
conditionName,
conditionType,
'OPEN' as conditionStatus,
conditionCategory,
conditionCategoryCode,
underlyingDxCode,
recaptureEvidence,
chartReviewEvidence,
pharmEvidence,
labEvidence	,
claimsEvidence,
otherEvidence,
riskFactors,
runDate,

from
{{ref('all_suspects_to_providers_int')}}

where
conditionStatus in ( 'OPEN', 'INDETERMINATE','NO RECORDS','NEW')
)


select * from opptys