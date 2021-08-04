select
  patientId,
  claimId,
  lineNumber as fillNumber,
  dateFrom as filledOn,
  daysSupply,
  ndc,
  amountAllowed,
  claimLineStatus

from  {{ ref('mrt_claims_self_service') }}
where costclassification1 = 'RX'
