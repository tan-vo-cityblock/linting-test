
select
  patientId,
  costClassification1,
  sum(coalesce(amountPlanPaid, 0)) as planPaidDollars,
  count(distinct admissionId) as admissionCount,
  count(distinct concat(patientId, cast(dateFrom as string))) as edDayCount
  
from {{ ref('mrt_claims_rolling_twelve_months') }}
    
group by 1, 2
