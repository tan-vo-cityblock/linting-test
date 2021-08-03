select
  patientId,
  count(distinct monthFrom) as memberMonths

from {{ ref('mrt_member_eligibility') }}
where {{ filter_for_lagged_window() }}
group by 1
