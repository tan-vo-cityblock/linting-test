with member_monthly_costs as (

  select
    patientId,
    monthFrom,
    yearFrom,
    yearSeq,
    claimType,
    sum(amountPlanPaid) as planPaidDollars

  from {{ ref('mrt_claims_last_three_years_paid') }}
  group by 1, 2, 3, 4, 5

),

member_eligibility_by_month as (

  select
    patientId,
    monthFrom,
    yearFrom

  from {{ ref('mrt_member_eligibility') }}
),

final as (

  select
    mmc.patientId,
    mmc.monthFrom,
    mmc.yearFrom,
    mmc.yearSeq,
    mebm.patientId is not null as isEligibleMonth,
    mmc.claimType,
    mmc.planPaidDollars
  from member_monthly_costs mmc
  left join member_eligibility_by_month mebm
  using (patientId, monthFrom, yearFrom)
)

select *
from final
