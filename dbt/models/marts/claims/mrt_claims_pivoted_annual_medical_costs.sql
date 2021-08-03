with annual_medical_costs as (

  select
    patientId,
    yearSeq,
    count(isEligibleMonth) as eligibleMonthCount,
    sum(planPaidDollars) as planPaidDollars

  from {{ ref('mrt_claims_monthly_costs_and_eligibility') }}
  where claimType = 'medical' and isEligibleMonth
  group by 1, 2

),

annual_costs_per_eligible_month as (

  select
    patientId,
    yearSeq,
    round(coalesce(planPaidDollars, 0) / eligibleMonthCount, 2) as planPaidDollarsPerMemberMonth

  from annual_medical_costs

),

pivoted_annual_medical_costs as (

  select
    patientId,

    {{ dbt_utils.pivot(
        'yearSeq',
        [1, 2, 3],
        agg='max',
        prefix='planPaidDollarsPerMemberMonthYear',
        then_value='planPaidDollarsPerMemberMonth'
      )
    }}

  from annual_costs_per_eligible_month
  group by 1

)

select *
from pivoted_annual_medical_costs
