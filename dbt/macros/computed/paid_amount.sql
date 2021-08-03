
{% macro paid_amount(

    slug,
    min_paid_amount,
    acute=False,
    medical_claims_only=False,
    member_table=var('cf_member_table'),
    type='paid_amount'

  )
%}

with paid_amounts as (

  select
    patientId,
    sum(amountPlanPaid) as planPaidAmount

  from {{ ref('mrt_claims_self_service') }}

  where

  {% if acute == True %}

    (
      acsInpatientFlag or
      acsEdFlag or
      (edServiceFlag and costCategory = 'inpatient')
    ) and

  {% endif %}

{% if medical_claims_only == True %}

    (claimType = 'medical')

    and

  {% endif %}

    (date_diff(current_date, dateFrom, month) between 6 and 17) and
    claimLineStatus != 'Denied'

  group by patientId

),

eligible_members as (

  select distinct patientId

  from {{ ref('mrt_member_month_spine_cu_denom') }}

  where date_diff(current_date, eligDate, month) between 6 and 17

),

filtered_paid_amounts as (

  select *

  from paid_amounts

  inner join eligible_members
  using (patientId)

),

members_meeting_criteria as (

  select patientId
  from filtered_paid_amounts
  where planPaidAmount >= {{ min_paid_amount }}

),

{{ finish_computed_field_query(slug, type, final_cte_name = 'members_meeting_criteria') }}

{% endmacro %}
