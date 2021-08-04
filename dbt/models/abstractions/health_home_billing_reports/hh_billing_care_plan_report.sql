select
--since patients has all the different interventions, we want to just pick lines with a care plan event
distinct
cin as MBR_ID,
'05541231' as HH_MMIS_ID,
'05541231' as CMA_MMIS_ID,
completed_plan as completed,
FORMAT_DATE("%m%d%Y", plan_date) as plan_date,
initial_plan,
member_id,
SUBSTR(member_id, 2, LENGTH(member_id) - 3) as trim_member_id,
reporting_date
from {{ ref('hh_billing_patients') }}
where initial_plan is not null
--You only want those with an plan_date and a completed_plan flag
and plan_date is not null
and completed_plan is not null

