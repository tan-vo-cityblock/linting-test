select
distinct
cin as MBR_ID,
'05541231' as HH_MMIS_ID,
'05541231' as CMA_MMIS_ID,
completed_assess as completed,
FORMAT_DATE("%m%d%Y", assess_date) as assess_date,
initial_assessment,
LIVING,
LIVING_PESTS,
LIVING_MOLD,
LIVING_LEAD,
LIVING_HEAT,
LIVING_OVEN,
LIVING_SMOKE,
LIVING_LEAKS,
LIVING_NONE,
FOOD_WORRIED,
FOOD_MONEY,
TRANSPORTATION,
UTILITIES,
SAFETY_PHYSICAL,
SAFETY_INSULT,
SAFETY_THREATEN,
SAFETY_SCREAM,
member_id,
SUBSTR(member_id, 2, LENGTH(member_id) - 3) as trim_member_id,
reporting_date
from {{ ref('hh_billing_patients') }}
where initial_assessment is not null
--You only want those with an assess_date and a completed_assess flag
and assess_date is not null
and completed_assess is not null
--since patients has all the different interventions, we want to just pick lines with an assessment event
