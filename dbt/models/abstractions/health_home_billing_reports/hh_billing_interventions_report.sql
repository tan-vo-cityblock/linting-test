select
cin as MBR_ID,
'05541231' as HH_MMIS_ID,
'05541231' as CMA_MMIS_ID,
FORMAT_DATE("%m%d%Y", intervention_date) as intervention_date,
mode,
target,
completed_intervention as completed,
outreach,
care_manage,
care_coord_health_promote,
transition_care,
patient_family_support,
comm_social,
member_id,
SUBSTR(member_id, 2, LENGTH(member_id) - 3) as trim_member_id
from {{ ref('hh_billing_patients') }}
where (intervention_date is not null and intervention_date >= date(consentedAt))
