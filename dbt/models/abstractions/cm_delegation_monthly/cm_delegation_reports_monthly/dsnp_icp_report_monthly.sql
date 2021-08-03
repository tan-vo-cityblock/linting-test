with cases as (
select
    pf.line_of_business,
    pf.partner,
    count(distinct case when case_active = true then pf.patientid else null end) AS num_cases_active,
    count(distinct case when case_active = true and has_rn_team_lead = "yes" then pf.patientid else null end) as cases_with_rn_lead
 from {{ ref('cm_del_patients_final_flags_ytd') }} pf
 Left join {{ ref('cm_del_users_ytd') }} users
  On pf.patientid = users.patientid
  and pf.partner = users.partner
  group by 1,2
),

icps as (
select
    pf.line_of_business,
    pf.partner,
    count(distinct case when case_active = true and generated_care_plan = true then pf.patientid else null end) as total_icps_initiated,
    count(distinct case when case_active = true and updated_care_plan = true then pf.patientid else null end) as total_icps_updated
From {{ ref('cm_del_member_action_plan_monthly') }} map
  left join {{ ref('cm_del_patients_final_flags_monthly') }} pf
    on map.patientid = pf.patientid
group by 1,2
),

icts as (
select
    pf.line_of_business,
    pf.partner,
    count(distinct case when progressNoteTitle = "caseConference" then memberInteractionKey else null end) as Total_icts_held,
 from {{ ref('cm_del_interactions_monthly') }} interactions
left join {{ ref('cm_del_patients_final_flags_monthly') }} pf
on interactions.patientid = pf.patientid
group by 1,2
),

DSNP_ICP_Report as (
Select
line_of_business,
partner,
num_cases_active as Total_cases,
Round( cases_with_rn_lead/nullif(num_cases_active,0)*100,2) as percent_cases_with_rn_lead,
total_icps_initiated,
total_icps_updated,
total_icts_held
from cases
left join icps using (partner,line_of_business)
left join icts using (partner,line_of_business)
Where line_of_business = "DSNP"
)

select * from DSNP_ICP_Report
