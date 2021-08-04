with cases as(
Select
line_of_business,
partner,
count(distinct case when attributed_preconsented = true then patientid else null end) AS total_attributed,
count(distinct case when assessed_screened = true then patientid else null end) AS total_assessed_screened,
count(distinct case when outreached = true then patientid else null end) AS total_outreached,
count(distinct case when unreached = true then patientid else null end) AS total_unreached,
count(distinct case when refused_cm = true then patientid else null end) AS total_refused_cm,
count(distinct case when accepted_cm = true then patientid else null end) AS total_accepted_cm,
count(distinct case when case_active = true then patientid else null end) AS num_cases_active,
count(distinct case when case_closed = true then patientid else null end) AS Num_cases_closed,
from {{ ref('cm_del_patients_final_flags_monthly') }}
group by 1,2
),

---YTD for engagement rate
total_number_of_cases as(
select
line_of_business,
partner,
count(distinct case when accepted_cm = true then patientid else null end) AS total_accepted_cm_ytd,
count(distinct case when attributed_preconsented = true then patientid else null end) AS total_attributed_ytd,
from {{ ref('cm_del_patients_final_flags_ytd') }}
group by 1,2
),

hospitalizations as (
Select
pf.line_of_business,
pf.partner,
claims_completion_month_year,
count(DISTINCT case when case_active_current_claims_month = true then pf.patientid else null end) as Total_members_active,
count(DISTINCT case when hospitalized_current_claims_month = true and case_active_current_claims_month = true
    then pf.patientid else null end) as Total_members_hospitalized,
from {{ ref('cm_del_encounters_monthly') }} encounters
left join {{ ref('cm_del_patients_final_flags_monthly') }} pf
on encounters.patientid = pf.patientid
Group by 1,2,3
),

goals as (
	Select
		line_of_business,
    partner,
    count(distinct case when short_term_goal_closed = true then map.patientid else null end) as Total_short_term_goals_closed,
    count(distinct case when short_term_goal = true then pf.patientid else null end) as total_short_term_goals_created,
    count(distinct case when long_term_goal_closed = true then pf.patientid else null end) as Total_long_term_goals_closed,
    count(distinct case when long_term_goal = true then pf.patientid else null end) as total_long_term_goals_created,
	From {{ ref('cm_del_member_action_plan_monthly') }} map
  left join {{ ref('cm_del_patients_final_flags_monthly') }} pf
    on map.patientid = pf.patientid
	Group by 1,2
),
--this to be YTD
users as (
select
pf.partner,
pf.line_of_business,
min(num_nurses_plus_non_clinical) as num_cm_nurses_dedicated,
round((count(distinct case when case_active = true then pf.patientid else null end)/ nullif(min(num_nurses_plus_non_clinical),0)),3) as avg_caseload_per_nurse,
from {{ ref('cm_del_users_ytd') }} users
left join {{ ref('cm_del_patients_final_flags_ytd') }} pf
    on users.patientid = pf.patientid
group by 1,2
),

cm_delegation_summary_monthly as (
select
partner,
line_of_business,
total_attributed,
total_assessed_screened,
total_outreached,
total_unreached,
total_accepted_cm,
total_refused_cm,
total_accepted_cm_ytd AS total_cases_ytd,
total_attributed_ytd as total_attributed_ytd,
round(((total_accepted_cm_ytd/nullif(total_attributed_ytd,0))*100),2) AS engagement_rate_ytd,
claims_completion_month_year as claims_reporting_month,
Total_members_hospitalized as Total_members_hospitalized_claims_month,
Total_members_active as Total_members_active_thru_claims_month,
round(((Total_members_hospitalized/nullif(Total_members_active,0))*100),2) as Percent_mbrs_hospitalized,
total_accepted_cm as num_initial_cases_opened,
num_cases_active,
Num_cases_closed,
Total_short_term_goals_closed,
total_short_term_goals_created,
total_long_term_goals_closed,
total_long_term_goals_created,
num_cm_nurses_dedicated,
avg_caseload_per_nurse
from cases
left join hospitalizations using (line_of_business,partner)
left join goals using (line_of_business,partner)
left join users using (line_of_business,partner)
left join total_number_of_cases using (line_of_business,partner)
)

select * from cm_delegation_summary_monthly
