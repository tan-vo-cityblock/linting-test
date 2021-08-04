--this report is technically YTD but we filter to only get those who have a risk level change in the previous month
with tends_in_month as (
	Select
	patientid,
count(distinct memberInteractionKey) as tends_in_month
from {{ ref('cm_del_interactions_ytd') }}
Where interaction_in_month = true
And isSuccessfulTend = true
Group by 1
)


select
pf.Memberid as member_id,
pf.Firstname as first_name,
pf.Lastname as last_name,
pf.Age,
pf.Line_of_business,
pf.cohortName as cohort_name,
pf.Pcpname as pcp_name,
date(Acu.initial_date) as date_initial_risk_level_assigned,
Acu.initial_acuity as initial_risk_level,
Acu.latest_acuity_description as latest_risk_level,
date(Acu.latest_acuity_date) as date_risk_level_changed,
cm.care_management_model,
Tends_in_month.tends_in_month as tends_in_month,
pf.partner
from {{ ref('cm_del_patients_final_flags_ytd') }} pf
Left join {{ ref('cm_del_acuity_ytd') }} acu
	On pf.patientid = acu.patientid
Left join {{ ref('cm_del_care_management_model_ytd') }} cm
 on Pf.patientid = cm.patientid
Left join tends_in_month
on pf.patientid = tends_in_month.patientid
--this is OVER all case actives hence using ytd tables
Where case_active = true
--need to use these for the between because the ytd tables do not have reporting last and first dates
and date(latest_acuity_date) between date_trunc(date_sub({{var("cm_reporting_claims_date_monthly")}}, interval 1 month), month)
    and last_day(date_sub({{var("cm_reporting_claims_date_monthly")}}, interval 1 month), month)