with outreaches as (
Select
patientid,
count (distinct (case when outreachModality = "phone" then Outreachattemptid else null end)) as phonecounts,
count (distinct (case when outreachModality = "calledCityblockMain" then Outreachattemptid else null end)) as calledcounts,
count (distinct (case when outreachModality = "careTransitionsPhone" then Outreachattemptid else null end)) as transitionscounts,
count (distinct (case when outreachModality = "homeVisit" then Outreachattemptid else null end)) as homeVisitcounts,
count (distinct (case when outreachModality = "hubVisit" then Outreachattemptid else null end)) as hubVisitcounts,
count (distinct (case when outreachModality = "community" then Outreachattemptid else null end)) as communitycounts,
count (distinct (case when outreachModality = "careTransitionsInPerson" then Outreachattemptid else null end)) as careTransitionsInPersoncounts,
count (distinct (case when outreachModality = "metAtProvidersOffice" then Outreachattemptid else null end)) as metAtProvidersOfficecounts,
count (distinct (case when outreachModality = "sentInfoMail" then Outreachattemptid else null end)) as sentInfoMailcounts
from {{ ref('cm_del_outreach_attempts_monthly') }}
Group by 1
),

goals as (
Select
pf.patientid,
count (distinct (case when short_term_goal = true then Goalid else null end)) as total_short_term_goals,
count (distinct (case when short_term_goal_closed = true then Goalid else null end)) as total_short_term_goals_completed,
count (distinct (case when long_term_goal = true then Goalid else null end)) as total_long_term_goals,
count (distinct (case when long_term_goal_closed = true then Goalid else null end)) as total_long_term_goals_completed
From {{ ref('cm_del_patients_final_flags_monthly') }} pf
Left join {{ ref('cm_del_member_action_plan_monthly') }} map
On pf.patientid = map.patientid
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
date(Pf.consentedat_tp) as date_case_screened,
case when consentedat_tp is not null then "TRUE" else "FALSE" end as enrolled_in_case_management,
cm.care_management_model,
date(Pf.consentedat_tp) as date_case_opened,
Case when pf.partner = "Emblem Health" then "Emblem Health Registry"
	When pf.partner = "ConnectiCare" then "ConnectiCare Registry"
	Else null end as referral_source,
pf.primary_diagnosis,
pf.secondary_diagnosis,
Pf.category as case_management_diagnosis,
(coalesce(Phonecounts,0)+coalesce(calledcounts,0)+coalesce(transitionscounts,0)) as  num_telephonic_outreaches,
(coalesce(homeVisitcounts,0)+coalesce(hubVisitcounts,0)+coalesce(communitycounts,0)
+coalesce(careTransitionsInPersoncounts,0)+coalesce(metAtProvidersOfficecounts,0)) as num_in_person_outreaches,
(2+coalesce(sentInfoMailcounts,0)) as num_mail_outreaches,
date(Pf.disenrolledat_tp) as date_case_closed,
abs(Case when consentedat_tp is not null then pf.days_in_cm else null end) as days_case_remained_open,
coalesce(total_short_term_goals,0) as total_short_term_goals,
coalesce(total_short_term_goals_completed,0) as total_short_term_goals_completed,
coalesce(total_long_term_goals,0) as total_long_term_goals,
coalesce(Total_long_term_goals_completed,0) as Total_long_term_goals_completed,
pf.partner
from {{ ref('cm_del_patients_final_flags_monthly') }} pf
Left join {{ ref('cm_del_care_management_model_ytd') }} cm
 on Pf.patientid = cm.patientid
Left join outreaches
on pf.patientid = outreaches.patientid
Left join goals
on Pf.patientid = goals.patientid
where (consentedAt is null or consentedAt_tp is not null)
and (pf.disenrolledAt is null or disenrolledAt_tp is not null)
