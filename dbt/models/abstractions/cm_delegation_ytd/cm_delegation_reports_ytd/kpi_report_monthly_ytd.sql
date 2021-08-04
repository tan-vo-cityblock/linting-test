with unable as (
select
patientid,
partner,
Line_of_business,
reporting_month_year,
((sum(case when event_type = 'reachedat' then 1 else 0 end) = 0) and
(sum(case when had_3_outreach_attempts is true then 1 else 0 end) >0)) as unable_to_contactx3,
(sum(case when do_not_call is true then 1 else 0 end ) > 0) as do_not_call
From  {{ ref('cm_del_kpi_ytd') }} kpi
group by 1,2,3,4
),

initials as (
  -- For each patient + reporting month, get whether case was active and/or closed cumulative thru period
Select
patientid,
partner,
Line_of_business,
reporting_month_year,
coalesce((sum (case when event_type = "consentedat" then 1 else 0 end) >0),false) as case_active_thru_period, -- number of case status flag
coalesce((sum (case when event_type = "case_closed_at" then 1 else 0 end) >0),false) as case_closed_thru_period, -- number of case status flag
min(case when event_type = "consentedat" then event_date else null end) as consented_at,
min(case when event_type = "case_closed_at" then event_date else null end) as case_closed_at,
From  {{ ref('cm_del_kpi_ytd') }} kpi
group by 1,2,3,4
),

initials2 as (
  -- For each patient + reporting month, get for active not closed members their consent date and whether they had goal closed thru period
  select
  patientid,
  partner,
  Line_of_business,
  reporting_month_year,
  case_active_thru_period is true and case_closed_thru_period is false as case_active_thru_period, -- Remove case closed mbrs from case active
  case_closed_thru_period,
  consented_at,
  case_closed_at,
  coalesce((initials.case_active_thru_period is true and case_closed_thru_period is false AND
  (sum(case when event_type = "any_goal_closed_at" and event_type is not null then 1 else 0 end)=0)),false) as engaged_no_goal_met, -- engaged with no goal met # DP: why event type not null?
  -- Days in CM calc
  abs(case when case_active_thru_period is true then date_diff(reporting_month_end,date(consented_at),day)
      when case_closed_thru_period is true then date_diff(date(case_closed_at), date(consented_at),day)
  else null end) as days_in_cm_thru
  From  {{ ref('cm_del_kpi_ytd') }} kpi
  left join initials using (patientid,partner,Line_of_business,reporting_month_year)
  group by 1,2,3,4,initials.case_active_thru_period,6,7,8,10
),

averages as (
select
partner,
Line_of_business,
reporting_month_year,
round(avg(case when (case_active_thru_period and engaged_no_goal_met is false) then days_in_cm_thru else null end),2) as average_days_cm_members_with_goal_met,
round(avg(case when (case_active_thru_period and engaged_no_goal_met is true) then days_in_cm_thru else null end),2) as average_days_cm_members_without_goal_met
from initials2
group by 1,2,3
),

claims_completion_month as (
  -- Partner level date
  select
  partner,
  min(claims_completion_month) as claims_completion_month,
  min(claims_completion_month_year) as cpy
  from initials2
  left join  {{ ref('cm_del_encounters_ytd') }}  enc
    on initials2.patientId = enc.patientid
  group by partner
),

days_in_cm_thru_period_cat as (
  -- Categorize days in CM, get claims completion month and other dates
  select
  days_in_cm.patientid,
  days_in_cm.partner,
  days_in_cm.Line_of_business,
  days_in_cm.reporting_month_year,
  days_in_cm.reporting_month_year as rpy,
  claims_completion_month.cpy,
  claims_completion_month.claims_completion_month as claims_reporting_month, --do not change for quarterly report
  case when days_in_cm_thru between 1 and 30 then '1,30'
       when days_in_cm_thru between 31 and 60 then '31,60'
       when days_in_cm_thru between 61 and 91 then '61,90'
       when days_in_cm_thru between 91 and 120 then '91,120'
       when days_in_cm_thru between 121 and 150 then '121,150'
       when days_in_cm_thru between 151 and 180 then '151,180'
       when days_in_cm_thru between 181 and 365 then '181,365'
     else 'over' end as days_in_cm_thru_cat,
  max(delegation_at) as del
  From initials2 days_in_cm
  left join claims_completion_month using(partner)
  left join {{ ref('cm_del_delegation_dates_ytd') }}
    on days_in_cm.patientid = cm_del_delegation_dates_ytd.patientid
  group by 1,2,3,4,5,6,7,days_in_cm_thru
),

num_admits as (
select
  KPI.patientid,
  kpi.partner,
  kpi.Line_of_business,
  kpi.event_month_year,
  del,
  rpy,
  cpy,
  claims_reporting_month,
  count(distinct
          case when case_active_thru_period is true and event_type = "adm_date"
        then kpi.pat_clm_id else null end) as total_inpatient_admits,
  count(distinct
          case when case_active_thru_period is false and case_closed_thru_period is false and event_type = "adm_date"
       then kpi.pat_clm_id else null end) as num_admits_prior_to_enrolled,
  count(distinct
        case when case_active_thru_period is true and days_in_cm_thru_cat = '1,30' and event_type = "adm_date"
        then kpi.pat_clm_id else null end) as num_admits_active_1to30_days,
  count(distinct
      case when case_active_thru_period is true and days_in_cm_thru_cat = '31,60' and event_type = "adm_date"
      then kpi.pat_clm_id else null end) as num_admits_active_31to60_days,
  count(distinct
      case when case_active_thru_period is true and days_in_cm_thru_cat = '61,90' and event_type = "adm_date"
      then kpi.pat_clm_id else null end) as num_admits_active_61to90_days,
  count(distinct
      case when case_active_thru_period is true and days_in_cm_thru_cat = '91,120' and event_type = "adm_date"
      then kpi.pat_clm_id else null end) as num_admits_active_91to120_days,
  count(distinct
      case when case_active_thru_period is true and days_in_cm_thru_cat = '121,150' and event_type = "adm_date"
      then kpi.pat_clm_id else null end) as num_admits_active_121to150_days,
  count(distinct
      case when case_active_thru_period is true and days_in_cm_thru_cat = '151,180' and event_type = "adm_date"
      then kpi.pat_clm_id else null end) as num_admits_active_151to180_days,
  count(distinct
      case when case_active_thru_period is true and days_in_cm_thru_cat = '181,365' and event_type = "adm_date"
      then kpi.pat_clm_id else null end) as num_admits_active_1st_year
From  {{ ref('cm_del_kpi_ytd') }} kpi
left join days_in_cm_thru_period_cat cat
  on kpi.patientid = cat.patientid
  and kpi.event_month_year = cat.reporting_month_year
  and kpi.partner = cat.partner
  and kpi.line_of_business = cat.line_of_business
left join initials2
  on kpi.patientid = initials2.patientid
  and kpi.event_month_year = initials2.reporting_month_year
  and kpi.partner = initials2.partner
  and kpi.line_of_business = initials2.line_of_business
group by 1,2,3,4,5,6,7,8
),

non_cumulative1 as (
select
kpi.partner,
kpi.Line_of_business,
kpi.event_month_year,
count(distinct case when event_type = 'outreachattemptedat' then outreachAttemptId else null end) as num_member_outreaches,
count(distinct case when event_type = 'outreachattemptedat' then kpi.patientId else null end) as num_unique_members_outreached,
count(distinct case when event_type = 'consentedat' then kpi.patientId else null end) as num_initial_cases_opened
From  {{ ref('cm_del_kpi_ytd') }} kpi
group by 1,2,3
),

non_cumulative2 as (
select
num_admits.partner,
num_admits.Line_of_business,
num_admits.event_month_year,
claims_reporting_month,
sum(case when rpy <= cpy and del <= claims_reporting_month then total_inpatient_admits else null end) as total_inpatient_admits,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_prior_to_enrolled else null end) as num_admits_prior_to_enrolled,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_1to30_days else null end) as num_admits_active_1to30_days,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_31to60_days else null end) as num_admits_active_31to60_days,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_61to90_days else null end) as num_admits_active_61to90_days,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_91to120_days else null end) as num_admits_active_91to120_days,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_121to150_days else null end) as num_admits_active_121to150_days,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_151to180_days else null end) as num_admits_active_151to180_days,
sum(case when rpy <= cpy and del <= claims_reporting_month then num_admits_active_1st_year else null end) as num_admits_active_1st_year
from num_admits
group by 1,2,3,4
),

final_cumulative as (select
partner,
Line_of_business,
reporting_month_year,
count(distinct case when event_type = 'cohortattributiondate' then patientid else null end) as total_cases,
count(distinct case when case_active_thru_period then patientid else null end) as num_members_engaged,
count(distinct case when case_closed_thru_period then patientid else null end) as num_members_engaged_closed,
count(distinct case when (case_active_thru_period and engaged_no_goal_met is false ) then patientid else null end) as num_engaged_goal_met,
count(distinct case when (case_active_thru_period and engaged_no_goal_met is true) then patientid else null end) as num_engaged_goal_not_met,
count(distinct case when event_type = 'disenrolledat' then patientid else null end) as num_deceased_delegated_terminated,
count(distinct case when (refused_cm is true and unable.do_not_call is true) then patientid else null end) as num_refused_cm,
count(distinct case when unable_to_contactx3 = true then patientid else null end) as num_unable_to_contact_x3,
From  {{ ref('cm_del_kpi_ytd') }} kpi
left join initials2 using (patientid,partner,Line_of_business,reporting_month_year)
left join unable using (patientid,partner,Line_of_business,reporting_month_year)
group by 1,2,3
),

final_non_cumulative as (
SELECT *
from non_cumulative1 f1
left join non_cumulative2 f2 using (partner, line_of_business, event_month_year)
),


final_kpi as (
select
final_cumulative.partner,
final_cumulative.Line_of_business,
final_cumulative.reporting_month_year,
total_cases,
num_member_outreaches,
num_unique_members_outreached,
"N/A" as number_cases_pended,
num_members_engaged,
num_initial_cases_opened,
num_members_engaged_closed,
num_engaged_goal_met,
round((num_engaged_goal_met/nullif(num_members_engaged,0))*100,2) as percent_engaged_goal_met,
average_days_cm_members_with_goal_met,
num_engaged_goal_not_met,
round((num_engaged_goal_not_met/nullif(num_members_engaged,0))*100,2) as percent_engaged_goal_not_met,
average_days_cm_members_without_goal_met,
num_deceased_delegated_terminated,
num_refused_cm,
num_unable_to_contact_x3,
claims_reporting_month,
total_inpatient_admits,
num_admits_prior_to_enrolled,
num_admits_active_1to30_days,
num_admits_active_31to60_days,
num_admits_active_61to90_days,
num_admits_active_91to120_days,
num_admits_active_121to150_days,
num_admits_active_151to180_days,
num_admits_active_1st_year
from final_cumulative
left join final_non_cumulative
    on final_cumulative.reporting_month_year = final_non_cumulative.event_month_year
    and final_cumulative.line_of_business = final_non_cumulative.line_of_business
    and final_cumulative.partner = final_non_cumulative.partner
left join averages
    on final_cumulative.reporting_month_year = averages.reporting_month_year
    and final_cumulative.line_of_business = averages.line_of_business
    and final_cumulative.partner = averages.partner
where final_cumulative.Line_of_business is not null
order by 2,3
)

select * from final_kpi
