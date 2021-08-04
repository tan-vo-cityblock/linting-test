--this is everything up until the last day of the previous month - cumulative

with historical_current_state as (
  select
    patientid,
    historicalState,
    case when date(patientStateCreatedAt) < date_trunc(current_date, month)  then date(patientStateCreatedAt)
     else null end as patientStateCreatedAtdate
  from {{ ref('member_historical_states') }}
  where date_sub(date_trunc(current_date, month), interval 1 day) = calendardate
),

member_state_dates as (
  select
    patientid,
    historicalState,
    case when date(attributedat) < date_trunc(current_date, month) then attributedat else null end as attributedat,
    case when date(consentedat) < date_trunc(current_date, month) then consentedat else null end as consentedat,
    case when date(enrolledat) < date_trunc(current_date, month) then enrolledat else null end as enrolledat,
    case when date(contactattemptedat) < date_trunc(current_date, month) and historicalState not in ('disenrolled_after_consent', 'disenrolled') then contactattemptedat else null end as contactattemptedat,
    case when date(interestedat) < date_trunc(current_date, month) then interestedat else null end as interestedat,
    case when  historicalState in ('disenrolled_after_consent', 'disenrolled') then patientStateCreatedAtdate else null end as disenrolledat,
  from {{ ref('member_states') }}
  left join historical_current_state using (patientid)
),

number_members as (
     select
        patientid,
        historicalState,
        count (distinct case when disenrolledat is null then patientid else null end) as total_members_non_disenrolled,
        count (distinct case when attributedat is not null then patientid else null end) as total_members_attributed,
        count (distinct case when consentedat is not null then patientid else null end) as total_members_consented,
        count (distinct case when  contactattemptedat is not null then patientid else null end) as total_members_contactedattempted,
        count (distinct case when enrolledat is not null or interestedat is not null or consentedat is not null  then patientid else null end) as total_members_interested_or_consented_or_enrolled,
--        count (distinct case when enrolledat is not null or consentedat is not null then patientid else null end) as total_members_enrolled_or_consented,
        count(distinct patientid) as total_members
    from {{ ref('member') }}
    left join member_state_dates
     using(patientId)
    where partnername = 'CareFirst'
    group by 1,2
),

--this is to get only members who are not enrolled in HH for certain metrics in the final CTE
number_members_HH as (
     select
        hh.patientid,
        count (distinct case when disenrolledat is null then patientid else null end) as total_members_non_disenrolled_hh,
        count (distinct case when  contactattemptedat is not null then patientid else null end) as total_members_contactedattempted_hh,
        count (distinct case when enrolledat is not null or interestedat is not null or consentedat is not null  then patientid else null end) as total_members_interested_or_consented_or_enrolled_hh,
        count(distinct patientid) as total_members_hh
    from {{ ref('hh_enrolled_exclusion') }} hh
    left join member_state_dates
     using(patientId)
    group by 1
),

--total number of patients who have completed a comprehensive assessment within 90 days of attribution or consented date
assessments_completed_attributed_consented as (
  select
    patientId,
    count(distinct case when (DATE_DIFF(date(minComprehensiveAssessmentAt), date(attributedat), DAY) between 0 and 90) and attributedat is not null then patientid else null end) as assessments_completed_attributed_90days,
    count(distinct case when (DATE_DIFF(date(minComprehensiveAssessmentAt), date(consentedat), DAY) between 0 and 90) and consentedat is not null then patientid else null end) as assessments_completed_consented_90days,
    count(distinct case when
                (((DATE_DIFF(date(minComprehensiveAssessmentAt), date(consentedat), DAY) between 0 and 90)  and consentedat is not null)  OR
                   ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(enrolledat), DAY) between 0 and 90) and enrolledat is not null) OR
                  ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(interestedat), DAY) between 0 and 90) and enrolledat is not null)
                  )
          then patientid else null end) as assessments_completed_interested_or_consented_or_enrolled_90days,
  from  {{ ref('member_commons_completion') }}
  left join member_state_dates ms
        using(patientId)
  where minComprehensiveAssessmentAt is not null
  and date(minComprehensiveAssessmentAt) < date_trunc(current_date, month)
  group by 1
),

assessments_completed_attributed_consented_hh as (
  select
    patientId,
    count(distinct case when
                (((DATE_DIFF(date(minComprehensiveAssessmentAt), date(consentedat), DAY) between 0 and 90)  and consentedat is not null)  OR
                   ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(enrolledat), DAY) between 0 and 90) and enrolledat is not null) OR
                  ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(interestedat), DAY) between 0 and 90) and enrolledat is not null)
                  )
          then patientid else null end) as assessments_completed_interested_or_consented_or_enrolled_90days_hh,
  from {{ ref('hh_enrolled_exclusion') }} hh
  left join {{ ref('member_commons_completion') }} using (patientid)
  left join member_state_dates ms
        using(patientId)
  where minComprehensiveAssessmentAt is not null
  and date(minComprehensiveAssessmentAt) < date_trunc(current_date, month)
  group by 1
),

------------With at least 1 MAP task within 30 days of completed HRA/comprehensive assessment-------------------
members_completed_assessment_30days as (
  select
    patientid,
    count(*)
  from {{ ref('member_commons_completion') }}
  where minComprehensiveAssessmentAt is not null
  and date(minComprehensiveAssessmentAt) < date_trunc(current_date, month)
  and date(minTaskAt) < date_trunc(current_date, month)
  and DATE_DIFF(date(mintaskat),date(minComprehensiveAssessmentAt), DAY) <= 30
  group by 1
),

------more than 2 outreach attempts +/- two weeks of attemptedat date--------
lead_lag_outreach_attempted as (select
*,
lag(attemptedAt,1) over (partition by patientid order by attemptedAt desc) as attempted_lag,
lead(attemptedAt,1) over (partition by patientid order by attemptedAt desc) as attempted_lead,
from  {{ source('commons', 'outreach_attempt') }}
left join number_members using (patientId)
where historicalState in ('assigned', 'contact_attempted', 'attributed')
and date(attemptedAt) < date_trunc(current_date, month)
and personReached = 'noOne'
),

fourteen_days_outreachattempted as (select
patientid,
(abs(date_diff(date(attempted_lag), date(attemptedat), day)) <=14 OR
abs(date_diff(date(attempted_lead), date(attemptedat), day)) <=14) as within_two_weeks,
from lead_lag_outreach_attempted
),

count_of_truths as (select
patientid,
count(within_two_weeks) as count_withing_two_weeks_truths
from fourteen_days_outreachattempted
where within_two_weeks is true
group by 1
),

final_outreach_attempts as (select
patientid,
count(*) as count_hard_to_reach
from count_of_truths
where count_withing_two_weeks_truths > 2
group by 1
),

--getting the number of members with at least one PCP visit in the last year from the end of last month---------
members_with_1_pcp as (
select
patientid,
count(*) as pcps_visits
from {{ ref('mrt_claims_self_service_all') }}
where primaryCareProviderFlag = true
and datefrom between date_sub(date_trunc(current_date, month), interval 1 year) and date_trunc(current_date, month)
group by 1
having pcps_visits >= 1
)


select
      sum(total_members) as total_members,
      sum(total_members_non_disenrolled) as total_members_non_disenrolled,
 --HRA complete within 90 days of assignment: #members with completed HRA assessment within 90 days of attributed date/#Total of members attributed.
    sum(assessments_completed_attributed_90days) as assessments_completed_attributed_90days,
    sum(total_members_attributed) as total_members_attributed,
    round(((sum(assessments_completed_attributed_90days)/nullif(sum(total_members_attributed),0))*100),1) as hra_complete_within_90_days_of_assignment,
 --HRA complete within 90 days of consent: #of members completed HRA assessment within 90 of consent date/#of members consented
    sum(total_members_consented) as total_members_consented,
    sum(assessments_completed_consented_90days) as assessments_completed_consented_90days,
    round(((sum(assessments_completed_consented_90days)/nullif(sum(total_members_consented),0))*100),1) as hra_complete_within_90_days_of_consent,
 --Member Action Plan within 30 days: #of member with at least 1 MAP goal within 30 days of HRA/# of member completed HRA
    count(distinct members_completed_assessment_30days.patientid) as members_with_completed_map_goal_30_dates_hra,
    count(distinct assessments_completed_attributed_consented.patientid) as member_completed_assessment,
    round(((count(distinct members_completed_assessment_30days.patientid)/nullif(count(distinct assessments_completed_attributed_consented.patientid),0))*100),1) as completed_hra_within30days,
 --Enrollees with 3 unsuccessful outreach attempts:#members flagged  hard to reach (call attempts >2 within any given 2 weeks) / total number of members not disnerolled
    sum(count_hard_to_reach) as hard_to_reach_count,
    round(((sum(count_hard_to_reach)/nullif(sum(total_members_non_disenrolled),0))*100),1) as hard_to_reach,
 --Members engaged with PCP: #member with at least one PCP claim in the last 12 months/totals members not disenrolled
    count(distinct members_with_1_pcp.patientid) as members_with_1_pcp,
    round(((count(distinct members_with_1_pcp.patientid)/nullif(sum(total_members_non_disenrolled),0))*100),1) as members_with_1_pcp_visit,

------------------------------------------Health Home Metrics (excludes members in HH)----------------------------------------------------
"without_hh" as without_hh,
 --Cohort members with attempted contact: #members have had a contacted attempt (non disenrolled)/totals members non disenrolled
    sum(total_members_contactedattempted_hh) as total_members_contactedattempted_hh,
    round(((sum(total_members_contactedattempted_hh)/nullif(sum(total_members_non_disenrolled_hh),0))*100),1) as contact_attempted_hh,
 --Cohort members successfully contacted engaged and/or consented:
 ----#members of members interested, consented, or enrolled/total members not disenrolled
    sum(total_members_interested_or_consented_or_enrolled_hh) as total_members_interested_or_consented_or_enrolled_final_hh,
    round(((sum(total_members_interested_or_consented_or_enrolled_hh)/nullif(sum(total_members_non_disenrolled_hh),0))*100),1) as members_interested_or_enrolled_or_consented_hh,
 --Assessment completion within ninety (90) days of Cohort Members engaged and/or consenting for Care Management:
 ----#of members with HRA assessment within 90days of consented or enrolled/total members consented/enrolled/interested
    sum(assessments_completed_interested_or_consented_or_enrolled_90days_hh) as assessments_completed_interested_or_consented_or_enrolled_90days_hh,
    sum(total_members_interested_or_consented_or_enrolled_hh) as total_members_interested_or_consented_or_enrolled_hh,
    round(((sum(assessments_completed_interested_or_consented_or_enrolled_90days_hh)/nullif(sum(total_members_interested_or_consented_or_enrolled_hh),0))*100),1) as hra_completed_within90_days__interested_or_enrolled_or_consented_hh,
  --HH numbers
    sum(total_members_hh) as total_members_hh,
    sum(total_members_non_disenrolled_hh) as total_members_non_disenrolled_hh,

 ---------------------------------------------------------same metrics but WITH HH members--------------------------------------------------------------------
"with_hh" as with_hh,
   --Cohort members with attempted contact: #members have had a contacted attempt (non disenrolled)/totals members non disenrolled
    sum(total_members_contactedattempted) as total_members_contactedattempted,
    round(((sum(total_members_contactedattempted)/nullif(sum(total_members_non_disenrolled),0))*100),1) as contact_attempted,
 --Cohort members successfully contacted engaged and/or consented:
 ----#members of members interested, consented, or enrolled/total members not disenrolled
    sum(total_members_interested_or_consented_or_enrolled) as total_members_interested_or_consented_or_enrolled_final,
    round(((sum(total_members_interested_or_consented_or_enrolled)/nullif(sum(total_members_non_disenrolled),0))*100),1) as members_interested_or_enrolled_or_consented,
 --Assessment completion within ninety (90) days of Cohort Members engaged and/or consenting for Care Management:
 ----#of members with HRA assessment within 90days of consented or enrolled/total members consented/enrolled/interested
    sum(assessments_completed_interested_or_consented_or_enrolled_90days) as assessments_completed_interested_or_consented_or_enrolled_90days,
    sum(total_members_interested_or_consented_or_enrolled) as total_members_interested_or_consented_or_enrolled,
    round(((sum(assessments_completed_interested_or_consented_or_enrolled_90days)/nullif(sum(total_members_interested_or_consented_or_enrolled),0))*100),1) as hra_completed_within90_days__interested_or_enrolled_or_consented
from  number_members
left join assessments_completed_attributed_consented using (patientid)
left join members_completed_assessment_30days using (patientid)
left join final_outreach_attempts using (patientid)
left join members_with_1_pcp using (patientid)
left join number_members_hh using (patientid)
left join assessments_completed_attributed_consented_hh using (patientid)
