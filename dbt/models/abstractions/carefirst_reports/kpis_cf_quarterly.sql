--this report is on everything from the previous quarter only

with historical_current_state as (
  select
    patientid,
    historicalState as historicalState_lastday,
    case when date(patientStateCreatedAt) < last_day(date_sub(current_date, interval 1 quarter), quarter) then date(patientStateCreatedAt)
    else null end as patientStateCreatedAtdate_lastday
  from {{ ref('member_historical_states') }}
  where last_day(date_sub(current_date, interval 1 quarter), quarter) = calendardate
),

disenrolled_firstday as (
  select
    patientid,
    historicalState as historicalState_firstday,
    case when date(patientStateCreatedAt) < last_day(date_sub(current_date, interval 1 quarter), quarter) then date(patientStateCreatedAt)
    else null end as patientStateCreatedAtdate_firstday
  from {{ ref('member_historical_states') }}
  where DATE_TRUNC(date_sub(current_date, interval 1 quarter), quarter) = calendardate
),

member_state_dates as (
  select
    patientid,
    historicalState_lastday as historicalState,
    case when date(attributedat) <= last_day(date_sub(current_date, interval 1 quarter), quarter)
            then attributedat else null end as attributedat_until_lastday,
    case when date(attributedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and historicalState_lastday not in ('disenrolled_after_consent', 'disenrolled')
            then attributedat else null end as attributedat_within_time,
    case when date(attributedat) between date_trunc(date_sub(current_date, interval 2 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            then attributedat else null end as attributedat_90days,
    case when date(consentedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and date(attributedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and historicalState_lastday not in ('disenrolled_after_consent', 'disenrolled')
            then consentedat else null end as consentedat_within_time,
    case when date(consentedat) between date_trunc(date_sub(current_date, interval 2 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            then consentedat else null end as consentedat_90days,
    case when date(enrolledat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and date(attributedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and historicalState_lastday not in ('disenrolled_after_consent', 'disenrolled')
            then enrolledat else null end as enrolledat_within_time,
    case when date(enrolledat) between date_trunc(date_sub(current_date, interval 2 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            then enrolledat else null end as enrolledat_90days,
  --for contacted attempted we want anyone attributed in that time period with ANY contacted attempted (not specific to time period)
    case when date(contactattemptedat) is not null
            and date(attributedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and historicalState_lastday not in ('disenrolled_after_consent', 'disenrolled')
            then contactattemptedat else null end as contactattemptedat_within_time,
    case when date(interestedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and date(attributedat) between date_trunc(date_sub(current_date, interval 1 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            and historicalState_lastday not in ('disenrolled_after_consent', 'disenrolled')
            then interestedat else null end as interestedat_within_time,
    case when date(interestedat) between date_trunc(date_sub(current_date, interval 2 quarter), quarter) and last_day(date_sub(current_date, interval 1 quarter), quarter)
            then interestedat else null end as interestedat_90days,
    case when  historicalState_lastday in ('disenrolled_after_consent', 'disenrolled') then patientStateCreatedAtdate_lastday else null end as disenrolledat_lastday,
    case when  historicalState_firstday in ('disenrolled_after_consent', 'disenrolled') then patientStateCreatedAtdate_firstday else null end as disenrolledat_firstday
  from {{ ref('member_states') }} ms
  left join historical_current_state using (patientid)
  left join disenrolled_firstday using (patientid)
),

number_members as (
    select
        patientid,
        historicalState,
         count (distinct case when disenrolledat_lastday is null then patientid else null end) as total_members_non_disenrolled_lastday,
        count (distinct case when disenrolledat_firstday is null then patientid else null end) as total_members_non_disenrolled_firstday,
        count (distinct case when attributedat_until_lastday is not null then patientid else null end) as total_members_attributed_uptill_lastday,
        count (distinct case when attributedat_within_time is not null then patientid else null end) as total_members_attributed_within_time,
        count (distinct case when attributedat_90days is not null then patientid else null end) as total_members_attributedat_90days,
        count (distinct case when consentedat_within_time is not null then patientid else null end) as total_members_consented_within_time,
        count (distinct case when consentedat_90days is not null then patientid else null end) as total_members_consented_90days,
        count (distinct case when enrolledat_within_time is not null then patientid else null end) as total_members_enrolled_within_time,
        count (distinct case when enrolledat_90days is not null then patientid else null end) as total_members_enrolled_90days,
        count (distinct case when  contactattemptedat_within_time is not null then patientid else null end) as total_members_contactedattempted_within_time,
        count (distinct case when enrolledat_within_time is not null or interestedat_within_time is not null or consentedat_within_time is not null  then patientid else null end) as total_members_enrolled_or_consented_or_interested_within_time,
        count (distinct case when consentedat_90days is not null or enrolledat_90days is not null or interestedat_90days is not null  then patientid else null end) as total_members_enrolled_or_consented_or_interested_90days,
    from {{ ref('member') }}  m
    left join member_state_dates
     using(patientId)
    where partnername = 'CareFirst'
    group by 1,2
),

number_members_hh as (
    select
        patientid,
        historicalState,
        count (distinct case when disenrolledat_firstday is null then patientid else null end) as total_members_non_disenrolled_firstday_hh,
        count (distinct case when attributedat_within_time is not null then patientid else null end) as total_members_attributed_within_time_hh,
        count (distinct case when  contactattemptedat_within_time is not null then patientid else null end) as total_members_contactedattempted_within_time_hh,
        count (distinct case when enrolledat_within_time is not null or interestedat_within_time is not null or consentedat_within_time is not null  then patientid else null end) as total_members_enrolled_or_consented_or_interested_within_time_hh,
        count (distinct case when consentedat_90days is not null or enrolledat_90days is not null or interestedat_90days is not null  then patientid else null end) as total_members_enrolled_or_consented_or_interested_90days_hh,
    from {{ ref('hh_enrolled_exclusion') }} hh
    left join member_state_dates
     using(patientId)
    group by 1,2
),

--total number of patients who have completed a comprehensive assessment within 90 days of attribution or consented date
assessments_completed_attributed_consented as (
  select
    patientId,
    count(distinct case when DATE_DIFF(date(minComprehensiveAssessmentAt), date(attributedat_90days), DAY) between 0 and 90 and attributedat_90days is not null then patientid else null end) as assessments_completed_attributed_90days,
    count(distinct case when DATE_DIFF(date(minComprehensiveAssessmentAt), date(consentedat_90days), DAY) between 0 and 90 and consentedat_90days is not null then patientid else null end) as assessments_completed_consented_90days,
    count(distinct case when
                (((DATE_DIFF(date(minComprehensiveAssessmentAt), date(consentedat_90days), DAY) between 0 and 90 ) and consentedat_90days is not null)  OR
                   ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(enrolledat_90days), DAY) between 0 and 90 ) and enrolledat_90days is not null) OR
                   ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(interestedat_90days), DAY) between 0 and 90 ) and interestedat_90days is not null)
                   )
          then patientid else null end) as assessments_completed_consented_or_enrolled_or_interested_90days
  from {{ ref('member_commons_completion') }}  mcc
  left join member_state_dates
    using(patientId)
  where minComprehensiveAssessmentAt is not null
  and date(minComprehensiveAssessmentAt) <= last_day(date_sub(current_date, interval 1 quarter), quarter)
  group by 1
),

assessments_completed_attributed_consented_hh as (
  select
    patientId,
    count(distinct case when
                (((DATE_DIFF(date(minComprehensiveAssessmentAt), date(consentedat_90days), DAY) between 0 and 90 ) and consentedat_90days is not null)  OR
                   ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(enrolledat_90days), DAY) between 0 and 90 ) and enrolledat_90days is not null) OR
                   ((DATE_DIFF(date(minComprehensiveAssessmentAt), date(interestedat_90days), DAY) between 0 and 90 ) and interestedat_90days is not null)
                   )
          then patientid else null end) as assessments_completed_consented_or_enrolled_or_interested_90days_hh
  from {{ ref('hh_enrolled_exclusion') }} hh
  left join {{ ref('member_commons_completion') }} using (patientid)
  left join member_state_dates
    using(patientId)
  where minComprehensiveAssessmentAt is not null
  and date(minComprehensiveAssessmentAt) <= last_day(date_sub(current_date, interval 1 quarter), quarter)
  group by 1
),

------------With at least 1 MAP goal within 30 days of completed HRA/comprehensive assessment-------------------
members_completed_assessment_30days as (
  select
    patientid,
    DATE_DIFF(date(mintaskat),date(minComprehensiveAssessmentAt), DAY) <= 30 as memberscompleted30
  from  {{ ref('member_commons_completion') }}  mcc
  where minComprehensiveAssessmentAt is not null
  and date(minComprehensiveAssessmentAt) between date_trunc(date_sub(current_date, interval 6 month), month) and last_day(date_sub(current_date, interval 1 quarter), quarter)
  and date(mintaskat) <= last_day(date_sub(current_date, interval 1 quarter), quarter)
),

------more than 2 outreach attempts +/- two weeks of attemptedat date--------
lead_lag_outreach_attempted as (select
*,
lag(attemptedAt,1) over (partition by patientid order by attemptedAt desc) as attempted_lag,
lead(attemptedAt,1) over (partition by patientid order by attemptedAt desc) as attempted_lead,
from  {{ source('commons', 'outreach_attempt') }}
left join number_members using (patientId)
where historicalState in ('assigned', 'contact_attempted', 'attributed')
--want to get two weeks before the 1st of the quarter and 2 weeks after the last day of the quarter
and date(attemptedAt) between date_sub(date_trunc(date_sub(current_date, interval 1 quarter), quarter), interval 2 week) and date_add(last_day(date_sub(current_date, interval 1 quarter), quarter), interval 2 week)
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
count(within_two_weeks) as count_within_two_weeks_truths
from fourteen_days_outreachattempted
where within_two_weeks is true
group by 1
),

final_outreach_attempts as (select
patientid,
count(*) as count_hard_to_reach
from count_of_truths
where count_within_two_weeks_truths > 2
group by 1
),

--getting the number of members with at least one PCP visit in the last year from the end of last quarter---------
members_with_1_pcp as (
select
patientid,
count(*) as pcps_visits
from  {{ ref('mrt_claims_self_service_all') }}
where primaryCareProviderFlag = true
and datefrom between date_sub(date_sub(current_date,interval 1 quarter), interval 1 year) and last_day(date_sub(current_date, interval 1 quarter), quarter)
group by 1
having pcps_visits >= 1
)

select
    sum(total_members_attributed_uptill_lastday) as total_members_attributed_uptill_lastday,
    sum(total_members_non_disenrolled_lastday) as total_members_non_disenrolled_lastday,
    sum(total_members_non_disenrolled_firstday) as total_members_non_disenrolled_firstday,
    sum(total_members_attributed_within_time) as total_members_new_attributed_within_time_notdisenrolled,
 --HRA complete within 90 days of assignment: #members with completed HRA assessment within 90 days of attributed date/#Total of members attributed.
    sum(assessments_completed_attributed_90days) as assessments_completed_attributed_90days,
    sum(total_members_attributedat_90days) as total_members_attributedat_90days,
    round(((sum(assessments_completed_attributed_90days)/nullif(sum(total_members_attributedat_90days),0))*100),1) as hra_complete_within_90_days_of_assignment,
 --HRA complete within 90 days of consent: #of members completed HRA assessment within 90 of consent date/#of members consented
    sum(total_members_consented_90days) as total_members_consented_90days,
    sum(assessments_completed_consented_90days) as assessments_completed_consented_90days,
    round(((sum(assessments_completed_consented_90days)/nullif(sum(total_members_consented_90days),0))*100),1) as hra_complete_within_90_days_of_consent,
 --Member Action Plan within 30 days: #of member with at least 1 MAP goal within 30 days of HRA/# of member completed HRA
    count(distinct case when memberscompleted30 is true then members_completed_assessment_30days.patientid else null end) as members_with_completed_map_goal_30_dates_hra,
    count(distinct members_completed_assessment_30days.patientid) as member_completed_assessment_60days,
    round((((count(distinct case when memberscompleted30 is true then members_completed_assessment_30days.patientid else null end))/nullif(count(distinct members_completed_assessment_30days.patientid),0))*100),1) as completed_hra_within30days,
 --Enrollees with 3 unsuccessful outreach attempts:#members flagged  hard to reach (call attempts >2 within any given 2 weeks) / total number of members not disnerolled
    sum(count_hard_to_reach) as hard_to_reach_count,
    round(((sum(count_hard_to_reach)/nullif(sum(total_members_non_disenrolled_lastday),0))*100),1) as hard_to_reach,
 --Members engaged with PCP: #member with at least one PCP claim in the last 12 months/totals members not disenrolled
    count(distinct members_with_1_pcp.patientid) as members_with_1_pcp,
    round(((count(distinct members_with_1_pcp.patientid)/nullif(sum(total_members_non_disenrolled_lastday),0))*100),1) as members_with_1_pcp_visit_one_year,

 -------------------------------------------------------Health Home Metrics (excludes members in HH)---------------------------------------------------------------------
"without_hh" as without_hh,
 --Cohort members with attempted contact: #members have had a contacted attempt (non disenrolled)/members newly attributed in time period not disenrolled
    sum(total_members_contactedattempted_within_time_hh) as total_members_contactedattempted_within_time_notdisenrolled_hh,
    round(((sum(total_members_contactedattempted_within_time_hh)/nullif(sum(total_members_attributed_within_time_hh),0))*100),1) as contact_attempted_hh,
 --Cohort members successfully contacted engaged and/or consented:
 ----#members of members interested, consented, or enrolled/members newly attributed in time period not disenrolled
    sum(total_members_enrolled_or_consented_or_interested_within_time_hh) as total_members_enrolled_or_consented_or_interested_within_time_notdisenrolled_hh,
    round(((sum(total_members_enrolled_or_consented_or_interested_within_time_hh)/nullif(sum(total_members_attributed_within_time_hh),0))*100),1) as members_enrolled_or_consented_or_interested_within_time_hh,
 --Assessment completion within ninety (90) days of Cohort Members engaged and/or consenting for Care Management:
 ----#of members with HRA assessment within 90days of consented or enrolled/total members consented/enrolled/interested
    sum(assessments_completed_consented_or_enrolled_or_interested_90days_hh) as assessments_completed_consented_or_enrolled_or_interested_90days_hh,
    sum(total_members_enrolled_or_consented_or_interested_90days_hh) as total_members_enrolled_or_consented_or_interested_90days_hh,
    round(((sum(assessments_completed_consented_or_enrolled_or_interested_90days_hh)/nullif(sum(total_members_enrolled_or_consented_or_interested_90days_hh),0))*100),1) as hra_completed_within90_days_enrolled_or_consented_or_interested_hh,
    sum(total_members_non_disenrolled_firstday_hh) as total_members_non_disenrolled_firstday_hh,
    sum(total_members_attributed_within_time_hh) as total_members_attributed_within_time_nondisenrolled_hh,

---------------------------------------------------------same metrics but WITH HH members--------------------------------------------------------------------
"with_hh" as with_hh,
 --Cohort members with attempted contact: #members have had a contacted attempt (non disenrolled)/members newly attributed in time period not disenrolled
    sum(total_members_contactedattempted_within_time) as total_members_contactedattempted_within_time_notdisenrolled,
    round(((sum(total_members_contactedattempted_within_time)/nullif(sum(total_members_attributed_within_time),0))*100),1) as contact_attempted,
 --Cohort members successfully contacted engaged and/or consented:
 ----#members of members interested, consented, or enrolled/members newly attributed in time period not disenrolled
    sum(total_members_enrolled_or_consented_or_interested_within_time) as total_members_enrolled_or_consented_or_interested_within_time_notdisenrolled,
    round(((sum(total_members_enrolled_or_consented_or_interested_within_time)/nullif(sum(total_members_attributed_within_time),0))*100),1) as members_enrolled_or_consented_or_interested_within_time,
 --Assessment completion within ninety (90) days of Cohort Members engaged and/or consenting for Care Management:
 ----#of members with HRA assessment within 90days of consented or enrolled/total members consented/enrolled/interested
    sum(assessments_completed_consented_or_enrolled_or_interested_90days) as assessments_completed_consented_or_enrolled_or_interested_90days,
    sum(total_members_enrolled_or_consented_or_interested_90days) as total_members_enrolled_or_consented_or_interested_90days,
    round(((sum(assessments_completed_consented_or_enrolled_or_interested_90days)/nullif(sum(total_members_enrolled_or_consented_or_interested_90days),0))*100),1) as hra_completed_within90_days_enrolled_or_consented_or_interested
from  number_members
left join assessments_completed_attributed_consented using (patientid)
left join members_completed_assessment_30days using (patientid)
left join final_outreach_attempts using (patientid)
left join members_with_1_pcp using (patientid)
left join number_members_hh using (patientid)
left join assessments_completed_attributed_consented_hh using (patientid)
