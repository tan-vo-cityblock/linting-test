--Need to make a wide table instead of a long table
--and using the dates listed below to get a specific event_type for when those dates happened
--the KPIs are aggregated totals of the event types
With  kpi_long as (
    Select
        pf.Patientid,
        NULL as outreachAttemptId,
        NULL as Pat_clm_id,
        consentedat as event_date,
        "consentedat" as event_type
    from {{ ref('cm_del_patients_final_flags_ytd') }} pf
    Where Consentedat is not null

    Union all

        Select
        pf.Patientid,
        NULL as outreachAttemptId,
        NULL as Pat_clm_id,
        reachedat as event_date,
        "reachedat" as event_type
    from {{ ref('cm_del_patients_final_flags_ytd') }} pf
    where reachedAt is not null

    Union all

    Select
        pf.Patientid,
        NULL as outreachAttemptId,
        NULL as Pat_clm_id,
        case_closed_at as event_date,
        "case_closed_at" as event_type
    from {{ ref('cm_del_patients_final_flags_ytd') }} pf
    Where case_closed_at is not null

    Union all

    Select
        Patientid,
        NULL as outreachAttemptId,
        NULL as Pat_clm_id,
        any_goal_closed_at as event_date,
        "any_goal_closed_at" as event_type
    from {{ ref('cm_del_member_action_plan_ytd') }}
    Where any_goal_closed_at is not null

    Union all

    Select
        pf.Patientid,
        NULL as outreachAttemptId,
        NULL as Pat_clm_id,
        disenrolledat as event_date,
        "disenrolledat" as event_type
    from {{ ref('cm_del_patients_final_flags_ytd') }} pf
    Where disenrolledAt is not null

    Union all

    Select
        pf.Patientid,
        NULL as outreachAttemptId,
        NULL as Pat_clm_id,
        cast(cohortAttributionDate as timestamp) as event_date,
        "cohortattributiondate" as event_type
    from {{ ref('cm_del_patients_final_flags_ytd') }} pf
    Where cohortAttributionDate is not null

    Union all

    Select
        Patientid,
        NULL as outreachAttemptId,
        Pat_clm_id,
        cast(adm_date as timestamp) as event_date,
        "adm_date" as event_type
    from  {{ ref('cm_del_encounters_ytd') }}
    Where adm_date is not null

    Union all

    Select
        Patientid,
        outreachAttemptId,
        NULL as Pat_clm_id,
        outreachAttemptedAt as event_date,
        "outreachattemptedat" as event_type
    from {{ ref('cm_del_outreach_attempts_ytd') }}
    Where outreachAttemptedAt is not null
),


kpi_long2 as (
select * from kpi_long
group by 1,2,3,4,5
),

event_dates as (
select
*,
extract (year from event_date)||'.'||extract (quarter from event_date) As event_quarter_year,
extract (year from event_date)||'.'||lpad(cast(extract(month from date(event_date)) as string), 2, '0')  As event_month_year,
DATE_ADD(date_trunc(date(event_date),quarter),interval 1 quarter) as event_quarter_end,
DATE_ADD(date_trunc(date(event_date),month),interval 1 month) as event_month_end
from kpi_long2
),

event_periods as (
SELECT DISTINCT
event_month_year,
event_quarter_year,
event_quarter_end,
event_month_end
FROM event_dates
-- Restrict to event periods to before reporting date
inner join {{ ref('cm_del_reporting_dates_ytd') }} rd
    on Event_date <= rd.reporting_datetime
),

reporting_dates as (
select DISTINCT
e1.event_quarter_year,
e1.event_month_year,
e2.event_quarter_year As reporting_quarter_year,
e2.event_month_year  As reporting_month_year,
e1.event_quarter_end,
e1.event_month_end,
e2.event_quarter_end As reporting_quarter_end,
e2.event_month_end  As reporting_month_end
from event_periods e1
-- Fuzzy join so that every event period is associated with all reporting periods on or after event period
inner join event_periods e2
    on e1.event_month_year <= e2.event_month_year
)

Select
kpi_long.Patientid,
Refused_cm,
kpi_long.outreachAttemptId,
Pat_clm_id,
Line_of_business,
Partner,
kpi_long.Event_date,
Event_type,
--this will give cumulative results
reporting_month_year,
reporting_quarter_year,
reporting_quarter_end,
reporting_month_end,
--need this to get non-cumulative results
kpi_long.event_quarter_year,
kpi_long.event_month_year,
had_3_outreach_attempts,
do_not_call
From event_dates kpi_long
inner join {{ ref('cm_del_patients_final_flags_ytd') }}  pf --only want patients from the Partners so inner join
    on kpi_long.patientid = pf.patientid
left join {{ ref('cm_del_outreach_attempts_ytd') }} outreach_attempts
    on kpi_long.outreachAttemptId = outreach_attempts.outreachAttemptId
    and pf.patientid = outreach_attempts.patientid
inner join {{ ref('cm_del_reporting_dates_ytd') }}  rd
    on Event_date <= rd.reporting_datetime
left join reporting_dates
ON kpi_long.event_month_year = reporting_dates.event_month_year -- This pulls in all reporting months on/after event_month_year
