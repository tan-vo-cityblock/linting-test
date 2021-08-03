--BQ: https://console.cloud.google.com/bigquery?sq=915813449633:49fdfdd2ff6a46adae0dee06826aa44e
with outreachattemptmain as (

    SELECT
        outreach_attempt.patientId,
        id as outreachAttemptId,
        modality as outreachModality,
        attemptedAt as outreachAttemptedAt,
        direction as outreachDirection,
        isScheduledVisit as outreachScheduledVisit,
        outcome as outreachOutcome,
        personReached as outreachPersonReached,
        ROW_NUMBER() OVER (PARTITION BY outreach_attempt.patientid
                           ORDER BY attemptedAt ASC) as rownumbers
    FROM {{ source('commons', 'outreach_attempt') }}
--we want to make sure attemptedAt is between delegation date AND reporting date.
    inner join {{ ref('cm_del_reporting_dates_ytd') }}
        on attemptedAt <= reporting_datetime
    INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
        on  date(attemptedAt) >= delegation_at
        and outreach_attempt.patientid = dd.patientid
    WHERE deletedAt is null
),

--Get doNotCall list (one row per mem) with dates for use in KPI report
donotcall as (
    select
        patientId,
        outreachAttemptId,
        true as do_not_call,
        date(outreachAttemptedAt) as donotcall_event_date,
        rownumbers as earliest
    FROM outreachattemptmain
    where outreachOutcome = "doNotCall"
    and rownumbers = 1
),

--Get had_3_outreach_attempts list -# unable to contact: outreached 3+ times but not yet reached
three_outreach_attempts as (
    select
        m.patientId,
        outreachAttemptId,
        true as had_3_outreach_attempts,
        date(m.outreachAttemptedAt) as had_3_outreach_attempts_event_date,
        rownumbers as count_attempts
    from outreachattemptmain m
    where rownumbers = 3
)

select
    m.patientid,
    m.outreachAttemptId,
    outreachModality,
    outreachAttemptedAt,
    outreachDirection,
    outreachScheduledVisit,
    outreachOutcome,
    outreachPersonReached,
    coalesce(dnc.do_not_call,FALSE) as do_not_call,
    dnc.earliest,
    dnc.donotcall_event_date,
    coalesce(toa.had_3_outreach_attempts,FALSE) as had_3_outreach_attempts,
    toa.had_3_outreach_attempts_event_date,
    current_date as date_run
    from outreachattemptmain m
    left join donotcall dnc
        on m.patientid = dnc.patientId
        and m.outreachAttemptId = dnc.outreachAttemptId
    left join three_outreach_attempts toa
        on m.patientid = toa.patientid
        and m.outreachAttemptId = toa.outreachAttemptId

