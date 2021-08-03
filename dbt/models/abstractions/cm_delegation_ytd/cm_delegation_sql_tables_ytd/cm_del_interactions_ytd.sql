SELECT
    progressNoteId,
    memberInteractionKey,
    member_interactions.patientId,
    userId,
    eventType as progressNoteTitle,
    eventTimestamp as progressNoteAt,
    location,
    isAttemptedTend,
    isSuccessfulTend,
    isAttemptedConnection,
    isSuccessfulConnection,
    isAttemptedInPersonVisit,
    isSuccessfulInPersonVisit,
    coalesce(reporting_month_yr = extract(year from eventTimestamp)||'.'||lpad(cast(extract(month from date(eventTimestamp)) as string), 2, '0'),
    false) as interaction_in_month,
    current_date as date_run
FROM {{ ref('member_interactions') }}
--we want to make sure eventTimestamp is between delegation date AND reporting date.
inner join {{ ref('cm_del_reporting_dates_ytd') }}
    on eventTimestamp <= reporting_datetime
INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on date(eventTimestamp) >= delegation_at
     and member_interactions.patientid = dd.patientid
WHERE isSuccessfulTend
