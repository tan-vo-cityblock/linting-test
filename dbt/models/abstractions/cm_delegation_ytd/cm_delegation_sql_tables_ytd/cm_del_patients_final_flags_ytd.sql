--this take the patients table and creates flags for different cases/statuses (active, closed, contactattempted,etc)

with assessed as (
    select
        *,
        coalesce ((attributedAt is not null AND
           ((consentedAt is not null AND disenrolledAt is not null)
               OR disenrolledAt is null)
           ),FALSE) AS assessed_screened,
        coalesce ((attributedAt is not null AND
           ((consentedAt is not null AND disenrolledAt is not null)
               OR disenrolledAt is null)
           ),FALSE) AS attributed_preconsented
    From {{ ref('cm_del_patients_ytd') }}
),

outreached_accepted as (
    Select
        *,
        coalesce((assessed_screened = true and contactAttemptedAt is not null),false) as Outreached,
        coalesce((assessed_screened = true and consentedAt is not null),false) as accepted_cm
    From assessed
),

casesactive_closed as (
  select
      *,
     coalesce((accepted_cm = true AND disenrolledAt is not null),false) as case_closed,
     coalesce((accepted_cm = true AND disenrolledAt is null),FALSE) as case_active
  from outreached_accepted
 )

select
*,
coalesce((Assessed_screened = true AND reachedAt is null),false) as reached,
coalesce((Outreached = true and reachedAt is null),false) as unreached,
coalesce((assessed_screened and donotcall = true),FALSE) as refused_cm,
coalesce((accepted_cm = true
    AND reporting_month_yr = EXTRACT (YEAR from consentedAt)||'.'||lpad(cast(extract(month from date(consentedAt)) as string), 2, '0')),
    false) as case_opened_current_month,
coalesce((accepted_cm = true
    AND reporting_quarter_yr = EXTRACT (YEAR from consentedAt)||'.'||EXTRACT (quarter from consentedAt)),
    false) as case_opened_current_quarter,
coalesce((disenrolledAt is not null),false) as terminated,
case
    when consentedat is not null then date_diff(date(consentedAt), reporting_date,day)
    when disenrolledAt is not null then date_diff(date(disenrolledAT), date(consentedAt),day)
    else NULL end as days_in_cm,
case when case_closed is true then disenrolledAt else null end as case_closed_at,
case when case_active is true then consentedat else null end as case_active_at
From casesactive_closed
