--this take the patients table and creates flags for different cases/statuses (active, closed, contactattempted,etc)

with assessed as (
   select
       *,
       ---all newly attributed, non-disenrolled members
        coalesce ((attributedAt_tp is not null AND
           ((consentedAt_tp is not null AND disenrolledAt_tp is not null)
               OR disenrolledAt_tp is null)
           ),FALSE) AS attributed_preconsented,
        ---all those with assessments completed in tp
         coalesce ((minInitialAssessmentAt_tp is not null
           ),FALSE) as assessed_screened,
        ---hard to reach based on unreach logic: Unreachable: 120+ Days, Past Initial Onboarding & never reached
         coalesce((date_diff(date(reporting_datetime),cohort_or_attributed_date, day) > 120
    and (reachedat is null and consentedat is null and enrolledat is null and interestedAt is null and veryInterestedAt is null and notinterestedat is null)
           ),false) as hard_to_reach
   From {{ ref('cm_del_patients_quarterly') }}
),

outreached_accepted as (
   Select
       *,
        ---attrbitued OR re-engaged members have been outreached
        coalesce((attributed_preconsented = true OR (
        contactAttemptedAt_tp is not null
        or reachedAt_tp is not null
        or interestedAt_tp is not null
        or veryInterestedAt_tp is not null
        or notInterestedAt_tp is not null
        or consentedAt_tp is not null
        or enrolledAt_tp is not null
        )),false) as Outreached,
        --consented or enrolled members
        coalesce((
        consentedAt_tp is not null
        or enrolledAt_tp is not null),false) as accepted_cm
   From assessed
),

casesactive_closed as (
 select
     *,
    coalesce((accepted_cm = true AND disenrolledAt_tp is not null),false) as case_closed,
    coalesce((accepted_cm = true AND disenrolledAt_tp is null),FALSE) as case_active
 from outreached_accepted
)

select
casesactive_closed.*,
hard_to_reach as unreached,
coalesce((attributed_preconsented and donotcall = true),FALSE) as refused_cm,
coalesce((disenrolledAt_tp is not null),false) as terminated,
case
   when consentedat is not null then date_diff(date(consentedAt), reporting_date,day)
   when disenrolledAt is not null then date_diff(date(disenrolledAT), date(consentedAt),day)
   else NULL end as days_in_cm,
--case when case_closed is true then disenrolledAt_tp else null end as case_closed_at,
--case when case_active is true then consentedat_tp else null end as case_active_at
From casesactive_closed
