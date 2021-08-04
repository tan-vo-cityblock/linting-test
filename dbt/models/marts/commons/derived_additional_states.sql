
with outreach_calls as (

    select 
        patientId, 
        count(distinct id) as count_attempted_calls

    from {{ source('commons', 'outreach_attempt') }}

    where modality = "phone"
        and personReached = "noOne"

    group by 1

),

outreach_visits as (

    select 
        patientId, 
        count(distinct id) as count_attempted_visits

    from {{ source('commons', 'outreach_attempt') }}

    where modality in ("community", "homeVisit", "hub", "ehr", "outreachEvent", "other")
        and personReached = "noOne"
        and isScheduledVisit = FALSE

    group by 1
),

not_interested as (

    select patientId

    from {{ source('commons', 'patient_state') }}

    where 
        currentState = 'not_interested' and 
        deletedAt is null

),

contact_attempted as (

    select 
        patientId, 
        min(createdAt) as createdAt

    from {{ source('commons', 'patient_state') }}

    where currentState = 'contact_attempted'

    group by patientId
),

hard_to_reach as (

    select 
        ps.patientId

    from {{ source('commons', 'patient_state') }} as ps

    left join outreach_calls as oc
        on ps.patientId = oc.patientId

    left join outreach_visits as ov
        on ps.patientId = ov.patientId

    left join not_interested as ni
        on ps.patientId = ni.patientId

    left join contact_attempted as ca
        on ps.patientId = ca.patientId

    where
            -- Make sure no consented/enrolled members appear here
            ps.currentState in ('attributed', 'assigned', 'contact_attempted', 'reached', 'interested', 'very_interested')
        and ps.deletedAt is null
        and
            -- Contact attempted at least 30 days ago + (3+ calls and 2+ attempted visits)
            timestamp_diff(current_timestamp(), ca.createdAt, day) >= 30
        and (count_attempted_calls >= 3
        and count_attempted_visits >= 2)

        or
            -- Reached and not interested
            ni.patientId is not null
),

derived_additional_states as (

    -- return the hard to reach patients
    select distinct
        p.id as patientId, 
        hr.patientId is not null as hard_to_reach

    from {{ source('commons', 'patient') }} as p

    left join hard_to_reach as hr
        on p.id = hr.patientId

)

select * from derived_additional_states