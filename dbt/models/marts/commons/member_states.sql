with patient_state as (

    select
        patientId,
        createdAt,
        currentState,
        updatedById,
        deletedAt

    from {{ source('commons', 'patient_state') }}
),

contact_attempted as (
        
    select 
        patientId, 
        min(CreatedAt) as CreatedAt,
        max(createdAt) as latestContactAttemptedAt

    from patient_state
    
    where currentState in ('contact_attempted', 'outreach')

    group by patientId

),

user as (

    select userId,
           username,
           userRole
    from {{ ref('user') }}
),


{% for state in ["reached", "interested", "very_interested", "not_interested", 
    "enrolled", "assigned", "attributed", "consented", "disenrolled"] %}

{{ state }} as (

    select 
        patientId,
        currentState,
        max(CreatedAt) as CreatedAt
    
    from patient_state
        
    where currentState = '{{ state }}'
        
    group by patientId, currentState

),

 {{ state ~ '_status' }}  as (
    select
        ps.patientId,
        ps.CreatedAt,
        u.userName as {{ state ~ 'by' }},
        u.userRole as {{ state ~ 'byRole' }}

    from patient_state as ps
    join {{ state }} as s
    using (patientId, currentState, createdAt)
    left join user as u
        on u.userId = ps.updatedById
),

{% endfor %}

disenrollment_reasons as (

    select distinct
      patientId,
      reason,
      rank() over(partition by patientId order by createdAt asc) as rnk,
      rank() over(partition by patientId order by createdAt desc) as rnkDesc

    from {{ source('commons', 'patient_disenrollment') }} as pd
    
    where currentState = 'accepted'
        
),

earliest_reason as (

  select patientId,
         reason
  from disenrollment_reasons
  where rnk = 1

),

latest_reason as (

  select
    patientId,
    reason as latestDisenrollmentReason

  from disenrollment_reasons
  where rnkDesc = 1

),

-- Get current state per member
current_state as (

    select 
        patientId, 
        currentState

    from patient_state
        
    where deletedAt IS NULL
        
),

member_states as (
        
    select
        p.id as patientId,
          
        case
            when cs.currentState = 'disenrolled' 
                and (c.CreatedAt is not null or e.CreatedAt is not null) 
                then 'disenrolled_after_consent'
                else cs.currentState 
        end as currentState,

        case
            when cs.currentState = 'attributed' then 0
            when cs.currentState = 'assigned' then 1
            when cs.currentState = 'contact_attempted' then 2
            when cs.currentState = 'reached' then 3
            when cs.currentState = 'interested' then 4
            when cs.currentState = 'very_interested' then 5
            when cs.currentState = 'consented' then 6
            when cs.currentState = 'enrolled' then 7
            when cs.currentState = 'disenrolled' AND (c.CreatedAt is not null or e.CreatedAt is not null) then 8
            when cs.currentState = 'disenrolled' then 9
            when cs.currentState = 'not_interested' then 10
            when cs.currentState = 'ineligible' then -1
        end as stateOrder,
        ad.CreatedAt as attributedAt,
        a.CreatedAt as assignedAt,
        COALESCE(ca.CreatedAt, r.CreatedAt, i.CreatedAt, vi.CreatedAt, c.CreatedAt, e.CreatedAt, ni.createdAt) as contactAttemptedAt,
        ca.latestContactAttemptedAt,
        COALESCE(r.CreatedAt, i.CreatedAt, vi.CreatedAt, c.CreatedAt, e.CreatedAt, ni.createdAt) as reachedAt,
        COALESCE(i.CreatedAt, vi.CreatedAt, c.CreatedAt, e.CreatedAt) as interestedAt,
        COALESCE(vi.CreatedAt, c.CreatedAt, e.CreatedAt) as veryInterestedAt,
        COALESCE(c.CreatedAt, e.CreatedAt) as consentedAt,
        e.CreatedAt as enrolledAt,
        d.CreatedAt as disenrolledAt,
        d.disenrolledbyRole,
        d.disenrolledby,
        ni.CreatedAt as notInterestedAt,
        er.reason as disenrollmentReason,
        lr.latestDisenrollmentReason,
        c.consentedby,
        c.consentedbyRole,
        coalesce(timestamp_diff(ad.createdAt, d.createdAt, day) >= 30, false) as isReattributed
          
    from {{ source('commons', 'patient') }} as p

    left join contact_attempted ca 
        on ca.patientId = p.Id
    
    left join reached r 
        on r.patientId = p.Id
        
    left join interested i 
        on i.patientId = p.Id
    
    left join very_interested vi 
        on vi.patientId = p.Id
    
    left join not_interested ni 
        on ni.patientId = p.Id
        
    left join assigned a 
        on a.patientId = p.Id
    
    left join enrolled_status e
        on e.patientId = p.Id
        
    left join disenrolled_status d
        on d.patientId = p.Id
        
    left join earliest_reason er 
        on er.patientId = p.Id

    left join latest_reason lr
        on lr.patientId = p.Id
        
    left join consented_status c
        on c.patientId = p.Id

    left join attributed ad 
        on ad.patientId = p.Id
    
    left join current_state cs 
        on cs.patientId = p.Id

)

select * from member_states

        