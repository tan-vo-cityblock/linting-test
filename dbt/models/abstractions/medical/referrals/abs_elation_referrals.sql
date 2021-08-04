with 

referral_orders as (
  select 
    cast(patient_id as string) as elationId,
    * except(patient_id)
  from {{ source('elation', 'referral_order_latest') }}
),
  
patient_id as (
  select
    mem.id as patientId,
    trim(m.mrn) as elationId
  from {{ source('member_index', 'member') }} mem
  inner join {{ source('member_index', 'mrn') }} m
    on mem.mrnId = m.id
  where mem.deletedAt is null
    and m.name = 'elation' 
),

result as (
  select
    p.patientId,
    r.elationId,
    u.userId,
    r.id as referralId,
    r.creation_time as createdAt,
    r.last_modified as updatedAt,
    r.deletion_time as deletedAt,
    r.practice_id as recipientPracticeId,
    r.recipient_org_name as practiceName, 
    nullif(trim(concat(r.recipient_firstname," ", r.recipient_lastname)), '') as recipientName,
    r.recipient_credentials as credentials,
    r.recipient_npi as npi, 
    r.recipient_specialty as specialty,
    r.subject as referralSubject,
    r.body as referralText,
    r.delivery_method as referralMethod
  from referral_orders r
  left join patient_id p 
    using(elationId)
  left join {{ ref('user') }} u 
    on r.prescribing_user_id = u.elationUserId 
)

select * from result
