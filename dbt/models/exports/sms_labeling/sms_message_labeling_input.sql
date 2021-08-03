with sms_messages as (

  select id as messageId,
         externalMessageId,
         patientId,
         userId,
         direction,
         body as messageBody,
         provider,
         extract(year from providerCreatedAt)  as year,
         extract(week from providerCreatedAt)  as week,
         providerCreatedAt,
         createdAt,
         updatedAt
  from {{ ref('sms_message') }}
  where providerCreatedAt is not null
),

sessions_conversations as (

  select messageId,
         sessionId,
         conversationId
  from {{ ref('sms_session_conversation') }}
),

users as (

  select userId,
         userName,
         userRole
  from {{ ref('user') }}
  where userPhone is not null
),

members as (

  select patientId,
         firstName             as memberFirstName,
         lastName              as memberLastName,
         patientHomeMarketName as memberHomeMarketName
  from {{ ref('member') }}
),

member_info as (

  select patientId,
         language
  from {{ ref('member_info') }}
),

member_acuity_score as (

  select
    patientId,
    score as acuityScore,
    createdAt,
    updatedAt
  from {{ source('commons', 'patient_acuity') }}
),

appointments_by_day as (

  select patientId,
         date(startAt, "America/New_York") as appointmentScheduledOn,
         string_agg(title, ", ")    as appointmentScheduledSameDay
  from {{ ref('member_appointment_statuses') }}
  group by 1, 2
),

final as (

  select sms.messageId,
         sc.sessionId,
         sc.conversationId,
         sms.externalMessageId,
         sms.patientId,
         m.memberHomeMarketName,
         mi.language,
         a.acuityScore,
         sms.userId,
         u.userRole,
         ap.appointmentScheduledSameDay,
         sms.direction,
         sms.messageBody,
         sms.provider,
         sms.year,
         sms.week,
         sms.providerCreatedAt,
         sms.createdAt,
         sms.updatedAt
  from sms_messages sms
  left join sessions_conversations sc on sms.messageId = sc.messageId
  left join members m on sms.patientId = m.patientId
  left join member_info mi on sms.patientId = mi.patientId
  left join member_acuity_score a on sms.patientId = a.patientId and sms.providerCreatedAt between a.createdAt and a.updatedAt
  left join appointments_by_day ap on sms.patientId = ap.patientId and extract(date from sms.providerCreatedAt) = ap.appointmentScheduledOn
  left join users u on sms.userId = u.userId
)

select * from final
order by memberHomeMarketName, patientId, providerCreatedAt
