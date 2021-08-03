with sms_messages as (

  select
    id,
    userId,
    patientId,
    messageId as externalMessageId,
    contactNumber,
    case
      when direction = 'toUser' then 'incoming'
      when direction = 'fromUser' then 'outgoing'
      else direction end as direction,
    body,
    mediaUrls,
    provider,
    providerCreatedAt,
    createdAt,
    updatedAt,
    deletedAt
  from {{ source('commons', 'sms_message') }}
),

karuna_messages as (

  select
    id,
    userId,
    patientId,
    externalMessageId,
    contactNumber,
    direction,
    body,
    cast(null as string) as mediaUrls,
    'karuna' as provider,
    providerCreatedAt,
    dataLoadedAt as createdAt,
    cast(null as timestamp) as updatedAt,
    cast(null as timestamp) as deletedAt
  from {{ source('karuna', 'text_message_karuna') }}
),

member_phone_numbers as (

  select
    patientId,
    phoneNumber,
    row_number() over (partition by phoneNumber order by isPrimaryPhone desc, hasMultipleNumbers) as recordNumber
  from {{ ref('member_phone_numbers') }}
),

-- Resolve potential sharing of phone numbers between member by keeping only the first member who has that number
-- prioritizing first the member who has that phone number listed as their primary number, and next the member who has
-- only one phone number
phone_number_member_lookup as (

  select
    phoneNumber,
    patientId
  from member_phone_numbers
  where recordNumber = 1
),

final as (

  select
    sms.id,
    sms.userId,
    coalesce(sms.patientId, pnml.patientId) as patientId,
    sms.externalMessageId,
    sms.contactNumber,
    sms.direction,
    sms.body,
    sms.mediaUrls,
    sms.provider,
    sms.providerCreatedAt,
    sms.createdAt,
    sms.updatedAt,
    sms.deletedAt
  from sms_messages sms
  left join phone_number_member_lookup pnml
    on sms.contactNumber = pnml.phoneNumber

    union all

  select
    id,
    userId,
    patientId,
    externalMessageId,
    contactNumber,
    direction,
    body,
    mediaUrls,
    provider,
    providerCreatedAt,
    createdAt,
    updatedAt,
    deletedAt
  from karuna_messages
)

select * from final
