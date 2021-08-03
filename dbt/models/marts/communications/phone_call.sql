with commons_phone_calls as (

  select id,
         callId as externalCallId,
         userId,
         patientId,
         contactNumber,
         callReceiver as initialCallReceiver,
         callReceiver as finalCallReceiver,
         case when direction = 'toUser' then 'incoming'
              when direction = 'fromUser' then 'outgoing'
           end as direction,
         duration as durationSeconds,
         case
           when callStatus in ('incoming', 'outgoing', 'completed') then 'completed'
           when callStatus in ('missed', 'no-answer', 'abandoned', 'busy', 'voicemail') then 'missed'
           -- Outgoing calls using Twilio can have failed status, meaning Twilio could not connect the call
           when provider = 'twilio' and callStatus = 'failed' then 'failed'
           -- Outgoing calls using Dialpad can be canceled by the user before the recipient can answer
           when provider = 'dialpad' and callStatus = 'canceled' then 'canceled'
           end as callStatus,
         case
           when callStatus = 'busy' then 'busy'
           when callStatus = 'voicemail' then 'voicemail'
           when callStatus = 'no-answer' then 'no-answer'
           when callStatus = 'abandoned' then 'abandoned'
           end as missedReason,
         cast(null as boolean) as isTransferredToVirtualHub,
         isTransferredToClinic,
         cast(null as boolean) as isTransferredToFieldTeam,
         cast(null as boolean) as isTransferredToAnsweringService,
         isTransferredToVoicemail,
         cast(null as boolean) as isRecorded,
         device,
         provider,
         providerCreatedAt,
         twilioUpdatedAt as providerUpdatedAt,
         createdAt,
         updatedAt,
         deletedAt
  from {{ source('commons', 'phone_call') }}
),

karuna_phone_calls as (

  select id,
         externalCallId,
         userId,
         patientId,
         contactNumber,
         case
           when isInitiallyToFieldTeam = true then 'field team'
           when isInitiallyToFieldTeam = false then 'virtual hub'
           end as initialCallReceiver,
         case
           -- If a call was transferred from virtual hub (through Talkdesk), this is assumed to be to a field team member (TODO: find a way to make sure)
           when isTransferredFromTalkdesk and not isTransferredToAnsweringService then 'field team'
           -- If there is a matching talkdesk call, and the call did not go to answering service, then virtual hub picked up
           when hasMatchingTalkdeskCall and not isTransferredToAnsweringService then 'virtual hub'
           -- If a call was intended for field team and not transferred to Talkdesk or answering service, the field team member picked up the call
           when isInitiallyToFieldTeam and not isTransferredToAnsweringService and not hasMatchingTalkdeskCall then 'field team'
           end as finalCallReceiver,
         direction,
         durationSeconds,
         case
           when (isTransferredToAnsweringService or isAbandoned) then 'missed'
           else 'completed'
           end as callStatus,
         case
           when isTransferredToAnsweringService then 'voicemail'
           when isAbandoned then 'abandoned'
           end as missedReason,
         hasMatchingTalkdeskCall as isTransferredToVirtualHub,
         cast(null as boolean) as isTransferredToClinic,
         isTransferredFromTalkdesk as isTransferredToFieldTeam,
         isTransferredToAnsweringService,
         isTransferredToVoicemail,
         isRecorded,
         isAbandoned,
         contactPhoneType as device,
         'karuna' as provider,
         providerCreatedAt,
         cast(null as timestamp) as providerUpdatedAt,
         dataLoadedAt as createdAt,
         cast(null as timestamp) as updatedAt,
         cast(null as timestamp) as deletedAt
  from {{ source('karuna', 'phone_call_karuna') }}
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
    c.id,
    c.externalCallId,
    c.userId,
    coalesce(c.patientId, pnml.patientId) as patientId,
    c.contactNumber,
    c.initialCallReceiver,
    c.finalCallReceiver,
    c.direction,
    c.durationSeconds,
    c.callStatus,
    c.missedReason,
    c.isTransferredToVoicemail,
    c.isTransferredToAnsweringService,
    c.isTransferredToClinic,
    c.isTransferredToVirtualHub,
    c.isTransferredToFieldTeam,
    c.device,
    c.provider,
    c.providerCreatedAt,
    c.providerUpdatedAt,
    c.createdAt,
    c.updatedAt,
    c.deletedAt
  from commons_phone_calls c
  left join phone_number_member_lookup pnml
    on c.contactNumber = pnml.phoneNumber

    union all

  select
    id,
    externalCallId,
    userId,
    patientId,
    contactNumber,
    initialCallReceiver,
    finalCallReceiver,
    direction,
    durationSeconds,
    callStatus,
    missedReason,
    isTransferredToVoicemail,
    isTransferredToAnsweringService,
    isTransferredToClinic,
    isTransferredToVirtualHub,
    isTransferredToFieldTeam,
    device,
    provider,
    providerCreatedAt,
    providerUpdatedAt,
    createdAt,
    updatedAt,
    deletedAt
  from karuna_phone_calls
)

select * from final
