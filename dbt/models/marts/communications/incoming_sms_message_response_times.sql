{{
  config(
    sort='providerCreatedAt'
  )
}}

with messages as (
  select
    *,
    coalesce(patientId, contactNumber) as contactIdentifier
  from {{ ref('sms_message') }}
),

calls as (

  select
    *,
    coalesce(patientId, contactNumber) as contactIdentifier
  from {{ ref('phone_call') }}
),

-- These are the messages for which we want to track response times
incoming_messages as (

  select id,
         userId,
         patientId,
         contactNumber,
         contactIdentifier,
         providerCreatedAt
  from messages
  where direction = 'incoming'
),


outgoing_messages as (
  select id,
         userId,
         patientId,
         contactNumber,
         contactIdentifier,
         direction,
         'sms_message' as responseType,
         providerCreatedAt
  from messages
  where direction = 'outgoing'
),

outgoing_calls_or_call_attempts as (

  select id,
         userId,
         patientId,
         contactNumber,
         contactIdentifier,
         direction,
         'phone_call' as responseType,
         providerCreatedAt
  from calls
  where direction = 'outgoing'
),

incoming_successful_calls as (

  select id,
         userId,
         patientId,
         contactNumber,
         contactIdentifier,
         direction,
         'phone_call' as responseType,
         providerCreatedAt
  from calls
  where direction = 'incoming'
        and callStatus in ('completed', 'incoming')
),

-- All possible ways in which a member can be responded to are listed below:
-- 1. Message from care team to member
-- 2. (Attempted) call from care team to member
-- 3. Successful call from member to care team
all_responses as (

  select * from outgoing_messages
  
    union all
    
  select * from outgoing_calls_or_call_attempts

    union all

  select * from incoming_successful_calls

),

-- For incoming messages from known members, find the earliest responses to that same member, even if the response
-- was sent to a different phone number associated with that member

-- Collect information on earliest response, irrespective of whether it was the same user that responded
incoming_message_ids_with_all_responses as (

  select
    im.id,

    r_all.id                                                                as earliestResponseId,
    r_all.responseType                                                      as earliestResponseType,
    r_all.userId                                                            as earliestResponseUserId,
    r_all.direction                                                         as earliestResponseDirection,
    r_all.providerCreatedAt                                                 as earliestResponseSentAt,
    timestamp_diff(r_all.providerCreatedAt, im.providerCreatedAt, second)   as earliestResponseTimeSeconds, 
    row_number() over (partition by im.id order by r_all.providerCreatedAt) as earliestResponseRecordNumber
  from incoming_messages im
  left join all_responses r_all
    on im.contactIdentifier = r_all.contactIdentifier
       and r_all.providerCreatedAt > im.providerCreatedAt
),

-- Collect information on the earliest response by the same user
incoming_message_ids_with_same_user_responses as (

  select
    im.id,

    r_same_user.id                                                                as earliestSameUserResponseId,
    r_same_user.responseType                                                      as earliestSameUserResponseType,
    r_same_user.direction                                                         as earliestSameUserResponseDirection,
    r_same_user.providerCreatedAt                                                 as earliestSameUserResponseSentAt,
    timestamp_diff(r_same_user.providerCreatedAt, im.providerCreatedAt, second)   as earliestSameUserResponseTimeSeconds,
    row_number() over (partition by im.id order by r_same_user.providerCreatedAt) as earliestSameUserResponseRecordNumber
  from incoming_messages im
  left join all_responses r_same_user
    on im.contactIdentifier = r_same_user.contactIdentifier
       and im.userId = r_same_user.userId
       and r_same_user.providerCreatedAt > im.providerCreatedAt
),

final as (

  select
    im.id,
    im.userId,
    im.patientId,
    im.contactNumber,
    im.providerCreatedAt,
    r_all.earliestResponseId,
    r_all.earliestResponseType,
    r_all.earliestResponseUserId,
    r_all.earliestResponseDirection,
    r_all.earliestResponseSentAt,
    r_all.earliestResponseTimeSeconds, 
    r_same_user.earliestSameUserResponseId,
    r_same_user.earliestSameUserResponseType,
    r_same_user.earliestSameUserResponseDirection,
    r_same_user.earliestSameUserResponseSentAt,
    r_same_user.earliestSameUserResponseTimeSeconds
  from incoming_messages im
  left join incoming_message_ids_with_all_responses r_all
    using (id)
  left join incoming_message_ids_with_same_user_responses r_same_user
    using (id)
  where r_all.earliestResponseRecordNumber = 1
        and r_same_user.earliestSameUserResponseRecordNumber = 1
)

select * from final
