with phone_calls as (

  select id as callId,
         cast(null as string) as messageId,
         userId,
         patientId,
         contactNumber,
         coalesce(patientId, contactNumber) as contactIdentifier,
         'phone_call' as type,
         direction,
         callStatus,
         providerCreatedAt
  from {{ ref('phone_call') }}
),

outgoing_sms_messages as (

  select cast(null as string) as callId,
         id as messageId,
         userId,
         coalesce(patientId, contactNumber) as contactIdentifier,
         'sms_message' as type,
         direction,
         cast(null as string) as callStatus,
         providerCreatedAt
  from {{ ref('sms_message') }}
  where direction = 'outgoing'
),

incoming_calls as (

  select callId,
         userId,
         patientId,
         contactNumber,
         contactIdentifier,
         providerCreatedAt
  from phone_calls
  where direction = 'incoming'
),

return_calls_and_attempts as (

  select callId,
         messageId,
         userId,
         contactIdentifier,
         type,
         direction,
         callStatus,
         providerCreatedAt
  from phone_calls
  where direction = 'outgoing'
),

successful_incoming_calls as (

  select callId,
         messageId,
         userId,
         contactIdentifier,
         type,
         direction,
         callStatus,
         providerCreatedAt
   from phone_calls
   where direction = 'incoming'
         and callStatus = 'completed'
),

all_potential_responses as (

  select * from return_calls_and_attempts

    union all

  select * from successful_incoming_calls

    union all

  select * from outgoing_sms_messages
),

incoming_call_ids_with_all_responses as (

  select ic.callId,
         apr_all.callId as earliestResponseCallId,
         apr_all.messageId as earliestResponseMessageId,
         apr_all.userId as earliestResponseUserId,
         timestamp_diff(apr_all.providerCreatedAt, ic.providerCreatedAt, second) as earliestResponseTimeSeconds,
         apr_all.type as earliestResponseType,
         apr_all.direction as earliestResponseDirection,
         apr_all.callStatus as earliestResponseCallStatus,
         row_number() over (partition by ic.callId order by apr_all.providerCreatedAt) as earliestResponseRecordNumber,
         apr_all.providerCreatedAt as earliestResponseAt
  from incoming_calls ic
  left join all_potential_responses apr_all
    on ic.contactIdentifier = apr_all.contactIdentifier
       and ic.providerCreatedAt < apr_all.providerCreatedAt
),

incoming_call_ids_with_same_user_responses as (

  select ic.callId,
         apr_same.callId as earliestSameUserResponseCallId,
         apr_same.messageId as earliestSameUserResponseMessageId,
         timestamp_diff(apr_same.providerCreatedAt, ic.providerCreatedAt, second) as earliestSameUserResponseTimeSeconds,
         apr_same.type as earliestSameUserResponseType,
         apr_same.direction as earliestSameUserResponseDirection,
         apr_same.callStatus as earliestSameUserResponseCallStatus,
         row_number() over (partition by ic.callId order by apr_same.providerCreatedAt) as earliestSameUserResponseRecordNumber,
         apr_same.providerCreatedAt as earliestSameUserResponseAt
  from incoming_calls ic
  left join all_potential_responses apr_same
    on ic.contactIdentifier = apr_same.contactIdentifier
       and ic.userId = apr_same.userId
       and ic.providerCreatedAt < apr_same.providerCreatedAt
),

final as (

  select ic.callId,
         ic.userId,
         ic.patientId,
         ic.contactNumber,
         ic_all_resp.earliestResponseCallId,
         ic_all_resp.earliestResponseMessageId,
         ic_all_resp.earliestResponseUserId,
         ic_all_resp.earliestResponseTimeSeconds,
         ic_all_resp.earliestResponseType,
         ic_all_resp.earliestResponseDirection,
         ic_all_resp.earliestResponseCallStatus,
         ic_same_user_resp.earliestSameUserResponseCallId,
         ic_same_user_resp.earliestSameUserResponseMessageId,
         ic_same_user_resp.earliestSameUserResponseTimeSeconds,
         ic_same_user_resp.earliestSameUserResponseType,
         ic_same_user_resp.earliestSameUserResponseDirection,
         ic_same_user_resp.earliestSameUserResponseCallStatus,
         ic.providerCreatedAt,
         ic_all_resp.earliestResponseAt,
         ic_same_user_resp.earliestSameUserResponseAt
  from incoming_calls ic
  left join incoming_call_ids_with_all_responses ic_all_resp
    using (callId)
  left join incoming_call_ids_with_same_user_responses ic_same_user_resp
    using (callId)
  where ic_all_resp.earliestResponseRecordNumber = 1
        and ic_same_user_resp.earliestSameUserResponseRecordNumber = 1
)

select * from final
