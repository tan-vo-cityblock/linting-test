
with sms_messages as (

  select
    id as messageId,
    userId,
    patientId,
    contactNumber,
    direction,
    providerCreatedAt,
    -- Generate a unique hash for each pair that has contact with each other: if patientId is known, combine all
    -- phone numbers belonging to that member. Otherwise, the other half of the conversation is split out by phone number
    to_hex(md5(coalesce(userId, 'null') || coalesce(patientId, contactNumber))) as userContactHash,
    to_hex(md5(coalesce(userId, 'null') || coalesce(patientId, contactNumber) || direction)) as userContactDirectionHash
  from {{ ref('sms_message') }}
  where providerCreatedAt is not null  -- Our logic relies on knowing what time a message is sent
    and userId is not null
),

-- For known members, generate session and conversation information
previous_direction_and_time as (

  select
    *,
    lag(direction) over (partition by userContactHash order by providerCreatedAt) as previousDirection,
    lag(providerCreatedAt) over (partition by userContactHash order by providerCreatedAt) as previousProviderCreatedAt

  from sms_messages
),

new_session_conversation_flag as (
  select
    *,
    case when (previousDirection is null
               or previousDirection != direction
               or timestamp_diff(providerCreatedAt, previousProviderCreatedAt, second) > 200
               ) then 1
               else 0 end as newSessionFlag,
    case when (previousProviderCreatedAt is null
               or timestamp_diff(providerCreatedAt, previousProviderCreatedAt, hour) > 24
               ) then 1
               else 0 end as newConversationFlag
  from previous_direction_and_time
),

session_conversation_number as (

  select
    *,
    sum(newSessionFlag) over (partition by userContactDirectionHash order by providerCreatedAt) as sessionNumber,
    sum(newConversationFlag) over (partition by userContactHash order by providerCreatedAt) as conversationNumber
  from new_session_conversation_flag
),

final as (

  select
    messageId,

    -- Generate single uuid per session and conversation
    first_value(generate_uuid()) over (partition by userContactDirectionHash, sessionNumber order by providerCreatedAt) as sessionId,
    first_value(generate_uuid()) over (partition by userContactHash, conversationNumber order by providerCreatedAt) as conversationId,
    
    userId,
    patientId,
    contactNumber,
    direction,
    providerCreatedAt
  from session_conversation_number
)

select *
from final
order by providerCreatedAt
