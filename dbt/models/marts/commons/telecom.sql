with dialpad_call as (
    select
      pc.*,
      CAST(JSON_EXTRACT_SCALAR(providerPayload, '$.date_ended') as DATETIME) date_ended,
      JSON_EXTRACT_SCALAR(providerPayload, '$.target_id') target_id,
      JSON_EXTRACT_SCALAR(providerPayload, '$.transferred_from_target_id') transferred_from_target_id,
      JSON_EXTRACT_SCALAR(providerPayload, '$.transferred_to_contact_id') transferred_to_contact_id


      from {{ source('commons', 'phone_call' )}} as pc
    where provider='dialpad'
    and direction = 'toUser'
),
transferred_inbound as (
    select
      p1.id as callIdUser,
      p2.id as callIdDept,
      p1.userId,
      p1.contactNumber,
      p1.patientId,
      p1.direction,
      p1.provider,
      p1.providerCreatedAt providerCreatedAtUser,
      p2.providerCreatedAt as providerCreatedAtDept,
      p1.date_ended as dateEndedUser,
      p2.date_ended as dateEndedDept,
      p1.callStatus as callStatusUser,
      CASE WHEN p2.isTransferredToClinic is true then 'clinic_transfer' else p2.callStatus END as callStatusDept,
      p1.duration + p2.duration as duration
    FROM
      dialpad_call p1, dialpad_call p2
    WHERE p1.userId=p2.userId and p1.contactNumber=p2.contactNumber and p1.target_id=p2.transferred_from_target_id --p2.target_id=p1.transferred_to_contact_id
    AND p1.callReceiver='user' and p2.callReceiver='department' and p1.isTransferredToVoicemail is true
    AND DATETIME_DIFF(CAST(p2.providerCreatedAt AS DATETIME), p1.date_ended, SECOND) BETWEEN 0 AND 5
),
non_transferred_inbound as (
    select * from dialpad_call where id NOT IN (select callIdUser from transferred_inbound) and callReceiver='user' # crucial callReceiver line here
),
dialpad_outbound_and_twilio as (
    select *
    from {{ source('commons', 'phone_call' )}} as pc
    where provider='twilio' or (provider='dialpad' and direction = 'fromUser') and callReceiver='user' # optional callReceiver line here
),
dialpad_twilio_union as (
    select callIdUser, userId, contactNumber, patientId, providerCreatedAtUser, providerCreatedAtDept, direction, duration, callStatusUser, callStatusDept,
    case when callStatusDept is null then callStatusUser else callStatusDept end as callStatusFinal, provider
    from transferred_inbound

    union all

    select id as callIdUser, userId, contactNumber, patientId, providerCreatedAt as providerCreatedAtUser, NULL as providerCreatedAtDept, direction, duration, callStatus as callStatusUser, NULL as callStatusDept, callStatus as callStatusFinal, provider
    from non_transferred_inbound

    union all

    select id as callIdUser, userId, contactNumber, patientId, providerCreatedAt as providerCreatedAtUser, NULL as providerCreatedAtDept, direction, duration, callStatus as callStatusUser, NULL as callStatusDept, callStatus as callStatusFinal, provider
    from dialpad_outbound_and_twilio
),

-- Final abstraction table with 1 row per call "bundle"
dialpad_twilio_final as (
    select t.*, CASE when user.phone is not null then TRUE else FALSE END AS isInternalContactNumber, "Phone call" as type
    from dialpad_twilio_union t
    left join {{ source('commons', 'user' )}} as user
  on t.contactNumber = user.phone
),

-- Add response times
response_pre as (
    SELECT A.*,
    TIMESTAMP_DIFF(B.providerCreatedAtUser, A.providerCreatedAtUser, MINUTE) as responseTimeMins,
    B.callStatusFinal='outgoing' as responseCompleted,
    ROW_NUMBER() OVER(PARTITION BY A.callIdUser ORDER BY B.providerCreatedAtUser) AS row_number -- label responses chronologically.  Select the first outside CTE
    FROM
    dialpad_twilio_final as A
    inner join
    dialpad_twilio_final as B
    on A.contactNumber=B.contactNumber --and A.type=B.type
    where A.direction="toUser" and B.direction="fromUser"
    and TIMESTAMP_DIFF(B.providerCreatedAtUser, A.providerCreatedAtUser, MINUTE) BETWEEN 0 and 60*32 -- require row_number to begin from non negative repsonses!
    and A.callStatusFinal IN ("no-answer","busy","missed","abandoned","voicemail")
)

select d.*, r.callIdUser callIdUserResponse, r.providerCreatedAtUser providerCreatedAtUserResponse, responseTimeMins, responseCompleted
from dialpad_twilio_final d
left join response_pre r
on d.callIdUser=r.callIdUser
and r.row_number=1  # 'where' doesn't preserve nulls since final result is filtered, 'and' preserves nulls