with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),

progress as (
  select m.patientId,
  'Progress Note' as noteType,
  m.eventType, 
  m.location,
  m.eventTimeStamp, 
  m.status,
  coalesce(m.progressNoteText, n.text)as progressNoteText,
  u.userName,
  u.userRole
  from `cityblock-analytics.mrt_commons.member_interactions` m
  left join `cbh-db-mirror-prod.commons_mirror.note_transformed` n
  on m.progressNoteId = n.id
  left join `cityblock-analytics.mrt_commons.user` u
  on u.userId = m.userId
),
  
final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  p.noteType,
  p.eventType as progressNoteType, 
  p.location as progressNoteLocation,
  p.eventTimeStamp as progressNoteDateTime, 
  --p.status as progressNoteOutcome,
  p.progressNoteText,
  p.userName,
  p.userRole
  from info i
  left join progress p
  on i.patientId = p.patientId )

select * from final
order by progressNoteDateTime
