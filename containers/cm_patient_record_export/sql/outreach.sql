with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),

outreach as (
  select patientId,
  'Outreach Note' as noteType,
  modality,
  attemptedAt,
  observations,
  outcome,
  notes
  from `cbh-db-mirror-prod.commons_mirror.outreach_attempt`
  --where deletedAt is null
  ),
  
final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  o.noteType,
  o.modality as outreachNoteType,
  o.attemptedAt as outreachNoteDateTime,
  o.observations as outreachNoteObservations,
  o.outcome as outreachNoteOutcome,
  o.notes as outreachNoteText
  from info i
  inner join outreach o
  on i.patientId = o.patientId )

select * from final
order by outreachNoteDateTime
