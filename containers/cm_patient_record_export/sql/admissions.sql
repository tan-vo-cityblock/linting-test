with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),

admissions as (
  select patientId,
  eventDateTime,
  eventType,
  visitType,
  notes,
  chiefComplaintCode,
  chiefComplaintDescription,
  dischargeDateTime
  from `cbh-db-mirror-prod.commons_mirror.hie_event` ),

final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  a.eventDateTime,
  a.eventType,
  --a.visitType,
  --a.notes as eventNotes,
 -- a.chiefComplaintCode as eventComplaintCode,
  a.chiefComplaintDescription as eventComplaintDescription,
  a.dischargeDateTime as eventDischargeDateTime 
  from info i
  inner join admissions a
  on i.patientId = a.patientId )

select * from final
order by eventDateTime
