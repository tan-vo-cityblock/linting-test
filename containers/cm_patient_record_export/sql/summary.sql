with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),

acuity as (
  select patientId,
  currentMemberAcuityScore, 
  currentMemberAcuityDescription
  from `cityblock-analytics.mrt_commons.member_current_acuity` ),

summary as (
  select patientId,
  summary
  from `cbh-db-mirror-prod.commons_mirror.patient_health_summary`
  where deletedAt is null), 

final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  a.currentMemberAcuityScore as acuityScore,
  a.currentMemberAcuityDescription as acuityClassification,
  REPLACE(s.summary, '\n\n\n', '\n') as memberimpressionSummary
  from  info i
  inner join acuity a
  on i.patientId = a.patientId
  inner join summary s
  on i.patientId = s.patientId )
  
select * from final


