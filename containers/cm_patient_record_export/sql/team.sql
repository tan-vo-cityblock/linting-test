with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),

status as (
  select patientId,
  cohortName,
  patientHomeClinicName,
  currentState,
  partnerAttributedPcpName,
  partnerAttributedIsCbhPcp
  from `cityblock-analytics.mrt_commons.member` ),

emergency_contact as (
 select distinct
        e.id,
        e.patientId,
        concat(e.firstName, ' ', e.lastName) as emergencyContactName,
        e.relationToPatient,
        p.emergencyContactNumber
 from `cbh-db-mirror-prod.commons_mirror.patient_contact` as e

 inner join

 (  select
        phoneId,
        patientContactId,
        b.phoneNumber as emergencyContactNumber
    from `cbh-db-mirror-prod.commons_mirror.patient_contact_phone` a
    inner join `cbh-db-mirror-prod.commons_mirror.phone` b
        on a.phoneId = b.id ) p

  on e.id = p.patientContactId
 ),

external_team as (

    select patientId,
           mcc.patientExternalProviderId,
           providerName,
           providerRole,
           p.phoneNumber as contact,
           organizationName
    from `cityblock-analytics.mrt_commons.member_external_care_team` mcc
    inner join `cbh-db-mirror-prod.commons_mirror.patient_external_provider_phone` epp
        on epp.patientExternalProviderId = mcc.patientExternalProviderId
    inner join `cbh-db-mirror-prod.commons_mirror.phone` p
        on p.id = epp.phoneId
 ),

team as (
-- CBH team
select patientId,
  careTeamMemberAssignedAt,
  userName as careTeamMemberName,
  userRole as careTeamMemberRole,
  userEmail as contact,
  'Cityblock' as teamName,
  'Cityblock' as organization,
   isPrimaryContact
  from `cityblock-analytics.mrt_commons.member_current_care_team_all`
  --needs to be removed for MA tufts market.
 -- where userRole = 'Nurse_Care_Manager' OR userRole = 'Community_Health_Partner'

 union all
-- external team
select patientId,
       timestamp(null) as careTeamMemberAssignedAt,
       providerName as careTeamMemberName,
       providerRole as careTeamMemberRole,
       contact,
       'External' as teamName,
       organizationName as organization,
       false as isPrimaryContact
    from external_team
 ),
  
final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  s.cohortName,
  s.patientHomeClinicName as homeClinicName,
  s.currentState as currentStatus,
  --s.partnerAttributedPcpName,
  --s.partnerAttributedIsCbhPcp,
  t.careTeamMemberAssignedAt as dateAssignedToCareTeam,
  t.careTeamMemberName,
  t.careTeamMemberRole,
  t.contact,
  t.teamName,
  t.Organization,
  t.isPrimaryContact
  from info i
  inner join status s
  on i.patientId = s.patientId
  inner join team t
  on s.patientId = t.patientId

  union all

  select i.patientId as memberId,
         i.patientName as memberName,
         s.cohortName,
         s.patientHomeClinicName as homeClinicName,
         s.currentState as currentStatus,
         timestamp(null) as dateAssignedToCareTeam,
         e.emergencyContactName as careTeamMemberName,
         e.relationToPatient as careTeamMemberRole,
         e.emergencyContactNumber as contact,
         'External' as teamName,
         null as Organization,
         false as isPrimaryContact
    from info i
  inner join status s
  on i.patientId = s.patientId
  inner join emergency_contact e
  on e.patientId = i.patientId
  )
  
select * from final
order by teamName, dateAssignedToCareTeam nulls last
