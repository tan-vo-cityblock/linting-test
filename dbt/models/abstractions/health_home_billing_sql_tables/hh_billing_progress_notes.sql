-- obtaining note info for progress notes
with timeline as (
  select patientId,
  id as progressNoteId,
  groupId as progressNoteGroupId,
  status as progressNoteStatus,
  eventType as progressNoteType,
  location as progressNoteLocation,
  eventTimeStamp as progressNoteTimestamp,
  isTransitionOfCare
  FROM {{ ref('timeline_event') }}
  where deletedAt is null
),

-- obtaining attendee info for progress notes
attendee as (
  select eventGroupId,
  max(case
   when lower(idSource) = 'patient' then true
   when idSource is null and lower(attendee) = 'member' then true
   else false
   end) as hasMemberAttendee,
 max(case
   when idSource is null and lower(attendee) = 'child' then true
   when idSource is null and lower(attendee) = 'other family' then true
   when idSource is null and lower(attendee) = 'spouse' then true
   when idSource is null and lower(attendee) = 'sibling' then true
   when idSource is null and lower(attendee) = 'parent or guardian' then true
   when idSource is null and lower(attendee) = 'friend' then true
   when idSource is null and lower(attendee) = 'partner' then true
   when idSource is null and lower(attendee) = 'grandparent' then true
   when idSource is null and lower(attendee) = 'member agent/support' then true
   else false
   end) as hasFamilyAttendee,
 max(case
   when lower(idSource) = 'user' then true
   when lower(attendee) = 'chp' then true
   when lower(attendee) = 'chp lead' then true
   when lower(attendee) = 'outreach specialist' then true
   when lower(attendee) = 'community engagement manager' then true
   else false
   end) as hasCareTeamAttendee,
 max(case
   when idSource is null and lower(attendee) = 'provider' then true
   when idSource is null and lower(attendee) = 'external provider' then true
   when idSource is null and lower(attendee) = 'external care team member' then true
   else false
   end) as hasExternalProviderAttendee,
 max(case
   when idSource is null and lower(attendee) = 'community-based organization' then true
   when idSource is null and lower(attendee) = 'cbo' then true
   else false
   end) as hasCboAttendee,
 max(case
   when idSource is null and lower(attendee) = 'government agency' then true
   when idSource is null and lower(attendee) = 'social service agency' then true
   when idSource is null and lower(attendee) = 'insurance plan representative' then true
   when idSource is null and lower(attendee) = 'other' then true
   else false
   end) as hasGovernmentOrOtherAttendee
 from {{ source('commons', 'event_attendee') }}
 where deletedAt is null
 group by eventGroupId
 ),

--joining all together into progress note table
interventions as (
select
  t.patientId,
  t.progressNoteId,
  t.progressNoteGroupId,
  t.progressNoteTimestamp,
  date(t.progressNoteTimestamp) as intervention_date,
  t.progressNoteType,
  case
    when progressNoteType  = "phoneCall" then 2
    when progressNoteType  = "email" then 4
    when progressNoteType  = "videoCall" then 6
    else 3 end as mode,
  case
    when hasMemberAttendee is true then 1
    when hasFamilyAttendee is true then 5
    when progressNoteType = "caseConference" AND hasCareTeamAttendee is true then 3
    when hasExternalProviderAttendee is true then 4
    when hasCareTeamAttendee is true then 2
    when hasCboAttendee is true then 4
    when hasGovernmentOrOtherAttendee is true then 6
    else 6 end as target,
  5 as outreach,
  t.progressNoteLocation,
  t.progressNoteStatus,
  a.hasMemberAttendee,
  a.hasFamilyAttendee,
  a.hasCareTeamAttendee,
  a.hasExternalProviderAttendee,
  a.hasCboAttendee,
  a.hasGovernmentOrOtherAttendee,
  t.isTransitionOfCare
from timeline t
inner join attendee a
  on t.progressNoteGroupId = a.eventGroupId
  or t.progressNoteId = a.eventGroupId
),

interventions_final as (select
*,
case
    when progressNoteStatus = 'success'
        AND mode != 4 AND mode != 5 then 1
    else 5 end as completed_intervention,
case
    when target = 3 then 1
    when hasExternalProviderAttendee is true then 1
    else 5 end as care_coord_health_promote,
case when isTransitionOfCare is true then 1 else 5 end as transition_care,
case when hasFamilyAttendee is true then 1 else 5 end as patient_family_support,
case when hasCboAttendee is true then 1 else 5 end as comm_social,
from interventions
where (progressNoteLocation is not null
and not (progressNoteLocation = "Messaging")
and not (progressNoteType = "text")
and not (progressNoteType = "research")
and not (progressNoteType = "legacyUnlabeled"))
)

select
patientId,
intervention_date,
extract (year from intervention_date)||'.'||extract (quarter from intervention_date) as completed_quarter_yr,
completed_intervention,
mode,
target,
outreach,
care_coord_health_promote,
transition_care,
patient_family_support,
comm_social,
case
    when care_coord_health_promote = 5
        and transition_care = 5
        and patient_family_support = 5
        and comm_social = 5
      then 1
    else 5 end as care_manage
from interventions_final
