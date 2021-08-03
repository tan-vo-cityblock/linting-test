select
  id,
  id as groupId,
  patientId,
  userId,
  'outreach' as noteType,
  direction,
  modality,

  case
    when modality in ('phone', 'careTransitionsPhone')
      then 'phoneCall'
    when modality in ('homeVisit', 'careTransitionsInPerson')
      then 'inPersonVisitOrMeeting'
    else modality
  end as groupedModality,

  case
    when modality = 'community' then 'Community'
    when modality = 'hub' then 'Hub'
    when modality = 'homeVisit' then 'Member Home'
    else null
  end as location,

  if(personReached = 'noOne', 'attempt', 'success') as status,
  cast(null as bool) as isLegacy,
  if(modality like 'careTransitions%', true, false) as isTransitionOfCare,
  cast(null as bool) as isReferral,
  string(null) as referral,
  string(null) as referralReason,
  isScheduledVisit as isScheduledOutreach,
  outcome as outreachOutcome,
  notInterestedReason as outreachNotInterestedReason,
  attemptedAt as interactionAt,
  createdAt,
  deletedAt,
  personReached as attendees,
  string(null) as templateSlug,
  notes as noteText

from {{ source('commons', 'outreach_attempt') }}
