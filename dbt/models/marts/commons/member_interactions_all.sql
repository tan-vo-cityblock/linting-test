with commons_notes as (

  select * except (userId, groupedModality, interactionAt, noteText),
    userId as createdById,
    groupedModality as eventType,
    interactionAt as eventTimestamp,
    noteText as progressNoteText

  from {{ ref('abs_commons_notes') }}

),

mapped_note_types as (

  select
    noteType as eventType,
    mappedType
  from {{ ref('dta_medical_mapped_note_types') }}

),

event_attendees as (

  select distinct
    eventGroupId as groupId,
    idSource,
    attendee
  from {{ source('commons','event_attendee') }}
  where deletedAt is null

),

outreach_notes as (

  select
    groupId,
    attendees = 'member' as hasMemberAttendee,
    attendees = 'support' as hasHealthCareProxyAttendee

  from commons_notes
  where
    noteType = 'outreach' and
    attendees in ('member', 'support')

),

outreach_member_attendees as (

  select groupId, hasMemberAttendee
  from outreach_notes
  where hasMemberAttendee

),

outreach_health_care_proxy_attendees as (

  select groupId, hasHealthCareProxyAttendee
  from outreach_notes
  where hasHealthCareProxyAttendee

),

{%- set definition_list = 'standard', 'toc' -%}

{#
  In the list below, any of the first four attendees classify a note as a standard connection or in-person visit.
  Any of the attendees classify a note as a transition-of-care (toc) connection or in-person visit.
#}

{%- set attendees = [
      'patient', 'Parent or Guardian', 'Health Care Proxy', 'Short Term Surrogate',
      'Partner', 'Child', 'Sibling', 'Grandparent', 'Other Family', 'Friend', 'Health Facility'
    ]
-%}

{%- set cte_list = [] -%}
{%- set standard_field_list = [] -%}
{%- set toc_field_list = [] -%}

{%- for attendee in attendees -%}

  {%- if attendee == 'patient' -%}

    {%- set cte_name = 'progress_member_attendees' -%}
    {%- set field_name = 'hasMemberAttendee' -%}

  {%- elif attendee == 'Health Care Proxy' -%}

    {%- set cte_name = 'progress_health_care_proxy_attendees' -%}
    {%- set field_name = 'hasHealthCareProxyAttendee' -%}

  {%- else -%}

    {%- set cte_name = attendee.lower() | replace(" ", "_") ~ '_attendees' -%}

    {%- if attendee == 'Parent or Guardian' -%}

      {%- set field_name = 'hasParentOrGuardianAttendee' -%}

    {%- else -%}

      {%- set field_name = 'has' ~ attendee | replace(" ", "") ~ 'Attendee' -%}

    {%- endif -%}

  {%- endif -%}

  {%- if attendee not in ['patient', 'Health Care Proxy'] -%}

    {%- set _ = cte_list.append(cte_name) -%}

  {%- endif -%}

  {%- set _ = toc_field_list.append(field_name) -%}

  {%- if attendee in ['patient', 'Parent or Guardian', 'Health Care Proxy', 'Short Term Surrogate'] -%}

    {%- set _ = standard_field_list.append(field_name) -%}

  {%- endif -%}

  {{ cte_name }} as (

    select
      groupId,
      true as {{ field_name }}
    from event_attendees
    where

      {% if attendee == 'patient' %}

        idSource = '{{ attendee }}'

      {% else %}

        attendee = '{{ attendee }}'

      {% endif %}

    ),

{% endfor %}

member_attendees as (

  select * from outreach_member_attendees
  union all
  select * from progress_member_attendees

),

health_care_proxy_attendees as (

    select * from outreach_health_care_proxy_attendees
    union all
    select * from progress_health_care_proxy_attendees

),

{%- set standard_attendees_or_clause = chain_operator(standard_field_list, 'or') -%}
{%- set toc_attendees_or_clause = chain_operator(toc_field_list, 'or') -%}

member_interactions_combined as (

  select
   if(n.noteType = 'progress', 'commons_progress_notes', 'commons_outreach_notes') as eventSource,
   n.patientId,
   n.createdById as userId,
   n.id as progressNoteId,
   n.groupId as progressNoteGroupId,
   n.eventType,
   n.location,
   mil.title as legacyTitle,
   mil.location as legacyLocation,
   n.status,
   n.isTransitionOfCare,
   n.direction,
   n.isReferral,
   n.referral,
   n.referralReason,
   timestamp_trunc(coalesce(eventTimestamp, mil.progressNoteTimestamp), second) as eventTimestamp,
   n.createdAt as progressNoteFirstCompletedAt,
   concat(n.patientId, n.createdById, n.eventType, n.groupId) as memberInteractionKey,
   isLegacy,
   case
    when attempted_tends_flag is null and isLegacy is TRUE
    then TRUE
    end as isExcludedLegacyProgressNote,
   true as isAttemptedTend,
   case
    when isLegacy
    then mil.tends_flag
    when eventType in ('mail','fax','videoCall','email','research','caseConference')
    then TRUE
    when eventType in ('phoneCall','inPersonVisitOrMeeting','text') and status = 'success'
    then TRUE
    end as isSuccessfulTend,

{% for definition in definition_list %}

  case
    when isLegacy
    then mil.attempted_connections_flag
    when eventType in ('text','videoCall','email','caseConference','phoneCall','inPersonVisitOrMeeting')

    {% if definition == 'standard' %}

    and ({{ standard_attendees_or_clause }})
    then true
  end as isAttemptedConnection,

    {% else %}

    and ({{ toc_attendees_or_clause }})
    then true
  end as isAttemptedTocConnection,

    {% endif %}

  case
    when isLegacy
    then mil.connections_flag
    when eventType in ('caseConference','email')

    {% if definition == 'standard' %}

    and ({{ standard_attendees_or_clause }})

    {% else %}

    and ({{ toc_attendees_or_clause }})

    {% endif %}

    then true
    when eventType in ('phoneCall','inPersonVisitOrMeeting','videoCall','text')
    and status = 'success'

    {% if definition == 'standard' %}

    and ({{ standard_attendees_or_clause }})
    then true
    end as isSuccessfulConnection,

    {% else %}

    and ({{ toc_attendees_or_clause }})
    then true
    end as isSuccessfulTocConnection,

    {% endif %}

  case
    when isLegacy
    then mil.attempted_f2fVisits_flag
    when eventType in ('inPersonVisitOrMeeting', 'videoCall', 'caseConference')

    {% if definition == 'standard' %}

    and ({{ standard_attendees_or_clause }})
    then true
  end as isAttemptedInPersonVisit,

    {% else %}

    and ({{ toc_attendees_or_clause }})
    then true
  end as isAttemptedTocInPersonVisit,

    {% endif %}

  case
    when isLegacy
    then mil.f2fVisits_flag
    when eventType in ('inPersonVisitOrMeeting')
    and status = 'success'

    {% if definition == 'standard' %}

    and ({{ standard_attendees_or_clause }})

    {% else %}

    and ({{ toc_attendees_or_clause }})

    {% endif %}

    then true
    when eventType in ('videoCall', 'caseConference')

    {% if definition == 'standard' %}

    and ({{ standard_attendees_or_clause }})
    then true
  end as isSuccessfulInPersonVisit,

    {% else %}

    and ({{ toc_attendees_or_clause }})
    then true
  end as isSuccessfulTocInPersonVisit,

    {% endif %}

  {% endfor %}

   false as isAssociatedWithClinicalIntake,
   string(null) as clinicalIntakeId,

  {% for field in toc_field_list %}

    {{ field }},

  {% endfor %}

  templateSlug,
  progressNoteText

  from commons_notes n
  left join {{ source('legacy', 'member_interactions_legacy_unused') }} mil
  on (n.id = mil.progressNoteid)

  left join member_attendees
  using (groupId)

  left join health_care_proxy_attendees
  using (groupId)

  {% for cte in cte_list %}

  left join {{ cte }}
  using (groupId)

  {% endfor %}
),

commons_final_output as (

  select * except (isExcludedLegacyProgressNote)
  from member_interactions_combined
  where isExcludedLegacyProgressNote is not TRUE

),

elation_epic_final_output as (

select
  case
    when encounterSource = 'elation' then 'elation_notes'
    when encounterSource = 'acpny' then 'epic_notes'
  end as eventSource,
  patientId,
  userId,
  encounterId as progressNoteId,
  encounterId as progressNoteGroupId,
  encounterType as eventType,
  --warning this needs to be updated at a later date once accurate location data flows through into Elation
  --we may pull from address column instead if needed
  case
    when
    (encounterDateTime < '2019-11-20' or encounterType in ('House Call Visit Note', 'RN Home Visit') )
    then 'Member Home'
  --Else statement here will be updated at a later date once we get clarity from market team on how EHR data is entered
    else 'Member Home or Hub'
  end as location,
  cast(null as string) as legacyTitle,
  cast(null as string) as legacyLocation,
  'success' as status,
  cast(null as bool) as isTransitionOfCare,
  cast(null as string) as direction,
  cast(null as bool) as isReferral,
  cast(null as string) as Referral,
  cast(null as string) as referralReason,
  encounterDateTime as eventTimestamp,
  encounterDateTime as progressNoteFirstCompletedAt,
  concat(patientId, userId, encounterType, encounterId) as memberInteractionKey,
  FALSE as isLegacy,
  TRUE as isAttemptedTend,
  TRUE as isSuccessfulTend,

{%- set status_list = ['Attempted', 'Successful'] -%}

{% for definition in definition_list %}

  {% for status in status_list %}

    encounterIdType = 'Visit Note' or encounterSource = 'acpny' as

      {% if definition == 'standard' %}

        is{{ status }}Connection,

      {% else %}

        is{{ status }}TocConnection,

      {% endif %}

  {% endfor %}

  {% for status in status_list %}

    encounterIdType = 'Visit Note' or (
    encounterSource = 'acpny' and
    encounterType not in ('Telephone', 'Patient Outreach', 'Telephonic Check In')
    ) as

    {% if definition == 'standard' %}

      is{{ status }}InPersonVisit,

    {% else %}

      is{{ status }}TocInPersonVisit,

    {% endif %}

  {% endfor %}

{% endfor %}

  isAssociatedWithClinicalIntake,
  clinicalIntakeId,

  {% for field in toc_field_list %}

    {% if field == 'hasMemberAttendee' %}
    {# mark member as only known attendee for EHR notes #}

      true as {{ field }},

    {% else %}

      false as {{ field }},

    {% endif %}

  {% endfor %}

  cast(null as string) as templateSlug,
  encounterNotes as progressNoteText

from {{ ref ('abs_ehr_note_interactions') }}

where encounterDateTime > '2019-06-30'

),

combined_notes as (

  select *
  from commons_final_output
  union all
  select *
  from elation_epic_final_output

),

{%- set initial_fields = 'eventSource, patientId, userId, progressNoteId, progressNoteGroupId, eventType' -%}

final as (

  select
    {{ initial_fields }},
    coalesce(mnt.mappedType, cn.eventType) as groupedEventType,
    cn.* except ({{ initial_fields }})
  from combined_notes cn
  left join mapped_note_types mnt
  using (eventType)

)

select *
from final
