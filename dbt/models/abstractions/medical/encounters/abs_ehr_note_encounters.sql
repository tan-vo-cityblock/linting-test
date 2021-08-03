
with base_encounters as (

  select
    patient.patientId as patientId,
    lower(patient.source.name) as encounterSource,
    identifier.id as encounterId,
    identifier.idType as encounterIdType,
    encounter.type.name as encounterType,
    encounter.dateTime.instant as encounterDateTime,
    provider_identifier.id as providerId,
    lower(regexp_replace(provider.firstName || provider.lastName, r"[ -]", "")) as providerName,
    location.name as locationName,
    nullif(trim(reason.notes), '') as encounterNote,
    coalesce(
      reason.name like '%Clinical Intake%' and
      encounter.type.name not like 'Telephon%', false
    ) as isClinicalIntakeEncounter

  from {{ ref('src_patient_encounters_latest') }}
  
  left join unnest(encounter.identifiers) as identifier
  left join unnest(encounter.providers) as provider
  left join unnest(provider.identifiers) as provider_identifier
  left join unnest(encounter.locations) as location
  left join unnest(encounter.reasons) as reason
  
  where 
   ( patient.source.name = 'elation' and
    identifier.idType in ('Visit Note', 'Non-Visit Note'))
    or 
   ( lower(patient.source.name) = 'acpny' and 
    encounter.type.name in ('Telephone','Office Visit','Patient Outreach','Telemedicine','Telephonic Check In', 'Procedure visit',
    'Initial consult','Office Diagnostic Testing','Infusion','Anticoag visit','Home Care Visit',
    'Cardiology Services','Routine Prenatal','Education','Postpartum Visit','Follow-Up','Initial Prenatal'))
),

user_identifiers as (

  select
    userId,
    lower(regexp_replace(userName, r"[ -]", "")) as userName,
    userRole,
    userNpi,
    userEmail
    
  from {{ ref('user') }}

  where userTerminationDate is null

),

member_tiebreakers as (

  select
    id as patientId,
    updatedAt,
    deletedAt is not null as isDeleted
    
  from {{ source('member_index', 'member') }}

),

aggregated_encounters as (

  select
    * except (locationName, encounterNote),
    string_agg(distinct locationName) as locationName,
    string_agg(distinct encounterNote, '; ' order by encounterNote) as encounterNotes
  
  from base_encounters  
  
  {{ dbt_utils.group_by(n = 9) }}

),

final as (

  select distinct
    e.* except (providerId, providerName, locationName, encounterNotes),
    u.userId,
    e.locationName,
    e.encounterNotes,
    rank() over(
      partition by e.encounterId
      order by
        u.userRole = 'Physician' desc,
        u.userRole != 'Support_Specialist' desc,
        u.userRole like '%Nurse_Practitioner' desc,
        e.providerId = u.userEmail desc,
        m.isDeleted desc,
        m.updatedAt desc
    ) as rnk
  
  from aggregated_encounters e
  
  inner join user_identifiers u
  on
    e.providerId = u.userEmail or
    e.providerId = u.userNpi or
    e.providerName = u.userName

  left join member_tiebreakers m
  using (patientId)

)

select * except(rnk) from final where rnk = 1
