with members as (  

  select
    patientId,
    upper(firstName) as firstName,
    upper(lastName) as lastName,
    currentCareModel,
    mrn,
    currentState,
    lineOfBusiness,
    memberPod

  from {{ ref('member') }}
  
),

high_priority_members as (

  select
    patientId,
    count(distinct conditionName) as conditionCount

  from {{ ref('all_suspects_ranked') }}
  where
    partner = 'emblem' and
    conditionStatus = 'OPEN' and
    extract(year from runDate) = extract(year from current_date)

  group by patientId

),

users as (

  select
    userId,
    userName as providerName,
    userRole
    
  from {{ ref('user') }}
  where
    userRole like '%Nurse_Practitioner%' or
    userRole like '%Physician%'

),

abs_ehr_note_encounters as (

  select
    patientId,
    userId,
    encounterType as eventType,
    encounterNotes as eventDescription,
    encounterDateTime as eventTimestamp,
    null as appointmentLength,
    'past_appointment' as appointmentStatus,
    
    case
      when lower(encounterType) like ('%phone%') or lower(encounterType) like '%phonic%'
        then 'Telephone' 
      when lower(encounterType) like '%video%' or lower(encounterType) like '%telehealth%' or lower(encounterType) like '%telemedicine%'
        then 'Video' 
      when lower(encounterType) like ('%house%') or lower(encounterType) like ('%home%') or lower(encounterType) like '%home%'
        then 'Home' 
      else 'Other In-Person'
    end as modality,
    
    false as wasRescheduled,
    false as isFutureAppointment, 
    cast(encounterDateTime as date) as createdDate,
    isClinicalIntakeEncounter as isClinicalIntake,
    true as wasCompleted
    
  from {{ ref('abs_ehr_note_encounters') }}
  where encounterSource = 'acpny'

),

member_appointment_statuses as (

  select
    patientId,
    providerName,
    appointmentType, 
    description,
    startAt,
    timestamp_diff(endAt, startAt, minute),
    derivedAppointmentStatus,
    appointmentModality,
    wasRescheduled,
    true as isFutureAppointment, 
    cast(createdAt as date),
    isClinicalIntakeAppointment,
    wasCompleted
  
  from {{ ref('member_appointment_statuses') }}
  where
    derivedAppointmentStatus = 'upcoming_appointment' and
    isLatestAppointmentStatus and
    facility in ('Cityblock Virtual Hub', 'CBCE')

),

past_appointments as (

  select  
    n.patientId,
    u.providerName, 
    u.userRole as providerCredentials,
    n.* except (patientId, userId)

  from abs_ehr_note_encounters n

  inner join users u
  using (userId)
  
),

future_appointments as (

  select
    a.patientId,
    a.providerName,
    u.userRole, 
    a.* except (patientId, providerName)
    
  from member_appointment_statuses a
  
  inner join users u
  using (providerName)
    
),

combined_appointments as (

  select * from past_appointments
  union all
  select * from future_appointments

),

member_appointments as (

  select
    a.patientId,
    'Cityblock - New York' as practiceName,
    extract(time from eventTimestamp) as eventTime, 
    extract(date from eventTimestamp) as eventDate,
    m.* except (patientId),
    a.* except (patientId)
  
  from combined_appointments a
  
  inner join members m
  using (patientId)

),

final as (

  select 
    a.patientId,
    a.firstName,
    a.lastName,
    a.currentCareModel,
    a.mrn,
    a.providerName,
    a.practiceName,
    a.providerCredentials,
    a.eventType,
    a.eventDescription, 
    a.eventTimestamp,
    a.appointmentStatus,
    a.modality,
    a.eventTime, 
    a.eventDate, 
    a.appointmentLength,
    a.wasRescheduled,
    a.isFutureAppointment,
    a.createdDate,
    a.currentState,
    hp.patientId is not null as isHighPriorityPatient,
    a.isClinicalIntake,
    a.eventDate as intakeVisitDate,
    a.wasCompleted,
    a.lineOfBusiness,
    a.memberPod,
    hp.conditionCount

  from member_appointments a

  left join high_priority_members hp
  using (patientId)

)

select * from final  
