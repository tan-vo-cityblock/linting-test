with base as (

  select distinct
    coalesce(appt.id, ehr.ehrAppointmentId) as id,
    appt.id as commonsAppointmentId,
    appt.patientId,
    appt.googleCalendarId,
    appt.googleCalendarEventId,
    appt.startAt,
    appt.endAt,
    case 
      when appt.title = '[EPIC] Cityblock Health' 
       and ehr.ehrName = 'elation' 
        then '[Elation] Cityblock Health'
      else appt.title end as title,
    coalesce( ehr.appointmentDescription, appt.description) as description,
    appt.generatedBy,
    appt.clinicId,
    appt.addressId,
    appt.locationFreeText,
    coalesce( ehr.appointmentCreatedAt, appt.createdAt) as createdAt,
    appt.createdById,
    appt.updatedAt,
    appt.deletedAt,
    appt.providerName,
    prov.lastName is not null as isCityblockProvider,
    appt.providerCredentials,
    appt.htmlLink,
    appt.googleRoomResourceEmail,
    coalesce( ehr.ehrFacility, appt.facility) as facility, 

    CASE 
      WHEN appt.generatedBy ='siu' THEN 'ehr' 
      WHEN appt.generatedBy = 'commons' THEN 'commons'
    END AS derivedSource,
      
    CASE 
      WHEN appt.deletedAt IS NOT NULL
      THEN 'deleted_appointment'
      
      WHEN appt.startAt > current_timestamp()
      THEN 'upcoming_appointment'
      
      WHEN appt.startAt < current_timestamp()
      THEN 'past_appointment'
      
      END AS derivedAppointmentStatus,

    CASE 
      WHEN (appt.updatedAt < appt.startAt AND appt.updatedAt > appt.createdAt)
        THEN TRUE  
      ELSE FALSE
      END AS wasRescheduled,

    -- appointment-level characteristics from elation & epic appointment abstractions upstream
    ifnull(ehr.ehrName, 'commons') as ehrName,
    ehr.ehrAppointmentId, 
    ehr.appointmentAt, -- should match the "startAt" above stored in commons appt table
    ehr.appointmentDuration,
    ehr.mrn, 
    ehr.appointmentType,
    ehr.appointmentModality,
    ehr.isClinicalIntakeAppointment,
    ehr.npi as physicianNpi, -- epic only
    ehr.ehrUserId, -- ehr user id of the physicican
    ehr.userId, -- commons user id of the physician
    ehr.appointmentCreatedByEhrUserId,
    ehr.ehrDepartment,
    -- appointment-status-level characteristics
    ehr.appointmentStatus,
    ehr.appointmentStatusCreatedAt,
    ehr.isLatestAppointmentStatus

  from {{ ref('src_commons_appointments') }} appt

  left join {{ ref('src_cityblock_providers') }} prov
    on upper(appt.providerName )  =  upper(concat( prov.firstName, ' ', prov.lastName )) 

-- join EHR unioned appointment data
-- changed to full outer because there are some ehr appts that are not present in commons appt table and these need to be surfaced for market ops users
  full outer join {{ ref('abs_ehr_appointment_statuses') }} ehr
    on appt.id = ehr.commonsAppointmentId
),

flags as (
  -- min date of each of these key appointment statuses, per appointment 
  select 
    id,
    {% for type in ["scheduled", "cancelled", "completed", "noShowed"] %}
    max(case when appointmentStatus = '{{ type }}' then true else false end) as {{ 'was' ~ type|capitalize }},
    min(case when appointmentStatus = '{{ type }}' then appointmentStatusCreatedAt else null end) as {{ 'was' ~ type|capitalize ~ 'At'}},
    {% endfor %}
  from base
  group by 1
)

select * from base
left join flags using(id)
