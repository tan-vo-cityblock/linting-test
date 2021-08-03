with elation_appointments as (

  select
    id as ehrAppointmentId,
    mrn,
    physician_user_id as ehrUserId,
    appt_type as appointmentType,
    description,
    duration,
    timestamp(appt_time, 'America/Los_Angeles') as appointmentAt,
    timestamp(creation_time, 'America/Los_Angeles') as createdAt,
    status,
    billing_note as billingNote,
    -- elation business logic for clinical intake:
    case
      when
        (
          appt_type like 'Annual Provider Visit%' or
          appt_type like '%Medical Intake%'
        ) 

        and

        (
          appt_type like '%Home' or
          appt_type like '%Hub' or
          appt_type like '%Televideo'
        )
        then true
      else false
    end as isClinicalIntakeAppointment

  from {{ ref('src_elation_appointment_latest') }}

),

elation_appointment_statuses as (
  select * 
  from  {{ source('elation', 'appointment_status_latest') }} 
  where deletion_time is null
),

member as (

  select patientId, mrn
  from {{ ref('src_member') }}

),

user as (

  select
    id as ehrUserId,
    commonsUserId as userId,
    fullName as userName

  from {{ ref('src_elation_user_latest') }}

),

commons_appointments as (

  select * from {{ ref('abs_commons_appointments') }}

),

final_prep as (

  select
    ea.* except(status),
    m.patientId,
    u.userId,
    ca.commonsAppointmentId,
    eas.* except(id, creation_time, appointment_id),
    timestamp(eas.creation_time, 'America/Los_Angeles') as appointmentStatusCreatedAt,
    max(eas.creation_time) over(partition by eas.appointment_id) = eas.creation_time as isLatestAppointmentStatus,
    'elation' as ehrName

  from elation_appointments ea

  inner join member m
    using (mrn)
  
  inner join user u
    using (ehrUserId)

  left join commons_appointments ca
    using (patientId, userName, appointmentAt)

  left join elation_appointment_statuses eas 
    on ea.ehrAppointmentId = cast(eas.appointment_id as string)
),

-- order and column names matter here because we are unioning with other ehr's
final as (
  select distinct
    commonsAppointmentId, 
    ehrAppointmentId, 
    patientId, 
    mrn, 
    cast(null as string) as npi,
    cast(null as string) as ehrDepartment,
    cast(null as string) as ehrFacility,
    ehrUserId,
    userId,
    created_by_user_id as appointmentCreatedByEhrUserId, 
    appointmentAt, 
    createdAt as appointmentCreatedAt, 
    appointmentType, 
    description as appointmentDescription, 
    duration as appointmentDuration, 
    status as appointmentStatus, 
    appointmentStatusCreatedAt, 
    isLatestAppointmentStatus, 
    isClinicalIntakeAppointment, 
    ehrName
  from final_prep
)

-- extra step to fill in null commonsAppointmentIds where appointmentAt has been rescheduled over time
select 
  coalesce( f1.commonsAppointmentId, f2.commonsAppointmentId ) as commonsAppointmentId,
  f1.* except(commonsAppointmentId)
from final f1
left join (select distinct commonsAppointmentId, ehrAppointmentId from final where commonsAppointmentId is not null) f2
  on f1.ehrAppointmentId = f2.ehrAppointmentId
