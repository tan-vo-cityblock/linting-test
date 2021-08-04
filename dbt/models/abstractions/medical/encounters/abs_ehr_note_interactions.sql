
with notes as (

  select *,
    date(encounterDateTime) as encounterDate
    
  from {{ ref('abs_ehr_note_encounters') }}

),

clinical_intake_appts as (

  select distinct
    ehrAppointmentId as clinicalIntakeId,
    patientId,
    userId,
    date(appointmentAt) as encounterDate,
    appointmentCreatedAt

  from {{ ref('abs_ehr_appointment_statuses') }}
  where
    appointmentStatus = 'completed' and
    isClinicalIntakeAppointment

),

ranked_appts as (

  select *,
    rank() over(partition by patientId, encounterDate order by appointmentCreatedAt) as rnk

  from clinical_intake_appts

),

final as (

  select
    n.* except (encounterDate),

    case
      when n.encounterSource = 'acpny' then n.isClinicalIntakeEncounter
      else a.clinicalIntakeId is not null 
    end as isAssociatedWithClinicalIntake,
    
    case
      when n.encounterSource = 'acpny' then n.encounterId
      else a.clinicalIntakeId
    end as clinicalIntakeId

  from notes n
  left join ranked_appts a
  on
    n.patientId = a.patientId and
    n.userId = a.userId and
    n.encounterDate = a.encounterDate and
    n.encounterIdType = 'Visit Note' and
    n.encounterType in ('Telemedicine Note', 'House Call Visit Note', 'Office Visit Note')

)

select * from final
