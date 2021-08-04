with commons_appointments as (

  select * from {{ ref('abs_commons_appointments') }}

),

-- due to historic issues with populating patientids on redox streaming data, we cannot depend on patientids from the redox messages table
member_mrns as  (
  select distinct
    mem.id as patientId,
    trim(mrn.mrn) as mrn 
  from {{ source('member_index', 'mrn') }} mrn
  left join {{ source('member_index', 'member') }} mem
    on mrn.id = mem.mrnId
  where mrn.deletedAt is null
    and mem.deletedAt is null
),

-- manually tracked historic backfill apv's in a static table uploaded from a google sheet
historic_apvs as (
  select
    patientId,
    userName,
    timestamp(datetime(appointmentDate, appointmentTime), "America/New_York") as appointmentAt,
    true as isAnnualProviderVisit
  from {{ source('medical_uploads', 'epic_apv_backfill') }}
),

-- pulling encounter.reasons.name, a free text field, to enable clinical intake business logic below 
encounter_reason_name as (
  select 
    name, 
    id,
    encounter.type.name as encounterType
  from {{ ref('src_patient_encounters_latest') }}
  left join unnest(encounter.identifiers) as identifier
  left join unnest(encounter.reasons) as reason
),

epic_appointments as (

  select
    mem_mrn.patientId,
    rm.patient.externalId as mrn,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitNumber'),'\"','') as ehrAppointmentId,
    cast(REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitDateTime'),'\"','') as timestamp) as appointmentAt,
    cast(REPLACE(JSON_EXTRACT(payload, '$.Visit.Duration'),'\"','') as int64) as appointmentDuration,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.Status'),'\"','') as status,
    cast(REPLACE(JSON_EXTRACT(payload, '$.Meta.EventDateTime'),'\"','') as timestamp) as appointmentStatusCreatedAt,
    max(cast(REPLACE(JSON_EXTRACT(payload, '$.Meta.EventDateTime'),'\"','') as timestamp)) over(partition by REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitNumber'),'\"','')) = cast(REPLACE(JSON_EXTRACT(payload, '$.Meta.EventDateTime'),'\"','') as timestamp) as isLatestAppointmentStatus,
    min(cast(REPLACE(JSON_EXTRACT(payload, '$.Meta.EventDateTime'),'\"','') as timestamp)) over(partition by REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitNumber'),'\"','')) as appointmentCreatedAt, -- min appt status createdat = appt createdat
    REPLACE(JSON_EXTRACT(payload, '$.Visit.Reason'),'\"','') as appointmentType,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.CancelReason'),'\"','') as cancelReason,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.Location.Facility'),'\"','') as ehrFacility,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.Location.Department'),'\"','') as ehrDepartment,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitProvider.FirstName'),'\"','') || ' ' || REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitProvider.LastName'),'\"','') as userName,
    REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitProvider.ID'),'\"','') as npi,
    'epic' as ehrName,
    -- epic business logic for clinical intake:
    -- per charlotte, katie and this PRD for clinical intake tracking this is how to identify clinical intakes in epic data
    -- https://docs.google.com/document/d/1PtoMf5LmcAjOa-b3P2ruOkVkZYtrWSPSnpx2bGIi-Ug/edit?usp=sharing_eil&ts=60105d90
    coalesce(ern.name like '%Clinical Intake%' and ern.encounterType not like 'Telephon%', false) as isClinicalIntakeAppointment,

  from {{ source('streaming', 'redox_messages')}} rm

  left join encounter_reason_name ern 
    on REPLACE(JSON_EXTRACT(payload, '$.Visit.VisitNumber'),'\"','') = ern.id

  left join member_mrns mem_mrn
    on rm.patient.externalId = mem_mrn.mrn

  where rm.eventType like 'Scheduling.%'
    and REPLACE(JSON_EXTRACT(payload, '$.Visit.Location.Facility'),'\"','') like '%CB%'
),

final as (
  -- order and column names matter here because we are unioning with other ehr's
  select distinct
    coalesce( ca.commonsAppointmentId, ca2.id) as commonsAppointmentId,
    ea.ehrAppointmentId, 
    ea.patientId, 
    ea.mrn, 
    ea.npi,
    ea.ehrDepartment,
    ehrFacility,
    cast(null as int64) as ehrUserId,
    cast(null as string) as userId,
    cast(null as int64) as appointmentCreatedByEhrUserId, 
    ea.appointmentAt, 
    ea.appointmentCreatedAt, 
    ea.appointmentType, 
    cast(null as string) as appointmentDescription,
    ea.appointmentDuration, 
    ea.status as appointmentStatus, 
    ea.appointmentStatusCreatedAt, 
    ea.isLatestAppointmentStatus, 
    coalesce(ha.isAnnualProviderVisit, ea.isClinicalIntakeAppointment) as isClinicalIntakeAppointment,
    ea.ehrName
  from epic_appointments ea

  left join commons_appointments ca
    on ea.patientId = ca.patientId and 
       ea.userName = ca.userName and 
       ea.appointmentAt = ca.appointmentAt

  left join {{ source('commons', 'patient_siu_event')}} pse
    on ea.ehrAppointmentId = pse.visitId

  left join {{ source('commons', 'appointment')}} ca2
    on pse.appointmentId = ca2.id

  left join historic_apvs ha 
    on ea.patientId = ha.patientId and 
       ea.userName = ha.userName and 
       ea.appointmentAt = ha.appointmentAt
)

-- extra step to fill in null commonsAppointmentIds where appointmentAt has been rescheduled over time
select 
  coalesce( f1.commonsAppointmentId, f2.commonsAppointmentId ) as commonsAppointmentId,
  f1.* except(commonsAppointmentId)
from final f1
left join (select distinct commonsAppointmentId, ehrAppointmentId from final where commonsAppointmentId is not null) f2
  on f1.ehrAppointmentId = f2.ehrAppointmentId
