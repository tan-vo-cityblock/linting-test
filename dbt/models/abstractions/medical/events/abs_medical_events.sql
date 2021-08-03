with events as (

  select *
  from {{ ref('stg_patient_hie_events') }}
  union all
  select *
  from {{ ref('stg_prior_auth_events') }}

),

patient_states as (

  select
    patientId,
    currentState,
    createdAt,
    deletedAt
  from {{ source('commons', 'patient_state') }}

),

member_states as (

  select
    patientId,
    currentState,
    consentedAt,
    disenrolledAt
  from {{ ref('member_states') }}

),

final as (

  select
    e.id as patientVisitTypeEventId,
    {{ dbt_utils.surrogate_key(['e.patientId', 'e.visitId']) }} as patientVisitId,
    e.eventSource,
    e.patientId,
    case
      when e.eventType like 'LTC/Skilled Nursing%' and e.facilityType is null then 'LTC' --should this be LTAC?
      when e.eventType like 'Skilled Nursing%' and e.facilityType is null then 'SNF'
      else e.facilityType
    end as facilityType,
    e.visitId,
    e.visitType,
    case
      when e.eventType like '%Admit' then 'Admit'
      when e.eventType like '%Discharge' then 'Discharge'
    end as eventType,
    e.locationName,
    e.eventAt,
    e.eventReceivedAt,
    timestamp_diff(e.eventReceivedAt, e.eventAt, day) <= 3 as eventReceivedWithinThreeDays,
    extract(dayofweek from date(eventReceivedAt, 'America/New_York')) in (1, 7) as eventReceivedOnWeekend,
    ms.consentedAt,
    ms.disenrolledAt,
    ps.currentState as memberStateAtEventReceipt,
    case
      when ps.currentState in ('attributed', 'assigned', 'contact_attempted', 'reached', 'interested', 'not_interested')
      then 'pre-consented'
      when ps.currentState in ('consented', 'enrolled')
      then 'consented'
      when ps.currentState = 'disenrolled'
      then 'disenrolled'
    end as memberStateCategoryAtEventReceipt,
    dx,
    covidLabResult
  from events e
  left join member_states ms
  using (patientId)
  left join patient_states ps
  on e.patientId = ps.patientId
  and e.eventReceivedAt >= ps.createdAt
  and
    (
      e.eventReceivedAt < ps.deletedAt or
      ps.deletedAt is null
    )
  where eventAt is not null

)

select *
from final
