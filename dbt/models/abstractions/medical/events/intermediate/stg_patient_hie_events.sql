{{ config(tags=['nightly']) }}

with member_hie_events as (

  select
    patient.patientId,
    visitNumber as visitId,
    max(facilityType) as facilityType,
    case
      when documentType in ('Inpatient') and visitType in ('Unknown') then 'Inpatient'
      when documentType in ('Emergency') and visitType in ('Unknown') then 'Emergency'
      when (documentType in ('Outpatient') and visitType in ('Unknown','O')) or visitType in ('O') then 'Outpatient'
      when (documentType in ('Silent') and visitType in ('Unknown','S')) or visitType in ('S') then 'Silent' --per Healthix, S = silent admit/encounter
      when documentType in ('General') and visitType in ('Unknown') then 'General' --per Healthix, G = general admit
      when visitType in ('ED','Emergency') then 'Emergency'
      when visitType in ('IA','Inpatient') then 'Inpatient'
      when visitType in ('Observation') then 'Observation'
      else visitType
    end as visitType,
    eventType,
    string_agg(distinct locationName, ", " order by locationName) as locationName,
    case
      when eventType like '%Admit'
      then min(eventDateTime.instant)
      else max(dischargeDateTime.instant)
    end as eventAt,
    case
      when eventType like '%Admit'
      then min(receivedAt)
      else max(receivedAt)
    end as eventReceivedAt,
    array_agg(und.code ignore nulls) as dx,
    max(case
          when trim(lower(covidLabResult)) like 'detected%' then 2
          when lower(covidLabResult) like '%positive%' then 2
          when trim(lower(covidLabResult)) like 'invalid%' then 1
          when trim(lower(covidLabResult)) like '%not%detec%' then 0
          when trim(lower(covidLabResult)) like '%negative%' then 0 end) as covidLabResult
    from {{ ref('src_patient_hie_events') }}
    left join unnest(diagnoses) as und
    where patient.patientId is not null
      and visitNumber != '0'
      and (eventType like '%Admit' or eventType like '%Discharge')
      and receivedAt is not null
    group by patient.patientId, visitNumber, visitType, eventType

),

final as (
  select
    {{ dbt_utils.surrogate_key(['patientId', 'visitId', 'visitType', 'eventType']) }} as id,
    'hie' as eventSource,
    *
  from member_hie_events

)

select *
from final
