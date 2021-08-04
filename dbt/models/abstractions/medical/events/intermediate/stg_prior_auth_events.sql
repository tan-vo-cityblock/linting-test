with base as (

{# replace received dates with admission dates for historical authorizations #}
  select * replace (
    case
      when admitDate < '2020-11-24'
        then admitDate
      else receivedDate
    end as receivedDate
  )

  from {{ ref('abs_prior_auth_emblem_cci_latest_version_dates') }}
  where isConcurrentInpatientAuthorization

),

diagnoses as (

  select
    id,
    array_agg(distinct diagnosis.diagnosisCode ignore nulls) as dx
    
  from base
  left join unnest(diagnosis) as diagnosis
  
  group by 1

),

final as (

  select
    id,
    'prior auth' as eventSource,
    memberIdentifier.patientId,
    authorizationId as visitId,
    placeOfService as facilityType,
    'Inpatient' as visitType,
    'Admit' as eventType,
    facilityIdentifier.facilityName as locationName,
    timestamp(admitDate, 'America/New_York') as eventAt,
    timestamp(receivedDate, 'America/New_York') as eventReceivedAt,
    dx,
    null as covidLabResult

  from base

  left join diagnoses
  using (id)

)

select * from final
