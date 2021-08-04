with hie_events as (

  select
    payload,
    insertedAt,
    json_extract(rm.payload,"$[Meta][Message].ID") as messageId,
    json_extract(rm.payload,"$[Note][Components]") as components,
    regexp_extract(json_extract(rm.payload,"$[Note][Components]"), r"^.+Value\":\"COVID-19 labOrder\^.+?\^(.+?)\^.+?\"") as covidLabOrder,
    regexp_extract(json_extract(rm.payload,"$[Note][Components]"), r"^.+Value\":\"COVID-19 labResult\^.+?\^.+?\^.*?\^(.+?)\"") as covidLabResult,
    regexp_extract(json_extract(rm.payload,"$[Note][Components]"), r"^.+Value\":\"COVID-19 labResult\^.+?\^(.+?)\^.*?\^.+?\"") as covidLabResultName,

  from {{ source('streaming', 'redox_messages') }} rm
  where eventType in ('PatientAdmin.Discharge', 'PatientAdmin.Arrival', 'Notes.New')

  ),

mapped_facilities as (

  select hie_events.* except(locationName, fullLocationName),
  coalesce(hie_events.fullLocationName, hie_events.locationName) as locationName,
  map.facilityName,
  map.mappedFacilityName,
  map.mappedFacilityType

  from {{ source('medical', 'patient_hie_events') }} hie_events
  left join {{ source('facility', 'facility_map') }} map
  on hie_events.locationName = map.facilityName
  and hie_events.patient.source.name = map.pingSource

),

final as (

  select
  mf.* except (facilityType, locationName, facilityName, mappedFacilityName, mappedFacilityType),
  coalesce(mf.mappedFacilityName, mf.locationName) as locationName,
  coalesce(mf.mappedFacilityType, mf.facilityType) as facilityType,
  hie_events.insertedAt as receivedAt,
  hie_events.covidLabOrder,
  hie_events.covidLabResultName,
  hie_events.covidLabResult,

  from mapped_facilities mf
  inner join hie_events
  on mf.messageId = hie_events.messageId

)

select *
from final
