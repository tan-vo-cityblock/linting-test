

{{
  config(
    materialized='ephemeral'
  )
}}

with source as (

    select * from {{ ref('src_patient_hie_events') }}


),

hie_filtered as (

    select distinct
        patient.patientId,
        hie.visitNumber,
        hie.messageId,
        eventDateTime.instant as eventDTInstant,
        dischargeDateTime.instant as dischargeDTInstant,
        case when REGEXP_CONTAINS(eventType, 'Expire') then 'Discharge' else eventType end as eventType,
        locationName,
        visitType,
        dischargeDisposition,
        source as hieSource,
        receivedAt,
        fullText

    from source as hie

    where REGEXP_CONTAINS(eventType, 'Admit|Discharge|Expire') and -- Incarceration
      patient.patientId is not null and
      hie.source = 'Healthix' -- Emblem only at the beginning

)

select * from hie_filtered
