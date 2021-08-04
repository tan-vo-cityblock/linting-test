

{{
  config(
    materialized='view'
  )
}}

with vitals as (

  select distinct
    patient.patientId,
    lower(patient.source.name) as source,
    vitalSign.Code as code,
    vitalSign.CodeSystem as codeSystem,
    lower(vitalSign.CodeSystemName) as codeSystemName,
    case
      when vitalSign.Code = '29463-7' then 'body weight'
      when vitalSign.Code = '39156-5' then 'body mass index'
      when vitalSign.Code = '59408-5' then 'oxygen saturation'
      when vitalSign.Code = '8302-2'  then 'body height'
      else lower(vitalSign.Name) end as codeName,
    lower(vitalSign.status) as status,
    vitalSign.DateTime.instant as timestamp,
    safe_cast(vitalSign.Value as float64) as value,
    case
      when vitalSign.Code = '8867-4' then '/min' 
      else lower(vitalSign.Units) end as units
  from 
    {{ source('medical', 'patient_vital_signs') }} )

select * from vitals
order by patientId, timestamp desc, code
