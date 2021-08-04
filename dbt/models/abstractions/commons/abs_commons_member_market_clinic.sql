
{{ config(materialized='table') }}

with patient as (

  select
    id as patientId,
    memberId,
    homeMarketId,
    homeClinicId

  from {{ source('commons', 'patient') }}

),

market as (

  select
    id as homeMarketId,
    name as patientHomeMarketName

  from {{ source('commons', 'market') }}

  where deletedAt is null

),

clinic as (

  select
    id as homeClinicId,
    name as patientHomeClinicName

  from {{ source('commons', 'clinic') }}

),

final as (

  select
    p.patientId,
    p.memberId,
    m.homeMarketId as patientHomeMarketId,
    m.patientHomeMarketName,
    c.homeClinicId as patientHomeClinicId,
    c.patientHomeClinicName

  from patient p

  left join market m
  using (homeMarketId)

  left join clinic c
  using (homeClinicId)

)

select * from final
