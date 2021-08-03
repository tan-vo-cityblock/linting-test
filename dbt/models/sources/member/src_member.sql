{{ config(materialized='table', tags=['nightly']) }}

with member as (

  select 
    id as patientId,
    cbhId,
    cohortId,
    partnerId,
    mrnId,
    createdAt

  from {{ source('member_index', 'member') }}

  where
    (cohortId > 0 or cohortId is null) and
    deletedAt is null

),

cohort as (

  select
    id as cohortId,
    name as cohortName,
    goLiveDate as cohortGoLiveDate

  from {{ source('member_index', 'cohort') }}

  where deletedAt is null

),

partner as (

  select
    id as partnerId,

    case
      when name = 'emblem' then 'Emblem Health'
      when name = 'connecticare' then 'ConnectiCare'
      when name in ('bcbsNC', 'medicareNC', 'medicaidNC', 'selfPay') then 'BCBSNC'
      when name = 'tufts' then 'Tufts'
      when name = 'carefirst' then 'CareFirst'
      when name = 'cardinal' then 'Cardinal'
      else name
    end as partnerName

  from {{ source('member_index', 'partner') }}

  where deletedAt is null

),

mrn as (

  select
    id as mrnId,
    m.mrn
    
  from {{ source('member_index', 'mrn') }} m
  
  where deletedAt is null

),

final as (

  select
    m.patientId,
    m.cbhId,
    m.cohortId,
    c.cohortName,
    c.cohortGoLiveDate,
    p.partnerId,
    p.partnerName,
    mrn.mrnId,
    mrn.mrn,
    m.createdAt

  from member m

  left join cohort c
  using (cohortId)

  left join partner p
  using (partnerId)

  left join mrn
  using (mrnId)

)

select * from final
