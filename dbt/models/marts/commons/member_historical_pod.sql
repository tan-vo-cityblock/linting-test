
with dates as (

  select calendarDate
  
  from 
    unnest(
      generate_date_array(
        date('2018-06-29'), current_date('America/New_York'), interval 1 day
      )
    ) as calendarDate

),

relationships as (

  select
    patientId,
    userId,
    createdAt,
    deletedAt

  from {{ source('commons', 'care_team') }}

),

chps as (

  select 
    id as userId,
    userRole

  from {{ source('commons', 'user') }}

  where userRole like ('%Community_Health_Partner')

),

pods as (

  select
    userId,
    clinicPodId,
    createdAt,
    deletedAt

  from {{ source('commons', 'clinic_pod_member') }}

),

pod_names as (

  select 
    id as clinicPodId, 
    name as podName,
    clinicId

  from {{ source('commons', 'clinic_pod') }}

),

clinic_names as (

  select
    id as clinicId,
    name as clinicName,
    marketId

  from {{ source('commons', 'clinic') }}

),

market_names as (

  select
    id as marketId,
    name as marketName

  from {{ source('commons', 'market') }}

),

chp_relationships as (

  select
    r.patientId,
    r.userId,
    c.userRole,
    r.createdAt,
    r.deletedAt

  from relationships r

  inner join chps c
  using (userId)

),

chp_relationship_order as (

  select
    d.calendarDate,
    cr.patientId,
    cr.userId,
    cr.userRole,
    row_number() over (

      partition by cr.patientId, d.calendarDate 
      order by cr.createdAt desc, cr.userRole asc

    ) as rowNum

  from dates d

  cross join chp_relationships cr

  where
    d.calendarDate >= date(cr.createdAt, "America/New_York") and
    (
      d.calendarDate < date(cr.deletedAt, "America/New_York") or
      cr.deletedAt is null
    )
  
),

pod_order as (

  select
    d.calendarDate,
    p.userId,
    p.clinicPodId,
    row_number() over (

      partition by p.userId, d.calendarDate 
      order by p.createdAt desc

    ) as rowNum

  from dates d

  cross join pods p

  where
    d.calendarDate >= date(p.createdAt, "America/New_York") and
    (
      d.calendarDate < date(p.deletedAt, "America/New_York") or
      p.deletedAt is null
    )

),

daily_chp_relationships as (

  select * except (rowNum)
  from chp_relationship_order
  where rowNum = 1
  

),

daily_pods as (

  select * except (rowNum)
  from pod_order
  where rowNum = 1
  

),

final as (

  select
    dcr.calendarDate,
    dcr.patientId,
    dcr.userId,
    dcr.userRole,
    dp.clinicPodId,
    pn.podName,
    cn.clinicName,
    mn.marketName

  from daily_chp_relationships dcr

  left join daily_pods dp
  using (calendarDate, userId)

  left join pod_names pn
  using (clinicPodId)

  left join clinic_names cn
  using (clinicId)

  left join market_names mn
  using (marketId)

  where dp.clinicPodId is not null
  
)

select * from final
