with all_mailers as (
(
select distinct
    o.patientId,
    o.modality as outreachType,
    date(o.attemptedAt) as outreachDate,
    time(o.attemptedAt) as outreachTime
    
  from {{ ref('outreach_attempt') }} o
  inner join {{ ref('member') }} m
  using (patientId)
  where 
    (m.patientHomeMarketName = 'Massachusetts') and 
    m.currentState not in ('disenrolled', 'disenrolled_after_consent') and
    o.modality in ('mail') and
    o.deletedAt is null
)
    
  union distinct
  
(
  select distinct
    mi.patientId,
    mi.eventType as outreachType,
    date(mi.eventTimestamp) as outreachDate,
    time(mi.eventTimestamp) as outreachTime

  from {{ ref('member_interactions') }} mi
  inner join {{ ref('member') }} m
  using (patientId)
  where 
    (m.patientHomeMarketName = 'Massachusetts') and 
    m.currentState not in ('disenrolled', 'disenrolled_after_consent') and
    mi.eventType in ('mail')
)
)

select 
  *,
  
  case 
    when row_number() over(partition by patientId order by outreachDate) = 1 then true
    else false
  end as isFirstMailer,

from all_mailers