with admit as (
  select 
    h.patientId, 
    h.eventType, 
    date(h.createdAt) as admitDate,

    case
      when h.visitType in ('Emergency','ED') then 'ed'
      when h.visitType in ('Inpatient','IA') then 'ip'
      when h.visitType in ('Observation','O') then 'o' 
      else lower(h.visitType) 
    end as admitType

  from {{ source('commons','hie_event') }} h
  inner join {{ ref('member') }} m
  using (patientId)
  where 
    h.eventType in ('Admit','Skilled Nursing Admit','LTC/Skilled Nursing Admit') and
    h.deletedAt is null and
    (m.patientHomeMarketName = 'Massachusetts') and 
    m.currentState not in ('disenrolled', 'disenrolled_after_consent') and
    date(h.createdAt) >= '2020-03-02' and visitType != "Unknown"
       
),

outreach as (
  select 
    patientId,
    modality as eventType,
    date(attemptedAt) as outreachAt
    
  from {{ ref('outreach_attempt') }}
  where 
    modality in ('phone','homeVisit','careTransitionsPhone','careTransitionsInPerson','hub','metAtProvidersOffice','text') and
    deletedAt is null
    
  union distinct
  
  select
    patientId,
    eventType,
    date(eventTimestamp) as outreachAt

  from {{ ref('member_interactions') }}
  where 
    eventType in ('videoCall','phoneCall','inPersonVisitOrMeeting','Telephone Visit Note','Telemedicine Note','Office Visit Note','RN Home Visit','Visit Note','House Call Visit Note','Hospital Visit Note','text')
    
),

outreach_in_time as (
  select
    a.patientId,
    admitDate,
    outreachAt,
    true as outreachWithin24Hours

  from admit a
  inner join outreach o
  on a.patientId = o.patientId and 
  date_diff(outreachAt, admitDate, day) <= 1 and
  date_diff(outreachAt, admitDate, day) >= 0
  
),

final as (
  select distinct
    a.patientId,
    a.admitDate,
    a.admitType,
    coalesce(o.outreachWithin24Hours, false) as outreachWithin24Hours
  
  from admit a
  left join outreach_in_time o
  on a.patientId = o.patientId and a.admitDate = o.admitDate

  
)

select * from final
