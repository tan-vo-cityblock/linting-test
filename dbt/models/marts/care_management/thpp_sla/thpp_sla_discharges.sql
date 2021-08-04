with discharge as (
  select 
    h.patientId, 
    h.eventType,
    date(h.createdAt) as dischargeDate,

    case
      when h.visitType in ('Emergency','ED') then 'ed'
      when h.visitType in ('Inpatient','IA') then 'ip'
      when h.visitType in ('Observation','O') then 'o' 
      else lower(h.visitType) 
    end as dischargeType

  from {{ source('commons','hie_event') }} h
  inner join {{ ref('member') }} m
  using (patientId)
  where 
    h.eventType in ('Discharge','LTC/Skilled Nursing Discharge','Skilled Nursing Discharge') and
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
    modality in ('phone','outreachEvent','homeVisit','careTransitionsPhone','careTransitionsInPerson','hub','metAtProvidersOffice', 'text', 'smsMessage') and
    deletedAt is null
    
  union distinct
  
  select
    patientId,
    eventType,
    date(eventTimestamp) as outreachAt

  from {{ ref('member_interactions') }}
  where 
    eventType in ('videoCall','phone','phoneCall','inPersonVisitOrMeeting','Telephone Visit Note','Telemedicine Note','Office Visit Note','RN Home Visit','Visit Note','House Call Visit Note','Hospital Visit Note','text')
    
),

outreach_in_time as (
  select
    d.patientId,
    dischargeDate,
    outreachAt,
    true as outreachWithin48Hours

  from discharge d
  inner join outreach o
  on d.patientId = o.patientId and 
  date_diff(outreachAt, dischargeDate, day) <= 2 and
  date_diff(outreachAt, dischargeDate, day) >= 0
  
),

final as (
  select distinct
    d.patientId,
    d.dischargeDate,
    d.dischargeType,
    coalesce(o.outreachWithin48Hours, false) as outreachWithin48Hours
  
  from discharge d
  left join outreach_in_time o
  on d.patientId = o.patientId and d.dischargeDate = o.dischargeDate
  
)

select * from final
