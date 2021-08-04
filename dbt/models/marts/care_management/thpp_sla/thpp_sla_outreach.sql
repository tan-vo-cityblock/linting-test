with all_outreach_attempts as (
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
   -- m.currentState not in ('disenrolled', 'disenrolled_after_consent') and
    o.modality in ('outreachEvent','other','phone','phoneCall','text','smsMessage','homeVisit','careTransitionsPhone','careTransitionsInPerson','hub','metAtProvidersOffice') and
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
   -- m.currentState not in ('disenrolled', 'disenrolled_after_consent') and
    mi.eventType in ('outreachEvent','other','videoCall','text','smsMessage','phoneCall','phone','inPersonVisitOrMeeting','Telephone Visit Note','Telemedicine Note','Office Visit Note','RN Home Visit','Visit Note','House Call Visit Note','Hospital Visit Note')
    
)
)

select 
  *, 
  case 
    when row_number() over(partition by patientId order by outreachDate) = 1 then true
    else false
  end as isFirstOutreachAt,

  case 
    when row_number() over(partition by patientId order by outreachDate) = 2 then true
    else false
  end as isSecondOutreachAt,
  
  case 
    when row_number() over(partition by patientId order by outreachDate) = 3 then true
    else false
  end as isThirdOutreachAt

from all_outreach_attempts