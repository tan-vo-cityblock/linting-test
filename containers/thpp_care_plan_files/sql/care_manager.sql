with info as (
  select distinct
    m.patientId,
    mup.userId as careManagerId,
    mup.userName as careManagerName,
    mup.userEmail as careManagerContactInfo,
    format_date('%C%y%m%d',u.userCreationDate) as careManagerCreateDate,
    mup.userId as careManagerCreateBy
      
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_user_pod` mup
  using (patientId)
  left join `cityblock-analytics.mrt_commons.user` u
  on mup.userId = u.userId
  
  where m.patientHomeMarketName	= 'Massachusetts' and mup.userId is not null
  
), 

final as (
  select
    'A' as careManagerTransType,	
    careManagerId,
    max(careManagerName) as careManagerName,
    'EMAIL' as careManagerContactType,
    max(careManagerContactInfo) as careManagerContactInfo,
    'Y' as careManagerActive,
    max(careManagerCreateDate) as careManagerCreateDate,
    careManagerCreateBy,
    NULL as careManagerUpdateDate,
    NULL as careManagerUpdateBy
    
    from info
    
    group by careManagerId
    
)

select * from final
