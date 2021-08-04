with info as (
  select distinct
    m.patientId,
    g.id as goalId,
    substr(title, 0,95) as goalName,
    substr(description,0,195) as goalDescription,
    
    case
      when g.completedAt is null then 'Y'
      else 'N'
    end as goalActive,
    
    format_date('%C%y%m%d', date(g.createdAt)) as goalCreateDate,
    g.createdById as goalCreateBy,
    format_date('%C%y%m%d', date(g.updatedAt)) as goalUpdateDate,
    substr(title, 0,250) as goalLaymanName,
    description as goalLaymenDescription
    
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_info` mi
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.goal` g
  using (patientId)
  
  where m.patientHomeMarketName	= 'Massachusetts' and g.deletedAt is null and g.id is not null
  
), 

final as (
  select
    'A' as goalTransType,
    goalId,	
    max(goalName) as goalName,
    max(goalDescription) as goalDescription,
    NULL as goalType,
    max(goalActive) as goalActive,
    max(goalCreateDate) as goalCreateDate,
    max(goalCreateBy) as goalCreateBy,
    max(goalUpdateDate) as goalUpdateDate,
    NULL as goalUpdateBy,	
    max(goalLaymanName) as goalLaymanName,
    max(goalLaymenDescription) as goalLaymenDescription,	
    0 as goalMemberViewable,
    1 as goalProviderViewable	
    
    from info
    
    group by goalId
    
)

select * from final
