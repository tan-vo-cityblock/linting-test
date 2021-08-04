with info as (
  select distinct
    m.patientId,
    t.id as interventionId,
    substr(t.title,0,95) as interventionName,
    
    case
      when t.completedAt is null then 'Y'
      else 'N'
    end as interventionActive,
    
    format_date('%C%y%m%d', date(t.createdAt)) as interventionCreateDate,
    t.createdById as interventionCreateBy,
    format_date('%C%y%m%d', date(t.updatedAt)) as interventionUpdateDate,
    substr(t.title,0,250) as interventionLaymanName
    
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_info` mi
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.task` t
  using (patientId)
  
  where m.patientHomeMarketName	= 'Massachusetts' and t.deletedAt is null and t.id is not null
  
), 

final as (
  select
    'A' as interventionTransType,
    interventionId,
    max(interventionName) as interventionName,
    NULL as interventionType,
    NULL as interventionDescription,
    NULL as interventionComments,
    max(interventionActive) as interventionActive,
    max(interventionCreateDate) as interventionCreateDate,	
    max(interventionCreateBy) as interventionCreateBy,	
    max(interventionUpdateDate) as interventionUpdateDate,	
    NULL as interventionUpdateBy,
    max(interventionLaymanName) as interventionLaymanName,
    NULL as interventionLaymanDescription,
    0 as interventionMemberViewable,
    1 as interventionProviderViewable	 
    
    from info
    
    group by interventionId
    
)

select * from final
