with info as (
  select distinct
    m.patientId,
    gl.id as problemId,
    substr(gl.label,0,95) as problemName,
    
    case
      when gl.deletedAt is null then 'Y'
      else 'N'
    end as problemActive,
    
    format_date('%C%y%m%d', date(gl.createdAt)) as problemCreateDate,
    gl.createdById as problemCreateBy,
    format_date('%C%y%m%d', date(gl.updatedAt)) as problemUpdateDate,
    gl.updatedById as problemUpdateBy,
    substr(gl.label,0,250) as problemLaymanName
    
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_info` mi
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.baseline_assessment_completion` bac
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.goal_label` gl
  using (patientId)
  
  where m.patientHomeMarketName	= 'Massachusetts' and gl.deletedAt is null and gl.id is not null
  
), 

final as (
  select
    'A' as problemTransType,
    problemId,
    max(problemName) as problemName,
    NULL as problemDescription,
    NULL as problemType,
    NULL as problemResourceId,
    max(problemActive) as problemActive,
    max(problemCreateDate) as problemCreateDate,
    max(problemCreateBy) as problemCreateBy,
    max(problemUpdateDate) as problemUpdateDate,
    max(problemUpdateBy) as problemUpdateBy,
    max(problemLaymanName) as problemLaymanName,
    NULL as problemLaymanDescription,
    0 as problemMemberViewable,
    1 as problemProviderViewable,
  
  from info
  
  group by problemId
    
)

select * from final
