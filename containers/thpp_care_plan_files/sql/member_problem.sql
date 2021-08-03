with info as (
  select distinct
    m.patientId,
    concat(mi.memberId, gl.id) as memberProblemId,
    mi.memberId,
    gl.id as problemId,
    bac.id as caseId,
    format_date('%C%y%m%d', date(gl.createdAt)) as memberProblemStartDate,
    format_date('%C%y%m%d', date(gl.deletedAt)) as memberProblemEndDate,
    
    case
      when gl.deletedAt is null then 'Y'
      else 'N'
    end as memberProblemActive,
    
    
    format_date('%C%y%m%d', date(gl.createdAt)) as memberProblemCreateDate,
    gl.createdById as memberProblemCreateBy,
    format_date('%C%y%m%d', date(gl.updatedAt)) as memberProblemUpdateDate,
    gl.updatedById as memberProblemUpdateBy
    
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
    'A' as memberProblemTransType,
    memberProblemId,
    memberId,
    problemId,
    NULL as memberProblemSequence,
    caseId,
    NULL as carePlanId,
    NULL as memberProblemParentType,
    NULL as memberProblemParentValue,	
    NULL as memberProblemComments,
    NULL as memberProblemResourceId,
    NULL as memberProblemWillingToChange,
    NULL as memberProblemWillingToChangeReason,
    memberProblemStartDate,
    memberProblemEndDate,
    memberProblemActive,
    NULL as memberProblemStatus,
    memberProblemCreateDate,
    memberProblemCreateBy,
    memberProblemUpdateDate,
    memberProblemUpdateBy	
  
    from info
    
)

select * from final
