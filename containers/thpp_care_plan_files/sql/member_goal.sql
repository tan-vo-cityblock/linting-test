with info as (
  select distinct
    m.patientId,
    concat(mi.memberId, g.id) as memberGoalId,
    mi.memberId,
    g.id as goalId,
    # row_number() over (order by patientId) as memberGoalSequence,
    bac.id as caseId,
    format_date('%C%y%m%d', g.dueAt) as memberGoalDueDate,
    format_date('%C%y%m%d', date(g.completedAt)) as memberGoalActualDate,

    case
      when g.completedAt is not null then '03'
      when g.createdAt is not null
        and g.deletedAt is null
        and g.completedAt is null then 'P1'
      when g.deletedAt is not null then '01'
    end as memberGoalStatus,

    case
      when g.completedAt is null then 'Y'
      else 'N'
    end as memberGoalActive,
    
    format_date('%C%y%m%d', date(g.createdAt)) as memberGoalCreateDate,
    g.createdById as memberGoalCreateBy,
    format_date('%C%y%m%d', date(g.updatedAt)) as memberGoalUpdateDate,
    format_date('%C%y%m%d', date(g.deletedAt)) as memberGoalDeleteDate
    
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_info` mi
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.baseline_assessment_completion` bac
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.goal` g
  using (patientId)
  
  where m.patientHomeMarketName	= 'Massachusetts' and g.id is not null
  
), 

final as (
  select
    'A' as memberGoalTransType,	
    memberGoalId,	
    memberId,	
    goalId,	
    NULL as memberGoalSequence,	
    caseId,
    NULL as carePlanId,
    NULL as memberGoalParentType,	
    NULL as memberGoalParentValue,
    NULL as memberGoalShortTerm,
    memberGoalDueDate,
    memberGoalActualDate,
    NULL as memberGoalAchievementLevel,
    NULL as memberGoalVarianceReason,	
    memberGoalStatus,
    memberGoalActive,
    memberGoalCreateDate,	
    memberGoalCreateBy,
    memberGoalUpdateDate,	
    NULL as memberGoalUpdateBy,
    NULL as memberGoalComments	
    
    from info
    
)

select * from final
