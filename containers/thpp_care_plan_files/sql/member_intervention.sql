with info as (
  select distinct
    m.patientId,
    concat(mi.memberId, t.id) as memberInterventionId,
    mi.memberId,
    t.id as interventionId,
    bac.id as caseId,
    # t.groupId as memberInterventionParentValue,
    format_date('%C%y%m%d', t.dueAt) as memberInterventionDueDate,
    format_date('%C%y%m%d', date(t.completedAt)) as memberInterventionActualDate,

    case
      when t.createdAt is not null
        and t.completedAt is null then '8'
    end as memberInterventionStatus,

    case
      when t.completedAt is null then 'Y'
      else 'N'
    end as memberInterventionActive,
    
    format_date('%C%y%m%d', date(t.createdAt)) as memberInterventionCreateDate,
    t.createdById as memberInterventionCreateBy,
    format_date('%C%y%m%d', date(t.updatedAt)) as memberInterventionUpdateDate
    
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_info` mi
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.baseline_assessment_completion` bac
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.task` t
  using (patientId)
  
  where m.patientHomeMarketName	= 'Massachusetts' and t.deletedAt is null and t.id is not null
  
), 

final as (
  select
    'A' as memberInterventionTransType,
    memberInterventionId,
    memberId,	
    interventionId,
    NULL as memberInterventionSequence,
    caseId,	
    NULL as carePlanId,
    NULL as memberInterventionParentType,
    NULL as memberInterventionParentValue,
    NULL as memberInterventionComments,
    memberInterventionDueDate,
    memberInterventionActualDate,
    memberInterventionStatus,
    NULL as memberInterventionAchievementLevel,
    NULL as memberInterventionVarianceReason,
    memberInterventionActive,
    memberInterventionCreateDate,
    memberInterventionCreateBy,	
    memberInterventionUpdateDate,
    NULL as memberInterventionUpdateBy	 
    
    from info
    
)

select * from final
