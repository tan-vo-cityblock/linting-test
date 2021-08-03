with info as (
  select distinct
    m.patientId,
    bac.id as caseId,
    mi.memberId,
    
    case
      when m.currentState in ('consented','enrolled') then 'Y'
      else 'N'
    end as caseActive,
    
    mup.userId as caseCareManagerId,
    format_date('%C%y%m%d', date(bac.createdAt)) as caseCreateDate,
    format_date('%C%y%m%d', date(bac.updatedAt)) as caseUpdateDate,
    format_date('%C%y%m%d', date(bac.deletedAt)) as caseDeleteDate
      
  from `cityblock-analytics.mrt_commons.member` m
  left join `cityblock-analytics.mrt_commons.member_info` mi
  using (patientId)
  left join `cbh-db-mirror-prod.commons_mirror.baseline_assessment_completion` bac
  using (patientId)
  left join `cityblock-analytics.mrt_commons.member_user_pod` mup
  using (patientId)
  
  where m.patientHomeMarketName	= 'Massachusetts'
  
), 

final as (
  select
    'A' as caseTransType,
    caseId, 
    memberId,
    row_number() over (partition by caseId, memberId, caseCreateDate) as caseSequence,
    NULL as caseCarePlanId,
    'NWHD01' as caseType,
    'Cityblock Unify Case Management' as caseName,
    caseActive,
    case when caseDeleteDate is null then '5' else '3' end as caseStatus,
    caseCareManagerId,
    caseCreateDate,	
    NULL as caseCreateBy,	
    caseUpdateDate,
    NULL as caseUpdateBy,
    1 as caseIsAssigned
    
    from info
    
)

select * from final
