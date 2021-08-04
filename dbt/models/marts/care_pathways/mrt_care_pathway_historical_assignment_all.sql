with incremental_assignments as (

  select *,
    lead(createdAt) over(partition by patientId, pathwaySlug order by createdAt) as nextMemberPathwayCreatedAt,
    lead(createdAt) over(partition by patientId, memberPathwayRank order by createdAt) as nextMemberPathwayRankCreatedAt,

  from {{ ref('mrt_care_pathway_incremental_assignment_all') }}

),

pathways as (

  select * except (nextMemberPathwayCreatedAt, nextMemberPathwayRankCreatedAt),
    coalesce(nextMemberPathwayCreatedAt, nextMemberPathwayRankCreatedAt) as deletedAt
  
  from incremental_assignments

),

final as (

  select *,
    deletedAt is null as isCurrentPathway,
    memberPathwayRank = 1 and deletedAt is null as isTopCurrentPathway

  from pathways

)

select * from final
