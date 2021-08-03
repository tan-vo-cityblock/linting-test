
with pathway_assignments as (

  select
    id as assignmentId,
    patientId,
    pathwaySlug,
    memberPathwayRank,
    current_timestamp as createdAt

  from {{ ref('mrt_care_pathway_assignment') }}

),

{% if is_incremental() %}

  ranked_assignments as (

    select
      patientId,
      pathwaySlug,
      memberPathwayRank,
      rank() over(partition by patientId, pathwaySlug order by createdAt desc) as rnk

    from {{ this }}

  ),

  latest_assignments as (

    select * except (rnk)
    from ranked_assignments
    where rnk = 1

  ),

{% endif %}

final as (

  select p.*
  from pathway_assignments p

{% if is_incremental() %}

  left join latest_assignments l
  using (patientId, pathwaySlug)

  where
    p.memberPathwayRank != l.memberPathwayRank or
    l.memberPathwayRank is null

{% endif %}

)

select * from final
