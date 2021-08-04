
with abs_care_pathway_values as (

  select
    patientId,
    pathwaySlug,
    pathwayValue,
    partnerPathwayRank

  from {{ ref('abs_care_pathway_values') }}

),

pathway_members as (

  select distinct patientId
  from abs_care_pathway_values

),

active_pathway_values as (

  select
    patientId,
    pathwaySlug,
    partnerPathwayRank
    
  from abs_care_pathway_values
  
  where
    pathwayValue = 'true' and
    partnerPathwayRank is not null

),

final as (

  select
    generate_uuid() as id,
    m.patientId,
    coalesce(p.pathwaySlug, 'base-care-model') as pathwaySlug,
    coalesce(rank() over(partition by p.patientId order by p.partnerPathwayRank), 1) as memberPathwayRank

  from pathway_members m
  
  left join active_pathway_values p
  using (patientId)

)

select * from final
