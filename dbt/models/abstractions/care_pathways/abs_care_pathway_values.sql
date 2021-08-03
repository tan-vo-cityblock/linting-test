
with field_values as (

  select 
    patientId, 
    fieldSlug,
    fieldValue as pathwayValue

  from {{ ref('all_computed_fields') }}

  where fieldSlug in 

    {{ list_to_sql(

        dbt_utils.get_column_values(table = ref('pathway_fields'), column = 'computedFieldSlug')
    
    ) }}

),

active_member_partners as (

  select patientId, partnerName
  from {{ ref('abs_active_member_partners') }}

),

pathway_fields as (

  select 
    slug as pathwaySlug, 
    computedFieldSlug as fieldSlug

  from {{ ref('pathway_fields') }}

),

partner_pathway_hierarchy as (

  select 
    partnerName,
    pathwaySlug,
    partnerPathwayRank

  from {{ ref('partner_pathway_hierarchy') }}

),

final as (

  select 
    fv.patientId,
    mp.partnerName,
    pf.pathwaySlug,
    fv.pathwayValue,
    ph.partnerPathwayRank
    
  from field_values fv
  
  inner join active_member_partners mp
  using (patientId)
  
  inner join pathway_fields pf
  using (fieldSlug)
  
  left join partner_pathway_hierarchy ph
  using (partnerName, pathwaySlug)

)

select * from final
