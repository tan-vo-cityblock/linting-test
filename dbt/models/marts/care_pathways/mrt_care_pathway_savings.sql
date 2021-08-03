
with top_pathway_assignments as (

  select
    patientId,
    pathwaySlug as topPathwaySlug

  from {{ ref('mrt_care_pathway_assignment') }}

  where memberPathwayRank = 1

),

member_cost_classification_totals as (

  select
    patientId,
    costClassification1,
    planPaidDollars

  from {{ ref('mrt_claims_member_cost_classification_totals') }}

),

program_savings as ( 

  select 
    pathwaySlug as topPathwaySlug,
    category,
    reduction
    
  from {{ ref('pathway_cost_category_reduction') }}
  
),

cost_category_mapping as (

  select
    costCat1 as costClassification1,
    costCat2 as category

  from {{ source('medical_economics', 'cost_cat_mapping') }}

),

member_months as (

  select patientId, memberMonths

  from {{ ref('mrt_member_eligibility_rolling_twelve_months') }}

),

program_savings_with_categories as (

  select 
    ps.topPathwaySlug,
    ps.reduction,
    ccm.costClassification1

  from program_savings ps

  left join cost_category_mapping ccm
  using (category)

),

member_pathway_savings as (

  select
    tpa.patientId,
    tpa.topPathwaySlug,
    round(sum(mcct.planPaidDollars * abs(coalesce(psc.reduction, 0))) / mm.memberMonths, 2) as savingsDollarsPerMemberMonth,
    round(sum(mcct.planPaidDollars) / mm.memberMonths, 2) as planPaidDollarsPerMemberMonth,
    sum(mcct.planPaidDollars) as planPaidDollars,
    mm.memberMonths
    
  from top_pathway_assignments tpa

  left join member_cost_classification_totals mcct
  using (patientId)

  left join program_savings_with_categories psc
  on
    tpa.topPathwaySlug = psc.topPathwaySlug and
    mcct.costClassification1 = psc.costClassification1

  inner join member_months mm
  using (patientId)

  group by tpa.patientId, tpa.topPathwaySlug, mm.memberMonths

)

select * from member_pathway_savings
