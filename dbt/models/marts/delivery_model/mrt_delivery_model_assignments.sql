{%- set community_slug = var('community_slug') -%}
{%- set virtual_slug = var('virtual_slug') -%}

with weighted_values as (

  select
  patientId,
  marketName,
  initialModelAssignment,
  sum(weightedValue) as totalWeightedValue

  from {{ ref('mrt_delivery_model_weighted_values') }}
  group by 1, 2, 3

),

--- this section is specific to the NC market ---

percent_ranked_north_carolina_members as ( --getting the % rank for NC members

  select *,
  percent_rank() over(order by totalWeightedValue desc) prnk

  from weighted_values
  where marketName = 'North Carolina'

),

north_carolina_top_20_highest_scores as ( --top 20% of highest scoring members from NC

    select *

    from percent_ranked_north_carolina_members
    where prnk <= 0.2
),

--------------------------------------------------

community_proportions as (

  select
  marketName,
  communityProportion

  from {{ ref('dta_delivery_model_market_proportions') }}

),

community_and_screened_members as (

  select
  wv.marketName,
  cast(count(distinct wv.patientId) * cp.communityProportion as int64) as numCommunityMembers,

  count(distinct
    case
      when wv.initialModelAssignment = '{{ community_slug }}'
      then wv.patientId
      else null
    end) as numInitialScreens

  from weighted_values wv

  inner join community_proportions cp
  using (marketName)

  group by wv.marketName, cp.communityProportion

),

remaining_members as (

  select
  marketName,
  numCommunityMembers - numInitialScreens as numRemainingMembers

  from community_and_screened_members

),

weighted_value_ranks as (

  select
  patientId,
  marketName,
  rank() over(partition by marketName order by totalWeightedValue desc, patientId) rnk

  from weighted_values

  where initialModelAssignment = '{{ virtual_slug }}'

),

additional_community_members as (

  select r.patientId
  from weighted_value_ranks r
  inner join remaining_members m
  using (marketName)
  where r.rnk <= m.numRemainingMembers

),

standard_market_approach as (

  select
  w.patientId,
  w.marketName,
  case
    when w.initialModelAssignment = '{{ community_slug }}'
    then w.initialModelAssignment
    when a.patientId is not null
    then '{{ community_slug }}'
    else '{{ virtual_slug }}'
  end as deliveryModel,
  w.totalWeightedValue

  from weighted_values w

  left join additional_community_members a
  using (patientId)

  where marketName <> 'North Carolina'

),

north_carolina_market_approach as (

  select
  w.patientId,
  w.marketName,
  case
    when w.initialModelAssignment = '{{ community_slug }}'
    then w.initialModelAssignment
    when nc.patientId is not null
    then '{{ virtual_slug }}'
    else '{{ community_slug }}'
  end as deliveryModel,
  w.totalWeightedValue

  from weighted_values w

  left join north_carolina_top_20_highest_scores nc
  using (patientId)

  left join additional_community_members a
  using (patientId)

  where w.marketName = 'North Carolina'

),

final as (

  select *
  from standard_market_approach

  union all

  select *
  from north_carolina_market_approach

)

select *
from final
