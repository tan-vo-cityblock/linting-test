with

-- grabbing hb member golive and cohort info from the commons abstraction member table
-- for members without a cohort, imputing their assignedAt date as their cohort golive
mems as (
  select 
    m.patientId,
    mmc.memberId,
    m.partnerName,
    m.cohortName,
    coalesce(m.cohortGoLiveDate, date(ms.assignedAt) ) as cohortGoLiveDate
  from {{ ref('src_member') }} m
  left join {{ ref('abs_commons_member_market_clinic') }} mmc
    using(patientId)
  left join {{ ref('member_states') }} ms
    using(patientId)
  where partnerName = 'Healthy Blue'
),

clms as (
  select distinct
    memberIdentifier.partnerMemberId as memberId,
    header.date.from as dateFrom
  from {{ source('healthy_blue', 'Facility') }}

  union distinct

  select distinct
    memberIdentifier.partnerMemberId as memberId,
    lines.date.from	as dateFrom
  from {{ source('healthy_blue', 'Professional') }},
  unnest(lines) as lines
),

-- all hb mems included in the hb historic claims data
-- if member has 1 claim in a given qtr, assume they are elig the whole qtr
elig_qtrs as (
  select distinct 
    memberId,
    date_trunc(dateFrom, quarter) as eligQtr
  from clms
),

-- convert quarters to months
elig_mos as (
  select distinct
    memberId,
    eligDate,
  from elig_qtrs,
  unnest(generate_date_array(eligQtr, current_date(), interval 1 month)) as eligDate
),

-- pulling it all together, imputing some of this.
-- these are all the columns included in the mm_spine_cu_denoms
final as (
  select distinct
    mems.patientId, 
    em.eligDate, 
    'Medicaid (Non HARP)' as lineOfBusinessGrouped,
    date('9999-01-01') as fileReceivedFromDate,
    mems.partnerName,
    mems.cohortName,
    m.cohortId,
    cast(null as string) as rateCell,
    mems.cohortGoLiveDate,
    mimm.revenueGoLiveDate,  
    true as isCityblockMemberMonth,
    date_diff(em.eligDate, coalesce(mimm.revenueGoLiveDate, mems.cohortGoLiveDate), month) monthsAfterRevenueGoLive
  from elig_mos em
  left join mems 
    --using(patientId)
    using(memberId)
  left join {{ source('member_index', 'member') }} m
    on mems.patientId = m.id
  left join {{ ref('member_index_member_management') }} mimm
    using(patientId)
  where eligDate < date_trunc(mems.cohortGoLiveDate, month)
  -- temporary code while we wait for services team to add golivedates to member service
    or mems.cohortGoLiveDate is null
)

select * from final
