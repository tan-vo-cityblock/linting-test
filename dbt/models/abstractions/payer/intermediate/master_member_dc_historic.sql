with

-- grabbing cf member golive and cohort info from the commons abstraction member table
-- for members without a cohort, imputing their assignedAt date as their cohort golive
cf_mems as (
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
  where partnerName = 'CareFirst'
),

-- all cf mems included in the dc district historic claims data
-- if member has 1 claim in a given qtr, assume they are elig the whole qtr
elig_qtrs as (

  select distinct 
    trim(cast(CurrentMedicaidID as string)) as memberId,
    date_trunc(PARSE_DATE('%Y%m%d', (cast(FirstServiceDateKey as string))), quarter) as eligQtr
  from `cbh-partner-integrations.carefirst_dc.HistoricalClaimData_20201002`

--   select distinct
--     memberIdentifier.patientId,
--     date_trunc(lines.date.from, quarter) as eligQtr
--   from {{ source('carefirst', 'Professional') }}, unnest(lines) as lines
--   where memberIdentifier.patientId is not null

--   union distinct

--   select
--     memberIdentifier.patientId,
--     date_trunc(header.date.from, quarter) as eligQtr
--   from {{ source('carefirst', 'Facility') }}
--   where memberIdentifier.patientId is not null

--  union distinct

--   select
--     memberIdentifier.patientId,
--     date_trunc(date.filled, quarter) as eligQtr
--   from {{ source('carefirst', 'Pharmacy') }}
--   where memberIdentifier.patientId is not null
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
    cm.patientId, 
    em.eligDate, 
    'Medicaid (Non HARP)' as lineOfBusinessGrouped,
    date('9999-01-01') as fileReceivedFromDate,
    cm.partnerName,
    cm.cohortName,
    m.cohortId,
    cast(null as string) as rateCell,
    cm.cohortGoLiveDate,
    mimm.revenueGoLiveDate,  
    true as isCityblockMemberMonth,
    date_diff(em.eligDate, coalesce(mimm.revenueGoLiveDate, cm.cohortGoLiveDate), month) monthsAfterRevenueGoLive
  from elig_mos em
  left join cf_mems cm 
    --using(patientId)
    using(memberId)
  left join {{ source('member_index', 'member') }} m
    on cm.patientId = m.id
  left join {{ ref('member_index_member_management') }} mimm
    using(patientId)
  where eligDate < date_trunc(cm.cohortGoLiveDate, month)
)

select * from final
