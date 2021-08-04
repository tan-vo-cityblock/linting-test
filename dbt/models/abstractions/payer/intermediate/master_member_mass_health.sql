with

-- grabbing tufts member golive and cohort info from the commons abstraction member table
-- for members without a cohort, imputing their assignedAt date as their cohort golive
tufts_mems as (
  select 
    m.patientId,
    m.partnerName,
    m.cohortName,
    coalesce(m.cohortGoLiveDate, date(ms.assignedAt) ) as cohortGoLiveDate
  from {{ ref('src_member') }} m
  left join {{ ref('abs_commons_member_market_clinic') }} mmc
    using(patientId)
  left join {{ ref('member_states') }} ms
    using(patientId)
  where partnerName = 'Tufts'
    and patientHomeMarketName = 'Massachusetts' -- is it right to limit to these folks? this excludes tufts virtual market
    and (cohortName != 'Tufts Cohort 1' or cohortName is null) -- we already have historic eligibility for these members
),

-- all tufts mems included in the mass health claims data
-- if member has 1 claim in a given qtr, assume they are elig the whole qtr
elig_qtrs as (
  select distinct
    patient.patientId,
    date_trunc(data.DOS_FROM_DT, quarter) as eligQtr
  from {{ source('tufts_silver', 'MassHealth_*') }}
  where patient.patientId is not null
),

-- convert quarters to months
elig_mos as (
  select distinct
    patientId,
    eligDate,
  from elig_qtrs,
  unnest(generate_date_array(eligQtr, current_date(), interval 1 month)) as eligDate
),

-- pulling it all together, imputing some of this.
-- these are all the columns included in the mm_spine_cu_denoms
final as (
  select distinct
    em.patientId, 
    em.eligDate, 
    'Duals' as lineOfBusinessGrouped,
    date('9999-01-01') as fileReceivedFromDate,
    tm.partnerName,
    tm.cohortName,
    m.cohortId,
    cast(null as string) as rateCell,
    tm.cohortGoLiveDate,
    mimm.revenueGoLiveDate,  
    true as isCityblockMemberMonth,
    date_diff(em.eligDate, coalesce(mimm.revenueGoLiveDate, tm.cohortGoLiveDate), month) monthsAfterRevenueGoLive
  from elig_mos em
  left join tufts_mems tm 
    using(patientId)
  left join {{ source('member_index', 'member') }} m
    on em.patientId = m.id
  left join {{ ref('member_index_member_management') }} mimm
    using(patientId)
  where eligDate < date_trunc(tm.cohortGoLiveDate, month)
)

select * from final
