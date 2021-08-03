
-- cost and use member months for pmpm denominators
-- this should be used as the date spine for c&u reporting
-- if there are claims occurring outside of these member months they should not be brought into the report (e.g. member was eligible for partner, but not for cbh)

with master_member_v1 as (
  select 
    patientId, 
    eligDate, 
    lineOfBusinessGrouped,
    fileReceivedFromDate,
    partnerName,
    cohortName,
    cohortId,
    rateCell,
    cohortGoLiveDate,
    revenueGoLiveDate,  
    isCityblockMemberMonth,
    monthsAfterRevenueGoLive,
    isFullyDelegated,
    isOutOfAreaOriginally,
  from {{ ref('master_member_v1') }}
  
  union distinct

-- for front end looker reporting/engagement tracking purposes, creating a copy of last month's eh digitial rows and imputing as current month's
  select 
    patientId, 
    date_add(eligDate, interval 1 month) as eligDate, 
    lineOfBusinessGrouped,
    fileReceivedFromDate,
    partnerName,
    cohortName,
    cohortId,
    rateCell,
    cohortGoLiveDate,
    revenueGoLiveDate,  
    isCityblockMemberMonth,
    monthsAfterRevenueGoLive,
    isFullyDelegated,
    isOutOfAreaOriginally,
  from {{ ref('master_member_v1') }}
  where cohortName = 'Emblem Medicaid Digital Cohort 1' 
    and patientId not in (select distinct patientId from {{ source('member_management', 'cohort8_from_digital_patientId_list') }} ) 
    and isCityblockMemberMonth is true
    and eligDate = (
                    select max(eligDate) 
                    from {{ ref('master_member_v1') }}
                    where cohortName = 'Emblem Medicaid Digital Cohort 1' 
                    and isCityblockMemberMonth is true
                   )
  
  union distinct

  select 
    patientId, 
    date_add(eligDate, interval 2 month) as eligDate, 
    lineOfBusinessGrouped,
    fileReceivedFromDate,
    partnerName,
    cohortName,
    cohortId,
    rateCell,
    cohortGoLiveDate,
    revenueGoLiveDate,  
    isCityblockMemberMonth,
    monthsAfterRevenueGoLive,
    isFullyDelegated,
    isOutOfAreaOriginally,
  from {{ ref('master_member_v1') }}
  where cohortName = 'Emblem Medicaid Digital Cohort 1' 
    and patientId not in (select distinct patientId from {{ source('member_management', 'cohort8_from_digital_patientId_list') }} ) 
    and isCityblockMemberMonth is true
    and eligDate = (
                    select max(eligDate) 
                    from {{ ref('master_member_v1') }}
                    where cohortName = 'Emblem Medicaid Digital Cohort 1' 
                    and isCityblockMemberMonth is true
                   )
  
  union distinct

  select 
    patientId, 
    eligDate, 
    lineOfBusinessGrouped,
    fileReceivedFromDate,
    partnerName,
    cohortName,
    cohortId,
    rateCell,
    cohortGoLiveDate,
    revenueGoLiveDate,  
    isCityblockMemberMonth,
    monthsAfterRevenueGoLive,
    0 as isFullyDelegated,
    0 as isOutOfAreaOriginally,
  from {{ ref('master_member_mass_health') }}

    union distinct
  
  select 
    patientId, 
    eligDate, 
    lineOfBusinessGrouped,
    fileReceivedFromDate,
    partnerName,
    cohortName,
    cohortId,
    rateCell,
    cohortGoLiveDate,
    revenueGoLiveDate,  
    isCityblockMemberMonth,
    monthsAfterRevenueGoLive,
    0 as isFullyDelegated,
    0 as isOutOfAreaOriginally,
  from {{ ref('master_member_dc_historic') }}

    union distinct
  
  select 
    patientId, 
    eligDate, 
    lineOfBusinessGrouped,
    fileReceivedFromDate,
    partnerName,
    cohortName,
    cohortId,
    rateCell,
    cohortGoLiveDate,
    revenueGoLiveDate,  
    isCityblockMemberMonth,
    monthsAfterRevenueGoLive,
    0 as isFullyDelegated,
    0 as isOutOfAreaOriginally,
  from {{ ref('master_member_healthy_blue_historic') }}
),

min_consented as (
-- makes assumption that earliest consent date is what we're interested in; even if member is later disenrolled (but eligible per payer), they will be counted as c&u denominator.
  select 
    patientId,
    case 
      when extract(day from min(ps.createdAt)) > 1 
        then date_add(date_trunc(date(min(ps.createdAt)), month), interval 1 month)
      when extract(day from min(ps.createdAt)) = 1 
        then date_trunc(date(min(ps.createdAt)), month)         
      else null end as eligDate
  from {{ source('commons', 'patient_state') }} ps
  where currentState in ('consented','enrolled')
  group by patientId
),

-- if member has concurrent LOB's taking the one with the highest expected premium/provider reimbursement
-- commercial > dsnp > medicare > medicaid
-- thus we are left with only 1 row per member per month
lob_temp as (
  select 
    patientId, 
    eligDate, 
    lineOfBusinessGrouped,
    case 
      when lineOfBusinessGrouped = 'Commercial' then 1
      when lineOfBusinessGrouped in ('DSNP','Duals') then 2
      when lineOfBusinessGrouped = 'Medicare Advantage' then 3
      when lineOfBusinessGrouped = 'Medicaid (Non HARP)' then 4
      when lineOfBusinessGrouped = 'Medicaid HARP' then 5
      else 6 end as lobRank
  from master_member_v1
),

lob_min as (
  select 
    patientId, 
    eligDate, 
    min(lobRank) as lobRank
  from lob_temp
  group by patientId, eligDate
),

lob_rank as (
  select 
    lm.patientId, 
    lm.eligDate, 
    lt.lineOfBusinessGrouped
  from lob_min lm
  left join lob_temp lt 
    using(patientId, eligDate, lobRank)
),

-- for tufts, members can switch rate cells mid month, and we only want to grab the new one, so we push 1 row pmpm only
max_file_from as (
  select 
    patientId,
    eligDate,
    --max(spanToDate) as maxSpanToDate
    max(fileReceivedFromDate) as maxFileReceivedFromDate
  from master_member_v1 mmt
  -- same logic as below re subsetting the mmt to the appropriate membermonths, but even within that there are dupes for tufts due to rate cell changes
  where mmt.isCityblockMemberMonth is true
  -- where mmt.memberId is not null  
  --   and mmt.isInLatestPayerFile = 1
  --   and mmt.isFullyDelegated != 1
  --   and mmt.isOutOfAreaOriginally != 1
  group by 1,2
),

result as (
  select distinct 
    mmt.patientId,
    mmt.eligDate,
    mmt.partnerName,
    mmt.cohortName,
    mmt.cohortId,
    mmt.rateCell,
    lr.lineOfBusinessGrouped,
    mmt.cohortGoLiveDate,
    mmt.revenueGoLiveDate,
    memhcc.group_1 as hccGroup1,
    memhcc.group_2 as hccGroup2,
    
    mmt.isCityblockMemberMonth,
    (mmt.isCityblockMemberMonth is true and mmt.monthsAfterRevenueGoLive >= 0) as isAtRisk,
    mmt.isFullyDelegated = 1 as isFullyDelegated,
    mmt.isOutOfAreaOriginally = 1 as isOutOfArea,
    
    -- consented for full month, per lesli logic
    case when mmt.eligDate >= mc.eligDate then 1 
         else 0 end as isConsentedFullMonth,
    -- performance period: nulls are 13+ months before revenuegolivedate and < 3.5 mos from today (runout period + data-shipment delay), and will be excluded from c&u denominators     
    case when mmt.monthsAfterRevenueGoLive between -12 and -1 then 0
         when mmt.eligDate > date_sub(date_sub(current_date(), interval 3 month), interval 15 day) then null
         when mmt.monthsAfterRevenueGoLive >= 0 then 1
         else null end as isPerformancePeriod
  from master_member_v1 mmt

  left join min_consented mc 
    using(patientId)

  left join lob_rank lr 
    on mmt.patientId = lr.patientId 
    and mmt.eligDate = lr.eligDate

  left outer join {{ref('member_hcc_groups')}} as memhcc on mmt.patientId=memhcc.patientId
  -- only keeping the latest version per mem per mo, this specifically addresses tufts mid-month changes in rateCell, by keeping the latter ratecell in that month
  inner join max_file_from mff 
    on (mmt.patientId = mff.patientId 
    and mmt.eligDate = mff.eligDate 
    and mmt.fileReceivedFromDate = mff.maxFileReceivedFromDate)

  --removing this where clause for now to surface both true and false months. requires education of users to toggle on/off as needed 
  --where mmt.isCityblockMemberMonth is true

  -- -- member has an eligibility record for that month in the partner file (indicated by the non-null memberId for that eligDate)
  -- mmt.memberId is not null  
  -- -- is in latest and greatest data-shipment from our partner
  -- -- note for cci this is a rolling 3 yr window, so the lastest file will not include 2016 anymore. need to adjust the mmt
  -- -- note for tufts the latest file doesn't include all the records, so using isLatestVersion for tufts instead. andy 3/24/20 we're handling this upstream in master member prep data unsorted.sql
  -- and mmt.isInLatestPayerFile = 1
  -- -- is not fully delegated (emblem only)
  -- and mmt.isFullyDelegated != 1
  -- -- was in-area upon first receipt of this member-month, even if partner retro-changed the member address
  -- and mmt.isOutOfAreaOriginally != 1
)

select distinct * from result

union distinct
  
select 
  patientId, 
  eligDate, 
  partnerName,
  cohortName,
  cohortId,
  rateCell,
  lineOfBusinessGrouped,
  cohortGoLiveDate,
  revenueGoLiveDate,  
  cast(null as string) as hccGroup1,
  cast(null as string) as hccGroup2,
  isCityblockMemberMonth,
  true as isAtRisk,
  false as isFullyDelegated,
  false as isOutOfArea,
  cast(null as int64) as isConsentedFullMonth,
  cast(null as int64) as isPerformancePeriod
from {{ ref('master_member_tufts_deidentified') }} 
