with master_member_tufts as (
    select distinct
        eligDate,
        partnerName,
     from {{ ref('master_member_v1') }}
     where partnerName = 'Tufts'
       and eligDate >= '2020-03-01'
)

  select 
    'deidentified' as patientId, 
    eligDate, 
    cast(null as string) as lineOfBusinessGrouped,
    cast(null as date) as fileReceivedFromDate,
    partnerName,
    cast(null as string) as cohortName,
    cast(null as int64) as cohortId,
    cast(null as string) as rateCell,
    cast(null as date) as cohortGoLiveDate,
    cast(null as date) as revenueGoLiveDate,  
    true as isCityblockMemberMonth,
    cast(null as int64) as monthsAfterRevenueGoLive
  from master_member_tufts
