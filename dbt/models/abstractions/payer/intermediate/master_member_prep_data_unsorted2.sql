
-- new flag added by andy 7/14/20, in light of cci contract change
-- need to tag mm's as being IN/OUT of cbh care/attribution 
-- regardless of the book of business data we are receiving on cci members
-- also simplifies what we are already doing downstream in mm spine and actuarial use cases 
-- will no longer need to select rows using several flags, this will be the master flag
-- https://docs.google.com/spreadsheets/d/1r6httetdcBel6Um-zOoLNVAheX7MOZKLNKRmAp54fqI/edit?ts=5f04f077#gid=644396663
-- https://docs.google.com/document/d/1AiM1RqfD0xtlXsDbZWRujarh5ANiqN3RAQ0s0dA_Fp8/edit
with finalresult_with_ooa as (
    select * from {{ ref('master_member_prep_data_unsorted') }}
),

is_cbh_member as (
  select distinct
    patientId,
    eligDate,
    fileReceivedFromDate,
    monthsAfterGoLive,
    partnerName,
    case 
    -- non-cci partners use fairly simple logic, not dependent on commons state
      when partnerName != 'ConnectiCare' and partnerName != 'Tufts' 
       and (isFullyDelegated = 1 or
            isOutOfAreaOriginally = 1)
        then false
    -- for tufts specifically pre 8/1/21, ooa should trigger a false, but post 8/1 we dont want it to trigger a false
      when partnerName = 'Tufts'
       and (isFullyDelegated = 1 or
            isOutOfAreaOriginally = 1)
       and eligDate < '2021-08-01'
       and cohortGoLiveDate < '2021-08-01'
        then false
    -- for tufts, since we receive new elig data daily and run tufts mmt daily, receivedTo should = current date otherwise false
      when partnerName = 'Tufts' and fileReceivedToDate < current_date()
        then false
    -- pre 6/1/20 logic is relatively simple for cci
    -- as long as member is not commons disenrolled as of 15th of mo, and is cci eligible as of 15th of mo, then they are 'IN'
    -- regardless of ooa or fd status
      when partnerName = 'ConnectiCare' 
       and eligDate < '2020-06-01'
       and (commonsState in ('disenrolled', 'disenrolled_after_consent')) 
        then false
    -- post 6/1/20 logic is more complex for cci
    -- post 6/1/20 if cci mem is ooa or expired, they should be disenrolled
      when partnerName = 'ConnectiCare' 
       and eligDate >= '2020-06-01' 
       and (isAfterDeceased = 1 or
            isOutOfAreaOriginally = 1 or
            commonsState in ('disenrolled', 'disenrolled_after_consent'))
        then false 
    -- post 1/1/21 if cci mem is fd and pre-consented, should be disenrolled   
      when partnerName = 'ConnectiCare' 
       and eligDate >= '2021-01-01' 
       and isFullyDelegated = 1 
       and commonsState not in ('consented', 'enrolled')
        then false 
    -- otherwise true (except for cci 1st 3mo rule which we handle below)    
      else true end as isCityblockMemberMonth
  from finalresult_with_ooa
  where isInLatestPayerFile = 1 -- not sure if we should leave this in or not. we won't be able to say as-of dates, a member was considered a cbh member
    and memberId is not null
),

-- applying cci 1st 3mo rules: if Member loses coverage or expires in the first 3 months of being assigned, retro remove those first months
cci_3mo as ( 
  select
    patientId,
    min(isCityblockMemberMonth) as isCityblockMemberMonth -- if any of the 1st 3mo's are false, going with that
  from is_cbh_member
  where monthsAfterGoLive between 0 and 3 -- only net new cci cohorts will have values 0-3 after 6/1/20
    and partnerName = 'ConnectiCare'
    and eligDate between '2020-06-01' and date_trunc(current_date(), month)
    and isCityblockMemberMonth is false -- only grabbing the members that do break the 3mo rule
  group by 1
),

-- if member breaks cci 1st 3mo rules, then all historic mm's for that member are marked false
-- is this appropriate? can the member ever come back?
-- other than the isinlatestpayer=1 rows and those already marked as true, every other row will default to false
final as (
  select 
    f.* , 
    coalesce(c.isCityblockMemberMonth, i.isCityblockMemberMonth, false) as isCityblockMemberMonth  
  from finalresult_with_ooa as f
  left join is_cbh_member as i 
    using(patientId, eligDate, fileReceivedFromDate)
  left join cci_3mo as c
    using(patientId)
)

select * from final