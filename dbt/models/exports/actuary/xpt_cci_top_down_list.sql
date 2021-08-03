with master_member_v1 as (

  select
    patientId,
    eligDate,
    concat("https://commons.cityblock.com/members/", patientId) as commonsLink,
    memberId,
    memberId is not null as partnerEligible,
    lineOfBusiness1,
    lineOfBusiness2 as subLineOfBusiness,
    lineOfBusinessGrouped,
    cohortName,
    cohortGoLiveDate,
    partnerName,
    doNotCall,   
    city,
    zip,
    latestMemberId as partnerMemberId,
    cast(isInLatestPayerFile as bool) as isInLatestPayerFile,
    case
      when memberId is not null
        then cast(isFullyDelegated as bool)
      else null
    end as isFullyDelegated,
    cast(isAfterGoLive as bool) as isAfterGoLive,
    cast(isEverDeceased as bool) as isEverDeceased,
    cast(isAfterDeceased as bool) as isAfterDeceased,
    cast(isOutOfAreaOriginally as bool) as isOutOfAreaOriginally,
    isCityblockMemberMonth

  from {{ ref('master_member_v1') }}
  where partnerName = 'ConnectiCare'

),

members as (

  select
    patientId,
    patientName as memberName,
    memberPod,
    currentState
    
  from {{ ref('member') }}

),

members_reached_in_last_45_days as  (

   select patientId
   from {{ ref('member_interactions') }}
   where isSuccessfulConnection
   group by patientId
   having max(eventTimestamp) > timestamp_sub(current_timestamp, interval 45 day)

),

member_states as (
  
  select
    patientId,
    consentedAt,
    latestDisenrollmentReason

  from {{ ref('member_states') }}

),

members_ever_consented as (

  select patientId
  from member_states
  where consentedAt is not null

),

latest_member_disenrollment_reasons as (
  
  select
    patientId,
    latestDisenrollmentReason as disenrollmentReason

  from member_states

),

non_null_member_ids as (

  select *
  from master_member_v1
  where memberId is not null

),

ranked_member_lines_of_business as (

  select 
    patientId, 
    lineOfBusinessGrouped,
    lineOfBusiness1,
    subLineOfBusiness,
    city,
    zip,
    ROW_NUMBER() OVER (PARTITION BY patientId ORDER BY eligDate DESC) as rn
    
  from non_null_member_ids
  where lineOfBusinessGrouped != 'Not Yet Labeled'

),

latest_member_lines_of_business as (

  select 
    patientId, 
    lineOfBusinessGrouped,
    lineOfBusiness1,
    subLineOfBusiness,
    city,
    zip
    
  from ranked_member_lines_of_business
  where rn = 1 
  
),

latest_member_eligibility_dates as (

  select 
    patientId, 
    max(eligDate) as latestEligDate
  
  from non_null_member_ids
   
  where isInLatestPayerFile is true
    and isAfterDeceased is false
    and isOutOfAreaOriginally is false
    and isFullyDelegated is false
  
  group by 1
  
),

base as (

  select
    DATE_SUB(DATE_ADD(mmt.eligDate, INTERVAL 1 MONTH), INTERVAL 1 DAY) as asOfDate,
    mmt.commonsLink,
    mmt.patientId,
    m.memberName,
    mmt.partnerMemberId,
    l.city,
    l.zip,
    mmt.partnerName,
    mmt.cohortName,
    mmt.cohortGoLiveDate,
    m.memberPod,
    l.lineOfBusinessGrouped,
    l.subLineOfBusiness,
    m.currentState,
    mmt.isCityblockMemberMonth,
    
    case 
      when m.currentState in ('reached', 'assigned',	'attributed', 'not_interested', 'contact_attempted') then 'Pre-Interested'       
      when m.currentState = 'interested' and r.patientId is not null then 'Interested - Contacted in last 45 days'
      when m.currentState = 'interested' and r.patientId is null then 'Interested - No contact in last 45 days'
      when m.currentState in ('consented', 'enrolled') then 'Post-Consent'
      when m.currentState like '%disenrolled%' then 'Disenrolled'
      else null
    end as currentStateCategorized,
    
    d.disenrollmentReason,
   
    mmt.partnerEligible,
    
    case 
      when mmt.memberId is null 
       or mmt.isEverDeceased is true
       or mmt.isOutOfAreaOriginally is true
       or mmt.isFullyDelegated is true
        then date_diff(mmt.eligDate, le.latestEligDate, month) 
      else 0
    end as nMonthsSinceLastEligible,
         
    mmt.isEverDeceased as isDeceased,
    mmt.isOutOfAreaOriginally as isOutOfArea, 
    mmt.isFullyDelegated,
    mmt.doNotCall,
    e.patientId is not null as everConsented,
    r.patientId is not null as reachedLast45d
      
  from master_member_v1 mmt

  left join  members m
  using (patientId)
  
  left join members_reached_in_last_45_days r
  using (patientId)
  
  left join members_ever_consented e 
  using (patientId)

  left join latest_member_disenrollment_reasons d
  using (patientId)
  
  left join latest_member_lines_of_business l
  using (patientId)
  
  left join latest_member_eligibility_dates le
  using (patientId)

  where
    mmt.eligDate = date_sub(date_trunc(current_date, month), interval 1 month) and
    mmt.isAfterGoLive is true
        
),

-- Disenrollment: 

-- All pre-interested members will be disenrolled immediately. Wilson will give notice to his team and the few CHPs doing outreach. 
-- All interested members with no contact in the last 45 days will be disenrolled immediately

members_to_disenroll as (
  select
    'Disenroll' as action,
    *
    
  from base
  
  where 
  
     -- pre-interested; or interested but no successful contact in 45d; non medicaid even if contacted within 45d are disenrolled
    
      ( currentStateCategorized = 'Pre-Interested' or 
       (currentStateCategorized = 'Interested - No contact in last 45 days') or
       (currentStateCategorized = 'Interested - Contacted in last 45 days' and lineOfBusinessGrouped not like '%Medicaid%')
      ) and      
     
     -- ineligible per the partner
      (partnerEligible is false or isOutOfArea is true or isFullyDelegated is true)

),

-- Disenrollment Continued (To outreach aka "pending disenrollment"):
-- Interested members with contact in the last 45 days:
  -- Medicaid members will be followed up on from the care team
  -- Non-medicaid members will be immediately disenrolled
  -- All post-consent members will be followed up on by the care team

members_to_outreach as (

  select
    'Verify' as action,
    *
    
  from base
  
where  

  -- all post-consented; those interested with successful contact in last 45d
    ( currentStateCategorized = 'Post-Consent' or 
      (currentStateCategorized = 'Interested - Contacted in last 45 days' and lineOfBusinessGrouped like '%Medicaid%' )
     ) and
  
  -- ineligible per the partner
    (partnerEligible is false or isOutOfArea is true or isFullyDelegated is true) and 
    
  -- donotcall  
    doNotCall is false
),

-- Re-enrollment:

-- We will re-enroll members who are in the eligibility file, in our zip codes, not fully delegated elsewhere, and whose reason for disenrollment from CB was not deceased or doNotCall
-- For members NOT previously consented, will be added back to general pool to be re-assigned
-- For members who WERE previously consented, will provide list to hub leads along with their last CHP to manage assignment

members_to_reenroll as (

  select
    'Reenroll' as action,
    *
    
  from base
  
where 

  -- state = disenrolled or disenrolled after consent
     currentStateCategorized = 'Disenrolled' and
  
  -- eligible per the partner
     partnerEligible is true and 
     isOutOfArea is false and
     isFullyDelegated is false and
     iscityblockmembermonth is true and
    
  -- disenrollment reason not deceased or donotcall
     disenrollmentReason not in ('deceased', 'doNotCall') and 
  
  -- not deceased or donotcall  
     doNotCall is false and
     isDeceased is false
     
),

combined_member_list as (

  select * from members_to_disenroll
  union all
  select * from members_to_outreach
  union all
  select * from members_to_reenroll

),

final as (

  select
  
    case 
      when action in ('Disenroll','Verify') and isFullyDelegated or (partnerEligible is false and lineOfBusinessGrouped in ('Medicaid (Non HARP)', 'Medicaid HARP')) 
        then 'Most Actionable'
      when action in ('Disenroll','Verify') and partnerEligible is false and lineOfBusinessGrouped in ('Medicare Advantage', 'DSNP') 
        then 'Possibly Actionable'
      when action in ('Disenroll','Verify') and (isDeceased or isOutOfArea or (partnerEligible is false and lineOfBusinessGrouped = 'Commercial')) 
        then 'Least Actionable'
      when action in ('Reenroll') 
        then 'Reenroll'
      else null 
        end as actionable,
        
    case 
      when action = 'Reenroll'
        then null
      when isDeceased 
        then 'deceased' 
      when isOutOfArea 
        then 'moved' 
      when isFullyDelegated 
        then 'other' 
      when partnerEligible is false 
        then 'ineligible' 
      else 'other' end as disenrollmentReason_FOR_ENG,
    *

  from combined_member_list

)

select *
from final
order by partnerName, action, actionable, currentStateCategorized
