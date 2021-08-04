
--  aseiden 8/9/19, updated 8/26/19, updated 9/10/19
--  aseiden split the master member table sql into two sections, this is the second step of adding flags/logic to the already mostly cleaned/reshaped prepared data.
-- moved earlier CTEs to a prep sql file called master_member_prep_data dot sql to prevent resources exceeded BQ error. doing this in two pieces.
--  creating the true "member master table", combining data from emblem, cci, and commons into a single table for looker dashboard backend
--  mapping to theresa's monthly reconciliation scenarios
--  ensuring every membermonth has 1 row, either populated or not, so that it can be counted or not towards denominators, etc.
--  TBD whether to fill in blank rows with 0's and copy of the most recent member info into null rows, or if patientId year month and commons state is enough

-- set dbt variables to be used below repeatedly

-- per jac 7/22/20
-- a) Mark an emblem member as outOfArea if they live outside of kings & queens county, AND
-- b) Mark an emblem member as outOfArea if they are Cohort 1-5 and live outside of kings county prior to 2020-07-01
-- updated 12/20/20
-- cohort 7 (id=18) golive 7/1/20 queens becomes in catchment going forward
-- cohort 8 (id=24) golive 1/1/21 nassau becomes in catchment for all cohorts going forward in 2021
{% set ooa_code = "(

    ( partnerName = 'Emblem Health' and source = 'emblem'
      and (
           ( county not in ('KINGS', 'QUEENS', 'NASSAU'))

            or

            ( 
             date(eligYear, eligMonth, 1) < '2020-07-01' 
             and county != 'KINGS'
             and cohortId < 18 
            )

            or 

            ( 
             date(eligYear, eligMonth, 1) < '2021-01-01' 
             and county not in ('KINGS','QUEENS')
             and cohortId < 24
            )

          )
      )

    or (partnerName = 'ConnectiCare' and zip not in (
      '06001', '06010', '06011', '06013', '06120', '06037', '06759', '06444', '06057', '06062', '06479', 
      '06489', '06716', '06786', '06782', '06705', '06787', '06492', '06501', '06450', '06405', '06505', 
      '06514', '06517', '06518', '06511', '06512', '06513', '06515', '06519', '06530', '06531', '06536', '06473', 
      '06477', '06516', '06525', '06457', '06460', '06461', '06451', '06790', '06718', '06111', '06416', '06604', 
      '06605', '06606', '06607', '06608', '06610', '06484', '06901', '06614', '06615', '06611', '06401', 
      '06403', '06410', '06762', '06770', '06478', '06483', '06488', '06702', '06703', '06704', '06706', '06708', 
      '06710', '06722', '06779', '06795', '06798', '06524', '06418', '06712', '06053', '06071', '06032') ) 

    or (partnerName = 'Tufts' and date(eligYear, eligMonth, 1) < '2021-08-01' and zip not in (
      '01005',  '01031',  '01037',  '01068',  '01074',  '01083', '01092', '01094',  '01331',  '01366',  '01368',  '01420',  
      '01430',  '01436',  '01438',  '01440',  '01441',  '01451', '01452', '01453',  '01462',  '01467',  '01468',  
      '01473',  '01475',  '01501',  '01503',  '01504',  '01505', '01506', '01507',  '01508',  '01509',  '01510',  
      '01515',  '01516',  '01518',  '01519',  '01520',  '01522', '01523', '01524',  '01525',  '01526',  '01527',  
      '01529',	'01531',	'01532',	'01534',	'01535',	'01536', '01537',	'01538',	'01540',	'01541',	'01542',	'01543',
      '01545',	'01546',	'01550',	'01560',	'01561',	'01562', '01564',	'01566',	'01568',	'01569',	'01570',	'01571',
      '01581',	'01583',	'01585',	'01586',	'01588',	'01590', '01601',	'01602',	'01603',	'01604',	'01605',	'01606',
      '01607',	'01608',	'01609',	'01610',	'01611',	'01612', '01613',	'01614',	'01615',	'01653',	'01655',	'01740', '01745',	
      '01747',	'01756',	'01757',	'01772') )

    or (partnerName = 'Tufts' and date(eligYear, eligMonth, 1) >= '2021-08-01' and state != 'MA' )

  )" %}
      -- this is not the full list of emblem zips so it's marking most cohort 6a-7 members as ooa. using kings and queens county instead for now
      -- and zip not in ('11378', '11379', '11385', '11414', '11416', '11417', '11418', '11419', '11420', '11421', '11430') 

    
-- need to eventually add at-risk pcp ids for cci. get from lauren/pavel?
{% set fd_list = " 
    ('0100', '0108', '0139', '0140', '0141','0142','0143', '0145', 
    '0146', '14BR', '14GR', '14RP', '14MM', '14MN', '14MV', '14MY', 
    '14HH', '14HY') " %}

--  result from the prep work sql, sorted version calculated separately
with result as (
  select * from {{ ref('master_member_prep_data_uuid') }}
),

--  max file date indicator per member-year-month
latest as (
  select distinct
    patientId as id,
    eligYear as yr,
    eligMonth as mo,
    max(fileReceivedToDate) as max,
    partnerName as payer,
    source as src,         
    1 as isLatestFile
  from result
  group by id, yr, mo, payer, source
),

--  isActiveInClaims = row that = max of the file dates across everybody
active as (
  select distinct
    payer,
    src,
    max(max) as maxfileReceivedToDate,
    1 as isActiveInClaims
  from latest
  group by 1,2
),

--  max file FROM date per payer & source (i.e. amysis vs facets)
latestfrom as (
  select distinct
    partnerName as payer,
    source as src,
    max(fileReceivedFromDate) as maxfileReceivedFromDate
  from result
  group by 1,2
),

--  is current payer month (month prior to the latest payer file)
-- latest firstofmonth/eligdate per partner, with any memberid's not null
currentpayermonth as (
   select
      partnerName as payer, 
      max(date(eligYear, eligMonth, 1)) as eligdt,
      1 as iscurrentpayermo
    from result
    where memberId is not null
    group by 1
 ),

--  retroactive changes made flag, per each member year month
retro as (
  select distinct
    patientId as id,
    eligYear as yr,
    eligMonth as mo,
    min(fileReceivedToDate) as min,
    max(fileReceivedToDate) as max,
    0 as isRetroChange
  from result
  group by id, yr, mo
),

--  dateofdeath ever not null or cbh disenrolled because member deceased, flagging this on all member rows
--  to create isAfterDeceased flag, taking the lesser of two dates: 1) dod in payer data, 2) commons state createdat date to disenrolled where reason =  deceased
dod as (
 select distinct
    patientId as id,
    min(coalesce(dateOfDeath, commonsStateStartDate)) as dod,
    1 as isEverDeceasedtemp
 from result       
 where dateOfDeath is not null 
    or (disenrollmentReason = 'deceased' and isNewlyDisenrolled = 1)
 group by patientId
),

-- latest memberId per patientId - calculated in a separate query now
memberids as (
  select * from {{ ref('latest_member_id') }}
),

-- out of area notice date
-- flagging each ooa person's first ooa file received from date, so we can create isAfterOoa flag below
ooa_start as (
    select distinct 
      patientId as id, 
      min(fileReceivedFromDate) as outOfAreaNoticeDate
    from result
    -- ooa logic stored as dbt var
    where {{ooa_code}}
    group by patientId
),

--  business logic as of 8/9/19
--  adding business logic for various scenarios outlined by theresa c and jac
--  aseiden note these are not currently mutually exclusive definitions, so we need to clarify these with TC and JJ.
scenarios as (
select
    r.rowid,
  --  building scenarios A-I  as found in https://docs.google.com/presentation/d/1-OBM3YKxuoVSWAcwiwfCZUD6tknUMoZei3A_GvGmWPk/edit#slide=id.g5cc51248c4_0_27
    CASE

  -- scenarios should only be applied to member month records up through the max elig month we have received for each payer
  -- before tagging any populated scenarios, first tagging all rows with null scenario that are too recent to apply a scenario to
  -- typically the payer data is ~1 month behind commons data
    WHEN date(r.eligYear,  r.eligMonth, 1) >= date(extract(year from l.maxfileReceivedFromDate), extract(month from l.maxfileReceivedFromDate), 1) THEN null

   --  Scenario A: Consented or enrolled member who is no longer eligible, Care continues to be provided to member over a 30-day pending period while options for re-enrollment are researched .
    WHEN commonsState in ('consented', 'enrolled') and memberId is null THEN 'A - Consented/enrolled member no longer eligible'

  --  Scenario B: Interested member who is no longer eligible, Communication continues over 30-day period while options for re-enrollment are researched.
    WHEN commonsState in ('interested') and memberId is null THEN 'B - Interested member no longer eligible'
  --  Scenario C: Unengaged member who is no longer eligible, disenrolled effective immediately.
    WHEN commonsState in ('reached', 'assigned', 'attributed', 'not_interested', 'contact_attempted') and memberId is null THEN 'C - Unengaged member no longer eligible'
  
  --  Scenario D: Member is deceased. aseiden assuming 8/7/19 that we find out of deceased status from the payer elig file and the member was not disenrolled already in commons
    WHEN commonsState != 'disenrolled' and (dateOfDeath is not null or d.isEverDeceasedtemp=1) THEN 'D - Member deceased'
  
  --  Scenario E: Member has been identified as ineligible by plan but is active under another Member ID or has since re-enrolled with the plan... aseiden 8/7/19  assuming this means member's commons state is currently disenrolled with a disenrollment reason due to loss of eligibility, however we now see in the new payer file that the member is indeed actively enrolled? (e.g. has reenrolled, or enrolled under a different memberid)
  --   aseiden 12/18/19 we deprecated fuzzyDummy upstream from here so there should be no more scenario E's

  --  Scenario F: CBH initiated disenrollment conflicts with current payer eligibility, demographic, and/or claims data. Moved out of service area or Deceased.
    WHEN commonsState = 'disenrolled' and disenrollmentReason != 'ineligible' and memberId is not null and not doNotCall THEN 'FG - Payer/Commons conflict' 
  -- moved might be an exception here, bc even if they move they may be in the elig file still
  --  Scenario G: Payer initiated disenrollment conflicts with current payer eligibility, demographic, and/or claims data. Moved out of service area or Deceased
  --  Identify members by comparing disenrolled members by reason code with current member month, claims, and demographic (zip code) data (i.e  - if deceased, but recent claims; if moved, but zip code is in catchment area)
  --  note this is only moved for now. consult katie re deceased with paid  claims, and  use lesli c&u data in DBT. known issue with naming convention of c&u files might be a blocker. Also, need to explicitly define additional logic besides moved, deceased, ...
        WHEN commonsState = 'disenrolled' and disenrollmentReason = 'moved' and not {{ooa_code}} and not doNotCall THEN 'FG - Payer/Commons conflict'       
 
  --  Scenario F/G Combined: Agnostic to whether CBH or Payer initiated the disenrollment, we are flagging for review (deprecates the above two scenarios for V1 member master table)
  --  just switched the above draft codes to impute 'FG' instead of 'F' or 'G'. revisit this for v2

  --  Scenario H: Disenrolled member re-enrolls with the plan, as long as they're not ooa or donotcall
        WHEN commonsState = 'disenrolled' and disenrollmentReason = 'ineligible' and memberId is not null and not {{ooa_code}} and not doNotCall THEN 'H - Candidate for reenrollment due to plan eligibility'
  
  --  Scenario I: Member is associated with fully delegated/at risk PCP and appearing in most recent eligibility file
        WHEN medicalCenterNumber in {{fd_list}} THEN 'I - Member fully delegated to non-CBH PCP' 
  ELSE null END as scenario

from result r

left join latestfrom l 
    on r.partnerName = l.payer and r.source = l.src

left join dod d 
    on r.patientId = d.id
),

--  identifying coverage lapses
lapses as (
   select patientId as id, 
          min(date(eligYear, eligMonth, 1)) as min,
          max(date(eligYear, eligMonth, 1)) as max
   from result
   where memberId is not null
   group by id
),

--  full results, joined with the maxfile, scenarios, denominator flags we created above
finalresult as (
   select distinct
         -- adding 1st of the month date for looker x-axis purposes
         date(eligYear, eligMonth, 1) as eligDate,
         * except(id, mo, yr, min, max, maxfileReceivedToDate, isLatestFile, isRetroChange, isActiveInClaims, payer, dod, isEverDeceasedtemp, iscurrentpayermo, rowid, src, eligdt), 
   
   --  PAYER ELIGIBILITY
     --  there  can be multiple records per patientId, month, year, but only one of those record will have isLatestFile = 1. this  should be used if we dont' want  to count dupes in the dashboards
         case when (isLatestFile = 1) or (memberId is null and commonsState is not null and commonsState!='disenrolled') then 1 else 0 end as isLatestVersion,
     --  if a given record merges with 'active' then it must be active as of current date or the latest fileToDate among all members, thus it's active. Even if a given record is the latest record for that member/yr/mo combo, doesn't mean it's 'active' today.
     --  with cci and tufts both not providing full historic data refresh every month, need to edit this logic to include older data as still "active"    
         case 
          -- this applies for all emblem data, and most cci data. will be included in the max file received from partner
          when isActiveInClaims = 1 then 1 
          -- for cci, anything older than 37mo from the file received date, but which is still the latest version of that memmonth should be kept
          when isLatestFile = 1 and r.partnerName = 'ConnectiCare' and date_add(date(r.eligYear, r.eligMonth, 1), interval +37 month) < r.fileReceivedFromDate then 1
          -- hard coded exception as a workaround for the last amysis medicare elig shard we want to use
          when isLatestFile = 1 and r.partnerName = 'ConnectiCare' and source in ('amysis_med') and fileReceivedToDate in ('2020-02-10') then 1
          -- for cci facets, member files are snapshots not rolling lookbacks. so if the row is the latest version of that member-month we're keeping it
          when isLatestFile = 1 and r.partnerName = 'ConnectiCare' and source in ('facets') then 1
          -- for tufts, anything that is the latest version of that memmonth should be kept. if tufts makes a retro elig change they will issue a new daily file with that month/span in it (andy 3/24/20)
          when (r.partnerName = 'Tufts') and ((isLatestFile = 1) or (memberId is null and commonsState is not null and commonsState!='disenrolled')) then 1
          else 0 end as isInLatestPayerFile,

     --  flagging the latest valid payer data month
         ifnull(c.iscurrentpayermo, 0) as isCurrentPayerMonth,
     --  indicator of eligibility status  on 15th of the month, including all  months of full elig, and appropriate months of partial
         ifnull(case
                   when (memberId is null
                       and (date(r.eligYear, r.eligMonth, 15) <= (lag(r.eligEndDate, 1) OVER (PARTITION BY r.patientId, r.fileReceivedFromDate ORDER BY r.patientId, r.eligYear, r.eligMonth ASC)))
                       and (date_add(date(r.eligYear, r.eligMonth, 1), interval +1 month) > (lag(r.eligEndDate, 1) OVER (PARTITION BY r.patientId, r.fileReceivedFromDate ORDER BY r.patientId, r.eligYear, r.eligMonth ASC))))
                   or (memberId is not null) then 1 end, 0) as isEligOnFifteenth,
     --  partialMonths -- note this may not accurately flag partials created by a retro termination b/c memberId will not be null
         ifnull(case
                 when memberId is null
                     and (date(r.eligYear, r.eligMonth, 1) < (lag(r.eligEndDate, 1) OVER (PARTITION BY r.patientId, r.fileReceivedFromDate ORDER BY r.patientId, r.eligYear, r.eligMonth ASC)))
                     and (date_add(date(r.eligYear, r.eligMonth, 1), interval +1 month) > (lag(r.eligEndDate, 1) OVER (PARTITION BY r.patientId, r.fileReceivedFromDate ORDER BY r.patientId, r.eligYear, r.eligMonth ASC)))
                 then 1 end, 0) as isPartialMonth,
     --  flagging coverage lapses. if the  month is between the min and max eligibility per member, but memberid is null
         case when memberId is null and date(r.eligYear, r.eligMonth, 1) between la.min and la.max then 1 else 0 end as isCoverageLapse,                 
     --  flagging upcoming eligiblityenddate that is not 12/31/CY or 12/31/99 and is > the 15th of the current memmonth record
         case when r.eligEndDate != '2199-12-31' and r.eligEndDate > date(eligYear, eligMonth, 15) and r.eligEndDate != date(eligYear, 12, 31) then 1 else 0 end as isLosingEligibility,
     --  flagging future records where we expect the member will be eligible in the future data (havent received it yet), given best info available today (eligibility end date on prior line is > eligDate)
         case when memberId is null and date(eligYear, eligMonth, 1) > la.max and (lag(r.eligEndDate, 1) OVER (PARTITION BY r.patientId ORDER BY r.eligYear, r.eligMonth ASC)) > date(eligYear, eligMonth, 15) then 1 else 0 end as isExpectedToBeEligible, 
     --  out of area cases - flagging all rows that are Ooa
         case when {{ooa_code}} then 1 else 0 end as isOutOfAreaCurrently,
     --  out of area cases - flagging rows with eligDate on/after ooa notification month          
         case when date(eligYear, eligMonth, 1) >= outOfAreaNoticeDate then 1 else 0 end as isAfterOutOfAreaNoticeDate,
     -- fully delegated flag, in addition to scenario I -- replace with dbt variable as this list subject to change
        case 
          when medicalCenterNumber in {{fd_list}} then 1
          else 0 end as isFullyDelegated, 
   
   --  RETRO CHANGES
     --  we only flagged the earliest file  version, so all later  versions are considered retro changes to the first version
     --  aseiden 8/14/19 what do we do when ppt has multiple retro changes to the same month, and then retro termed for that month?
     --  there was a record received with this patientId-yrmo combo, but it  was not in the most recent payer file, so it must be a retro term for that yrmo (i.e. the a.maxfilereceiveddate)
     --  however this does not hold true for CCI since they send us a rolling 3yrs instead of a full historic refresh, so need to exempt cci 3yr old records from this
         case when (memberId is not null and re.isRetroChange is null) then 1 else re.isRetroChange end as isAnyRetroChange,
         case when memberId is not null and re2.max < CURRENT_DATE() and not ( r.partnerName = 'ConnectiCare' and date_add(date(r.eligYear, r.eligMonth, 1), interval +37 month) < r.fileReceivedFromDate ) then 1 else 0 end as isRetroTerm,     
     --  i think this holds true for tufts, but remains to be seen. if tufts makes a retro change, the will  send a new record that includes the old month they are changing, therefore we can tag the older record as being retro changed

   --  COMMONS
         case when commonsState is not null then 1 else 0 end as isInCommons,
         case when commonsState is not null and commonsState!='disenrolled' then 1 else 0 end as isActiveInCommons,
         case when r.eligYear = extract(year from cohortGoLiveDate) and r.eligMonth = extract(month from cohortGoLiveDate) then 1 else 0 end as isGoLiveMonth,
     --  is the last day of the month >= the member's cohort's golivedate? if so, they may be counted for cost and use purposes
         case when date_add(date_add(date(r.eligYear, r.eligMonth, 1), interval +1 month), interval -1 day) >= coalesce(cohortGoLiveDate, revenueGoLiveDate) then 1 else 0 end as isAfterGoLive,
         DATE_DIFF(date(eligYear, eligMonth, 1), coalesce(cohortGoLiveDate, revenueGoLiveDate), MONTH) as monthsAfterGoLive,
         DATE_DIFF(date(eligYear, eligMonth, 1), revenueGoLiveDate, MONTH) as monthsAfterRevenueGoLive,

   --  DOD
         ifnull(isEverDeceasedtemp, 0) as isEverDeceased,
         case when date_add(date_add(date(r.eligYear, r.eligMonth, 1), interval +1 month), interval -1 day) >= d.dod then 1 else 0 end as isAfterDeceased
   
   from result as r

   left join latest as l 
      on r.patientId = l.id and r.eligYear = l.yr and r.eligMonth = l.mo and r.fileReceivedToDate = l.max
   
   left join active as a 
      on r.fileReceivedToDate = a.maxfileReceivedToDate and r.partnerName = a.payer and r.source = a.src
   
   left join currentpayermonth as c 
      on date(r.eligYear, r.eligMonth, 1) = c.eligdt and r.partnerName = c.payer
   
   left join retro as re 
      on r.patientId = re.id and r.eligYear = re.yr and r.eligMonth = re.mo and r.fileReceivedToDate = re.min
   
   left join retro as re2 
      on r.patientId = re2.id and r.eligYear = re2.yr and r.eligMonth = re2.mo
   
   left join scenarios as s 
      on r.rowid = s.rowid
   
   left join dod as d 
      on r.patientId = d.id
   
   left join lapses as la 
      on r.patientId = la.id
   
   left join memberids as mi 
      on r.patientId = mi.id
   
   left join ooa_start as o 
      on r.patientId = o.id
),

-- out of area status of each member month, initially. 
-- assumes that the earliest reported address per member_month will be closest to the true address at that time, because payers retro changes the addresses without preserving historic addresses.
ooa1 as (
  select 
    patientId, 
    eligDate, 
    min(fileReceivedFromDate) as fileReceivedFromDate 
  from finalresult 
  group by patientId, eligDate
),

ooa2 as (
  select 
    ooa1.patientId,
    ooa1.eligDate,
    fr.isOutOfAreaCurrently as isOutOfAreaOriginally
  from finalresult fr
  inner join ooa1 
      using(patientId, eligDate, fileReceivedFromDate)
),

-- joining ooa_initial back onto the final result
finalresult_with_ooa as (
  select * 
  from finalresult
  left join ooa2 
      using(patientId, eligDate)
)

select * from finalresult_with_ooa