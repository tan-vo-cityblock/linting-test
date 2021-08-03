
--  aseiden split the master member table sql into two sections, this is the data prep section, gathering data from payers and commons and reshaping it before the second step of adding flags/logic.
--  every distinct eligibility year*month that exists in the member master table for emblem, our first partner. might as well add all partners in case we discontinue with any of them
--  every member should be joined to this, so that they have one row per member per month, even if they were not eligible that month
--  unioning the commons state table for this, since there are more recent commons states than there are member months
with months as (
   select
     year,
     month,
     --  calculating last day of each month, or if today's date is before the end of the month, using today's date instead
     case when DATE_ADD( DATE_ADD(date(year, month, 1), INTERVAL 1 MONTH), INTERVAL -1 DAY) > current_date() 
          then current_date() 
          else DATE_ADD( DATE_ADD(date(year, month, 1), INTERVAL 1 MONTH), INTERVAL -1 DAY) 
          end as lastDay
   from (
        select
            extract(year from spanFromDate) as year,
            extract(month from spanFromDate) as month
        FROM {{ ref('master_member_emblem') }}

        union distinct

        select
            extract(year from spanFromDate) as year,
            extract(month from spanFromDate) as month
        FROM {{ ref('master_member_cci') }}
      
        union distinct

        select
            extract(year from spanFromDate) as year,
            extract(month from spanFromDate) as month
        FROM {{ ref('master_member_tufts') }}    

        union distinct

        select
            extract(year from spanFromDate) as year,
            extract(month from spanFromDate) as month
        FROM {{ ref('master_member_carefirst') }} 

        union distinct

        select
            extract(year from spanFromDate) as year,
            extract(month from spanFromDate) as month
        FROM {{ ref('master_member_cardinal') }} 

        union distinct

        select
            extract(year from spanFromDate) as year,
            extract(month from spanFromDate) as month
        FROM {{ ref('master_member_healthy_blue') }} 

        union distinct

        select
            extract(year from spanToDate) as year,
            extract(month from spanToDate) as month
        FROM {{ ref('master_member_patient_state') }}
       )
),

src_member as (

  select
    patientId,
    cohortName,
    cohortGoLiveDate,
    partnerName

  from {{ ref('src_member') }}

),

--  get list of all patientIds actively in the member service. excludes soft-deletes, negative cohorts.
members as (
   select distinct patientId as id
   from src_member
),

--  crossjoin to get one row per member per every possible  month in data
crossjoin as (
  select me.id, mo.year, mo.month, mo.lastDay from months mo
  cross join members me
),

--  get each member's currentState as of the last day of each month
--  per theresa we decided to use last day of month, however we can change this later. would have to make the update in 'months' above
eomstate as (
  select
     c.year,
     c.month,
     s.patientId,
     s.commonsState,
     s.commonsStatePriorToDisenrollment,
     s.disenrollmentReason,
     s.disenrollmentNote,
     s.spanFromDate as commonsStateStartDate
  from crossjoin as c
  left join {{source('member_index', 'member')}} as m
    on c.id = m.id
  left join {{ ref('master_member_patient_state') }} as s
      on c.id = s.patientId and
     -- for cci the key commonsState is as of the 15th of the month, not the last. other partners use EOM state.
      case 
        when m.partnerId = 2 
          then date(c.year,c.month,15) between s.spanFromDate and s.spanToDate
        --  and date(c.year,c.month,1) = date_trunc(s.spanFromDate, month) 
        --  and s.spanFromDate >= date(c.year,c.month,15)
        --   then s.spanFromDate between date(c.year,c.month,15) and date_sub(date_add(date(c.year,c.month,1), interval 1 month), interval 1 day)    
        -- when m.partnerId = 2 
        --  and date(c.year,c.month,1) = date_trunc(s.spanFromDate, month) 
        --  and s.spanFromDate < date(c.year,c.month,15)
        --   then s.spanFromDate between date_sub(date(c.year,c.month,15), interval 1 month) and date(c.year,c.month,14)   
          else c.lastDay between s.spanFromDate and s.spanToDate end
),

--  flagging pt-yr-mo if newly disenrolled, enrolled, or consented compared to the prior month, per theresa c; will join back in at the end
newstate as (
   select year as yr,
          month as mo,
          patientId as id,
          case when commonsState = 'disenrolled' and (lag(commonsState, 1) OVER (PARTITION BY patientId ORDER BY patientId, year, month ASC)) != 'disenrolled' then 1 else 0 end as isNewlyDisenrolled,
          case when commonsState = 'enrolled' and (lag(commonsState, 1) OVER (PARTITION BY patientId ORDER BY patientId, year, month ASC)) != 'enrolled' then 1 else 0 end as isNewlyEnrolled,
          case when commonsState = 'consented' and (lag(commonsState, 1) OVER (PARTITION BY patientId ORDER BY patientId, year, month ASC)) != 'consented' then 1 else 0 end as isNewlyConsented
   from eomstate
),

--  result table that has for each member month, the commons state as of the last day of the month
--  but for the current month (max month in the data), it has today's state, since last day of mo hasn't happened yet
--  because the spanToDate of the patient_state table we created has the date = current_date (must be run nightly) if the commons.patient_state.currentState deletedAt is null
result as (
  select distinct
     c.id as patientId,
     c.year as eligYear,
     c.month as eligMonth,
     mmt.memberId,
     mmt.NMI,
     mmt.medicareId,
     cp.medicaidId,
     pic.acpnyId as acpnyMRN,
     pic.elationId,
     fileReceivedFromDate,
     fileReceivedToDate,
     mmt.source,
     spanFromDate,
     spanToDate,
     eligStartDate,
     eligEndDate,
     mmt.lastName,
     mmt.firstName,
     mmt.dateOfBirth,
     dateOfDeath,
     mmt.gender,
     mmt.lineOfBusiness1,
     mmt.lineOfBusiness2,
     -- recoding LOB's into a clean version
     case when mmt.productDescription = 'Dual Eligible SNP - HMO' or mmt.lineOfBusiness1 = 'DSNP' then 'DSNP'
          when lower(mmt.lineOfBusiness1) = 'duals' then 'Duals'
          when mmt.lineOfBusiness1 in ('M', 'MR', 'Medicare', 'medicare') then 'Medicare Advantage'
          when (mmt.lineOfBusiness1 like '%FFS%' or mmt.lineOfBusiness1 like '%ffs%') and (mmt.lineOfBusiness1 like '%medicaid%' or mmt.lineOfBusiness1 like '%Medicaid%') then 'Medicaid FFS'                    
          when mmt.lineOfBusiness1 in ('MD', 'md', 'Medicaid', 'medicaid') and mmt.productDescription = 'HARP (Health and Recovery Plans)' then 'Medicaid HARP'          
          when mmt.lineOfBusiness1 in ('MD', 'md', 'Medicaid', 'medicaid') and (mmt.productDescription != 'HARP (Health and Recovery Plans)' or mmt.productDescription is null) then 'Medicaid (Non HARP)'
          when mmt.lineOfBusiness1 in ('C', 'HM', 'PS', 'ps', 'hm') then 'Commercial'
          else 'Not Yet Labeled' end as lineOfBusinessGrouped,
     mmt.medicaidPremiumGroup,
     mmt.productDescription,
     employerGroupId,
     medicalCenterNumber,
     deliverySystemCode,
     providerId,
     providerLocationSuffix,
     providerNPI,
     homeHealthStatus,
     healthHomeName,
     careManagementOrganization,
     e.commonsState,
     e.commonsStatePriorToDisenrollment,
     e.disenrollmentReason,
     e.disenrollmentNote,
     e.commonsStateStartDate,
     ns.isNewlyDisenrolled,
     ns.isNewlyEnrolled,
     ns.isNewlyConsented,
     upper(coalesce(mmt.rateCell, pic.category)) as rateCell,
     -- overwriting cohort name and id for emblem medicaid digital members who moved into a new cohort
     case 
      when coh8.patientId is not null and date(c.year, c.month, 1) between '2020-09-01' and '2020-12-31' then 'Emblem Medicaid Digital Cohort 1'
      when coh9.patientId is not null and date(c.year, c.month, 1) between '2020-09-01' and '2021-04-30' then 'Emblem Medicaid Digital Cohort 1'
      else coalesce(m.cohortName, pic.cohortName) end as cohortName,
     case
      when coh8.patientId is not null and date(c.year, c.month, 1) between '2020-09-01' and '2020-12-31' then 21
      when coh9.patientId is not null and date(c.year, c.month, 1) between '2020-09-01' and '2021-04-30' then 21
      else pic.cohortId end as cohortId,
     coalesce(m.cohortGoLiveDate, pic.cohortGoLiveDate) as cohortGoLiveDate,
     pic.revenueGoLiveDate,
     mmc.patientHomeMarketName as marketName,
     mmc.patientHomeClinicName as clinicName,
     case 
      when coalesce(m.partnerName, pic.partnerName) = 'emblem' then 'Emblem Health'
      else coalesce(m.partnerName, pic.partnerName) end as partnerName,
     cp.doNotCall,
     address1,
     address2,
     city,
     state,
     zip,
     county,
     phone
  --  start with all possible member months
  from crossjoin as c
  --  join with emblem and cci eligibility master tables
  left join (
       --  subquery to union the payer master member tables
              select * from {{ ref('master_member_emblem') }}
              union all
              select * from {{ ref('master_member_cci') }}
              union all
              select * from {{ ref('master_member_tufts') }}
              union all
              select * from {{ ref('master_member_carefirst') }}
              union all
              select * from {{ ref('master_member_cardinal') }}
              union all 
              select * from {{ ref('master_member_healthy_blue') }}
            ) as mmt
    on c.id = mmt.patientId
     and c.year = extract(year from mmt.spanFromDate)
     and c.month = extract(month from mmt.spanFromDate)  
  --  join with eomostate for the max month only
  left outer join eomstate as e
    on c.id = e.patientId
    and c.year = e.year
    and c.month = e.month
  --  join with commons cohort info from the member view (up to date, no historic versions, unchanging info) to all rows, not just payer rows
  --  excluding the n=67 people in the PIC and the payer files who are NOT in the member table, e.g. cohort = -2 folks who were inadvertently added as members
  left join src_member as m -- removed inner join to allow emblem digital members to flow through even though they are not in commons yet
    on c.id = m.patientId
  left join {{ ref('abs_commons_member_market_clinic') }} as mmc
    on c.id = mmc.patientId
  left join {{ source('commons', 'patient') }} as cp
    on c.id = cp.id
  -- joining to get rate cell from patient index cache on every membermonth row; rate cell is static for emblem and cci in the PIC, but not in the commons cohort data above
  -- 11/12/19 replaced with ref to member index view
  left outer join {{ ref('member_index_member_management') }} as pic on c.id = pic.patientId
  -- joining in newly changed state flags
  left outer join newstate as ns on c.id = ns.id and c.year = ns.yr and c.month = ns.mo
  -- emblem medicaid digital cohort serves as a pool of members to be moved into real cohorts later
  left join {{ source('member_management', 'cohort8_from_digital_patientId_list') }} as coh8 
    on c.id = coh8.patientId
  left join {{ source('member_management', 'cohort9_from_digital_patientId_list') }} as coh9 
    on c.id = coh9.patientId
)

select * from result