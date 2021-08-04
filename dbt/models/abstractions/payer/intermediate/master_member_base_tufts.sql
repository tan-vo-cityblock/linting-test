
--  tufts member master table (gold)
--  aseiden 2/27/20
--  note: adapted code from cci silver member master code;

--  get data dump (file) received dates, and add end dates = day prior for receivedFrome and receivedTo usage
with 

-- unlike emblem/cci we only recieve changes, not full replacement, so eff dates have to be at the memberid level not population level
filedates as (
   select
       *,
      --  imputing today's date where end date is null (the most recent file received)
       IFNULL((DATE_ADD(LEAD(eff_start) OVER(partition by MEMBERID, fromDate ORDER BY eff_start), INTERVAL -1 DAY)), CURRENT_DATE()) AS eff_end
   from (
        -- historical
         select distinct
            memberIdentifier.partnerMemberId as MEMBERID,
            dateEffective.from as fromDate,
            PARSE_DATE('%Y%m%d', '20200213') AS eff_start
         from {{ source('tufts', 'Member_20200213') }}

         union all

        -- daily add/drop
         select distinct
            memberIdentifier.partnerMemberId as MEMBERID,
            dateEffective.from as fromDate,
            PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start
         from {{ source('tufts', 'Member_Daily_*') }}
         where _TABLE_SUFFIX != '20210329'  -- tufts erroneously terminated most of our members and did not send a corrected daily file
        )    
),

pic as (
   select
      patientId,
      memberId as MEMBERID,
      elationId
   from {{ ref('member_index_member_management') }}   
   where memberIdSource = 'tufts'
   and deletedAt is null
   and (cohortId > 0 or cohortId is null)

),

memmo as (
   --  member_ memmo HISTORICAL
     select
         PARSE_DATE('%Y%m%d', '20200213') as eff_start,
         memberIdentifier.partnerMemberId as MEMBERID,
         SPANFROMDATE,
         -- using eligenddates which can be less than 1 month span. either way, if the member is elig 1 day in that month, we get paid cap and are at risk
         case 
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) <  dateEffective.to then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when dateEffective.to is null then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) >=  dateEffective.to then dateEffective.to
         end as SPANTODATE,
         dateEffective.from as ELIGIBILITYSTARTDATE,
         --  if there is no dateTo, using 2/28/20 because these are all cohort 1 people who went live on 3/1/20 and would have a member daily record in the 2/28 shard if they were still eligible
         coalesce(dateEffective.to, PARSE_DATE('%Y%m%d', '20200228'))  as ELIGIBILITYENDDATE,
    -- rewriting lob and lob2 as dsnp where appropriate as these have risk implications
         case when lower(eligibility.subLineOfBusiness) like '%unify%' or lower(eligibility.subLineOfBusiness) = 'dsnp' then 'Duals' else eligibility.lineOfBusiness end as LINEOFBUSINESS,
         case when lower(eligibility.subLineOfBusiness) like '%unify%' or lower(eligibility.subLineOfBusiness) = 'dsnp' then 'Duals' else eligibility.subLineOfBusiness end as LINEOFBUSINESS2,         
         cast(null as string) as medicaidPremiumGroup,
    --  dont receive these fields from tufts
         string(null) as MEDICALCENTERNUMBER, 
         string(null) as DELIVERYSYSTEMCODE, 
         string(null) as PRODUCTDESCRIPTION, 
         eligibility.partnerEmployerGroupId as EMPLOYERGROUPID,
         pcp.id as  PROVIDERID,
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
         string(null) as NMI,
    --  dont receive these fields from tufts
         string(null) as MEM_MEDICARE, 
         upper(demographic.name.last) as MEM_LNAME,
         upper(demographic.name.first) as MEM_FNAME,
         demographic.dateBirth as MEM_DOB,
         demographic.dateDeath as MEM_DOD, 
         demographic.gender as MEM_GENDER,
         upper(location.address1) as MEM_ADDR1,
         upper(location.address2) as MEM_ADDR2,
         upper(location.city) as MEM_CITY,
         upper(location.state) as MEM_STATE,
         location.zip as MEM_ZIP,
         upper(location.county) as MEM_COUNTY,            
         location.phone as MEM_PHONE,
         string(null) as careManagementOrganization, 
         string(null) as PROV_NPI,
         REGEXP_REPLACE(upper(coalesce(sc.data.GroupID, gc.eligibility.partnerBenefitPlanId)), 'UNIFY', '') as rateCell
     from {{ source('tufts', 'Member_20200213') }} gc,     
     unnest(generate_date_array((select date_trunc(dateEffective.from, month)), ifnull(dateEffective.to, PARSE_DATE('%Y%m%d', '20200213') ), interval 1 month)) as SPANFROMDATE
     -- joining silver claims to get rate cell for that row // tbd if this is ok given that we transform the rows from silver to gold?? talk to diana
     left outer join {{ source('tufts_silver', 'Member_20200213') }} sc on gc.surrogate.id = sc.identifier.surrogateId
     -- controlling for tufts inclusion of far off future and past dates. tufts data ranges from 2013-10-01 to fake future dates
     -- using received date as the cutoff - can't reliably predict future eligibility
     where SPANFROMDATE >= '2016-01-01' and SPANFROMDATE <= PARSE_DATE('%Y%m%d', '20200213')

     union all

     --  member_ memmo DAILY ADD/DROPS
     select
         PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX) as eff_start,
         memberIdentifier.partnerMemberId as MEMBERID,
         SPANFROMDATE,
         -- using eligenddates which can be less than 1 month span. either way, if the member is elig 1 day in that month, we get paid cap and are at risk
         case 
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) <  dateEffective.to then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when dateEffective.to is null then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) >=  dateEffective.to then dateEffective.to
         end as SPANTODATE,         
         dateEffective.from as ELIGIBILITYSTARTDATE,
         dateEffective.to as ELIGIBILITYENDDATE,
 -- rewriting lob and lob2 as dsnp where appropriate as these have risk implications
         case when lower(eligibility.subLineOfBusiness) like '%unify%' or lower(eligibility.subLineOfBusiness) = 'dsnp' then 'Duals' else eligibility.lineOfBusiness end as LINEOFBUSINESS,
         case when lower(eligibility.subLineOfBusiness) like '%unify%' or lower(eligibility.subLineOfBusiness) = 'dsnp' then 'Duals' else eligibility.subLineOfBusiness end as LINEOFBUSINESS2,
         cast(null as string) as medicaidPremiumGroup,
    --  dont receive these fields from tufts
         string(null) as MEDICALCENTERNUMBER, 
         string(null) as DELIVERYSYSTEMCODE, 
         string(null) as PRODUCTDESCRIPTION, 
         eligibility.partnerEmployerGroupId as EMPLOYERGROUPID,
         pcp.id as  PROVIDERID,
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
         string(null) as NMI,
    --  dont receive these fields from tufts
         string(null) as MEM_MEDICARE, 
         upper(demographic.name.last) as MEM_LNAME,
         upper(demographic.name.first) as MEM_FNAME,
         demographic.dateBirth as MEM_DOB,
         demographic.dateDeath as MEM_DOD, 
         demographic.gender as MEM_GENDER,
         upper(location.address1) as MEM_ADDR1,
         upper(location.address2) as MEM_ADDR2,
         upper(location.city) as MEM_CITY,
         upper(location.state) as MEM_STATE,
         case when length(location.zip) = 4 then concat('0', location.zip) else location.zip end as MEM_ZIP,
         upper(location.county) as MEM_COUNTY,            
         location.phone as MEM_PHONE,
         string(null) as careManagementOrganization, 
         string(null) as PROV_NPI,
         REGEXP_REPLACE(upper(coalesce(sc.data.GroupID, gc.eligibility.partnerBenefitPlanId)), 'UNIFY', '') as rateCell
     from {{ source('tufts', 'Member_Daily_*') }} gc,
     unnest(generate_date_array((select date_trunc(dateEffective.from, month)), ifnull(dateEffective.to, PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX)), interval 1 month)) as SPANFROMDATE
     -- joining silver claims to get rate cell for that row // tbd if this is ok given that we transform the rows from silver to gold?? talk to diana
     left outer join {{ source('tufts_silver', 'Member_Daily_*') }} sc on gc.surrogate.id = sc.identifier.surrogateId
     -- controlling for tufts inclusion of far off future and past dates. tufts data ranges from 2013-10-01 to fake future dates
     -- using current month as the cutoff for daily files
     where SPANFROMDATE >= '2016-01-01' and SPANFROMDATE <= date_trunc(current_date(), month) --PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX)
       and gc._TABLE_SUFFIX != '20210329' -- tufts erroneously terminated most of our members and did not send a corrected daily file
),

-- create member master table by joining demogs to member months on
-- and eff-start dates of the demog files linked to the dates of the memmo files
-- this will be a large concatenation of all member month files, linked to demogs and supplemented with fuzzy and ooa records
-- and next step will reduce size with islands logic
-- following DSA naming conventions, renaming many of these fields
memmast as (
   --  full list of fields
     select distinct
       --  ID NUMBERS
         d.patientId,
         m.MEMBERID as memberId,
         m.NMI,
         m.MEM_MEDICARE as medicareId,
         d.elationId,
       --  DATES
         m.eff_start as  effectiveFromDate,
         dt.eff_end as effectiveToDate,
         m.SPANFROMDATE as spanFromDate,
         m.SPANTODATE as  spanToDate,
         m.ELIGIBILITYSTARTDATE as  eligStartDate,
         m.ELIGIBILITYENDDATE as  eligEndDate,
       --  DEMOGRAPHICS
         m.MEM_LNAME as lastName,
         m.MEM_FNAME as firstName,
         m.MEM_DOB as dateOfBirth,
         m.MEM_DOD as dateOfDeath,
         m.MEM_GENDER as gender,
       --  INSURANCE INFO
         m.LINEOFBUSINESS as lineOfBusiness1,
         m.LINEOFBUSINESS2 as lineOfBusiness2,
         m.medicaidPremiumGroup,
         m.PRODUCTDESCRIPTION as productDescription,
         m.EMPLOYERGROUPID as employerGroupId,
         m.PROVIDERID as providerId,
         m.PROVIDERLOCATIONSUFFIX as providerLocationSuffix,
         m.PROV_NPI as providerNPI,
         m.MEDICALCENTERNUMBER as medicalCenterNumber,
         m.DELIVERYSYSTEMCODE as deliverySystemCode,
       --  placeholders
         STRING(NULL) as homeHealthStatus,
         STRING(NULL) as healthHomeName,
         m.careManagementOrganization,
         STRING(NULL) as atRisk,
         STRING(NULL) as fullyDelegated,
       --  CBH FIELDS
         m.rateCell,
       --  CONTACT INFO
         m.MEM_ADDR1 as address1,
         NULLIF(m.MEM_ADDR2, 'N/A') as address2,
         m.MEM_CITY as city,
         m.MEM_STATE as state,
         m.MEM_ZIP as zip,
         m.MEM_COUNTY as  county,            
         m.MEM_PHONE as phone,
         ROW_NUMBER() OVER(ORDER BY d.patientId, m.SPANFROMDATE, m.eff_start, m.MEMBERID) AS RowNum,
         -- creating a fingerprint/hash off a concat of all fields except effective ("file") dates and null placeholder fields
         FARM_FINGERPRINT(CONCAT( d.patientId, 
                                  ifnull(m.MEMBERID,'null'),
                                  ifnull(m.NMI,'null'),
                                  ifnull(m.MEM_MEDICARE,'null'),
                                  ifnull(d.elationId,'null'),
                                  ifnull(cast(m.SPANFROMDATE as string), 'null'),
                                  ifnull(cast(m.SPANTODATE as string),'null'),
                                  ifnull(m.MEM_LNAME,'null'),
                                  ifnull(m.MEM_FNAME,'null'),
                                  ifnull(cast(m.MEM_DOB as string), 'null'),
                                  ifnull(cast(m.MEM_DOD as string), 'null'),
                                  ifnull(m.MEM_GENDER,'null'),
                                  ifnull(m.LINEOFBUSINESS,'null'),
                                  ifnull(m.LINEOFBUSINESS2, 'null'),
                                  ifnull(m.medicaidPremiumGroup, 'null'),
                                  ifnull(m.PRODUCTDESCRIPTION, 'null'),
                                  ifnull(m.EMPLOYERGROUPID,'null'),
                                  ifnull(m.PROVIDERID, 'null'),
                                  ifnull(m.PROVIDERLOCATIONSUFFIX,'null'),
                                  ifnull(m.PROV_NPI, 'null'),
                                  ifnull(m.MEDICALCENTERNUMBER, 'null'),
                                  ifnull(m.DELIVERYSYSTEMCODE, 'null'),
                                  -- placeholder for currently empty fields, they will be empty for every row for now
                                  ifnull(m.careManagementOrganization, 'null'),
                                  ifnull(m.rateCell, 'null'),
                                  ifnull(m.MEM_ADDR1,'null'),
                                  ifnull(m.MEM_ADDR2, 'N/A'),
                                  ifnull(m.MEM_CITY,'null'),
                                  ifnull(m.MEM_STATE,'null'),
                                  ifnull(m.MEM_ZIP,'null'),
                                  ifnull(m.MEM_COUNTY, 'null'),          
                                  ifnull(m.MEM_PHONE,'null')
                              )) AS RowHash     
     from memmo as m
   --  join with pic/cohort
     inner join pic as d
       on m.MEMBERID = d.MEMBERID
 --  join to get eff_end dates - these are specific to each time a member span shows up in a daily file, so joining on memberid, eligstart, and filedate to get the eff_end, which corresponds to the next time that member span shows up with a change made to it
     inner join filedates as dt
       on m.eff_start = dt.eff_start
      and m.ELIGIBILITYSTARTDATE = dt.fromDate
      and m.MEMBERID = dt.MEMBERID
)

select * from memmast

    
