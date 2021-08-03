

--  cardinal member master table (silver)
--  aseiden 5/19/21
--  note: adapted code from cardinal silver member master code;

with 

-- unlike emblem/cci we only recieve changes, not full replacement, so eff dates have to be at the memberid level not population level
filedates as (
   select
       *,
      --  imputing today's date where end date is null (the most recent file received)
       IFNULL((DATE_ADD(LEAD(eff_start) OVER(partition by MEMBERID, fromDate ORDER BY eff_start), INTERVAL -1 DAY)), CURRENT_DATE()) AS eff_end
   from (
         select distinct
            data.MemberID as MEMBERID,
            data.EnrollmentDate as fromDate,
            PARSE_DATE('%Y%m%d', _table_suffix) AS eff_start
         from {{ source('cardinal_silver', 'Member_*') }}
        )    
),

pic as (
   select
      patientId,
      memberId as MEMBERID,
      elationId
   from {{ ref('member_index_member_management') }}   
   where memberIdSource = 'cardinal'
   and deletedAt is null
     and (cohortId > 0 or cohortId is null)
),

memmo as (
     --  member_ memmo historic + weekly ADD/DROPS
     select
         PARSE_DATE('%Y%m%d', mm._TABLE_SUFFIX) as eff_start,
         mm.data.MemberID as MEMBERID,
         SPANFROMDATE,
         -- using eligenddates which can be less than 1 month span. either way, if the member is elig 1 day in that month, we get paid cap and are at risk
         case 
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) <  mm.data.TerminationDate then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when mm.data.TerminationDate is null then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) >=  mm.data.TerminationDate then mm.data.TerminationDate
          end as SPANTODATE,         
         mm.data.EnrollmentDate as ELIGIBILITYSTARTDATE,
         mm.data.TerminationDate as ELIGIBILITYENDDATE,
    -- hardcoding medicaid lob1, and using prod desc as lob2 for now
         mm.data.LineOfBusiness as LINEOFBUSINESS,
         mm.data.SubLineOfBusiness as LINEOFBUSINESS2,
         mm.data.MedicaidPremiumGroup as medicaidPremiumGroup,
         string(null) as MEDICALCENTERNUMBER, 
         string(null) as DELIVERYSYSTEMCODE,
         string(null) as PRODUCTDESCRIPTION, 
         string(null) as EMPLOYERGROUPID,
         string(null) as  PROVIDERID,
         string(null) as PROVIDERLOCATIONSUFFIX,
         string(null) as NMI,
    --  member demographics file
         mm.data.MBI as MEM_MEDICARE, 
         mm.data.LastName as MEM_LNAME,
         mm.data.FirstName as MEM_FNAME,
         mm.data.DOB as MEM_DOB,
         case when mm.data.DOD	< PARSE_DATE('%Y%m%d', mm._TABLE_SUFFIX) then mm.data.DOD else null end as MEM_DOD, 
         substr(mm.data.Gender,1,1) as MEM_GENDER,
         mm.data.AddressLine1 as MEM_ADDR1,
         mm.data.AddressLine2 as MEM_ADDR2,
         mm.data.City as MEM_CITY,
         mm.data.State as MEM_STATE,
         substr(mm.data.PostalCode,0,5) as MEM_ZIP,
         mm.data.County as MEM_COUNTY,            
         mm.data.PrimaryPhone as MEM_PHONE,
         string(null) as careManagementOrganization, 
         mm.data.AttributedProviderNPI as PROV_NPI,
         string(null) as rateCell
     from {{ source('cardinal_silver', 'Member_*') }} mm
     cross join unnest(generate_date_array((select date_trunc(mm.data.EnrollmentDate, month)), ifnull(mm.data.TerminationDate, PARSE_DATE('%Y%m%d', mm._TABLE_SUFFIX)), interval 1 month)) as SPANFROMDATE  
     -- controlling for inclusion of far off future dates
     -- using current month as the cutoff for daily files
     where SPANFROMDATE < date_trunc(current_date(), month) 
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

    
