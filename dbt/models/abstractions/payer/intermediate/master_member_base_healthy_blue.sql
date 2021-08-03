
--  healthy blue member master table (silver)
--  aseiden 6/25/21
--  note: adapted code from tufts gold member master code;

with  

-- we receive weekly full files and daily incremental files. for simplicity sake, for MMT going to only take the weekly files. will determine this based on the file size.
filedates as (
   select
       *,
      --  imputing today's date where end date is null (the most recent file received)
       IFNULL((DATE_ADD(LEAD(eff_start) OVER(partition by MEMBERID, fromDate ORDER BY eff_start), INTERVAL -1 DAY)), CURRENT_DATE()) AS eff_end
   from (
         -- any shard with 10k+ rows in it is assumed to be a "full" membership file not a daily incremental
         with weeklys as (
          select
            _TABLE_SUFFIX as file_date
          from {{ source('healthy_blue_silver', 'member_*') }}
          group by 1
          having count(*) >= 10000
         )
         
         select distinct
            patient.externalid as MEMBERID,
            data.Enrollment_Start_Date as fromDate,
            PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start
         from {{ source('healthy_blue_silver', 'member_*') }} m
         inner join weeklys w on m._TABLE_SUFFIX = w.file_date
          )    
),

pic as (
   select
      patientId,
      memberId as MEMBERID,
      elationId
   from {{ ref('member_index_member_management') }}   
   where memberIdSource = 'bcbsNC'
   and deletedAt is null
   and cohortId > 0 or cohortId is null
),

memmo as (
     select
         PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX) as eff_start,
         patient.externalId as MEMBERID,
         SPANFROMDATE,
         -- using eligenddates which can be less than 1 month span. either way, if the member is elig 1 day in that month, we get paid cap and are at risk
         case 
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) <  data.Enrollment_End_Date then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when data.Enrollment_End_Date is null then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) >=  data.Enrollment_End_Date then data.Enrollment_End_Date
         end as SPANTODATE,         
         data.Enrollment_Start_Date as ELIGIBILITYSTARTDATE,
         data.Enrollment_End_Date as ELIGIBILITYENDDATE,
 -- rewriting lob and lob2 as dsnp where appropriate as these have risk implications
         'Medicaid' as LINEOFBUSINESS,
         'Healthy Blue' as LINEOFBUSINESS2,
         cast(null as string) as medicaidPremiumGroup,
         string(null) as MEDICALCENTERNUMBER, 
         string(null) as DELIVERYSYSTEMCODE, 
         string(null) as PRODUCTDESCRIPTION, 
         string(null) as EMPLOYERGROUPID,
         data.PCP_Identification_Code as  PROVIDERID, -- seems like cityblock is the pcp on all these
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
         string(null) as NMI,
         string(null) as MEM_MEDICARE, 
         upper(data.Member_Last_Name) as MEM_LNAME,
         upper(data.Member_First_Name) as MEM_FNAME,
         data.Member_DOB as MEM_DOB,
         data.Member_DOD as MEM_DOD, 
         substr(data.Member_Gender_Desc,1,1) as MEM_GENDER,
         upper(data.Member_Address_Line1) as MEM_ADDR1,
         upper(data.Member_Address_Line2) as MEM_ADDR2,
         upper(data.Member_City_Name) as MEM_CITY,
         upper(data.Member_State_Code) as MEM_STATE,
         substr(data.Member_ZIP_Code,0,5) as MEM_ZIP,
         data.Member_County_Code as MEM_COUNTY,            
         data.Member_Phone_Number as MEM_PHONE,
         string(null) as careManagementOrganization, 
         data.PCP_Identification_Code as PROV_NPI,
         data.Program_Category_Code as rateCell
     from {{ source('healthy_blue_silver', 'member_*') }} gc,

     -- unnesting to convert from spans to mm's
     unnest(generate_date_array((select date_trunc(data.Enrollment_Start_Date, month)), ifnull(data.Enrollment_End_Date, PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX)), interval 1 month)) as SPANFROMDATE
     
     -- limit to the weekly files; the ones with 10k+ rows
     inner join (
                  select
                    _TABLE_SUFFIX as file_date
                  from {{ source('healthy_blue_silver', 'member_*') }}
                  group by 1
                  having count(*) >= 10000
                ) w on gc._TABLE_SUFFIX = w.file_date

     -- controlling for inclusion of far off future and past dates
     -- using current month as the cutoff for weekly files
     where SPANFROMDATE >= '2016-01-01' and SPANFROMDATE <= date_trunc(current_date(), month) --PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX)

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

    
