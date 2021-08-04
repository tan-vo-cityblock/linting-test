
--  connecticare member master table (v1 silver)
--  aseiden 7/31/19
--  note: adapted code from emblem member master code;
--  several sections commented out below as not needed or different data structure in cci
-- updated 3/19/20 adding cci gold facets elig data

--  get data dump (file) received dates, and add end dates = day prior for receivedFrome and receivedTo usage
with filedates as (
       select
           *,
          --  imputing today's date where end date is null (the most recent file received)
           IFNULL((DATE_ADD(LEAD(eff_start) OVER(partition by source ORDER BY eff_start), INTERVAL -1 DAY)), CURRENT_DATE()) AS eff_end
       from (
           --  aseiden 7/31/19 note b/c there is a view called 'Member_' it prevents us from doing the full suffix
           --  workaround is to use the 2 from 2019 as part of the prefix, and then add it back in before casting to date field

           -- amysis medicare
             select distinct 
                PARSE_DATE('%Y%m%d', concat('2',_TABLE_SUFFIX)) AS eff_start,
                'amysis_med' as source
           --  grabs received dates from all member month files in emblem silver claims
             from {{ source('cci_silver', 'Member_med_2*') }}
             where patient.patientId is not null
           --  these 2 older cci files had non compatible struct 'patient'
             AND   _TABLE_SUFFIX <> '0190204'
             AND   _TABLE_SUFFIX <> '0190312'
           -- this is the last amysis_med file we want to use, pertaining to 12/19 eligibility
             AND   cast(_TABLE_SUFFIX as numeric) <= 0200211

            -- amysis commercial
            union all
            select distinct
                PARSE_DATE('%Y%m%d', concat('2',_TABLE_SUFFIX)) AS eff_start,
                'amysis' as source
           --  grabs received dates from all member month files in emblem silver claims
             from {{ source('cci_silver', 'Member_2*') }}
             where patient.patientId is not null
           --  these 2 older cci files had non compatible struct 'patient'
             AND   _TABLE_SUFFIX <> '0190204'
             AND   _TABLE_SUFFIX <> '0190312'

           -- cci facets gold
             union all 
             select distinct
                PARSE_DATE('%Y%m%d', _TABLE_SUFFIX ) AS eff_start,
                'facets' as source
             from {{ source('cci_facets', 'MemberV2_*') }}
             where memberIdentifier.patientId	is not null

           -- cci DSNP amysis silver - one-time data shipment
             union all 
             select
                PARSE_DATE('%Y%m%d', '20200526') AS eff_start,
                'amysis_dsnp' as source
            )    
),

-- aseiden 12/16/19 removed the unnecessary complexity here, shifted from PIC to member index mirror view, which is already in the format we need and already has all the fuzzy matched id's added to it
pic as (

       select
          patientId,
          memberId as MEMBERID,
          category as rateCell,
          elationId

       from {{ ref('member_index_member_management') }}   
       
       where memberIdSource = 'connecticare'
       and deletedAt is null
       and cohortId > 0

),

--  we have to append the member_ file to the member_med (medicare) file
--  union member_ memmo with member_med memmo, all available in silver claims
memmo as (

  select * from {{ ref('master_member_base_cci_amysis') }}

  union all

  select * from {{ ref('master_member_base_cci_facets') }}
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
         m.source,
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
         d.rateCell,
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
                                  ifnull(d.rateCell, 'null'),
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
 --  join to get eff_end dates
     inner join filedates as dt
       on m.eff_start = dt.eff_start and m.source = dt.source
)

select * from memmast