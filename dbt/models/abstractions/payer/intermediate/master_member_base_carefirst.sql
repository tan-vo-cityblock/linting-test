

--  carefirst member master table (silver)
--  aseiden 10/20/20
--  note: adapted code from tufts gold member master code;

--  get data dump (file) received dates, and add end dates = day prior for receivedFrome and receivedTo usage
with 

-- unlike emblem/cci we only recieve changes, not full replacement, so eff dates have to be at the memberid level not population level
filedates as (
   select
       *,
      --  imputing today's date where end date is null (the most recent file received)
       IFNULL((DATE_ADD(LEAD(eff_start) OVER(partition by MEMBERID, fromDate ORDER BY eff_start), INTERVAL -1 DAY)), CURRENT_DATE()) AS eff_end
   from (
        -- historical & daily
         select distinct
            data.MEM_ID as MEMBERID,
            data.SPAN_FROM_DATE as fromDate,
            PARSE_DATE('%Y%m%d', _table_suffix) AS eff_start
         from {{ source('carefirst_silver', 'member_month_*') }}
         where data.MEM_ID != 'MEM_ID'
        )    
),

pic as (
   select
      patientId,
      memberId as MEMBERID,
      elationId
   from {{ ref('member_index_member_management') }}   
   where memberIdSource = 'carefirst'
   and deletedAt is null
     and (cohortId > 0 or cohortId is null)
),

memmo as (
     --  member_ memmo historic + DAILY ADD/DROPS
     select
         PARSE_DATE('%Y%m%d', mm._TABLE_SUFFIX) as eff_start,
         mm.data.MEM_ID as MEMBERID,
         SPANFROMDATE,
         -- using eligenddates which can be less than 1 month span. either way, if the member is elig 1 day in that month, we get paid cap and are at risk
         case 
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) <  mm.data.SPAN_TO_DATE then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when mm.data.SPAN_TO_DATE is null then date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day)
          when date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) >=  mm.data.SPAN_TO_DATE then mm.data.SPAN_TO_DATE
          end as SPANTODATE,         
         mm.data.MEM_START_DATE as ELIGIBILITYSTARTDATE,
         mm.data.MEM_END_DATE as ELIGIBILITYENDDATE,
    -- hardcoding medicaid lob1, and using prod desc as lob2 for now
         'Medicaid' as LINEOFBUSINESS,
         mm.data.PROD_DESC as LINEOFBUSINESS2, -- jac to advise on subLOB
         mm.data.MEDICAID_PREM_GROUP as medicaidPremiumGroup,
         string(null) as MEDICALCENTERNUMBER, 
         string(null) as DELIVERYSYSTEMCODE,
         mm.data.PROD_DESC as PRODUCTDESCRIPTION, 
         mm.data.EMP_GROUP_ID as EMPLOYERGROUPID,
         mm.data.ATTRIBUTED_PCP_ID as  PROVIDERID,
         mm.data.ATTRIBUTED_PCP_LOC_NUMBER as PROVIDERLOCATIONSUFFIX,
         string(null) as NMI,
    --  member demographics file
         md.data.MEM_MEDICARE as MEM_MEDICARE, 
         md.data.MEM_LNAME as MEM_LNAME,
         md.data.MEM_FNAME as MEM_FNAME,
         md.data.MEM_DOB as MEM_DOB,
         case when md.data.MEM_DOD	< PARSE_DATE('%Y%m%d', md._TABLE_SUFFIX) then md.data.MEM_DOD else null end as MEM_DOD, 
         md.data.MEM_GENDER as MEM_GENDER,
         md.data.MEM_ADDR1 as MEM_ADDR1,
         md.data.MEM_ADDR2 as MEM_ADDR2,
         md.data.MEM_CITY as MEM_CITY,
         md.data.MEM_STATE as MEM_STATE,
         substr(md.data.MEM_ZIP,0,5) as MEM_ZIP,
         md.data.MEM_COUNTY as MEM_COUNTY,            
         md.data.MEM_PHONE_PRIMARY as MEM_PHONE,
         string(null) as careManagementOrganization, 
         p.data.PROV_NPI as PROV_NPI,
         string(null) as rateCell
     from {{ source('carefirst_silver', 'member_month_*') }} mm
     cross join unnest(generate_date_array((select date_trunc(mm.data.SPAN_FROM_DATE, month)), ifnull(mm.data.SPAN_TO_DATE, PARSE_DATE('%Y%m%d', mm._TABLE_SUFFIX)), interval 1 month)) as SPANFROMDATE  
     left join {{ source('carefirst_silver', 'member_demographics_*') }} md
        on mm.data.MEM_ID	= md.data.MEM_ID
        and mm._table_suffix = md._table_suffix
     left join {{ source('carefirst_silver', 'provider') }} p
        on mm.data.ATTRIBUTED_PCP_ID = p.data.PROV_ID
        and mm.data.ATTRIBUTED_PCP_LOC_NUMBER = p.data.PROV_GRP_ID
     -- controlling for inclusion of far off future and past dates
     -- using current month as the cutoff for daily files
     where SPANFROMDATE between '2020-10-01' and date_trunc(current_date(), month) --PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX)
     and mm.data.LOB = '400'
     and (md.data.MEM_ID != 'MEM_ID' or md.data.MEM_ID is null)
     and mm.data.MEM_ID != 'MEM_ID'
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

    
