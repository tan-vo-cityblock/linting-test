
with pic as (

       select
          memberId as MEMBERID

       from {{ ref('member_index_member_management') }}   
       
       where memberIdSource = 'connecticare'
       and deletedAt is null
       and cohortId > 0
),

memmo as (

   --  member_ memmo
     select
         PARSE_DATE('%Y%m%d', concat('2',_TABLE_SUFFIX)) as eff_start,
         patient.externalId as MEMBERID,
     --  dont receive these fields from cci
         CAST(null as date) as ELIGIBILITYSTARTDATE,
         CAST(null as date) as ELIGIBILITYENDDATE,
         member.LOB1 as LINEOFBUSINESS,
    --  added this field as null to emblem 
         member.LOB2 as LINEOFBUSINESS2,
         cast(null as string) as medicaidPremiumGroup,
    --  dont receive these fields from cci
         STRING(NULL) as MEDICALCENTERNUMBER, 
         STRING(NULL) as DELIVERYSYSTEMCODE, 
         STRING(NULL) as PRODUCTDESCRIPTION, 
         member.GrpNum as EMPLOYERGROUPID,
         member.PCP_Num as  PROVIDERID,
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
    --  for CCI (unlike emblem), memmo file contains all demogs, so adding all demogs here
         member.NMI as NMI,
    --  dont receive these fields from cci
         null as MEM_MEDICARE, 
         upper(member.Last_Name) as MEM_LNAME,
         upper(member.First_Name) as MEM_FNAME,
         member.DateBirth as MEM_DOB,
    --  dont receive these fields from cci  
         CAST(null as date) as MEM_DOD, 
         member.Gender as MEM_GENDER,
         upper(member.Address1) as MEM_ADDR1,
         upper(member.Address2) as MEM_ADDR2,
         upper(member.City) as MEM_CITY,
         upper(member.State) as MEM_STATE,
         member.Zip as MEM_ZIP,
         upper(member.County) as MEM_COUNTY,            
         member.Phone1 as MEM_PHONE,
   --  cci has  this emblem does not. including for now for pavel
         member.Partnership_Current as careManagementOrganization, 
         member.PCP_NPI as PROV_NPI,
         'amysis' as source,
         member.ApplyMo AS SPANFROMDATE,
         DATE_ADD(DATE_ADD(member.ApplyMo, INTERVAL 1 MONTH), INTERVAL -1 DAY) AS SPANTODATE
     from {{ source('cci_silver', 'Member_2*') }} m
   -- subsetting to externalids (memberids) that live in the member service, to reduce the size of this book of business table!
     inner join pic on m.patient.externalId = pic.MEMBERID
   --  excluding 2 known erroneous feeds
     where  _TABLE_SUFFIX not in ('0190204', '0190312')  
     
     union all 

   --  member_med memmo 
     select
         PARSE_DATE('%Y%m%d', concat('2',_TABLE_SUFFIX)) as eff_start,
         patient.externalId as MEMBERID,
         CAST(null as date) as ELIGIBILITYSTARTDATE,
         CAST(null as date) as ELIGIBILITYENDDATE, 
         member.LOB1 as LINEOFBUSINESS,
         member.LOB2 as LINEOFBUSINESS2,
         cast(null as string) as medicaidPremiumGroup,
         STRING(NULL) as MEDICALCENTERNUMBER, 
         STRING(NULL) as DELIVERYSYSTEMCODE, 
         STRING(NULL) as PRODUCTDESCRIPTION,  
         member.GrpNum as EMPLOYERGROUPID,
         member.PCP_Num as PROVIDERID,
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
    --  for CCI (unlike emblem), memmo file contains all demogs, so adding all demogs here
         member.NMI as NMI,
         STRING(NULL) as MEM_MEDICARE,
         upper(member.Last_Name) as MEM_LNAME,
         upper(member.First_Name) as MEM_FNAME,
         member.DateBirth as MEM_DOB,
         CAST(null as date) as MEM_DOD, 
         member.Gender as MEM_GENDER,
         upper(member.Address1) as MEM_ADDR1,
         upper(member.Address2) as MEM_ADDR2,
         upper(member.City) as MEM_CITY,
         upper(member.State) as MEM_STATE,
         member.Zip as MEM_ZIP,
         upper(member.County) as MEM_COUNTY,            
         member.Phone1 as MEM_PHONE,
         member.Partnership_Current as careManagementOrganization,
         member.PCP_NPI as PROV_NPI,
        'amysis_med' as source,
         member.ApplyMo AS SPANFROMDATE,
         DATE_ADD(DATE_ADD(member.ApplyMo, INTERVAL 1 MONTH), INTERVAL -1 DAY) AS SPANTODATE
     from {{ source('cci_silver', 'Member_med_2*') }} m
   -- subsetting to externalids (memberids) that live in the member service, to reduce the size of this book of business table!
     inner join pic on m.patient.externalId = pic.MEMBERID
   --  excluding 2 known erroneous feeds
     where  _TABLE_SUFFIX not in ('0190204', '0190312')  AND  cast(_TABLE_SUFFIX as numeric) <= 0200211

     union all 

    --  amysis dsnp member_med memmo 20200526 shipment
      select
         PARSE_DATE('%Y%m%d', '20200526') as eff_start,
         patient.externalId as MEMBERID,
         CAST(null as date) as ELIGIBILITYSTARTDATE,
         CAST(null as date) as ELIGIBILITYENDDATE, 
         -- hard-coding dsnp as lob because these were imported incorrectly as medicare
         'DSNP' as LINEOFBUSINESS, 
         STRING(NULL) as LINEOFBUSINESS2,
         cast(null as string) as medicaidPremiumGroup,
         STRING(NULL) as MEDICALCENTERNUMBER, 
         STRING(NULL) as DELIVERYSYSTEMCODE, 
         STRING(NULL) as PRODUCTDESCRIPTION,  
         member.GrpNum as EMPLOYERGROUPID,
         member.PCP_Num as PROVIDERID,
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
    --  for CCI (unlike emblem), memmo file contains all demogs, so adding all demogs here
         member.NMI as NMI,
         STRING(NULL) as MEM_MEDICARE,
         upper(member.Last_Name) as MEM_LNAME,
         upper(member.First_Name) as MEM_FNAME,
         member.DateBirth as MEM_DOB,
         CAST(null as date) as MEM_DOD, 
         member.Gender as MEM_GENDER,
         upper(member.Address1) as MEM_ADDR1,
         upper(member.Address2) as MEM_ADDR2,
         upper(member.City) as MEM_CITY,
         upper(member.State) as MEM_STATE,
         member.Zip as MEM_ZIP,
         upper(member.County) as MEM_COUNTY,            
         member.Phone1 as MEM_PHONE,
         member.Partnership_Current as careManagementOrganization,
         member.PCP_NPI as PROV_NPI,
        'amysis_dsnp' as source,
         member.ApplyMo AS SPANFROMDATE,
         DATE_ADD(DATE_ADD(member.ApplyMo, INTERVAL 1 MONTH), INTERVAL -1 DAY) AS SPANTODATE,
     from {{ source('cci_silver_dsnp', 'Member_med_20200526') }} m
   -- subsetting to externalids (memberids) that live in the member service, to reduce the size of this book of business table!
     inner join pic on m.patient.externalId = pic.MEMBERID
)

select * from memmo