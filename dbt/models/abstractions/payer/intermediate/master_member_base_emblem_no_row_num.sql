--  emblem member master table (v1 silver)
--  aseiden 7/24/19, updated 8/9/19; updated 8/20/19 with workaround for wildcard

--  get data dump (file) received dates, and add end dates = day prior
with filedates as (
      select
          *,
      --  imputing today's date where end date is null (the most recent file received)
          IFNULL((DATE_ADD(LEAD(eff_start) OVER(ORDER BY eff_start), INTERVAL -1 DAY)), CURRENT_DATE()) AS eff_end
      from (
            select distinct 
              PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start,
              'emblem' as source
            -- grabs received dates from all member month files in emblem silver claims
            from {{ source('emblem_silver', 'member_month_*') }}
            where patient.patientId is not null
            -- temporarily only taking files received 3/12/19 and beyond, because the demog files before that have not been loaded into silver claims yet
              and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= '2019-03-12'
            -- temporarily excluding emblem  data dumps from july/august 2019 for now. we determined they are missing 1/2 of our members so they skew the results of member management table
              and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2019-07-24'
              and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2019-08-14'
            -- excluding shard from 4/16 bc these were pre-members for actuary
              and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2020-04-16'
            
            -- emblem virtual 
            union distinct 
            select 
              PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start,
              'emblem_digital' as source
            from {{ source('emblem_silver_virtual', 'member_month_*') }}
           )   
),

-- pulling the file dates from the regular files and will use below for joining with the ooa supplemental files, which have different dates
filedatesjoin as (
      select
          eff_start,
          extract(month from eff_start) as month,
          extract(year from eff_start) as year
      from filedates
),

-- provider npi's from emblem provider file, to be joined in later via prov id as pcp npi
provider as (
      select distinct
          PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start,
          provider.PROV_NPI,
          provider.PROV_ID
          -- decided not to add PROV_LOC field here because it could cause duplication, even though we are adding it to the memmo table
      from {{ source('emblem_silver', 'providers_*') }}
      -- temporarily only taking files received 3/12/19 and beyond, because the demog files before that have not been loaded into silver claims yet
      where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= '2019-03-12'
      -- temporarily excluding emblem  data dumps from july/august 2019 for now. we determined they are missing 1/2 of our members so they skew the results of member management table
        and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2019-07-24'
        and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2019-08-14'
      -- excluding shard from 4/16 bc these were pre-members for actuary
        and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2020-04-16'

      union distinct

      select 
          PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start,
          provider.PROV_NPI,
          provider.PROV_ID
          -- decided not to add PROV_LOC field here because it could cause duplication, even though we are adding it to the memmo table
      from {{ source('emblem_silver_virtual', 'providers_*') }}
),

  --  unnesting current patient index cache to get constants like rate cell and acpny mrn, that should never change
  --  even if the member has a new memberid, these should not change. explicitly not taking emblemid from here now
  -- 11/12/19 replaced with member index mirror here and below, this is already a flattened view, unlike the old PIC
  -- 12/16/19 removed fuzzy matching altogether as these ids havfe already been added to the member index, and will continue to be. the member index view already is in the format we need:
pic as (

      select
         patientId,
         memberId as MEMBERID,
         category as rateCell,
         acpnyId as acpnyMRN

      from {{ ref('member_index_member_management') }}

      where memberIdSource = 'emblem'
      and deletedAt is null
      and cohortId > 0

),

--  load supplemental ooa demographic files, for non-kings
--  union with regular demographic files
--  note same memberid can potentially have multiple demog records, but the memmo file will determine which one was the active memmo at each month (?)
demog as (
      select
          f.eff_start,
          MEMBER_ID,
          NMI,
          MEM_MEDICARE,
          upper(MEM_LNAME) as MEM_LNAME,
          upper(MEM_FNAME) as MEM_FNAME,
          cast(MEM_DOB as DATE) as MEM_DOB,
          cast(MEM_DOD as DATE) as MEM_DOD,
          MEM_GENDER,
          upper(MEM_ADDR1) as MEM_ADDR1,
          upper(MEM_ADDR2) as MEM_ADDR2,
          upper(MEM_CITY) as MEM_CITY,
          upper(MEM_STATE) as MEM_STATE,
          MEM_ZIP,
          upper(MEM_COUNTY) as MEM_COUNTY,           
          MEM_PHONE
      from {{ source('member_management', 'CB_COHORT_MEMBERDEMO_EXTRACT_*') }}
      --  inserting the file dates from the regular files, based on month/year of the supplemental file dates
      --  even though the actual received date is different on the supplemental files
      join filedatesjoin as f
         on extract(month from PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) = f.month
        and extract(year from PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) = f.year
      where upper(MEM_COUNTY) != 'KINGS'
      union all
      --  fields below have to be in same order as above; column names are set above

      {% set emblem_source_list = ['emblem_silver', 'emblem_silver_virtual'] %}

      {% for emblem_source in emblem_source_list %}

        select
          PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS eff_start,
          patient.externalId as MEMBERID,
          demographic.NMI,          
          demographic.MEM_MEDICARE,
          upper(demographic.MEM_LNAME) as MEM_LNAME,
          upper(demographic.MEM_FNAME) as MEM_FNAME,
          demographic.MEM_DOB,
          demographic.MEM_DOD,
          demographic.MEM_GENDER,
          upper(demographic.MEM_ADDR1) as MEM_ADDR1,
          upper(demographic.MEM_ADDR2) as MEM_ADDR2,
          upper(demographic.MEM_CITY) as MEM_CITY,
          upper(demographic.MEM_STATE) as MEM_STATE,
          demographic.MEM_ZIP,
          upper(demographic.COUNTY) as MEM_COUNTY,
          demographic.MEM_PHONE
     
        from {{ source(emblem_source, 'member_demographics_*') }}   

        {% if emblem_source == 'emblem_silver' %}
         -- temporarily only taking files received 3/12/19 and beyond, because the demog files before that have not been loaded into silver claims yet
          where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= '2019-03-12'
         -- temporarily excluding emblem  data dumps from july/august 2019 for now. we determined they are missing 1/2 of our members so they skew the results of member management table
            and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2019-07-24'
            and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2019-08-14'
         -- excluding shard from 4/16 bc these were pre-members for actuary
            and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2020-04-16'
        {% else %}
         -- removing members from virtual data because they already appear in regular emblem data
          where patient.patientId not in (select distinct memberId as patientId from `emblem-data.cohort8.assignment_total`)
            and patient.patientId not in (select distinct patientId from {{ source('member_management', 'cohort9_from_digital_patientId_list') }} )

        {% endif %}  

        {% if not loop.last %} union all {% endif %}

      {% endfor %}

),

--  join supplemented pic to supplemented demographics, with datadumpdate
demogpic as (
        select distinct * from pic as p
        --  we should only retain CBH members with valid patientIds
        left join demog as d on p.MEMBERID = d.MEMBER_ID
        --  removing known 'bad dupe' patientIds;  eventually these may be scrubbed by eng
        left outer join {{ source('member_management', 'bad_dupes') }} as bd on p.patientId = bd.id
        where bd.id is null
),

--  union supplemental ooa memmo with regular memmo, all available  in silver  claims
--  next step  will join this to demogs on memberid and file received date
memmo as (
    --  supplemental ooa memmo
    select
        f.eff_start,
        MEMBERID,
        cast(SPANFROMDATE as DATE) as SPANFROMDATE,
        cast(SPANTODATE as DATE) as SPANTODATE,
        cast(ELIGIBILITYSTARTDATE as DATE) as ELIGIBILITYSTARTDATE,
        cast(ELIGIBILITYENDDATE as DATE) as ELIGIBILITYENDDATE,
        LINEOFBUSINESS,
        MEDICAIDPREMIUMGROUP,
        MEDICALCENTERNUMBER,
        DELIVERYSYSTEMCODE,
        PRODUCTDESCRIPTION,
        EMPLOYERGROUPID,
        PROVIDERID,
        PROVIDERLOCATIONSUFFIX,
        'emblem' as source
    --  eventually replace these with silver claims version (?)
    from {{ source('member_management', 'CB_COHORT_MEMBERMONTH_EXTRACT_*') }} as m
    --  inserting the file dates from the regular files, based on month/year of the supplemental file dates
    --  even though the actual received date is different on the supplemental files
    join filedatesjoin as f
      on extract(month from PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) = f.month
     and extract(year from PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) = f.year
    --   but going to keep the county field from the demogs not the memmo
    where m.MEMBERCOUNTY != 'Kings'
  
    union all
  
    --  regular memmo
    --  aseiden 8/20/19 temporarily unioning each table manually because wildcard did not work due to new schema issue
    select
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) as eff_start,
        month.MEMBERID,
        month.SPANFROMDATE,
        month.SPANTODATE,
        month.ELIGIBILITYSTARTDATE,
        month.ELIGIBILITYENDDATE,
        month.LINEOFBUSINESS,
        month.MEDICAIDPREMIUMGROUP,
        month.MEDICALCENTERNUMBER,
        month.DELIVERYSYSTEMCODE,
        month.PRODUCTDESCRIPTION,
        month.EMPLOYERGROUPID,
        month.PROVIDERID,
        month.PROVIDERLOCATIONSUFFIX,
        'emblem' as source
    from {{ source('emblem_silver', 'member_month_*') }}
    --  updated 10/22/19 shards between 3/12/19 and 6/12/19  will concat fine b/c it's before emblem changed the schema  on us
    --  when historical data pre-3/19 is added to BQ, switch the between to a less-than
    where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) between '2019-03-12' and '2019-06-12'
    
    union all
  
  -- wildcarding all date shards on or after 9/15/19
  -- this was the first CB_COHORT_* file we trust after emblem switched which schema they were sending us
  -- excluding emblem  data dumps from july/august 2019 for now. we determined they are missing 1/2 of our members so they skew the results of member management table
    select
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) as eff_start,
        month.MEMBERID,
        month.SPANFROMDATE,
        month.SPANTODATE,
        month.ELIGIBILITYSTARTDATE,
        month.ELIGIBILITYENDDATE,
        month.LINEOFBUSINESS,
        month.MEDICAIDPREMIUMGROUP,
        month.MEDICALCENTERNUMBER,
        month.DELIVERYSYSTEMCODE,
        month.PRODUCTDESCRIPTION,
        month.EMPLOYERGROUPID,
        month.PROVIDERID,
        month.PROVIDERLOCATIONSUFFIX,
        'emblem' as source
    from {{ source('emblem_silver', 'member_month_*') }}
    where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= '2019-09-15'
    -- excluding shard from 4/16 bc these were pre-members for actuary
      and PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) != '2020-04-16'
    -- excluding erroneously sent PBM eligibilities that are causing concurrent rows per member per month  
      and month.LINEOFBUSINESS != 'EP'

  union all
  
    -- emblem virtual/digital cohort 
    select
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) as eff_start,
        month.MEMBERID,
        month.SPANFROMDATE,
        month.SPANTODATE,
        month.ELIGIBILITYSTARTDATE,
        month.ELIGIBILITYENDDATE,
        month.LINEOFBUSINESS,
        month.MEDICAIDPREMIUMGROUP,
        month.MEDICALCENTERNUMBER,
        month.DELIVERYSYSTEMCODE,
        month.PRODUCTDESCRIPTION,
        month.EMPLOYERGROUPID,
        month.PROVIDERID,
        month.PROVIDERLOCATIONSUFFIX,
        'emblem_digital' as source
    from {{ source('emblem_silver_virtual', 'member_month_*') }}
    -- excluding erroneously sent PBM eligibilities that are causing concurrent rows per member per month  
      where month.LINEOFBUSINESS != 'EP'
      -- removing members from virtual data because they already appear in regular emblem data
        and patient.patientId not in (select distinct memberId as patientId from `emblem-data.cohort8.assignment_total`)
        and patient.patientId not in (select distinct patientId from {{ source('member_management', 'cohort9_from_digital_patientId_list') }} )
),

--  create member master table by joining demogs to member months on
--  and eff-start dates of the demog files linked to the dates of the memmo files
--  this will be a large concatenation of all member month files, linked to demogs and supplemented with fuzzy and ooa records
--  and next step will reduce size with islands logic
--  following DSA naming conventions, renaming many of these fields
-- 12/18/19 removed fuzzyDummy entirely. these id's are in the member index now, and the flag is not being used
memmast as (
    select distinct
        --  ID NUMBERS
        d.patientId,
        m.MEMBERID as memberId,
        d.NMI,
        d.MEM_MEDICARE as medicareId,
        d.acpnyMRN,
        --  DATES
        m.eff_start as  effectiveFromDate,
        dt.eff_end as effectiveToDate,
        m.SPANFROMDATE as spanFromDate,
        m.SPANTODATE as  spanToDate,
        m.ELIGIBILITYSTARTDATE as  eligStartDate,
        m.ELIGIBILITYENDDATE as  eligEndDate,
        m.source,
        --  DEMOGRAPHICS
        d.MEM_LNAME as lastName,
        d.MEM_FNAME as firstName,
        d.MEM_DOB as dateOfBirth,
        d.MEM_DOD as dateOfDeath,
        d.MEM_GENDER as gender,
        --  INSURANCE INFO
        m.LINEOFBUSINESS as lineOfBusiness,
        m.MEDICAIDPREMIUMGROUP as medicaidPremiumGroup,
        m.PRODUCTDESCRIPTION as productDescription,
        m.EMPLOYERGROUPID as employerGroupId,
        m.PROVIDERID as providerId,
        m.PROVIDERLOCATIONSUFFIX as providerLocationSuffix,
        p.PROV_NPI as providerNPI,
        m.MEDICALCENTERNUMBER as medicalCenterNumber,
        m.DELIVERYSYSTEMCODE as deliverySystemCode,
        STRING(NULL) as homeHealthStatus,
        STRING(NULL) as healthHomeName,
        STRING(NULL) as careManagementOrganization,
        STRING(NULL) as atRisk,
        STRING(NULL) as fullyDelegated,
        --  CBH FIELDS
        d.rateCell,
        --  CONTACT INFO
        d.MEM_ADDR1 as address1,
        NULLIF(d.MEM_ADDR2, 'N/A') as address2,
        d.MEM_CITY as city,
        d.MEM_STATE as state,
        d.MEM_ZIP as zip,
        d.MEM_COUNTY as  county,           
        d.MEM_PHONE as phone,
        -- creating a fingerprint/hash off a concat of all fields except effective ("file") dates and null placeholder fields
        FARM_FINGERPRINT(CONCAT(  d.patientId, 
                                  ifnull(m.MEMBERID,'null'),
                                  ifnull(d.NMI,'null'),
                                  ifnull(d.MEM_MEDICARE,'null'),
                                  ifnull(d.acpnyMRN,'null'),
                                  ifnull(cast(m.SPANFROMDATE as string), 'null'),
                                  ifnull(cast(m.SPANTODATE as string),'null'),
                                  ifnull(cast(m.ELIGIBILITYSTARTDATE as string),'null'),
                                  ifnull(cast(m.ELIGIBILITYENDDATE as string),'null'),
                                  ifnull(d.MEM_LNAME,'null'),
                                  ifnull(d.MEM_FNAME,'null'),
                                  ifnull(cast(d.MEM_DOB as string), 'null'),
                                  ifnull(cast(d.MEM_DOD as string), 'null'),
                                  ifnull(d.MEM_GENDER,'null'),
                                  ifnull(m.LINEOFBUSINESS,'null'),
                                  ifnull(m.MEDICAIDPREMIUMGROUP,'null'),
                                  ifnull(m.PRODUCTDESCRIPTION,'null'),
                                  ifnull(m.EMPLOYERGROUPID,'null'),
                                  ifnull(m.PROVIDERID,'null'),
                                  ifnull(m.PROVIDERLOCATIONSUFFIX,'null'),
                                  ifnull(p.PROV_NPI,'null'),
                                  ifnull(m.MEDICALCENTERNUMBER,'null'),
                                  ifnull(m.DELIVERYSYSTEMCODE,'null'),   
                                  -- placeholder for currently empty fields, they will be empty for every row for now
                                  ifnull(d.rateCell, 'null'),
                                  ifnull(d.MEM_ADDR1,'null'),
                                  ifnull(d.MEM_ADDR2, 'N/A'),
                                  ifnull(d.MEM_CITY,'null'),
                                  ifnull(d.MEM_STATE,'null'),
                                  ifnull(d.MEM_ZIP,'null'),
                                  ifnull(d.MEM_COUNTY, 'null'),          
                                  ifnull(d.MEM_PHONE,'null')
                              )) AS RowHash        
    from memmo as m
    --  join with demographics
    join demogpic as d
      on m.MEMBERID = d.MEMBERID
     and m.eff_start = d.eff_start
    --  join to get PCP NPI from provider table
    --  not joining on PROV_LOC, just want it as a reference in the MMT so we can pull specific locations for mailing lists as needed
    left outer join provider as p
      on m.PROVIDERID = p.PROV_ID
     and m.eff_start = p.eff_start
    --  join to get eff_end dates
    inner join filedates as dt
      on m.eff_start = dt.eff_start
     and m.source = dt.source
)

select * from memmast
