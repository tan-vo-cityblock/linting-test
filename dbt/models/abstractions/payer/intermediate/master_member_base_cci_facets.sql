
with pic as (

       select
          memberId as MEMBERID

       from {{ ref('member_index_member_management') }}   
       
       where memberIdSource = 'connecticare'
       and deletedAt is null
       and cohortId > 0
),

-- grabbing memberid's for dsnp ppl in cohort2 to impute their LOB in cte below
-- sidestepping platform team erroneously ingesting all facets members as medicare lob in silver
dsnp_comm as (
   select
    externalId,
    case
      when mid.sublineofbusiness = 'dual' and mid.lineofbusiness = 'medicare'
        then 'DSNP'
      when mid.lineofbusiness = 'commercial'
        then 'C'
      else null
      end as LINEOFBUSINESS
   from {{ source('member_index', 'member_datasource_identifier') }} mdi
   left join {{ source('member_index', 'member_insurance_details') }} mid
      on mdi.id = mid.memberDatasourceId
   where
    (
      (mid.sublineofbusiness = 'dual' and mid.lineofbusiness = 'medicare')
      or mid.lineofbusiness = 'commercial' 
    )
    and mdi.deletedAt is null
    and mid.deletedAt is null
),

memmo_pre as (

  --  facets member_med memmo 
     select
         PARSE_DATE('%Y%m%d', gc._TABLE_SUFFIX ) as eff_start,
         memberIdentifier.partnerMemberId as MEMBERID,
         dateEffective.from as ELIGIBILITYSTARTDATE,
         dateEffective.to as ELIGIBILITYENDDATE, 
         coalesce(dsnp_comm.LINEOFBUSINESS, eligibility.LINEOFBUSINESS) as LINEOFBUSINESS,
         eligibility.subLineOfBusiness as LINEOFBUSINESS2,
         cast(null as string) as medicaidPremiumGroup,
         STRING(NULL) as MEDICALCENTERNUMBER, 
         STRING(NULL) as DELIVERYSYSTEMCODE, 
         STRING(NULL) as PRODUCTDESCRIPTION,  
        --  eligibility.partnerEmployerGroupId as EMPLOYERGROUPID,
        --  pcp.id as PROVIDERID,
         cast(null as string) as EMPLOYERGROUPID, --sc.data.Group_Level_1_ID	 as EMPLOYERGROUPID,
         cast(null as string) as PROVIDERID, -- scp.data.Provider_ID as PROVIDERID,
         cast(null as string) as PROVIDERLOCATIONSUFFIX,
    --  for CCI (unlike emblem), memmo file contains all demogs, so adding all demogs here
         memberIdentifier.partnerMemberId as NMI,
         STRING(NULL) as MEM_MEDICARE,
         upper(demographic.name.last) as MEM_LNAME,
         upper(demographic.name.first) as MEM_FNAME,
         demographic.dateBirth as MEM_DOB,
         demographic.dateDeath as MEM_DOD, 
         demographic.gender as MEM_GENDER,
         upper(location.address1) as MEM_ADDR1,
         upper(location.address2) as MEM_ADDR2,
         upper(location.city) as MEM_CITY,
         upper(location.state) as MEM_STATE,
         substr(location.zip,1,5) as MEM_ZIP,
         upper(location.county) as MEM_COUNTY,            
         location.phone as MEM_PHONE,
         -- these are silver fields that will need to be joined in
         cast(null as string) as careManagementOrganization,
         cast(null as string) as PROV_NPI, --scp.data.National_Practitioner_Identifier as PROV_NPI,
        'facets' as source
        from {{ source('cci_facets', 'MemberV2_*') }} gc --,  
        -- subsetting to externalids (memberids) that live in the member service, to reduce the size of this book of business table!
        inner join pic on gc.memberIdentifier.partnerMemberId = pic.MEMBERID
        left join dsnp_comm on gc.memberIdentifier.partnerMemberId = dsnp_comm.externalId
        -- joining silver claims to get partner specific fields for that row
        -- commenting out neither join is providing much value and are main driver of resource exceeding (pcp npi isn't even working all npi's are null in the result)
        -- left join {{ source('cci_facets_silver', 'Attributed_PCP_med_*') }} scp 
        --     on gc.surrogate.id = scp.identifier.surrogateId
        -- left join {{ source('cci_facets_silver', 'Member_Facets_med_*') }} sc 
        --     on gc.surrogate.id = sc.identifier.surrogateId
        -- joining to cci gold provider view (max shard) to get pcp NPI's. hopefully doens't cause fanout!
        --left outer join {{ source('cci', 'Provider') }} pr on scp.data.Provider_ID = pr.identifier.id
),

memmo as (
  select 
    mp.* ,
    SPANFROMDATE,
    date_sub(date_add(SPANFROMDATE, interval 1 month), interval 1 day) as SPANTODATE,
  from memmo_pre mp,
  unnest(generate_date_array(
          ( select date_trunc(ELIGIBILITYSTARTDATE, month)), 
            case 
              when ELIGIBILITYENDDATE > current_date() or ELIGIBILITYENDDATE is null then current_date()
              else ELIGIBILITYENDDATE end
            , interval 1 month
                             )
         ) as SPANFROMDATE
  -- controlling for crazy dates in past/future. shouldnt include elig months beyond the fileReceivedDate b/c it messes things up downstream
  where SPANFROMDATE >= '2016-01-01' 
    and SPANFROMDATE <= eff_start
)

select * from memmo