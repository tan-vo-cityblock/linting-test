
with memmast_pre as (
  select * from {{ ref('master_member_base_cci') }}
),

-- idenfity when member first started showing up in facets data (fileReceivedDate)
facets_dates as (
    select 
        patientId,
        source,
        min(effectiveFromDate) as earliestFileReceivedDate
    from memmast_pre 
    where source = 'facets'
    group by 1,2
),

-- excluding "historic" rows from the facets data feed b/c they are duplicative with the amysis ones
-- unclear as of 1/21/21 whether members will  continue to show up in both amysis and facets, or whether the only source of overlap are these historic rows
-- for dsnp members, we want to preserve some of the historic facets rows since we were missing amysis data on them from  5/1/20-?
memmast as (
  select 
    mmp.* 
  from memmast_pre mmp
  left join facets_dates fd 
    using(patientId, source)

  where 
       (fd.earliestFileReceivedDate is null
        and mmp.source like '%amysis%') -- mm record is amysis

    or (fd.earliestFileReceivedDate is not null 
        and mmp.source not like '%amysis%'
        and fd.earliestFileReceivedDate <= mmp.spanfromdate) -- mm record is facets, but is not "historic", thus less likely to overlap with an existing amysis mm record
    
    or (fd.earliestFileReceivedDate is not null 
        and mmp.source not like '%amysis%'
        and mmp.spanfromdate >= '2020-05-01' 
        and mmp.lineOfBusiness1 = 'DSNP') -- mm record is "historic" facets, but is dsnp lob, for which we have a gap after 5/1/20
),

--  run result through islands logic
--  code adapted from https://bertwagner.com/2019/03/12/gaps-and-islands/
--  all fields from memmast as is, except effective from and to dates are replaced by min and max eff dates
grptemp as (
  select 
      RowNum,
     case when RowHash = lag(RowHash, 1) over (order by RowNum) then 0 else 1 end as IslandStartInd 

  from memmast
),

Grps as (
    select 
      RowNum,
      SUM(IslandStartInd) over (order by RowNum) as IslandId 
      
    from grptemp
),

Islands as (
  SELECT distinct
    --  ID NUMBERS
      patientId,
      memberId,
      NMI,
      medicareId,
      STRING(NULL) as acpnyMRN,
      elationId,
    --  DATES
      MIN(effectiveFromDate) AS fileReceivedFromDate,
      MAX(effectiveToDate) AS fileReceivedToDate,
      source,
      spanFromDate,
      spanToDate,
      eligStartDate,
      eligEndDate,
    --  DEMOGRAPHICS
      lastName,
      firstName,
      dateOfBirth,
      dateOfDeath,
      gender,
    --  INSURANCE INFO
      lineOfBusiness1,
      lineOfBusiness2,
      medicaidPremiumGroup,
      productDescription,
      employerGroupId,
      medicalCenterNumber,
      deliverySystemCode,
      providerId,
      providerLocationSuffix,
      providerNPI,
      homeHealthStatus,
      healthHomeName,
      careManagementOrganization,
    --  CBH FIELDS
      rateCell,         
    --  CONTACT INFO
      address1,
      address2,
      city,
      state,
      zip,
      county,            
      phone
   from memmast
   
   left outer join Grps using(RowNum)   
   
   group by
      IslandId,
    --  ID NUMBERS
      patientId,
      memberId,
      NMI,
      medicareId,
      elationId,
    --  DATES
      spanFromDate,
      spanToDate,
      eligStartDate,
      eligEndDate,
      source,
    --  DEMOGRAPHICS
      lastName,
      firstName,
      dateOfBirth,
      dateOfDeath,
      gender,
    --  INSURANCE INFO
      lineOfBusiness1,
      lineOfBusiness2,
      medicaidPremiumGroup,
      productDescription,
      employerGroupId,
      medicalCenterNumber,
      deliverySystemCode,
      providerId,
      providerLocationSuffix,
      providerNPI,
      homeHealthStatus,
      healthHomeName,
      careManagementOrganization, 
      atRisk,
      fullyDelegated,
    --  CBH FIELDS 
      rateCell,
    --  CONTACT INFO
      address1,
      address2,
      city,
      state,
      zip,
      county,            
      phone         
)

select * from Islands