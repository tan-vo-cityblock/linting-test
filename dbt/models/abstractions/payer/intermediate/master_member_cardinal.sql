

with memmast as (
  select * from {{ ref('master_member_base_cardinal') }}
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
      'cardinal' as source,
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
