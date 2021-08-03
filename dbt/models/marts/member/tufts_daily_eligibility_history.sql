
with base as (
  select
    * REPLACE ((SELECT AS STRUCT memberIdentifier.* EXCEPT (patientId)) AS memberIdentifier),
    parse_date('%Y%m%d', _table_suffix ) as fileReceivedDate, 
  from {{ source('tufts', 'Member_Daily_*') }}
  -- union all with historic eligibility tables to get pre cityblock eligibility for gold member view ?
),

base_with_patientIds as (
  select
    mdi.memberId as patientId,   
    base.*
  from base
  left join {{ source('member_index', 'member_datasource_identifier') }} mdi 
    on base.memberIdentifier.partnerMemberId = mdi.externalId
  where mdi.deletedAt is null
  -- and mdi.memberId is not null -- do NOT want to limit to members already in memberService
)

select
  patientId,
  memberIdentifier.partnerMemberId,
  fileReceivedDate,
  dateEffective.from as dateEffeciveFrom,
  dateEffective.to as dateEffectiveTo,
--   sc.data.PeriodBeginDate,
--   sc.data.PeriodEndDate,
  demographic.dateBirth,
  demographic.dateDeath,
  demographic.SSN,
  demographic.name.first as firstName,
  demographic.name.last as lastName,
  location.address1,
  location.address2,
  location.city,
  location.state,
  location.county,
  location.zip,
  location.email,
  location.phone,
  sc.data.SubscriberAddress,
  sc.data.SubscriberCity,
  sc.data.SubscriberState,
  substr(sc.data.SubscriberZip, 0, 5) as SubscriberZip,
  eligibility.lineOfBusiness,
  eligibility.subLineOfBusiness,
  eligibility.partnerBenefitPlanId
  
from base_with_patientIds base
left join {{ source('tufts_silver', 'Member_Daily_*') }} sc
  on base.surrogate.id = sc.identifier.surrogateId  
order by patientId, fileReceivedDate, dateEffective.from, dateEffective.to 