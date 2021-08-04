with member_dates as (

  select
    case
      when partnerName = 'Emblem Health' then 'emblem'
      else lower(partnerName)
    end as partner,
    min(fileReceivedFromDate) as minDateMember,
    max(fileReceivedFromDate) as maxDateMember
  from {{ ref('master_member_v1') }}
  group by partnerName
),

claim_dates as (

  select
    partner,
    coalesce(costClassification1 = 'RX', false) as isRx,
    min(dateFrom) as minDateClaim,
    max(dateFrom) as maxDateClaim
  from {{ ref('mrt_claims_self_service') }}
  group by 1, 2
),

br_market_partner as (

  select distinct
    patientHomeMarketName as market,
    case
      when partnerName = 'Emblem Health' then 'emblem'
      else lower(partnerName)
    end as partner
  from {{ ref('member') }}
)

select
  market,
  partner,
  isRx,
  minDateMember,
  minDateClaim,
  maxDateMember,
  maxDateClaim
from member_dates
left join claim_dates using (partner)
left join br_market_partner using (partner)
