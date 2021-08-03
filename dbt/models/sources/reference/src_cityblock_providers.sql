
{{ config(tags=["sheets"]) }}

with termed_providers as (

  select
    trim(providerName) as providerName,
    string(null) as overallStatus,
    lower(NPI) as npi,
    cityblockEmail,
    providerOrBHS

  from {{ source('cityblock_providers', 'src_cityblock_providers_term') }}

),

combined_tables as (

{% for suffix in ['ct', 'dc', 'la', 'ma', 'ny', 'oh' ] %}

  select
    trim(providerName) as providerName,
    trim(stateLicensing) as overallStatus,
    lower(NPI) as npi,
    cityblockEmail,
    providerOrBHS

  from {{ source('cityblock_providers', 'src_cityblock_providers_' ~ suffix) }}

  {% if not loop.last %} union distinct {% endif%}

{% endfor %}

  union distinct

  select * from termed_providers

)

select 
  a.providerName as name,
  trim(upper(SPLIT(a.providerName)[SAFE_OFFSET(0)])) as lastName,
  trim(upper(SPLIT(a.providerName)[SAFE_OFFSET(1)])) as firstName,
  max(a.overallStatus) as overallStatus,
  max(case when a.npi = 'pending' then null else a.npi end) as npi,
  max(a.cityblockEmail) as cityblockEmail,
  max(a.providerOrBHS) as providerOrBHS,
  case when b.providerName is null then true else false end as activeInMarket

from combined_tables a

left join termed_providers b
using (providerName)

where
  a.providerName is not null and
  a.NPI is not null

group by 1 ,2, 3, 8
