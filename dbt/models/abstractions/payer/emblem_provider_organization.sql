
with providers as (

  select
    providerIdentifier.id,
    npi,
    name

  from {{ source('emblem', 'Provider') }}

),

professional_affiliations as (

  select     
    header.provider.billing.id as providerBillingId,
    line.provider.servicing.id as providerServicingId,
    max(line.date.to) as maxToDate

  from {{ source('emblem', 'Professional') }}
  left join unnest(lines) as line

  where 
    header.provider.billing.id is not null and
    line.provider.servicing.id is not null
    
  group by header.provider.billing.id, line.provider.servicing.id

),

provider_npi_names as (

  select
    id,
    npi,
    name
  from providers
  where npi is not null

),

provider_id_names as (

  select
    id,
    name
  from providers

),

most_recent_affiliations as (

  select most_recent.* from (
  
    select array_agg(professional_affiliations order by maxToDate desc limit 1)[offset(0)] most_recent
    from professional_affiliations
    group by providerServicingId
    
  )
),

final_affiliations as (

  select
    p1.id as providerId,
    p1.npi as providerNpi,
    p1.name as providerName,
    p2.name as providerOrganization

  from provider_npi_names p1

  left join most_recent_affiliations mra
  on p1.id = mra.providerServicingId

  left join provider_id_names p2
  on 
    mra.providerBillingId = p2.id and
    p1.name != p2.name

)

select * from final_affiliations
