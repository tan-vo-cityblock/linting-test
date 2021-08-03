
{{
 config(
   materialized='view'
 )
}}

with emblem_assignments as (

  select
    m.identifier.patientId,
    attribution.date.from as assignedFrom,
    attribution.date.to as assignedTo,
    attribution.pcpId as pcpProviderId,
    p.providerNpi as pcpNpi,
    p.providerName as pcpName,
    p.providerOrganization as pcpOrganizationName
    
  from {{ source('emblem', 'Member') }} m,
  unnest(attributions) as attribution

  inner join {{ ref('emblem_provider_organization') }} p
  on attribution.pcpId = p.providerId

  where m.identifier.patientId is not null

),

--we are using 'cci' AND 'cci_facets' since the join between member.pcpid and provider.id are not always matching up in the cci gold claims
cci_assignments as (

{% for cci_source in ['cci', 'cci_facets']%}

  select distinct
    m.identifier.patientId,
    attributions.date.from as assignedFrom,
    attributions.date.to as assignedTo,
    attributions.PCPId as pcpProviderId,
    p.npi as pcpNpi,
    p.name as pcpName,
    p.affiliatedGroupName as pcpOrganizationName

  from {{ source(cci_source, 'Member') }} m,
  unnest(m.attributions) as attributions

  inner join {{ source(cci_source, 'Provider') }} p

  on
    attributions.PCPId = p.providerIdentifier.id and
    attributions.date.from >= p.dateEffective.from and

    (
      attributions.date.to < p.dateEffective.to or
      p.dateEffective.to is null
    )

  where
    m.identifier.patientId is not null and
    p.npi is not null

     {% if not loop.last %} union distinct {% endif%}

{% endfor %}
),
  
tufts_assignments as (

  select distinct
    m.memberidentifier.patientId,
    m.dateEffective.from as assignedFrom,
    m.dateEffective.to as assignedTo,
    m.pcp.id as pcpProviderId,
    p.npi as pcpNpi,
    p.name as pcpName,
    p.affiliatedGroupName as pcpOrganizationName

  from {{ source('tufts', 'Member') }} m

  inner join {{ source('tufts', 'Provider') }} p

  on 
    m.pcp.id = p.providerIdentifier.id and
    m.dateEffective.from >= p.dateEffective.from and

    (
      m.dateEffective.to < p.dateEffective.to or
      p.dateEffective.to is null
    )

  where 
    m.memberidentifier.patientId is not null and
    p.npi is not null  

),

all_assignments as (

  select * from emblem_assignments
  union all
  select * from cci_assignments
  union all
  select * from tufts_assignments
  
),

cbh_npis as (

  select npi
  from {{ ref('src_cityblock_providers') }} prov
  where overallStatus = 'Active'

),

final as (

  select
    patientId,
    assignedFrom,
    assignedTo,
    row_number() over(partition by patientId order by assignedFrom desc) = 1 as isLatestAssignment,
    pcpProviderId,
    pcpNpi,
    upper(pcpName) as pcpName,
    pcpNpi in (select * from cbh_npis) as isCbhPcp,
    upper(pcpOrganizationName) as pcpOrganizationName,
    pcpOrganizationName = 'Rapid Access Care @ ACP' as isAcpnyPcp
  from all_assignments

)

select * from final
order by patientId, assignedFrom desc
