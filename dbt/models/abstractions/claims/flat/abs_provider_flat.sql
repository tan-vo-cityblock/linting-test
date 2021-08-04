
{{ config(tags = ['payer_list']) }}

with emblem_affiliations as (

  select
    providerId,
    providerOrganization as affiliatedGroupName

  from {{ ref('emblem_provider_organization') }}

  where providerOrganization is not null

),

{%- set payer_list = var('payer_list') -%}

{% for source_name in payer_list %}
{{ source_name }}_expanded_records as (

    select

      providerIdentifier.*,
      dateEffective.*,
      specialties,
      taxonomies,
      pcpFlag,
      npi,
      name,
      affiliatedGroupName,
      entitytype,
      inNetworkFlag,
      locations

  from {{ source( source_name, 'Provider') }}

),

{{ source_name }}_flattened as (

    select

      {{ source_name }}_expanded_records.surrogate.id as surrogateId,
      {{ source_name }}_expanded_records.surrogate.project as surrogateProject,
      {{ source_name }}_expanded_records.surrogate.dataset as surrogateDataset,
      {{ source_name }}_expanded_records.surrogate.table as surrogateTable,

      {{ source_name }}_expanded_records.partnerProviderId as partnerProviderId,
      {{ source_name }}_expanded_records.partner as partner,
      {{ source_name }}_expanded_records.id as providerId,

      {{ source_name }}_expanded_records.from as dateEffectiveFrom,
      {{ source_name }}_expanded_records.to as dateEffectiveTo,

      un_specs.code as specialtiesCode,
      un_specs.codeset as specialtiesCodeset,
      un_specs.tier as specialtiesTier,

      un_taxons.code as taxonomiesCode,
      un_taxons.tier as taxonomiesTier,

      pcpFlag,
      npi,
      name,
      affiliatedGroupName,
      entityType,
      inNetworkFlag,

      un_locs.clinic.address1 as locationsClinicAddress1,
      un_locs.clinic.address2 as locationsClinicAddress2,
      un_locs.clinic.city as locationsClinicCity,
      un_locs.clinic.state as locationsClinicState,
      un_locs.clinic.county as locationsClinicCounty,
      un_locs.clinic.zip as locationsClinicZip,
      un_locs.clinic.country as locationsClinicCountry,
      un_locs.clinic.email as locationsClinicEmail,
      un_locs.clinic.phone as locationsClinicPhone,

      un_locs.mailing.address1 as locationsMailingAddress1,
      un_locs.mailing.address2 as locationsMailingAddress2,
      un_locs.mailing.city as locationsMailingCity,
      un_locs.mailing.state as locationsMailingState,
      un_locs.mailing.county as locationsMailingCounty,
      un_locs.mailing.zip as locationsMailingZip,
      un_locs.mailing.country as locationsMailingCountry,
      un_locs.mailing.email as locationsMailingEmail,
      un_locs.mailing.phone as locationsMailingPhone,

    from {{ source_name }}_expanded_records

    left join unnest(specialties) as un_specs
    left join unnest(taxonomies) as un_taxons
    left join unnest(locations) as un_locs

),
{% endfor %}

combined as (

  {% for source_name in payer_list -%}
  select * from {{ source_name }}_flattened
  {% if not loop.last -%} union all {%- endif %}
  {% endfor %}

),

final as (

  select c.* replace (

    case
      when c.partner = 'emblem'
        then e.affiliatedGroupName
      else c.affiliatedGroupName
    end as affiliatedGroupName

  )

  from combined c

  left join emblem_affiliations e
  using (providerId)

)

select * from final
