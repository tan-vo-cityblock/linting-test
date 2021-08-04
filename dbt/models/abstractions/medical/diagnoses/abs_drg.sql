
{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}
{%- set cte_list = [] -%}

with

{% for payer in payer_list %}

{% set _ = cte_list.append(payer ~ '_facility') %}

{{ payer }}_facility as (

  select distinct
      coalesce(base.memberIdentifier.patientId, base.memberIdentifier.commonId, base.memberIdentifier.partnerMemberId) as memberIdentifier,

      case
        when base.memberIdentifier.patientId is not null
          then 'patientId'
        when base.memberIdentifier.commonId is not null
          then 'commonId'
        else 'partnerMemberId'
      end as memberIdentifierField,

      base.claimId as sourceId,
      'claim' as sourceType,
      'facility' as source,
      '{{ payer }}' as partnerName,
      'payer' as partnerType,

      provider.npi as providerNpi,
      base.header.provider.billing.specialty as providerSpecialty,

        case
          when costs.costCategory = 'inpatient' 
            then coalesce(base.header.date.admit, base.header.date.from)
          else base.header.date.from
        end as serviceDateFrom,

        case
          when costs.costCategory = 'inpatient'
            then coalesce(base.header.date.discharge, base.header.date.to)
          else base.header.date.to
        end as serviceDateTo,

      case
        when (costs.costCategory = 'inpatient' and costs.costSubCategory = 'acute') then "inpatient"
        else "other"
        end
      as encounterType,

      base.header.drg.code as drgCode,
      base.header.drg.codeset as drgCodeset,

      services.edServiceFlag

  from {{ source(payer, 'Facility') }} as base

  left join {{ source(payer, 'Provider') }} as provider
  
  on 
      base.header.provider.billing.id = provider.providerIdentifier.id and
      (
      coalesce(base.header.date.admit, base.header.date.from) >= provider.dateEffective.from or
      provider.dateEffective.from is null
      )
       and
      (
      coalesce(base.header.date.discharge, base.header.date.to) < provider.dateEffective.to or
      provider.dateEffective.to is null
      )

  left join {{ ref('ftr_facility_costs_categories') }} as costs
    on base.claimId = costs.claimId

  left join {{ ref('ftr_facility_services_flags') }} as services
    on base.claimId = services.claimId

  where base.header.drg.code is not null

),

{% endfor %}

final as (

  {% for cte in cte_list %}

  select * from {{ cte }}

    {% if not loop.last %}

    union all

    {% endif %}

  {% endfor %}

)

select * from final
