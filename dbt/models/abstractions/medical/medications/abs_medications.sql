
{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}
{%- set cte_list = [] -%}

with

{% for payer in payer_list %}

{% set _ = cte_list.append(payer) %}

{{ payer }} as (

    select distinct
      coalesce(memberIdentifier.patientId, memberIdentifier.commonId, memberIdentifier.partnerMemberId) as memberIdentifier,

      case
        when memberIdentifier.patientId is not null
          then 'patientId'
        when memberIdentifier.commonId is not null
          then 'commonId'
        else 'partnerMemberId'
      end as memberIdentifierField,
      
      pharmacy.npi as pharmacyNpi,
      prescriber.npi as prescriberNpi,
      drug.partnerPrescriptionNumber,
      drug.ndc,
      drug.quantityDispensed,
      drug.daysSupply,
      drug.ingredient,
      coalesce(amount.copay, 0) + coalesce(amount.deductible, 0) + coalesce(amount.coinsurance, 0) as memberPaidAmount,
      amount.planPaid as planPaidAmount,
      date.filled as dateFilled,
      date_add(date.filled, interval drug.daysSupply - 1 day) as fillEnd,
      '{{ payer }}' as partnerName

    from {{ source(payer, 'Pharmacy') }} p

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

select
  {{ dbt_utils.surrogate_key(['memberIdentifier', 'pharmacyNpi', 'prescriberNpi', 'partnerPrescriptionNumber', 'ndc', 'quantityDispensed', 'daysSupply', 'ingredient', 'dateFilled', 'memberPaidAmount', 'planPaidAmount', 'partnerName']) }} as id,
  *

from final
