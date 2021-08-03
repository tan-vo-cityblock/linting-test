
{{ config(tags = ["nightly", "payer_list"]) }}

{%- set payer_list = var('payer_list') -%}
{%- set cte_list = [] -%}

with

{% for payer in payer_list %}

{% set _ = cte_list.append(payer ~ '_facility_icd') %}

{{ payer }}_facility_icd as (

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

        procedure.tier as procedureTier,
        procedure.codeset as procedureCodeset,
        procedure.code as procedureCode

    from {{ source(payer, 'Facility') }} as base

    left join unnest(base.header.procedures) as procedure

    left join {{ source(payer, 'Provider') }} as provider

    on
        base.header.provider.billing.id = provider.providerIdentifier.id and
        coalesce(base.header.date.admit, base.header.date.from) >= provider.dateEffective.from and

        (
            coalesce(base.header.date.discharge, base.header.date.to) < provider.dateEffective.to or
            provider.dateEffective.to is null
        )

    left join {{ ref('ftr_facility_costs_categories') }} as costs
      on base.claimId = costs.claimId

),

{% set _ = cte_list.append(payer ~ '_facility_cpt') %}

{{ payer }}_facility_cpt as (

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

        coalesce(base.header.date.admit, base.header.date.from) as serviceDateFrom,
        coalesce(base.header.date.discharge, base.header.date.to) as serviceDateTo,

        case
          when (costs.costCategory = 'inpatient' and costs.costSubCategory = 'acute') then "inpatient"
          else "other"
          end
        as encounterType,

        lines.procedure.tier as procedureTier,
        lines.procedure.codeset as procedureCodeset,
        lines.procedure.code as procedureCode

    from {{ source(payer, 'Facility') }} as base

    left join unnest(base.lines) as lines

    left join {{ source(payer, 'Provider') }} as provider

    on
        base.header.provider.billing.id = provider.providerIdentifier.id and
        coalesce(base.header.date.admit, base.header.date.from) >= provider.dateEffective.from and

        (
            coalesce(base.header.date.discharge, base.header.date.to) < provider.dateEffective.to or
            provider.dateEffective.to is null
        )

    left join {{ ref('ftr_facility_costs_categories') }} as costs
      on base.claimId = costs.claimId

),

{% set _ = cte_list.append(payer ~ '_professional') %}

{{ payer }}_professional as (

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
        'professional' as source,
        '{{ payer }}' as partnerName,
        'payer' as partnerType,

        provider.npi as providerNpi,
        lines.provider.servicing.specialty as providerSpecialty,

        lines.date.from as serviceDateFrom,
        lines.date.to as serviceDateTo,

        'other' as encounterType,

        lines.procedure.tier as procedureTier,
        lines.procedure.codeset as procedureCodeset,
        lines.procedure.code as procedureCode

    from {{ source(payer, 'Professional') }} as base

    left join unnest(lines) as lines

    left join {{ source(payer, 'Provider') }} as provider

    on
        base.header.provider.billing.id = provider.providerIdentifier.id and
        lines.date.from >= provider.dateEffective.from and

        (
            lines.date.to < provider.dateEffective.to or
            provider.dateEffective.to is null
        )

),

{% endfor %}

{% set _ = cte_list.append('medical_encounters') %}

medical_encounters as (

    select distinct
        patient.patientId,
        'patientId' as memberIdentifierField,

        messageId as sourceId,
        'message' as sourceType,
        'ccd' as source,
        lower(patient.source.name) as partnerName,
        'ehr' as partnerType,

        providerIdentifiers.id as providerNpi,
        string(null) as providerSpecialty,

        date(timestamp(encounter.DateTime.raw)) as serviceDateFrom,
        date(timestamp(encounter.DateTime.raw)) as serviceDateTo,

        lower(encounter.Type.Name) as encounterType,

        string(null) as procedureTier,

        case
          when encounter.Type.CodeSystemName like 'CPT%'
            then 'cpt'
          else null
          end
        as procedureCodeset,

        case
          when encounter.Type.CodeSystemName like 'CPT%'
            then encounter.Type.Code
          else null
          end
        as procedureCode

    from {{ ref('src_patient_encounters_latest') }}

    left join unnest(encounter.providers) as providers

    left join unnest(providers.identifiers) as providerIdentifiers

),

final as (

  {% for cte in cte_list %}

  select

  {{ dbt_utils.surrogate_key(['sourceId', 'procedureCode', 'serviceDateFrom', 'encounterType']) }} as id,
  *

  from {{ cte }}

    {% if not loop.last %}

    union all

    {% endif %}

  {% endfor %}

)

select * from final
