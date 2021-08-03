
{{ config(tags = ["nightly", "payer_list"]) }}

{%- set payer_list = var('payer_list') -%}
{%- set cte_list = [] -%}

with

{% for payer in payer_list %}

{% set _ = cte_list.append(payer ~ '_facility') %}

{{ payer }}_facility as (

    select 
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
        upper(trim(provider.name)) as providerName,
        upper(trim(provider.name)) as cityblockProviderName,
        case when prov.lastName is not null then 'Yes' end as cityblockProvider,

        prov.providerOrBHS as cityblockProviderType,
        prov.activeInMarket as cityblockProviderActiveInMarket,

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
          when (costs.costCategory = 'outpatient' and costs.costSubCategory = 'ed') then "ed"
          else "other"
          end
        as encounterType,

        diagnoses.codeset as diagnosisCodeset,
        replace(diagnoses.code, '.', '') as diagnosisCode,
        diagnoses.tier as diagnosisTier,

        (select array_agg(distinct claimLineStatus) from unnest(lines) where claimLineStatus is not null) as claimLineStatuses,
        array<string>[] as placesOfService

    from {{ source(payer, 'Facility') }} as base

    left join {{ source(payer, 'Provider') }} as provider

    on
        base.header.provider.billing.id = provider.providerIdentifier.id and
      
        (coalesce(base.header.date.admit, base.header.date.from) >= provider.dateEffective.from  or
            provider.dateEffective.from is null) and
        (coalesce(base.header.date.discharge, base.header.date.to) < provider.dateEffective.to or
            provider.dateEffective.to is null)

    left join unnest(base.header.diagnoses) as diagnoses

    left join {{ ref('src_cityblock_providers') }} prov
      on provider.NPI  =  prov.npi

    left join {{ ref('ftr_facility_costs_categories') }} as costs
      on base.claimId = costs.claimId

),

{% set _ = cte_list.append(payer ~ '_professional') %}

{{ payer }}_professional as (

    select 
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

        provider.NPI as providerNpi,
        upper(trim(provider.name)) as providerName,
        upper(trim(provider.name)) as cityblockProviderName,
        case when prov.lastName is not null then 'Yes' end as cityblockProvider,

        prov.providerOrBHS as cityblockProviderType,
        prov.activeInMarket as cityblockProviderActiveInMarket,

        lines[safe_offset(0)].provider.servicing.specialty as providerSpecialty,

        lines[safe_offset(0)].date.from as serviceDateFrom,
        lines[safe_offset(0)].date.to as serviceDateTo,

        'other' as encounterType,

        diagnoses.codeset as diagnosisCodeset,
        replace(diagnoses.Code, '.', '') as diagnosisCode,
        diagnoses.tier as diagnosisTier,

        (select array_agg(distinct claimLineStatus) from unnest(lines) where claimLineStatus is not null) as claimLineStatuses,
        (select array_agg(distinct placeOfService) from unnest(lines) where placeOfService is not null) as placesOfService

    from {{ source(payer, 'Professional') }} as base

    left join unnest(base.header.diagnoses) as diagnoses

    left join {{ source(payer, 'Provider') }} as provider
    
    on 
        lines[safe_offset(0)].provider.servicing.id = provider.providerIdentifier.id and

        (lines[safe_offset(0)].date.from >= provider.dateEffective.from  or
            provider.dateEffective.from is null) and
        (lines[safe_offset(0)].date.to < provider.dateEffective.to or
            provider.dateEffective.to is null)

    left join {{ ref('src_cityblock_providers') }} prov
      on provider.NPI  =  prov.npi

),

{% endfor %}

{% set _ = cte_list.append('medical_problems') %}
medical_problems as (

    select 
        patient.patientId,
        'patientId' as memberIdentifierField,

        messageId as sourceId,
        'message' as sourceType,
        lower(patient.source.name) as partnerName,
        'ehr' as partnerType,
        'problem_list' as source,

        string(null) as providerNpi,
        string(null) as providerName,
        string(null) as cityblockProviderName,
        string(null) as cityblockProvider,
        
        string(null) as cityblockProviderType,
        cast(null as BOOL) as cityblockProviderActiveInMarket,

        string(null) as providerSpecialty,

        problem.StartDate.date as serviceDateFrom,
        problem.StartDate.date as serviceDateTo,

        string(null) as encounterType,

        case
          when trim(lower(regexp_replace(problem.CodeSystemName, r"[- ]", ""))) = ''
            then null
          else trim(lower(regexp_replace(problem.CodeSystemName, r"[- ]", "")))
          end
        as diagnosisCodeset,
        replace(problem.Code, '.', '') as diagnosisCode,
        string(null) as diagnosisTier,

        array<string>[] as claimLineStatuses,
        array<string>[] as placesOfService

    from {{ ref('src_patient_problems_latest') }}

    where problem.StartDate.raw is not null

),

{% set _ = cte_list.append('medical_encounters') %}
medical_encounters as (

    select 
        patient.patientId,
        'patientId' as memberIdentifierField,

        messageId as sourceId,
        'message' as sourceType,
        'ccd' as source,
        lower(patient.source.name) as partnerName,
        'ehr' as partnerType,

        providerIdentifiers.id as providerNpi,
        upper(concat( trim(providers.firstName), ' ', trim(providers.lastName) )) as providerName,
        upper(coalesce(prov.name, prov2.name, prov3.name)) as cityblockProviderName,
        case when (prov.lastName is not null or prov2.lastName is not null or prov3.lastName is not null) then 'Yes' 
             end as cityblockProvider,
        
       trim(coalesce (prov.providerOrBHS, prov2.providerOrBHS, prov3.providerOrBHS)) as cityblockProviderType,
       coalesce (prov.activeInMarket, prov2.activeInMarket, prov3.activeInMarket) as cityblockProviderActiveInMarket,

        string(null) as providerSpecialty,

        date(timestamp(encounter.DateTime.raw)) as serviceDateFrom,
        date(timestamp(encounter.DateTime.raw)) as serviceDateTo,

        lower(encounter.Type.Name) as encounterType,

        case
          when trim(lower(regexp_replace(diagnoses.CodeSystemName, r"[- ]", ""))) = ''
            then null
          else trim(lower(regexp_replace(diagnoses.CodeSystemName, r"[- ]", "")))
          end
        as diagnosisCodeset,
        replace(diagnoses.Code, '.', '')  as diagnosisCode,
        string(null) as diagnosisTier,

        array<string>[] as claimLineStatuses,
        array<string>[] as placesOfService

    from {{ ref('src_patient_encounters_latest') }}

    left join unnest(encounter.providers) as providers

    left join unnest(providers.identifiers) as providerIdentifiers

    left join unnest(encounter.diagnoses) as diagnoses
   
    left join {{ ref('src_cityblock_providers') }} prov
    on concat(trim(upper(providers.firstName))," ", trim(upper(providers.lastName))) = concat(trim(upper(prov.firstName))," ",trim(upper(prov.lastName)))
    

    left join {{ ref('src_cityblock_providers') }} prov2
    on providerIdentifiers.id  =  prov2.npi 

    left join {{ ref('src_cityblock_providers') }} prov3
     on providerIdentifiers.id  =  prov3.cityblockEmail 

),

{% set _ = cte_list.append('hie_events') %}
hie_events as (

    select 
        patient.patientId,
        'patientId' as memberIdentifierField,

        messageId as sourceId,
        'message' as sourceType,
        'hie' as source,
        patient.source.name as partnerName,
        'hie' as partnerType,

        provider.npi as providerNpi,
        upper(concat( trim(provider.firstName), ' ', trim(provider.lastName))) as providerName,
        trim(upper(coalesce(prov.name, prov2.name))) as cityblockProviderName,
        case when prov.lastName is not null then 'Yes'
             when prov2.lastName is not null then 'Yes' 
                  end as cityblockProvider,

        coalesce (prov.providerOrBHS, prov2.providerOrBHS) as cityblockProviderType,
        coalesce (prov.activeInMarket, prov2.activeInMarket) as cityblockProviderActiveInMarket,

        string(null) as providerSpecialty,

        date(eventDateTime.instant) as serviceDateFrom,
        date(eventDateTime.instant) as serviceDateTo,

        lower(eventType) as encounterType,

        'icd10' as diagnosisCodeset,
        regexp_replace(diagnoses.code, r"[.]", "") as diagnosisCode,
        string(null) as diagnosisTier,

        array<string>[] as claimLineStatuses,
        array<string>[] as placesOfService

    from {{ source('medical', 'patient_hie_events') }}
    
    left join unnest(diagnoses) as diagnoses

    left join {{ ref('src_cityblock_providers') }} prov
    on trim(upper( provider.lastName))  =  trim(upper(prov.lastName))
    and trim(upper(provider.firstName))  =  trim(upper(prov.firstName))

    left join {{ ref('src_cityblock_providers') }} prov2
    on provider.npi  =  prov2.npi 

    where
      patient.patientId is not null and
      eventDateTime.instant is not null

),

final as (

  {% for cte in cte_list %}

  select
    {{ dbt_utils.surrogate_key(['sourceId', 'diagnosisCode', 'serviceDateFrom', 'encounterType']) }} as id,
    * 
  
  from {{ cte }}

    {% if not loop.last %}

    union all

    {% endif %}

  {% endfor %}

)

select * from final
