
{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}

with
{% for source_name in payer_list %}
{{ source_name }}_expanded_records as (

    select
        memberIdentifier.patientId as patientId,
        memberIdentifier.commonId as commonId,
        memberIdentifier.partnerMemberId,
        memberIdentifier.partner,
        '{{ source_name }}' as partnerSourceName,

        identifier.id as claimId,
        identifier.surrogate.id as surrogateId,
        identifier.surrogate.project as surrogateProject,
        identifier.surrogate.dataset as surrogateDataset,
        identifier.surrogate.table as surrogateTable,
        cast(null as string) as partnerClaimId,

        lineOfBusiness,
        subLineOfBusiness,

        claimLineStatus,

        date.filled as dateFilled,
        date.paid as datePaid,

        amount.allowed as amountAllowed,
        amount.planPaid as amountPlanPaid,

        pharmacy.npi as pharmacyNpi,
        pharmacy.id as pharmacyId,

        prescriber.npi as prescriberNpi,
        prescriber.id as prescriberId,
        prescriber.specialty as prescriberSpecialty,

        drug.ndc,
        drug.daysSupply,
        drug.quantityDispensed,
        drug.dispenseAsWritten,
        drug.dispenseMethod,
        drug.fillNumber,
        drug.formularyFlag,
        drug.partnerPrescriptionNumber,
        drug.brandIndicator,
        drug.ingredient,
        case
          when drugClass.codeset = 'GC3' then drugClass.code
          else null
          end as gc3code,

        amount.deductible,
        amount.coinsurance,
        amount.copay,
        amount.cob

    from {{ source( source_name, 'Pharmacy') }} p

    left join unnest(drug.classes) as drugClass

),

{{ source_name }}_flattened as (
  -- reducing fanout from above unnest with a select distinct here
    select distinct * from {{ source_name }}_expanded_records

)

{% if not loop.last -%} , {%- endif %}
{% endfor %}
{% for source_name in payer_list -%}
select * from {{ source_name }}_flattened
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
